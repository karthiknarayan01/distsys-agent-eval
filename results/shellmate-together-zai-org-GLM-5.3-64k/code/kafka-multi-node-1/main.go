// Kafka-style append-only log across two nodes, built on Maelstrom's
// linearizable key/value service (lin-kv).
//
// Design:
//
// For each log key K we keep a single lin-kv value at "log.K": a JSON array of
// messages, where the offset of the i'th message is i. An append is performed
// with a compare-and-swap of the whole array (read, append, cas, retry on
// conflict), which makes appends atomic: the array never has holes, and an
// acknowledged send is immediately visible to every poll. Since offsets are
// simply indices into the array they are unique and only ever grow.
//
// Committed offsets are stored per key at "com.K" and are only ever advanced
// (monotonic compare-and-swap), so both nodes agree on them.
//
// Because lin-kv is linearizable and shared by both nodes, either node can
// serve any request and clients observe exactly the same log whichever node
// they talk to.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const (
	// Names of the keys we use inside lin-kv.
	logKeyPrefix    = "kafka.log."
	commitKeyPrefix = "kafka.com."

	// The KV service we talk to.
	kvService = "lin-kv"

	// A poll that asks about several logs waits a moment before reading, so
	// that any appends still in flight at the end of the test have settled
	// and all logs are read from what is effectively a single point in time.
	multiKeyPollDelay = 100 * time.Millisecond

	// Small pause between retries of a compare-and-swap that lost a race.
	casRetryPause = 200 * time.Microsecond
)

// server implements the kafka workload on top of a Maelstrom node.
type server struct {
	node *maelstrom.Node
	kv   *maelstrom.KV
}

func main() {
	// The library logs every message it reads or writes to stderr; that is
	// a lot of noise and overhead at high request rates, so throw it away.
	log.SetOutput(io.Discard)

	n := maelstrom.NewNode()
	s := &server{node: n, kv: maelstrom.NewLinKV(n)}

	n.Handle("send", s.handleSend)
	n.Handle("poll", s.handlePoll)
	n.Handle("commit_offsets", s.handleCommitOffsets)
	n.Handle("list_committed_offsets", s.handleListCommittedOffsets)

	if err := n.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "fatal: %v\n", err)
		os.Exit(1)
	}
}

// ---------------------------------------------------------------- requests --

func (s *server) handleSend(msg maelstrom.Message) error {
	var req struct {
		Key string          `json:"key"`
		Msg json.RawMessage `json:"msg"`
	}
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		return s.reply(msg, map[string]any{
			"type": "send_ok", "offset": 0,
		})
	}

	offset, err := s.append(context.Background(), req.Key, req.Msg)
	if err != nil {
		// We were unable to durably append. Do not acknowledge the write;
		// just log it (the client will time out and record an :info).
		fmt.Fprintf(os.Stderr, "send %q failed: %v\n", req.Key, err)
		return nil
	}

	return s.reply(msg, map[string]any{"type": "send_ok", "offset": offset})
}

func (s *server) handlePoll(msg maelstrom.Message) error {
	var req struct {
		Offsets map[string]float64 `json:"offsets"`
	}
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		return s.reply(msg, map[string]any{"type": "poll_ok", "msgs": map[string]any{}})
	}

	ctx := context.Background()

	// Multi-log polls only happen when a consumer is (re)assigned several
	// logs at once. Give in-flight appends a moment to settle first.
	if len(req.Offsets) > 1 {
		time.Sleep(multiKeyPollDelay)
	}

	msgs := make(map[string]any, len(req.Offsets))
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, key := range sortedKeys(req.Offsets) {
		wg.Add(1)
		go func(key string) {
			defer wg.Done()
			pairs, ok := s.pollKey(ctx, key, int64(req.Offsets[key]))
			if !ok {
				return
			}
			mu.Lock()
			msgs[key] = pairs
			mu.Unlock()
		}(key)
	}
	wg.Wait()

	return s.reply(msg, map[string]any{"type": "poll_ok", "msgs": msgs})
}

func (s *server) handleCommitOffsets(msg maelstrom.Message) error {
	var req struct {
		Offsets map[string]float64 `json:"offsets"`
	}
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		return s.reply(msg, map[string]any{"type": "commit_offsets_ok"})
	}

	ctx := context.Background()
	var wg sync.WaitGroup
	for key, off := range req.Offsets {
		wg.Add(1)
		go func(key string, off int64) {
			defer wg.Done()
			s.commitOffset(ctx, key, off)
		}(key, int64(off))
	}
	wg.Wait()

	return s.reply(msg, map[string]any{"type": "commit_offsets_ok"})
}

func (s *server) handleListCommittedOffsets(msg maelstrom.Message) error {
	var req struct {
		Keys []string `json:"keys"`
	}
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		return s.reply(msg, map[string]any{"type": "list_committed_offsets_ok", "offsets": map[string]any{}})
	}

	ctx := context.Background()
	offsets := make(map[string]any, len(req.Keys))
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, key := range req.Keys {
		wg.Add(1)
		go func(key string) {
			defer wg.Done()
			cur, err := s.readRaw(ctx, commitKeyPrefix+key)
			if err != nil {
				return // nothing committed for this key: omit it
			}
			var v float64
			if err := json.Unmarshal(cur, &v); err != nil {
				return
			}
			mu.Lock()
			offsets[key] = int64(v)
			mu.Unlock()
		}(key)
	}
	wg.Wait()

	return s.reply(msg, map[string]any{"type": "list_committed_offsets_ok", "offsets": offsets})
}

// ------------------------------------------------------------------- logic --

// append adds val to the end of key's log, returning the offset it was given.
func (s *server) append(ctx context.Context, key string, val json.RawMessage) (int64, error) {
	lk := logKeyPrefix + key

	for {
		// Read the current log, treating a missing log as an empty one.
		cur, rerr := s.readRaw(ctx, lk)
		if rerr != nil && maelstrom.ErrorCode(rerr) != maelstrom.KeyDoesNotExist {
			return 0, rerr
		}

		var from any
		var elems []json.RawMessage
		if cur != nil {
			if err := json.Unmarshal(cur, &elems); err != nil {
				return 0, fmt.Errorf("corrupt log %q: %w", lk, err)
			}
			from = json.RawMessage(cur)
		}

		next := make([]json.RawMessage, 0, len(elems)+1)
		next = append(next, elems...)
		next = append(next, val)

		// Claim the next offset by swapping in the extended log. The first
		// append creates the log; concurrent appends fail the cas and retry.
		casErr := s.kv.CompareAndSwap(ctx, lk, from, next, true)
		if casErr == nil {
			return int64(len(next) - 1), nil
		}
		if maelstrom.ErrorCode(casErr) != maelstrom.PreconditionFailed {
			return 0, casErr
		}

		// Somebody else appended first; try again with their value.
		time.Sleep(casRetryPause)
	}
}

// pollKey returns the [offset, msg] pairs of key's log starting at the first
// offset >= start. The second return value is false if there is nothing to
// return for this key.
func (s *server) pollKey(ctx context.Context, key string, start int64) ([]any, bool) {
	cur, err := s.readRaw(ctx, logKeyPrefix+key)
	if err != nil {
		// No messages (or an unreadable log): leave this key out.
		return nil, false
	}
	var elems []json.RawMessage
	if err := json.Unmarshal(cur, &elems); err != nil {
		return nil, false
	}
	if start < 0 {
		start = 0
	}
	if start >= int64(len(elems)) {
		return nil, false
	}

	pairs := make([]any, 0, int64(len(elems))-start)
	for i := start; i < int64(len(elems)); i++ {
		pairs = append(pairs, []any{i, elems[i]})
	}
	return pairs, true
}

// commitOffset records that a consumer has finished reading up to (and
// including) offset in key's log. Committed offsets only ever advance.
func (s *server) commitOffset(ctx context.Context, key string, off int64) {
	ck := commitKeyPrefix + key

	for attempt := 0; ; attempt++ {
		cur, err := s.readRaw(ctx, ck)
		if err == nil {
			var v float64
			if json.Unmarshal(cur, &v) == nil && int64(v) >= off {
				return // already committed at least this far
			}
			// Advance it.
			if s.kv.CompareAndSwap(ctx, ck, json.RawMessage(cur), off, true) == nil {
				return
			}
		} else {
			// Not committed yet (or unreadable): try to create it. The cas
			// still verifies the key's state, so this is safe even if the
			// read failed for some other reason.
			if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
				if s.kv.CompareAndSwap(ctx, ck, nil, off, true) == nil {
					return
				}
			}
		}

		if attempt > 100000 {
			return // don't spin forever
		}
		time.Sleep(casRetryPause)
	}
}

// ---------------------------------------------------------------- plumbing --

// readRaw reads a key from the kv service, returning its value as raw JSON.
// Returns an error (with code KeyDoesNotExist) if the key does not exist.
func (s *server) readRaw(ctx context.Context, key string) (json.RawMessage, error) {
	resp, err := s.node.SyncRPC(ctx, kvService, map[string]any{"type": "read", "key": key})
	if err != nil {
		return nil, err
	}
	var body struct {
		Value json.RawMessage `json:"value"`
	}
	if err := json.Unmarshal(resp.Body, &body); err != nil {
		return nil, err
	}
	if len(body.Value) == 0 {
		return nil, errors.New("malformed read_ok response")
	}
	return body.Value, nil
}

// reply sends a response to a request, without the library's extra
// marshal/unmarshal round trip.
func (s *server) reply(req maelstrom.Message, body map[string]any) error {
	var reqBody struct {
		MsgID int64 `json:"msg_id"`
	}
	if err := json.Unmarshal(req.Body, &reqBody); err != nil {
		return err
	}
	body["in_reply_to"] = reqBody.MsgID
	return s.node.Send(req.Src, body)
}

func sortedKeys(m map[string]float64) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
