package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const (
	logPrefix    = "kafka/log/"
	commitPrefix = "kafka/commit/"

	// bulkPollKeys is the number of keys in a poll request above which the
	// request is considered a "bulk" poll.
	bulkPollKeys = 64

	// bulkPollPairs is the maximum number of [offset, value] pairs returned
	// for a single key of a bulk poll.  A client is always free to ask for
	// the rest later, and bounding the size of very large responses keeps
	// both the wire traffic and the amount of data a client has to digest
	// reasonable.
	bulkPollPairs = 4
)

// A logEntry is one value in an append-only log.  Values are kept as raw JSON
// so that the server does not change their representation.
type logEntry struct {
	Offset int64           `json:"offset"`
	Value  json.RawMessage `json:"value"`
}

// logState is the complete, atomically-updated state of one log.  It is
// encoded as a JSON string before being put in lin-kv.  Keeping the complete
// log in one CAS'd value makes assigning an offset and publishing its value a
// single atomic operation.
type logState struct {
	Next    int64      `json:"next"`
	Entries []logEntry `json:"entries"`
}

type sendRequest struct {
	maelstrom.MessageBody
	Key string          `json:"key"`
	Msg json.RawMessage `json:"msg"`
}

type pollRequest struct {
	maelstrom.MessageBody
	Offsets map[string]int64 `json:"offsets"`
}

type commitRequest struct {
	maelstrom.MessageBody
	Offsets map[string]int64 `json:"offsets"`
}

type listCommittedRequest struct {
	maelstrom.MessageBody
	Keys []string `json:"keys"`
}

// pollPair deliberately has the wire form [offset, value].
type pollPair [2]json.RawMessage
type pollPairs []pollPair

type service struct {
	node *maelstrom.Node
	kv   *maelstrom.KV

	// Serializing operations for a key within this node avoids needless
	// conflicts with ourselves.  The other node may still race us, so all
	// updates are still protected by compare-and-swap in lin-kv.
	locks sync.Map // map[string]*sync.Mutex

	// Caches of the values we last stored in lin-kv.  They let us attempt a
	// compare-and-swap without reading first, which roughly halves the
	// number of messages exchanged with the key/value store.  A stale
	// cache entry only costs one failed compare-and-swap: we then read the
	// current value and try again.
	logs    sync.Map // map[string]encodedLogState
	commits sync.Map // map[string]string
}

func main() {
	// The reference library logs every message at length, which is both
	// noisy and expensive for a high-rate workload.  Keep stderr quiet.
	log.SetOutput(io.Discard)

	node := maelstrom.NewNode()
	s := &service{
		node: node,
		kv:   maelstrom.NewLinKV(node),
	}

	node.Handle("send", s.handleSend)
	node.Handle("poll", s.handlePoll)
	node.Handle("commit_offsets", s.handleCommitOffsets)
	node.Handle("list_committed_offsets", s.handleListCommittedOffsets)

	if err := node.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "node failed: %v\n", err)
		os.Exit(1)
	}
}

func (s *service) lockFor(key string) *sync.Mutex {
	v, _ := s.locks.LoadOrStore(key, &sync.Mutex{})
	return v.(*sync.Mutex)
}

// lin-kv keys are strings.  Base64 keeps arbitrary user key names distinct
// and avoids collisions with keys used for committed offsets.
func storageKey(prefix, logKey string) string {
	return prefix + base64.RawURLEncoding.EncodeToString([]byte(logKey))
}

func (s *service) handleSend(msg maelstrom.Message) error {
	var req sendRequest
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		return err
	}

	offset, err := s.append(context.Background(), req.Key, req.Msg)
	if err != nil {
		return err
	}

	return s.node.Reply(msg, struct {
		Type   string `json:"type"`
		Offset int64  `json:"offset"`
	}{Type: "send_ok", Offset: offset})
}

// append assigns and publishes a value in one linearizable operation.  A
// value is not acknowledged until its offset and value are both in lin-kv.
func (s *service) append(ctx context.Context, logKey string, value json.RawMessage) (int64, error) {
	kvKey := storageKey(logPrefix, logKey)
	lock := s.lockFor(logKey)
	lock.Lock()
	defer lock.Unlock()

	for attempt := 0; ; attempt++ {
		// Fast path: try to append on top of the state we last stored.
		// If somebody else has appended since, the compare-and-swap fails
		// and we fall back to reading the current state.
		if cached, ok := s.logs.Load(logKey); ok {
			state := cached.(encodedLogState)
			next, offset, err := appendTo(state.logState, value)
			if err != nil {
				return 0, err
			}
			encoded, err := json.Marshal(next)
			if err != nil {
				return 0, fmt.Errorf("encode log state: %w", err)
			}
			to := string(encoded)

			err = s.kv.CompareAndSwap(ctx, kvKey, state.encoded, to, false)
			if err == nil {
				s.logs.Store(logKey, encodedLogState{encoded: to, logState: next})
				return offset, nil
			}
			if !isConflict(err) {
				return 0, fmt.Errorf("append to log %q: %w", logKey, err)
			}
		}

		current, exists, err := s.readLogState(ctx, kvKey)
		if err != nil {
			return 0, err
		}

		var next logState
		if !exists {
			next = logState{Next: 0, Entries: make([]logEntry, 0, 1)}
		} else {
			next = current.logState
		}

		offset := next.Next
		next.Entries = append(next.Entries, logEntry{Offset: offset, Value: cloneRaw(value)})
		next.Next++

		encoded, err := json.Marshal(next)
		if err != nil {
			return 0, fmt.Errorf("encode log state: %w", err)
		}
		to := string(encoded)

		var from any
		if exists {
			from = current.encoded
		}

		err = s.kv.CompareAndSwap(ctx, kvKey, from, to, !exists)
		if err == nil {
			s.logs.Store(logKey, encodedLogState{encoded: to, logState: next})
			return offset, nil
		}
		if !isConflict(err) {
			return 0, fmt.Errorf("append to log %q: %w", logKey, err)
		}

		// Another append (possibly on the other node) won the race.  Read
		// the new state and try again.
		backoff(attempt)
	}
}

// appendTo returns a copy of state with value appended, together with the
// offset the value is given.  The copy keeps the cached state immutable.
func appendTo(state logState, value json.RawMessage) (logState, int64, error) {
	entries := make([]logEntry, len(state.Entries), len(state.Entries)+1)
	copy(entries, state.Entries)
	offset := state.Next
	entries = append(entries, logEntry{Offset: offset, Value: cloneRaw(value)})
	return logState{Next: offset + 1, Entries: entries}, offset, nil
}

// encodedLogState couples a decoded state with the exact string read from
// lin-kv.  The latter is the compare-and-swap precondition.
type encodedLogState struct {
	encoded string
	logState
}

func (s *service) readLogState(ctx context.Context, kvKey string) (encodedLogState, bool, error) {
	v, err := s.kv.Read(ctx, kvKey)
	if err != nil {
		if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
			return encodedLogState{}, false, nil
		}
		return encodedLogState{}, false, fmt.Errorf("read log state: %w", err)
	}

	encoded, ok := v.(string)
	if !ok {
		return encodedLogState{}, false, fmt.Errorf("log state has unexpected type %T", v)
	}
	var state logState
	if err := json.Unmarshal([]byte(encoded), &state); err != nil {
		return encodedLogState{}, false, fmt.Errorf("decode log state: %w", err)
	}
	return encodedLogState{encoded: encoded, logState: state}, true, nil
}

func (s *service) handlePoll(msg maelstrom.Message) error {
	var req pollRequest
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		return err
	}

	ctx := context.Background()

	// A client may ask about a great many logs at once.  Returning every
	// value of every such log makes for an enormous reply, so for bulk
	// requests we return only the first few pairs of each log; the client
	// can ask for the remainder later.
	limit := 0
	if len(req.Offsets) > bulkPollKeys {
		limit = bulkPollPairs
	}

	type result struct {
		key   string
		pairs pollPairs
		err   error
	}

	results := make(chan result, len(req.Offsets))
	for key, offset := range req.Offsets {
		go func(key string, offset int64) {
			pairs, err := s.pollLog(ctx, key, offset, limit)
			results <- result{key: key, pairs: pairs, err: err}
		}(key, offset)
	}

	msgs := make(map[string]pollPairs, len(req.Offsets))
	for range req.Offsets {
		r := <-results
		if r.err != nil {
			return r.err
		}
		if len(r.pairs) != 0 {
			msgs[r.key] = r.pairs
		}
	}

	return s.node.Reply(msg, struct {
		Type string               `json:"type"`
		Msgs map[string]pollPairs `json:"msgs"`
	}{Type: "poll_ok", Msgs: msgs})
}

func (s *service) pollLog(ctx context.Context, logKey string, offset int64, limit int) (pollPairs, error) {
	kvKey := storageKey(logPrefix, logKey)
	state, exists, err := s.readLogState(ctx, kvKey)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, nil
	}

	// Entries are stored in offset order.  Find the first one at or after
	// the requested offset, then return every later entry.  Thus a poll can
	// never jump over a value which is already in the log.
	start := sortSearch(state.Entries, offset)
	if start == len(state.Entries) {
		return nil, nil
	}

	if limit > 0 && len(state.Entries)-start > limit {
		state.Entries = state.Entries[:start+limit]
	}

	pairs := make(pollPairs, 0, len(state.Entries)-start)
	for _, entry := range state.Entries[start:] {
		pairs = append(pairs, pollPair{
			json.RawMessage(strconv.FormatInt(entry.Offset, 10)),
			cloneRaw(entry.Value),
		})
	}
	return pairs, nil
}

// sortSearch returns the first index whose offset is >= wanted.
func sortSearch(entries []logEntry, wanted int64) int {
	lo, hi := 0, len(entries)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if entries[mid].Offset < wanted {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

func (s *service) handleCommitOffsets(msg maelstrom.Message) error {
	var req commitRequest
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		return err
	}

	ctx := context.Background()
	for key, offset := range req.Offsets {
		if err := s.commitOffset(ctx, key, offset); err != nil {
			return err
		}
	}

	return s.node.Reply(msg, struct {
		Type string `json:"type"`
	}{Type: "commit_offsets_ok"})
}

// commitOffset advances a committed offset.  It is monotonic: an older
// commit can never move the stored position backwards.
func (s *service) commitOffset(ctx context.Context, logKey string, offset int64) error {
	kvKey := storageKey(commitPrefix, logKey)
	lock := s.lockFor(logKey)
	lock.Lock()
	defer lock.Unlock()

	for attempt := 0; ; attempt++ {
		// Fast path: commits only ever move forwards, so if we already
		// know a committed offset which is at least as large as this one,
		// there is nothing to store.
		if cached, ok := s.commits.Load(logKey); ok {
			encoded := cached.(string)
			current, err := strconv.ParseInt(encoded, 10, 64)
			if err == nil && current >= offset {
				return nil
			}
			to := strconv.FormatInt(offset, 10)
			err = s.kv.CompareAndSwap(ctx, kvKey, encoded, to, false)
			if err == nil {
				s.commits.Store(logKey, to)
				return nil
			}
			if !isConflict(err) {
				return fmt.Errorf("commit offset for log %q: %w", logKey, err)
			}
		}

		v, err := s.kv.Read(ctx, kvKey)
		exists := true
		if err != nil {
			if maelstrom.ErrorCode(err) != maelstrom.KeyDoesNotExist {
				return fmt.Errorf("read committed offset: %w", err)
			}
			exists = false
		}

		var from any
		if exists {
			encoded, ok := v.(string)
			if !ok {
				return fmt.Errorf("committed offset has unexpected type %T", v)
			}
			current, err := strconv.ParseInt(encoded, 10, 64)
			if err != nil {
				return fmt.Errorf("decode committed offset: %w", err)
			}
			if current >= offset {
				s.commits.Store(logKey, encoded)
				return nil
			}
			from = encoded
		}

		to := strconv.FormatInt(offset, 10)
		err = s.kv.CompareAndSwap(ctx, kvKey, from, to, !exists)
		if err == nil {
			s.commits.Store(logKey, to)
			return nil
		}
		if !isConflict(err) {
			return fmt.Errorf("commit offset for log %q: %w", logKey, err)
		}
		backoff(attempt)
	}
}

func (s *service) handleListCommittedOffsets(msg maelstrom.Message) error {
	var req listCommittedRequest
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		return err
	}

	ctx := context.Background()
	type result struct {
		key    string
		offset int64
		found  bool
		err    error
	}

	results := make(chan result, len(req.Keys))
	for _, key := range req.Keys {
		go func(key string) {
			offset, found, err := s.readCommittedOffset(ctx, key)
			results <- result{key: key, offset: offset, found: found, err: err}
		}(key)
	}

	offsets := make(map[string]int64, len(req.Keys))
	for range req.Keys {
		r := <-results
		if r.err != nil {
			return r.err
		}
		if r.found {
			offsets[r.key] = r.offset
		}
	}

	return s.node.Reply(msg, struct {
		Type    string           `json:"type"`
		Offsets map[string]int64 `json:"offsets"`
	}{Type: "list_committed_offsets_ok", Offsets: offsets})
}

func (s *service) readCommittedOffset(ctx context.Context, logKey string) (int64, bool, error) {
	kvKey := storageKey(commitPrefix, logKey)
	v, err := s.kv.Read(ctx, kvKey)
	if err != nil {
		if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
			return 0, false, nil
		}
		return 0, false, fmt.Errorf("read committed offset: %w", err)
	}
	encoded, ok := v.(string)
	if !ok {
		return 0, false, fmt.Errorf("committed offset has unexpected type %T", v)
	}
	offset, err := strconv.ParseInt(encoded, 10, 64)
	if err != nil {
		return 0, false, fmt.Errorf("decode committed offset: %w", err)
	}
	return offset, true, nil
}

func isConflict(err error) bool {
	code := maelstrom.ErrorCode(err)
	return code == maelstrom.PreconditionFailed || code == maelstrom.KeyDoesNotExist
}

func cloneRaw(raw json.RawMessage) json.RawMessage {
	if raw == nil {
		return json.RawMessage("null")
	}
	return append(json.RawMessage(nil), raw...)
}

// A small randomized backoff breaks up CAS conflicts without materially
// increasing normal request latency.
func backoff(attempt int) {
	if attempt > 6 {
		attempt = 6
	}
	delay := (1 << attempt) * time.Millisecond
	delay += time.Duration(rand.Intn(3)) * time.Millisecond
	time.Sleep(delay)
}
