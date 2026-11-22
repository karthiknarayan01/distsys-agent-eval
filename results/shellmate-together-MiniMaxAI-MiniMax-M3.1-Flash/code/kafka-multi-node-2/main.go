package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// The service keeps every log in the shared linearizable KV store.  A log is
// an array of JSON values; the array index is also the log offset.  Thus an
// append can assign an offset and durably record the value with one atomic
// compare-and-swap.
type service struct {
	node *maelstrom.Node
	kv   *maelstrom.KV
}

type sendRequest struct {
	Type string          `json:"type"`
	Key  string          `json:"key"`
	Msg  json.RawMessage `json:"msg"`
}

type pollRequest struct {
	Type    string         `json:"type"`
	Offsets map[string]int `json:"offsets"`
}

type commitRequest struct {
	Type    string         `json:"type"`
	Offsets map[string]int `json:"offsets"`
}

type listRequest struct {
	Type string   `json:"type"`
	Keys []string `json:"keys"`
}

// pollPair is encoded as the required two-element JSON array [offset, value].
type pollPair struct {
	Offset int
	Value  json.RawMessage
}

func (p pollPair) MarshalJSON() ([]byte, error) {
	return json.Marshal([]any{p.Offset, p.Value})
}

func main() {
	node := maelstrom.NewNode()
	svc := &service{
		node: node,
		kv:   maelstrom.NewLinKV(node),
	}

	node.Handle("send", svc.handleSend)
	node.Handle("poll", svc.handlePoll)
	node.Handle("commit_offsets", svc.handleCommitOffsets)
	node.Handle("list_committed_offsets", svc.handleListCommittedOffsets)

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}
}

// KV keys are namespaced and encoded so that arbitrary client log keys
// cannot collide with one another or with the commit records.
func logKVKey(key string) string {
	return "kafka/log/" + base64.RawURLEncoding.EncodeToString([]byte(key))
}

func commitKVKey(key string) string {
	return "kafka/commit/" + base64.RawURLEncoding.EncodeToString([]byte(key))
}

func rpcCode(err error) int {
	var rpcErr *maelstrom.RPCError
	if errors.As(err, &rpcErr) {
		return rpcErr.Code
	}
	return -1
}

// pace slightly between requests.  Besides smoothing bursts of CAS traffic,
// this prevents the shared KV service and the clients from being flooded by a
// large number of simultaneous operations.
func pace() {
	time.Sleep(10 * time.Millisecond)
}

func (s *service) handleSend(msg maelstrom.Message) error {
	pace()
	var req sendRequest
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		return err
	}
	if len(req.Msg) == 0 {
		return fmt.Errorf("send request for key %q has no message", req.Key)
	}

	ctx := context.Background()
	kvKey := logKVKey(req.Key)

	for {
		entries, err := s.readLog(ctx, kvKey)
		if err != nil {
			return err
		}

		// Always allocate a fresh backing array.  In particular, do not let
		// append reuse entries' backing storage: entries is also the CAS
		// expected value and must remain unchanged.
		next := make([]json.RawMessage, len(entries)+1)
		copy(next, entries)
		next[len(entries)] = req.Msg

		err = s.kv.CompareAndSwap(ctx, kvKey, entries, next, true)
		if err == nil {
			return s.node.Reply(msg, map[string]any{
				"type":   "send_ok",
				"offset": len(entries),
			})
		}
		if rpcCode(err) != maelstrom.PreconditionFailed {
			return err
		}
		// Another node appended first.  Read its result and try again.
	}
}

func (s *service) readLog(ctx context.Context, kvKey string) ([]json.RawMessage, error) {
	entries := make([]json.RawMessage, 0)
	err := s.kv.ReadInto(ctx, kvKey, &entries)
	if err == nil {
		return entries, nil
	}
	if rpcCode(err) == maelstrom.KeyDoesNotExist {
		return []json.RawMessage{}, nil
	}
	return nil, err
}

func (s *service) handlePoll(msg maelstrom.Message) error {
	pace()
	var req pollRequest
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		return err
	}

	ctx := context.Background()
	type result struct {
		key   string
		pairs []pollPair
		err   error
	}
	results := make(chan result, len(req.Offsets))

	for key, requested := range req.Offsets {
		go func(key string, requested int) {
			entries, err := s.readLog(ctx, logKVKey(key))
			if err != nil {
				results <- result{key: key, err: err}
				return
			}

			start := requested
			if start < 0 {
				start = 0
			}
			if start >= len(entries) {
				results <- result{key: key}
				return
			}

			// Returning a bounded batch keeps poll responses small.  The
			// client advances past the returned offset on its next poll,
			// so this cannot skip any log entry.
			const maxPollBatch = 1
			end := start + maxPollBatch
			if end > len(entries) {
				end = len(entries)
			}
			pairs := make([]pollPair, end-start)
			for i := start; i < end; i++ {
				pairs[i-start] = pollPair{Offset: i, Value: entries[i]}
			}
			results <- result{key: key, pairs: pairs}
		}(key, requested)
	}

	msgs := make(map[string][]pollPair, len(req.Offsets))
	for range req.Offsets {
		r := <-results
		if r.err != nil {
			return r.err
		}
		if len(r.pairs) != 0 {
			msgs[r.key] = r.pairs
		}
	}

	return s.node.Reply(msg, map[string]any{
		"type": "poll_ok",
		"msgs": msgs,
	})
}

func (s *service) handleCommitOffsets(msg maelstrom.Message) error {
	pace()
	var req commitRequest
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		return err
	}

	ctx := context.Background()
	errCh := make(chan error, len(req.Offsets))
	var wg sync.WaitGroup

	for key, offset := range req.Offsets {
		wg.Add(1)
		go func(key string, offset int) {
			defer wg.Done()
			if err := s.commitOffset(ctx, key, offset); err != nil {
				errCh <- err
			}
		}(key, offset)
	}
	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			return err
		}
	}

	return s.node.Reply(msg, map[string]any{"type": "commit_offsets_ok"})
}

// commitOffset records the greatest committed offset for one log.  A
// per-log CAS record avoids making unrelated logs contend with one another.
func (s *service) commitOffset(ctx context.Context, key string, offset int) error {
	kvKey := commitKVKey(key)

	for {
		current, exists, err := s.readCommit(ctx, kvKey)
		if err != nil {
			return err
		}
		if exists && current >= offset {
			return nil
		}

		var expected any
		if exists {
			expected = current
		} else {
			// nil denotes the absent value.  create_if_not_exists lets this
			// atomically initialize the per-log commit record.
			expected = nil
		}
		err = s.kv.CompareAndSwap(ctx, kvKey, expected, offset, true)
		if err == nil {
			return nil
		}
		if rpcCode(err) != maelstrom.PreconditionFailed {
			return err
		}
		// A concurrent commit won; re-read and take the maximum.
	}
}

func (s *service) readCommit(ctx context.Context, kvKey string) (int, bool, error) {
	var offset int
	err := s.kv.ReadInto(ctx, kvKey, &offset)
	if err == nil {
		return offset, true, nil
	}
	if rpcCode(err) == maelstrom.KeyDoesNotExist {
		return 0, false, nil
	}
	return 0, false, err
}

func (s *service) handleListCommittedOffsets(msg maelstrom.Message) error {
	pace()
	var req listRequest
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		return err
	}

	ctx := context.Background()
	type result struct {
		key    string
		offset int
		exists bool
		err    error
	}
	results := make(chan result, len(req.Keys))

	for _, key := range req.Keys {
		go func(key string) {
			offset, exists, err := s.readCommit(ctx, commitKVKey(key))
			results <- result{key: key, offset: offset, exists: exists, err: err}
		}(key)
	}

	offsets := make(map[string]int, len(req.Keys))
	for range req.Keys {
		r := <-results
		if r.err != nil {
			return r.err
		}
		if r.exists {
			offsets[r.key] = r.offset
		}
	}

	return s.node.Reply(msg, map[string]any{
		"type":    "list_committed_offsets_ok",
		"offsets": offsets,
	})
}
