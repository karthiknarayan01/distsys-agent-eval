// Command node implements a Kafka-style append-only log service for a
// two-node Maelstrom cluster.
//
// Both nodes keep their state in the linearizable "lin-kv" service, so every
// node observes exactly the same logs and committed offsets.  Each log is a
// JSON array of values; a value's index in that array is its offset.  Appends
// use compare-and-swap, which gives every value a unique, monotonically
// growing offset even when several clients append concurrently.
//
// Committed offsets live in a single map which is also updated with
// compare-and-swap.  Commits are merged monotonically (the maximum offset
// wins), so a commit can never move a log backwards.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const (
	// lin-kv key prefixes.  Log keys are namespaced so that they cannot
	// collide with the committed-offset map.
	logKeyPrefix = "kafka-log:"
	committedKey = "kafka-committed-offsets"

	// A poll may return fewer entries than a log contains.  Capping the
	// response keeps individual poll replies reasonably small.
	maxPollEntries = 128

	// lin-kv operations are local to the simulated cluster and should be
	// quick; this is only a safety net.
	kvTimeout = 30 * time.Second
)

// pair is encoded as [offset, value], as required by the kafka workload.
type pair struct {
	offset int
	value  json.RawMessage
}

func (p pair) MarshalJSON() ([]byte, error) {
	offset, err := json.Marshal(p.offset)
	if err != nil {
		return nil, err
	}
	value := p.value
	if len(value) == 0 {
		value = json.RawMessage("null")
	}
	var out bytes.Buffer
	out.Grow(len(offset) + len(value) + 2)
	out.WriteByte('[')
	out.Write(offset)
	out.WriteByte(',')
	out.Write(value)
	out.WriteByte(']')
	return out.Bytes(), nil
}

type sendRequest struct {
	Key string          `json:"key"`
	Msg json.RawMessage `json:"msg"`
}

type pollRequest struct {
	Offsets map[string]int `json:"offsets"`
}

type commitRequest struct {
	Offsets map[string]int `json:"offsets"`
}

type listRequest struct {
	Keys []string `json:"keys"`
}

type service struct {
	node *maelstrom.Node
	kv   *maelstrom.KV
}

func main() {
	node := maelstrom.NewNode()
	s := &service{node: node, kv: maelstrom.NewLinKV(node)}

	node.Handle("send", s.send)
	node.Handle("poll", s.poll)
	node.Handle("commit_offsets", s.commitOffsets)
	node.Handle("list_committed_offsets", s.listCommittedOffsets)

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}
}

func requestContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), kvTimeout)
}

// send appends msg to the log named key and returns its offset.  The append
// is durable in lin-kv before the acknowledgement is sent.
func (s *service) send(m maelstrom.Message) error {
	var req sendRequest
	if err := json.Unmarshal(m.Body, &req); err != nil {
		return err
	}

	ctx, cancel := requestContext()
	defer cancel()
	key := logKeyPrefix + req.Key

	for {
		var values []json.RawMessage
		err := s.kv.ReadInto(ctx, key, &values)
		missing := false
		if err != nil {
			if maelstrom.ErrorCode(err) != maelstrom.KeyDoesNotExist {
				return err
			}
			missing = true
			values = nil
		}

		offset := len(values)
		next := make([]json.RawMessage, offset+1)
		copy(next, values)
		next[offset] = req.Msg

		// A nil slice is encoded as JSON null, which is how lin-kv
		// represents the absence of a key.
		var from any = values
		if missing {
			from = nil
		}
		err = s.kv.CompareAndSwap(ctx, key, from, next, true)
		if err == nil {
			return s.node.Reply(m, map[string]any{
				"type":   "send_ok",
				"offset": offset,
			})
		}
		if maelstrom.ErrorCode(err) != maelstrom.PreconditionFailed {
			return err
		}
		// Another append (possibly on the other node) won the race.  Read
		// the log again and retry with the new length.
	}
}

type pollResult struct {
	key   string
	items []pair
	err   error
}

// poll returns, for each requested log, the entries at or after the
// requested offset.  Logs with no matching entry are omitted.
func (s *service) poll(m maelstrom.Message) error {
	var req pollRequest
	if err := json.Unmarshal(m.Body, &req); err != nil {
		return err
	}

	ctx, cancel := requestContext()
	defer cancel()

	// A poll can name many logs.  Read them concurrently so that a large
	// poll does not turn into a long chain of round trips.
	results := make(chan pollResult, len(req.Offsets))
	var wg sync.WaitGroup
	for key, requested := range req.Offsets {
		wg.Add(1)
		go func(key string, requested int) {
			defer wg.Done()

			var values []json.RawMessage
			err := s.kv.ReadInto(ctx, logKeyPrefix+key, &values)
			if err != nil {
				if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
					results <- pollResult{key: key}
				} else {
					results <- pollResult{key: key, err: err}
				}
				return
			}

			if requested < 0 {
				requested = 0
			}
			if requested >= len(values) {
				results <- pollResult{key: key}
				return
			}

			end := requested + maxPollEntries
			if end > len(values) {
				end = len(values)
			}
			items := make([]pair, 0, end-requested)
			for offset := requested; offset < end; offset++ {
				items = append(items, pair{offset: offset, value: values[offset]})
			}
			results <- pollResult{key: key, items: items}
		}(key, requested)
	}
	wg.Wait()
	close(results)

	msgs := make(map[string][]pair, len(req.Offsets))
	for result := range results {
		if result.err != nil {
			return result.err
		}
		if len(result.items) != 0 {
			msgs[result.key] = result.items
		}
	}
	return s.node.Reply(m, map[string]any{
		"type": "poll_ok",
		"msgs": msgs,
	})
}

// commitOffsets records the greatest offset the client has read for each
// named log.  Commits are merged monotonically.
func (s *service) commitOffsets(m maelstrom.Message) error {
	var req commitRequest
	if err := json.Unmarshal(m.Body, &req); err != nil {
		return err
	}
	if len(req.Offsets) == 0 {
		return s.node.Reply(m, map[string]any{"type": "commit_offsets_ok"})
	}

	ctx, cancel := requestContext()
	defer cancel()
	for {
		var committed map[string]int
		err := s.kv.ReadInto(ctx, committedKey, &committed)
		missing := false
		if err != nil {
			if maelstrom.ErrorCode(err) != maelstrom.KeyDoesNotExist {
				return err
			}
			missing = true
			committed = map[string]int{}
		}

		next := make(map[string]int, len(committed)+len(req.Offsets))
		for key, offset := range committed {
			next[key] = offset
		}
		for key, offset := range req.Offsets {
			if old, ok := next[key]; !ok || offset > old {
				next[key] = offset
			}
		}

		var from any = committed
		if missing {
			from = nil
		}
		err = s.kv.CompareAndSwap(ctx, committedKey, from, next, true)
		if err == nil {
			return s.node.Reply(m, map[string]any{"type": "commit_offsets_ok"})
		}
		if maelstrom.ErrorCode(err) != maelstrom.PreconditionFailed {
			return err
		}
		// A concurrent commit won; merge our offsets into the new state.
	}
}

// listCommittedOffsets returns the committed offsets of the requested logs,
// omitting logs which have never been committed.
func (s *service) listCommittedOffsets(m maelstrom.Message) error {
	var req listRequest
	if err := json.Unmarshal(m.Body, &req); err != nil {
		return err
	}

	ctx, cancel := requestContext()
	defer cancel()
	var committed map[string]int
	err := s.kv.ReadInto(ctx, committedKey, &committed)
	if err != nil && maelstrom.ErrorCode(err) != maelstrom.KeyDoesNotExist {
		return err
	}

	offsets := make(map[string]int, len(req.Keys))
	for _, key := range req.Keys {
		if offset, ok := committed[key]; ok {
			offsets[key] = offset
		}
	}
	return s.node.Reply(m, map[string]any{
		"type":    "list_committed_offsets_ok",
		"offsets": offsets,
	})
}
