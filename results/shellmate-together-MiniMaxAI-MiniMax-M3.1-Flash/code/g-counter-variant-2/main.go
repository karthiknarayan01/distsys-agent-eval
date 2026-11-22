package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const (
	// Each node owns one counter key in the shared store.  The value stored
	// under that key is the cumulative number of visitors counted by that
	// node, so the grand total is simply the sum of all of the keys.
	keyPrefix = "visitor-tally/counter/"

	// Each node also owns a barrier key.  Writing a fresh value to it forces
	// the sequential store to apply the write to its newest state, which
	// gives us a read barrier: reads issued afterwards cannot observe a state
	// older than the barrier.
	barrierPrefix = "visitor-tally/barrier/"

	// The workload's client gives up after 5 seconds, so leave a little room
	// to actually deliver our reply.
	requestTimeout = 4 * time.Second
)

type countBody struct {
	maelstrom.MessageBody
	Visitors int64 `json:"visitors"`
}

type totalBody struct {
	maelstrom.MessageBody
}

// requestID identifies a client request so that a repeated request is never
// counted twice.
type requestID struct {
	client string
	msgID  int
}

type server struct {
	node *maelstrom.Node
	kv   *maelstrom.KV

	// mu serializes counting requests.  A node is the only writer of its own
	// counter key, and cumulative values have to reach the store in the same
	// order in which they were accumulated.
	mu sync.Mutex

	local      int64
	barrier    int64
	nodeIDs    []string
	seenCounts map[requestID]bool
}

func main() {
	node := maelstrom.NewNode()
	kv := maelstrom.NewSeqKV(node)

	s := &server{
		node:       node,
		kv:         kv,
		seenCounts: make(map[requestID]bool),
	}

	node.Handle("init", func(msg maelstrom.Message) error {
		s.nodeIDs = append([]string(nil), node.NodeIDs()...)
		return nil
	})

	node.Handle("count", s.handleCount)
	node.Handle("total", s.handleTotal)

	if err := node.Run(); err != nil {
		log.Fatalf("node stopped: %v", err)
	}
}

// handleCount adds visitors to this turnstile's cumulative counter and
// publishes the new value to the shared store before acknowledging.
func (s *server) handleCount(msg maelstrom.Message) error {
	var body countBody
	if err := unmarshalBody(msg, &body); err != nil {
		return maelstrom.NewRPCError(maelstrom.MalformedRequest, err.Error())
	}
	if body.Visitors < 0 {
		return maelstrom.NewRPCError(maelstrom.MalformedRequest, "visitors must be non-negative")
	}

	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()

	id := requestID{client: msg.Src, msgID: body.MsgID}

	// The lock is held until the write completes.  That keeps duplicate
	// requests idempotent and guarantees that the cumulative values reach the
	// store in the order in which they were accumulated.
	s.mu.Lock()
	if s.seenCounts[id] {
		// This request was already counted.  Publish the current cumulative
		// value again (which may repair an earlier uncertain write), but do
		// not increment it a second time.
		err := s.writeCounter(ctx, keyPrefix+s.node.ID(), s.local)
		s.mu.Unlock()
		if err != nil {
			return fmt.Errorf("republish counter: %w", err)
		}
		return s.node.Reply(msg, map[string]any{"type": "counted"})
	}

	// Remember the request before writing, so that even an uncertain request
	// cannot be counted twice if it is repeated.
	s.seenCounts[id] = true
	s.local += body.Visitors

	err := s.writeCounter(ctx, keyPrefix+s.node.ID(), s.local)
	s.mu.Unlock()
	if err != nil {
		// The count was not acknowledged, so it may be omitted from the
		// total.  The increment stays in the local counter and will be
		// published by the next successful write.
		return fmt.Errorf("publish counter: %w", err)
	}

	return s.node.Reply(msg, map[string]any{"type": "counted"})
}

// handleTotal answers with the sum of every turnstile's counter.
func (s *server) handleTotal(msg maelstrom.Message) error {
	var body totalBody
	if err := unmarshalBody(msg, &body); err != nil {
		return maelstrom.NewRPCError(maelstrom.MalformedRequest, err.Error())
	}

	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()

	// A seq-kv read may observe an older state.  First perform a changing
	// write from this client: sequential consistency then guarantees that the
	// following reads cannot go back before this point in the store's total
	// order.  All counter writes that were acknowledged before this request
	// are therefore visible to the reads below.
	if err := s.writeBarrier(ctx); err != nil {
		return fmt.Errorf("read barrier: %w", err)
	}

	s.mu.Lock()
	ids := append([]string(nil), s.nodeIDs...)
	local := s.local
	s.mu.Unlock()

	var total int64
	for _, id := range ids {
		if id == s.node.ID() {
			// The local cumulative value also covers a count whose store write
			// succeeded but whose acknowledgement was lost.  Such a count is
			// indeterminate from the client's point of view, so including it
			// is allowed.
			total += local
			continue
		}

		value, err := s.readCounter(ctx, keyPrefix+id)
		if err != nil {
			return fmt.Errorf("read counter for %s: %w", id, err)
		}
		total += value
	}

	return s.node.Reply(msg, map[string]any{
		"type":            "total_is",
		"visitors_so_far": total,
	})
}

// writeCounter publishes a cumulative counter value.  Retrying is useful when
// a response is lost: the value is cumulative, so retrying the same value is
// idempotent.
func (s *server) writeCounter(ctx context.Context, key string, value int64) error {
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		err := s.kv.Write(ctx, key, value)
		if err == nil {
			return nil
		}
		lastErr = err
		if ctx.Err() != nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	return lastErr
}

// writeBarrier writes a fresh value on every attempt.  A changing write is
// applied to the newest seq-kv state and advances this client's read
// position; a repeated no-op write would not necessarily do so.
func (s *server) writeBarrier(ctx context.Context) error {
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		s.mu.Lock()
		s.barrier++
		value := s.barrier
		s.mu.Unlock()

		err := s.kv.Write(ctx, barrierPrefix+s.node.ID(), value)
		if err == nil {
			return nil
		}
		lastErr = err
		if ctx.Err() != nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	return lastErr
}

// readCounter reads a node's cumulative counter, treating a missing key as
// zero.
func (s *server) readCounter(ctx context.Context, key string) (int64, error) {
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		value, err := s.kv.Read(ctx, key)
		if err == nil {
			switch v := value.(type) {
			case int:
				return int64(v), nil
			case int64:
				return v, nil
			case float64:
				return int64(v), nil
			default:
				return 0, fmt.Errorf("unexpected counter value %T", value)
			}
		}

		if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
			return 0, nil
		}
		lastErr = err
		if ctx.Err() != nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	return 0, lastErr
}

func unmarshalBody(msg maelstrom.Message, dst any) error {
	if len(msg.Body) == 0 {
		return errors.New("empty message body")
	}
	return json.Unmarshal(msg.Body, dst)
}
