package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const (
	keyPrefix = "visitor-tally/component/"
	opTimeout = 3 * time.Second
)

type server struct {
	node *maelstrom.Node
	kv   *maelstrom.KV

	// Only this node writes its own component.  The mutex both serializes
	// concurrent count requests and protects the cached component.
	mu         sync.Mutex
	component  int
	haveCached bool
}

type countBody struct {
	Type     string `json:"type"`
	MsgID    int    `json:"msg_id"`
	Visitors int    `json:"visitors"`
}

type totalBody struct {
	Type  string `json:"type"`
	MsgID int    `json:"msg_id"`
}

func componentKey(nodeID string) string {
	return keyPrefix + nodeID
}

func main() {
	n := maelstrom.NewNode()
	s := &server{
		node: n,
		kv:   maelstrom.NewSeqKV(n),
	}

	n.Handle("count", s.handleCount)
	n.Handle("total", s.handleTotal)

	if err := n.Run(); err != nil {
		log.Fatalf("node failed: %v", err)
	}
}

func (s *server) handleCount(msg maelstrom.Message) error {
	var body countBody
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return fmt.Errorf("decode count request: %w", err)
	}
	if body.Visitors < 0 {
		return fmt.Errorf("visitors must be non-negative, got %d", body.Visitors)
	}

	if err := s.add(body.Visitors); err != nil {
		// Do not acknowledge a count unless it has been durably recorded.
		return fmt.Errorf("record count: %w", err)
	}
	return s.node.Reply(msg, map[string]any{"type": "counted"})
}

// add increases this turnstile's component exactly once.  Each node owns a
// distinct key, so no two nodes can update the same component.  Compare-and-
// swap makes retries and concurrent requests safe.
func (s *server) add(visitors int) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
	defer cancel()

	key := componentKey(s.node.ID())

	for attempt := 0; attempt < 32; attempt++ {
		current := s.component
		exists := true
		create := false

		// The cache is exact while CAS replies are received normally.  If a
		// previous CAS committed but its reply was lost, CAS below will fail
		// and we re-read the authoritative value.
		if !s.haveCached {
			var err error
			current, exists, err = s.readComponent(ctx, key)
			if err != nil {
				return err
			}
			create = !exists
		}

		next := current + visitors
		err := s.kv.CompareAndSwap(ctx, key, current, next, create)
		if err == nil {
			s.component = next
			s.haveCached = true
			return nil
		}

		switch maelstrom.ErrorCode(err) {
		case maelstrom.PreconditionFailed, maelstrom.KeyDoesNotExist:
			// A stale read (or a previously committed CAS whose response was
			// lost) invalidated our expected value.  Read again and retry.
			s.haveCached = false
			continue
		default:
			return err
		}
	}

	return fmt.Errorf("could not update component after retries")
}

func (s *server) readComponent(ctx context.Context, key string) (value int, exists bool, err error) {
	v, err := s.kv.Read(ctx, key)
	if err != nil {
		if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
			return 0, false, nil
		}
		return 0, false, err
	}

	switch n := v.(type) {
	case int:
		return n, true, nil
	case int64:
		return int(n), true, nil
	case float64:
		return int(n), true, nil
	default:
		return 0, false, fmt.Errorf("unexpected component value %T", v)
	}
}

func (s *server) handleTotal(msg maelstrom.Message) error {
	var body totalBody
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return fmt.Errorf("decode total request: %w", err)
	}

	total, err := s.readTotal()
	if err != nil {
		return fmt.Errorf("read total: %w", err)
	}

	return s.node.Reply(msg, map[string]any{
		"type":            "total_is",
		"visitors_so_far": total,
	})
}

// readTotal sums the independently-owned G-counter components.  Since a
// component only grows, taking the maximum of repeated reads is safe and
// helps seq-kv's permitted stale reads catch up.
func (s *server) readTotal() (int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
	defer cancel()

	values := make(map[string]int, len(s.node.NodeIDs()))
	for _, id := range s.node.NodeIDs() {
		key := componentKey(id)
		value, _, err := s.readComponent(ctx, key)
		if err != nil {
			return 0, err
		}
		values[id] = value
	}

	// A second pass cannot decrease a component.  In particular, our own
	// cached value is newer than a stale seq-kv read.
	for _, id := range s.node.NodeIDs() {
		key := componentKey(id)
		value, _, err := s.readComponent(ctx, key)
		if err != nil {
			return 0, err
		}
		if value > values[id] {
			values[id] = value
		}
	}

	var total int64
	for _, value := range values {
		total += int64(value)
	}
	return total, nil
}
