package main

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// store holds the set of values this node has seen. It is safe for
// concurrent use because handlers may run in separate goroutines.
type store struct {
	mu      sync.Mutex
	values  map[int64]struct{}
	updates chan struct{} // closed once, after the first value is added
}

func newStore() *store {
	return &store{
		values:  make(map[int64]struct{}),
		updates: make(chan struct{}),
	}
}

// add records a value and reports whether it was new. The value is
// recorded before the function returns, so once a broadcast has been
// acknowledged the value is guaranteed to be reported by read.
func (s *store) add(v int64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.values[v]; ok {
		return false
	}
	s.values[v] = struct{}{}
	return true
}

// list returns every value currently held by the node.
func (s *store) list() []int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]int64, 0, len(s.values))
	for v := range s.values {
		out = append(out, v)
	}
	return out
}

func main() {
	n := maelstrom.NewNode()
	s := newStore()

	// broadcast: remember the value, then acknowledge it.
	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body struct {
			Message int64 `json:"message"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return fmt.Errorf("broadcast: bad body: %w", err)
		}
		s.add(body.Message)
		return n.Reply(msg, map[string]any{"type": "broadcast_ok"})
	})

	// read: report every value we hold.
	n.Handle("read", func(msg maelstrom.Message) error {
		return n.Reply(msg, map[string]any{
			"type":     "read_ok",
			"messages": s.list(),
		})
	})

	// topology: note the neighbours, then acknowledge. The answer is
	// the same whether or not we have any.
	n.Handle("topology", func(msg maelstrom.Message) error {
		var body struct {
			Topology map[string][]string `json:"topology"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return fmt.Errorf("topology: bad body: %w", err)
		}
		if peers, ok := body.Topology[n.ID()]; ok {
			log.Printf("topology: node %s neighbours %v", n.ID(), peers)
		}
		return n.Reply(msg, map[string]any{"type": "topology_ok"})
	})

	if err := n.Run(); err != nil {
		log.Fatalf("node %s: %v", n.ID(), err)
	}
}
