package main

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// server keeps a grow-only counter as a per-node map (a G-counter CRDT).
// Each node only ever increments its own entry; entries from other nodes are
// merged by taking the maximum, which makes the merge idempotent and safe
// under message loss and duplication.
type server struct {
	n        *maelstrom.Node
	mu       sync.Mutex
	counters map[string]int
}

func (s *server) add(msg maelstrom.Message) error {
	var body struct {
		Delta int `json:"delta"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	s.mu.Lock()
	s.counters[s.n.ID()] += body.Delta
	s.mu.Unlock()

	// Best-effort immediate propagation; periodic gossip covers lost messages.
	s.broadcast()

	return s.n.Reply(msg, map[string]any{"type": "add_ok"})
}

func (s *server) read(msg maelstrom.Message) error {
	s.mu.Lock()
	sum := 0
	for _, v := range s.counters {
		sum += v
	}
	s.mu.Unlock()

	return s.n.Reply(msg, map[string]any{"type": "read_ok", "value": sum})
}

func (s *server) replicate(msg maelstrom.Message) error {
	var body struct {
		Value map[string]int `json:"value"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		log.Printf("ignoring malformed replicate message: %v", err)
		return nil
	}

	s.mu.Lock()
	for k, v := range body.Value {
		if v > s.counters[k] {
			s.counters[k] = v
		}
	}
	s.mu.Unlock()

	return nil
}

// broadcast sends a snapshot of the local counter map to every other node.
func (s *server) broadcast() {
	s.mu.Lock()
	value := make(map[string]int, len(s.counters))
	for k, v := range s.counters {
		value[k] = v
	}
	s.mu.Unlock()

	for _, id := range s.n.NodeIDs() {
		if id == s.n.ID() {
			continue
		}
		if err := s.n.Send(id, map[string]any{"type": "replicate", "value": value}); err != nil {
			log.Printf("send replicate: %v", err)
		}
	}
}

func (s *server) gossipLoop() {
	for {
		s.broadcast()
		time.Sleep(500 * time.Millisecond)
	}
}

func main() {
	n := maelstrom.NewNode()
	s := &server{n: n, counters: make(map[string]int)}

	n.Handle("add", s.add)
	n.Handle("read", s.read)
	n.Handle("replicate", s.replicate)
	n.Handle("init", func(msg maelstrom.Message) error {
		go s.gossipLoop()
		return nil
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
