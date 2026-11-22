package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// seenSet keeps every value that has been broadcast to this node. A set is
// used so that duplicate broadcasts stay idempotent.
type seenSet struct {
	mu     sync.RWMutex
	values map[int]struct{}
}

func newSeenSet() *seenSet {
	return &seenSet{values: make(map[int]struct{})}
}

func (s *seenSet) add(v int) {
	s.mu.Lock()
	s.values[v] = struct{}{}
	s.mu.Unlock()
}

func (s *seenSet) snapshot() []int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]int, 0, len(s.values))
	for v := range s.values {
		out = append(out, v)
	}
	return out
}

type broadcastMessage struct {
	Type    string `json:"type"`
	Message int    `json:"message"`
}

type topologyMessage struct {
	Type     string              `json:"type"`
	Topology map[string][]string `json:"topology"`
}

func main() {
	n := maelstrom.NewNode()
	seen := newSeenSet()

	// broadcast: record the value and acknowledge it. The value is stored
	// before the reply is sent, so every acknowledged value is readable.
	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body broadcastMessage
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		seen.add(body.Message)
		return n.Reply(msg, map[string]any{"type": "broadcast_ok"})
	})

	// read: report every value received so far.
	n.Handle("read", func(msg maelstrom.Message) error {
		return n.Reply(msg, map[string]any{
			"type":     "read_ok",
			"messages": seen.snapshot(),
		})
	})

	// topology: record neighbourhood information and acknowledge it. The
	// cluster currently has a single node, so there is nothing to gossip to.
	n.Handle("topology", func(msg maelstrom.Message) error {
		var body topologyMessage
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		return n.Reply(msg, map[string]any{"type": "topology_ok"})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
