package main

import (
	"encoding/json"
	"sort"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type node struct {
	n *maelstrom.Node

	mu       sync.RWMutex
	messages map[int]struct{}
}

func main() {
	n := maelstrom.NewNode()
	s := &node{n: n, messages: make(map[int]struct{})}

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body struct {
			Message int `json:"message"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		s.mu.Lock()
		_, existed := s.messages[body.Message]
		s.messages[body.Message] = struct{}{}
		s.mu.Unlock()

		// Send the value straight to every other node.  The network is
		// reliable and there are only 25 nodes, so a direct full mesh gives
		// the smallest possible propagation delay (one network hop) while
		// remaining below the allowed message budget.
		if !existed {
			for _, id := range n.NodeIDs() {
				if id == n.ID() {
					continue
				}
				if err := n.Send(id, map[string]any{
					"type":    "gossip",
					"message": body.Message,
				}); err != nil {
					return err
				}
			}
		}

		return n.Reply(msg, map[string]any{"type": "broadcast_ok"})
	})

	n.Handle("gossip", func(msg maelstrom.Message) error {
		var body struct {
			Message int `json:"message"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		s.mu.Lock()
		s.messages[body.Message] = struct{}{}
		s.mu.Unlock()
		return nil
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		s.mu.RLock()
		values := make([]int, 0, len(s.messages))
		for v := range s.messages {
			values = append(values, v)
		}
		s.mu.RUnlock()
		sort.Ints(values)

		return n.Reply(msg, map[string]any{
			"type":     "read_ok",
			"messages": values,
		})
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		return n.Reply(msg, map[string]any{"type": "topology_ok"})
	})

	if err := n.Run(); err != nil {
		panic(err)
	}
}
