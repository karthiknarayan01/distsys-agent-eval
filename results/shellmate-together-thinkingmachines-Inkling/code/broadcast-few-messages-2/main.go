package main

import (
	"encoding/json"
	"log"
	"os"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type NodeState struct {
	n       *maelstrom.Node
	mu      sync.Mutex
	values  map[int]bool
	pending map[int]bool
}

func main() {
	n := maelstrom.NewNode()
	state := &NodeState{
		n:       n,
		values:  make(map[int]bool),
		pending: make(map[int]bool),
	}

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body struct {
			Type    string `json:"type"`
			MsgID   int    `json:"msg_id"`
			Message int    `json:"message"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		state.mu.Lock()
		state.values[body.Message] = true
		state.pending[body.Message] = true
		state.mu.Unlock()

		return n.Reply(msg, map[string]interface{}{"type": "broadcast_ok"})
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		state.mu.Lock()
		msgs := make([]int, 0, len(state.values))
		for v := range state.values {
			msgs = append(msgs, v)
		}
		state.mu.Unlock()

		return n.Reply(msg, map[string]interface{}{
			"type":      "read_ok",
			"messages": msgs,
		})
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		return n.Reply(msg, map[string]interface{}{"type": "topology_ok"})
	})

	n.Handle("broadcast_values", func(msg maelstrom.Message) error {
		var body struct {
			Type   string `json:"type"`
			Values []int  `json:"values"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		state.mu.Lock()
		for _, v := range body.Values {
			state.values[v] = true
		}
		state.mu.Unlock()

		return nil
	})

	// Start a ticker to propagate pending values.
	go func() {
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()
		for range ticker.C {
			state.sendPending()
		}
	}()

	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}
}

func (s *NodeState) sendPending() {
	s.mu.Lock()
	if len(s.pending) == 0 {
		s.mu.Unlock()
		return
	}
	values := make([]int, 0, len(s.pending))
	for v := range s.pending {
		values = append(values, v)
	}
	s.pending = make(map[int]bool)
	s.mu.Unlock()

	ids := s.n.NodeIDs()
	own := s.n.ID()
	if own == "" || len(ids) == 0 {
		// Not initialized yet; skip.
		return
	}

	body := map[string]interface{}{
		"type":   "broadcast_values",
		"values": values,
	}

	for _, id := range ids {
		if id == own {
			continue
		}
		s.n.Send(id, body)
	}
}
