package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type broadcastBody struct {
	Type    string `json:"type"`
	MsgID   int    `json:"msg_id,omitempty"`
	Message int    `json:"message"`
}

type gossipBody struct {
	Type    string `json:"type"`
	Message int    `json:"message"`
}

type server struct {
	mu       sync.RWMutex
	messages map[int]struct{}
	peers    []string
}

func main() {
	n := maelstrom.NewNode()
	s := &server{messages: make(map[int]struct{})}

	n.Handle("init", func(msg maelstrom.Message) error {
		// Maelstrom permits servers to choose their own communication graph.
		// A direct one-hop fanout has much lower propagation latency than
		// epidemic gossip over the suggested grid, while still using only 24
		// inter-node messages per broadcast.
		s.mu.Lock()
		s.peers = make([]string, 0, len(n.NodeIDs()))
		for _, id := range n.NodeIDs() {
			if id != n.ID() {
				s.peers = append(s.peers, id)
			}
		}
		s.mu.Unlock()
		return nil
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		return n.Reply(msg, map[string]any{"type": "topology_ok"})
	})

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body broadcastBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		first := s.add(body.Message)

		// Acknowledge after recording the value locally. Every other node gets
		// the value directly, so replication needs only one network hop.
		if first {
			s.mu.RLock()
			peers := append([]string(nil), s.peers...)
			s.mu.RUnlock()
			for _, peer := range peers {
				if err := n.Send(peer, gossipBody{Type: "gossip", Message: body.Message}); err != nil {
					log.Printf("failed to send value %d to %s: %v", body.Message, peer, err)
				}
			}
		}

		return n.Reply(msg, map[string]any{"type": "broadcast_ok"})
	})

	n.Handle("gossip", func(msg maelstrom.Message) error {
		var body gossipBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		s.add(body.Message)
		return nil
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		s.mu.RLock()
		messages := make([]int, 0, len(s.messages))
		for value := range s.messages {
			messages = append(messages, value)
		}
		s.mu.RUnlock()
		return n.Reply(msg, map[string]any{"type": "read_ok", "messages": messages})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

// add records a value and reports whether it was not already known.
func (s *server) add(value int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.messages[value]; ok {
		return false
	}
	s.messages[value] = struct{}{}
	return true
}
