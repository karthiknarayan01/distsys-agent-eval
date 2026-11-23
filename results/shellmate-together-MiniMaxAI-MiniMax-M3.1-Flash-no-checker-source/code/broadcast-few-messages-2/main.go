package main

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// The protocol carries integer values. Keeping them as int64 avoids the
// precision loss that decoding JSON numbers as float64 would introduce.
type requestBody struct {
	Type    string `json:"type"`
	Message int64  `json:"message"`
	MsgID   int    `json:"msg_id"`
}

type server struct {
	node *maelstrom.Node

	mu       sync.RWMutex
	messages map[int64]struct{}
}

func newServer() *server {
	return &server{
		node:     maelstrom.NewNode(),
		messages: make(map[int64]struct{}),
	}
}

func (s *server) add(v int64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.messages[v]; ok {
		return false
	}
	s.messages[v] = struct{}{}
	return true
}

func (s *server) snapshot() []int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]int64, 0, len(s.messages))
	for v := range s.messages {
		out = append(out, v)
	}
	return out
}

func (s *server) handleBroadcast(msg maelstrom.Message) error {
	var body requestBody
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return fmt.Errorf("decode broadcast body: %w", err)
	}

	// Every node starts with the value locally. The originating node then
	// sends it directly to each other node. Direct delivery gives a
	// one-hop dissemination path while using exactly 24 inter-node messages
	// per broadcast, the minimum possible for an un-batched value.
	s.add(body.Message)
	for _, dest := range s.node.NodeIDs() {
		if dest == s.node.ID() {
			continue
		}
		if err := s.node.Send(dest, map[string]any{
			"type":    "broadcast_value",
			"message": body.Message,
		}); err != nil {
			return fmt.Errorf("send value to %s: %w", dest, err)
		}
	}

	return s.node.Reply(msg, map[string]any{"type": "broadcast_ok"})
}

func (s *server) handleBroadcastValue(msg maelstrom.Message) error {
	var body requestBody
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return fmt.Errorf("decode broadcast_value body: %w", err)
	}
	s.add(body.Message)
	return nil
}

func (s *server) handleRead(msg maelstrom.Message) error {
	return s.node.Reply(msg, map[string]any{
		"type":     "read_ok",
		"messages": s.snapshot(),
	})
}

func (s *server) handleTopology(msg maelstrom.Message) error {
	// We do not need the suggested topology: dissemination is direct.
	return s.node.Reply(msg, map[string]any{"type": "topology_ok"})
}

func main() {
	s := newServer()
	s.node.Handle("broadcast", s.handleBroadcast)
	s.node.Handle("broadcast_value", s.handleBroadcastValue)
	s.node.Handle("read", s.handleRead)
	s.node.Handle("topology", s.handleTopology)

	if err := s.node.Run(); err != nil {
		fmt.Fprintln(os.Stderr, "node failed:", err)
		os.Exit(1)
	}
}
