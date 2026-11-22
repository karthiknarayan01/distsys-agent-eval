package main

import (
	"encoding/json"
	"log"
	"sort"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type server struct {
	n  *maelstrom.Node
	mu sync.Mutex
	vs map[int]struct{}
}

func main() {
	n := maelstrom.NewNode()
	s := &server{
		n:  n,
		vs: make(map[int]struct{}),
	}

	n.Handle("broadcast", s.handleBroadcast)
	n.Handle("read", s.handleRead)
	n.Handle("topology", s.handleTopology)
	n.Handle("gossip", s.handleGossip)

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

func (s *server) add(v int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.vs[v]; ok {
		return false
	}
	s.vs[v] = struct{}{}
	return true
}

func (s *server) forward(v int) {
	for _, id := range s.n.NodeIDs() {
		if id == s.n.ID() {
			continue
		}
		body := map[string]any{
			"type":    "gossip",
			"message": v,
		}
		if err := s.n.Send(id, body); err != nil {
			log.Printf("send gossip to %s: %v", id, err)
		}
	}
}

func (s *server) handleBroadcast(msg maelstrom.Message) error {
	var body struct {
		Message int `json:"message"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	if s.add(body.Message) {
		s.forward(body.Message)
	}

	return s.n.Reply(msg, map[string]any{"type": "broadcast_ok"})
}

func (s *server) handleGossip(msg maelstrom.Message) error {
	var body struct {
		Message int `json:"message"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	if s.add(body.Message) {
		s.forward(body.Message)
	}
	return nil
}

func (s *server) handleRead(msg maelstrom.Message) error {
	s.mu.Lock()
	vals := make([]int, 0, len(s.vs))
	for v := range s.vs {
		vals = append(vals, v)
	}
	s.mu.Unlock()

	sort.Ints(vals)

	return s.n.Reply(msg, map[string]any{
		"type":     "read_ok",
		"messages": vals,
	})
}

func (s *server) handleTopology(msg maelstrom.Message) error {
	return s.n.Reply(msg, map[string]any{"type": "topology_ok"})
}
