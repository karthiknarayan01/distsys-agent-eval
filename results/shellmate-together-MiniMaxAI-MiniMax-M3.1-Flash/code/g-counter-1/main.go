package main

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// counterState is a grow-only counter represented as one component per node.
// A node is the only writer of its own component, and components are merged
// by taking their maximum. Thus replication is idempotent and can never
// count an increment twice.
type counterState struct {
	Type  string           `json:"type"`
	State map[string]int64 `json:"state"`
}

type addRequest struct {
	Type  string `json:"type"`
	Delta int64  `json:"delta"`
}

type readResponse struct {
	Type  string `json:"type"`
	Value int64  `json:"value"`
}

type server struct {
	node *maelstrom.Node

	mu    sync.Mutex
	state map[string]int64
}

func newServer(node *maelstrom.Node) *server {
	return &server{
		node:  node,
		state: make(map[string]int64),
	}
}

// mergeState merges a replicated state into ours. Only the owner of a
// component may increase it, so max is a safe and idempotent merge.
func (s *server) mergeState(in map[string]int64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	changed := false
	for id, value := range in {
		if value < 0 {
			continue
		}
		if value > s.state[id] {
			s.state[id] = value
			changed = true
		}
	}
	return changed
}

func (s *server) snapshot() map[string]int64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make(map[string]int64, len(s.state))
	for id, value := range s.state {
		out[id] = value
	}
	return out
}

func (s *server) value() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	var total int64
	for _, component := range s.state {
		total += component
	}
	return total
}

// broadcast sends the complete counter state to every other node. Full-state
// anti-entropy makes retries safe: a lost message is simply sent again, and
// merging it again changes nothing.
func (s *server) broadcast() {
	state := s.snapshot()
	for _, id := range s.node.NodeIDs() {
		if id == s.node.ID() {
			continue
		}
		if err := s.node.Send(id, counterState{Type: "replicate", State: state}); err != nil {
			log.Printf("failed to replicate counter to %s: %v", id, err)
		}
	}
}

func (s *server) handleInit(msg maelstrom.Message) error {
	// Repeated full-state exchanges ensure that all components are present on
	// every node shortly after a partition is healed.
	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for range ticker.C {
			s.broadcast()
		}
	}()
	return nil
}

func (s *server) handleAdd(msg maelstrom.Message) error {
	var req addRequest
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		return err
	}
	if req.Delta < 0 {
		return &maelstrom.RPCError{Code: int(maelstrom.MalformedRequest), Text: "delta must be non-negative"}
	}

	// The local component is changed atomically. Concurrent adds on this node
	// cannot overwrite one another.
	s.mu.Lock()
	s.state[s.node.ID()] += req.Delta
	s.mu.Unlock()

	// An acknowledgement means this add is now represented in our local
	// component and will be disseminated after the network heals.
	if err := s.node.Reply(msg, map[string]any{"type": "add_ok"}); err != nil {
		return err
	}
	s.broadcast()
	return nil
}

func (s *server) handleRead(msg maelstrom.Message) error {
	return s.node.Reply(msg, readResponse{Type: "read_ok", Value: s.value()})
}

func (s *server) handleReplicate(msg maelstrom.Message) error {
	var update counterState
	if err := json.Unmarshal(msg.Body, &update); err != nil {
		return err
	}
	s.mergeState(update.State)
	return nil
}

func main() {
	log.SetFlags(0)
	node := maelstrom.NewNode()
	server := newServer(node)

	node.Handle("init", server.handleInit)
	node.Handle("add", server.handleAdd)
	node.Handle("read", server.handleRead)
	node.Handle("replicate", server.handleReplicate)

	if err := node.Run(); err != nil {
		log.Fatalf("node failed: %v", err)
	}
}
