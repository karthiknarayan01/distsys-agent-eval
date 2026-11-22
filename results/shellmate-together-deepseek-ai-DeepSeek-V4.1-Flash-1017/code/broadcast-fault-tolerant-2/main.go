package main

import (
	"encoding/json"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// server holds the values this node knows about.  The full set is periodically
// exchanged with every other node.  Since gossip is idempotent, a partition
// can lose any number of exchanges; once the network heals the next exchanges
// bring all nodes back into agreement.
type server struct {
	node *maelstrom.Node

	mu     sync.RWMutex
	values map[int]struct{}
	peers  []string

	startOnce sync.Once
}

type gossipBody struct {
	Type   string `json:"type"`
	Values []int  `json:"values"`
}

func main() {
	n := maelstrom.NewNode()
	s := &server{
		node:   n,
		values: make(map[int]struct{}),
	}

	n.Handle("init", s.handleInit)
	n.Handle("broadcast", s.handleBroadcast)
	n.Handle("read", s.handleRead)
	n.Handle("topology", s.handleTopology)
	n.Handle("gossip", s.handleGossip)

	if err := n.Run(); err != nil {
		panic(err)
	}
}

func (s *server) handleInit(msg maelstrom.Message) error {
	var body struct {
		NodeID  string   `json:"node_id"`
		NodeIDs []string `json:"node_ids"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	peers := make([]string, 0, len(body.NodeIDs))
	for _, id := range body.NodeIDs {
		if id != body.NodeID {
			peers = append(peers, id)
		}
	}

	s.mu.Lock()
	s.peers = peers
	s.mu.Unlock()

	// Begin anti-entropy only after the node knows its own ID and its peers.
	s.startOnce.Do(func() {
		go s.gossipLoop()
	})
	return nil
}

func (s *server) handleTopology(msg maelstrom.Message) error {
	// A full mesh is intentionally used: partition faults only stop messages
	// between groups of nodes, and a full mesh gives recovering groups the
	// greatest chance of having a working path.
	return s.node.Reply(msg, map[string]any{"type": "topology_ok"})
}

func (s *server) handleBroadcast(msg maelstrom.Message) error {
	var body struct {
		Message int `json:"message"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	s.mu.Lock()
	s.values[body.Message] = struct{}{}
	s.mu.Unlock()

	if err := s.node.Reply(msg, map[string]any{"type": "broadcast_ok"}); err != nil {
		return err
	}

	// Don't wait for the periodic tick to start disseminating this value.
	go s.gossip()
	return nil
}

func (s *server) handleRead(msg maelstrom.Message) error {
	s.mu.RLock()
	messages := make([]int, 0, len(s.values))
	for value := range s.values {
		messages = append(messages, value)
	}
	s.mu.RUnlock()

	return s.node.Reply(msg, map[string]any{
		"type":     "read_ok",
		"messages": messages,
	})
}

func (s *server) handleGossip(msg maelstrom.Message) error {
	var body struct {
		Values []int `json:"values"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	s.mu.Lock()
	for _, value := range body.Values {
		s.values[value] = struct{}{}
	}
	s.mu.Unlock()

	// Gossip has no application-level acknowledgement.  The sender will send
	// its whole set again shortly, and the value set is idempotent.
	return nil
}

func (s *server) gossipLoop() {
	ticker := time.NewTicker(75 * time.Millisecond)
	defer ticker.Stop()

	for range ticker.C {
		s.gossip()
	}
}

func (s *server) gossip() {
	s.mu.RLock()
	if len(s.values) == 0 {
		s.mu.RUnlock()
		return
	}

	values := make([]int, 0, len(s.values))
	for value := range s.values {
		values = append(values, value)
	}
	peers := append([]string(nil), s.peers...)
	s.mu.RUnlock()

	body := gossipBody{Type: "gossip", Values: values}
	for _, peer := range peers {
		// A failed send means the message was lost to the simulated network.
		// Periodic retransmission handles that case.
		_ = s.node.Send(peer, body)
	}
}
