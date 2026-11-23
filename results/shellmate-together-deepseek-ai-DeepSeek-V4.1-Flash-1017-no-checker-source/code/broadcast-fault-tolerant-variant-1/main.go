package main

import (
	"encoding/json"
	"log"
	"math/rand"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// gossipInterval is how often a node pushes its full knowledge to every peer.
// It bounds how long convergence takes once the network is healed; the test
// waits several seconds for recovery at the end, so this is plenty fast.
const gossipInterval = 250 * time.Millisecond

type server struct {
	node *maelstrom.Node

	mu     sync.Mutex
	rumors map[int]struct{}
	peers  []string
	begun  bool
}

func main() {
	n := maelstrom.NewNode()
	s := &server{node: n, rumors: make(map[int]struct{})}

	n.Handle("init", s.handleInit)
	n.Handle("tell", s.handleTell)
	n.Handle("recall", s.handleRecall)
	n.Handle("neighbors", s.handleNeighbors)
	n.Handle("gossip", s.handleGossip)

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

// handleInit records the cluster membership and starts anti-entropy. The
// library replies with init_ok after this handler returns.
func (s *server) handleInit(msg maelstrom.Message) error {
	s.mu.Lock()
	self := s.node.ID()
	s.peers = s.peers[:0]
	for _, id := range s.node.NodeIDs() {
		if id != self {
			s.peers = append(s.peers, id)
		}
	}
	start := !s.begun
	s.begun = true
	s.mu.Unlock()

	if start {
		go s.gossipLoop()
	}
	return nil
}

type tellBody struct {
	Type  string `json:"type"`
	Rumor int    `json:"rumor"`
}

func (s *server) handleTell(msg maelstrom.Message) error {
	var body tellBody
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	s.mu.Lock()
	_, known := s.rumors[body.Rumor]
	s.rumors[body.Rumor] = struct{}{}
	s.mu.Unlock()

	// Answer the client as soon as the rumor is stored locally.
	if err := s.node.Reply(msg, map[string]any{"type": "told"}); err != nil {
		return err
	}

	// Push the news out right away; periodic gossip repairs anything lost.
	if !known {
		go s.broadcast([]int{body.Rumor})
	}
	return nil
}

type recallBody struct {
	Type   string `json:"type"`
	Rumors []int  `json:"rumors"`
}

func (s *server) handleRecall(msg maelstrom.Message) error {
	return s.node.Reply(msg, map[string]any{
		"type":   "recalled",
		"rumors": s.snapshot(),
	})
}

func (s *server) handleNeighbors(msg maelstrom.Message) error {
	// Membership from init is enough: we gossip to every node, so the
	// suggested topology is only a hint. Acknowledge it as expected.
	return s.node.Reply(msg, map[string]any{"type": "neighbors_noted"})
}

type gossipBody struct {
	Type   string `json:"type"`
	Rumors []int  `json:"rumors"`
}

// handleGossip simply merges what a peer sends us. There is no reply: peers
// push on their own schedule, which keeps the message flow bounded and avoids
// endless back-and-forth during a partition.
func (s *server) handleGossip(msg maelstrom.Message) error {
	var body gossipBody
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	s.mu.Lock()
	for _, r := range body.Rumors {
		s.rumors[r] = struct{}{}
	}
	s.mu.Unlock()
	return nil
}

func (s *server) snapshot() []int {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]int, 0, len(s.rumors))
	for r := range s.rumors {
		out = append(out, r)
	}
	return out
}

func (s *server) peerList() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, len(s.peers))
	copy(out, s.peers)
	return out
}

// broadcast sends the given rumors to every peer. With no rumors to send and
// gossipAll false it does nothing.
func (s *server) broadcast(rumors []int) {
	if len(rumors) == 0 {
		return
	}
	for _, p := range s.peerList() {
		_ = s.node.Send(p, map[string]any{"type": "gossip", "rumors": rumors})
	}
}

func (s *server) gossipLoop() {
	// Stagger nodes a little so they do not all transmit in lockstep.
	time.Sleep(time.Duration(rand.Int63n(int64(gossipInterval))))

	ticker := time.NewTicker(gossipInterval)
	defer ticker.Stop()
	for range ticker.C {
		s.broadcast(s.snapshot())
	}
}
