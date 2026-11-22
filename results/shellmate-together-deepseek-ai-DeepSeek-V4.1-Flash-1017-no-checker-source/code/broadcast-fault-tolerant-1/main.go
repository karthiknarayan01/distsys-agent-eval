package main

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// gossipInterval is how often each node pushes its full known set to its peers.
// It is the anti-entropy fallback: any value lost across a partition is repaired
// shortly after the partition heals.
const gossipInterval = 60 * time.Millisecond

type server struct {
	node *maelstrom.Node

	mu    sync.Mutex
	vals  map[int]struct{}
	peers []string
}

type gossipBody struct {
	Type   string `json:"type"`
	Values []int  `json:"values"`
}

func main() {
	n := maelstrom.NewNode()
	s := &server{
		node: n,
		vals: make(map[int]struct{}),
	}

	n.Handle("broadcast", s.handleBroadcast)
	n.Handle("read", s.handleRead)
	n.Handle("topology", s.handleTopology)
	n.Handle("gossip", s.handleGossip)

	go s.gossipLoop()

	if err := n.Run(); err != nil {
		log.Printf("node exited: %v", err)
	}
}

func (s *server) handleBroadcast(msg maelstrom.Message) error {
	var body struct {
		Message int `json:"message"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	added := s.merge([]int{body.Message})
	if len(added) > 0 {
		// Eagerly push the new value so it spreads quickly.
		go s.announce(added, "")
	}

	return s.node.Reply(msg, map[string]any{"type": "broadcast_ok"})
}

func (s *server) handleRead(msg maelstrom.Message) error {
	return s.node.Reply(msg, map[string]any{
		"type":     "read_ok",
		"messages": s.snapshot(),
	})
}

func (s *server) handleTopology(msg maelstrom.Message) error {
	// We gossip to every other node: with five nodes this is cheap and it makes
	// convergence after a partition heal independent of the suggested topology.
	s.mu.Lock()
	s.peers = s.allPeersLocked()
	s.mu.Unlock()

	return s.node.Reply(msg, map[string]any{"type": "topology_ok"})
}

func (s *server) handleGossip(msg maelstrom.Message) error {
	var body gossipBody
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	added := s.merge(body.Values)
	if len(added) > 0 {
		// Pass newly learned values on right away, skipping the sender.
		go s.announce(added, msg.Src)
	}
	return nil
}

// merge adds values to the local set and returns the ones that were new.
func (s *server) merge(vals []int) []int {
	s.mu.Lock()
	defer s.mu.Unlock()

	var added []int
	for _, v := range vals {
		if _, ok := s.vals[v]; !ok {
			s.vals[v] = struct{}{}
			added = append(added, v)
		}
	}
	return added
}

func (s *server) snapshot() []int {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make([]int, 0, len(s.vals))
	for v := range s.vals {
		out = append(out, v)
	}
	return out
}

// allPeersLocked returns every node ID except our own. Callers hold s.mu, or are
// the single writer in handleTopology.
func (s *server) allPeersLocked() []string {
	ids := s.node.NodeIDs()
	peers := make([]string, 0, len(ids))
	for _, id := range ids {
		if id != s.node.ID() {
			peers = append(peers, id)
		}
	}
	return peers
}

func (s *server) peerList() []string {
	s.mu.Lock()
	peers := s.peers
	if len(peers) == 0 {
		peers = s.allPeersLocked()
	}
	out := make([]string, len(peers))
	copy(out, peers)
	s.mu.Unlock()
	return out
}

// announce sends vals to every peer except skip.
func (s *server) announce(vals []int, skip string) {
	if len(vals) == 0 {
		return
	}
	for _, p := range s.peerList() {
		if p == skip {
			continue
		}
		_ = s.node.Send(p, gossipBody{Type: "gossip", Values: vals})
	}
}

func (s *server) gossipLoop() {
	ticker := time.NewTicker(gossipInterval)
	defer ticker.Stop()

	for range ticker.C {
		vals := s.snapshot()
		if len(vals) == 0 {
			continue
		}
		for _, p := range s.peerList() {
			_ = s.node.Send(p, gossipBody{Type: "gossip", Values: vals})
		}
	}
}
