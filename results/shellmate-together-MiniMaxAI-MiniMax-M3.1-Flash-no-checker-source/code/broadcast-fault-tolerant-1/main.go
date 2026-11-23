package main

import (
	"encoding/json"
	"log"
	"os"
	"sort"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// Server implements an eventually consistent, grow-only set of broadcast
// values.  Values are pushed to peers as deltas and, periodically, the whole
// set is exchanged as an anti-entropy round.  Because set union is
// commutative, associative and idempotent, messages can arrive in any order,
// more than once, or be lost during a network partition without breaking
// convergence once the network heals.
type Server struct {
	node *maelstrom.Node

	mu     sync.RWMutex
	values map[int64]struct{}

	peersMu sync.RWMutex
	peers   []string

	startOnce sync.Once
}

func newServer() *Server {
	return &Server{
		node:   maelstrom.NewNode(),
		values: make(map[int64]struct{}),
	}
}

func (s *Server) setPeers(ids []string) {
	peers := make([]string, 0, len(ids))
	self := s.node.ID()
	for _, id := range ids {
		if id != self {
			peers = append(peers, id)
		}
	}
	s.peersMu.Lock()
	s.peers = peers
	s.peersMu.Unlock()
}

func (s *Server) peerList() []string {
	s.peersMu.RLock()
	defer s.peersMu.RUnlock()
	out := make([]string, len(s.peers))
	copy(out, s.peers)
	return out
}

// add inserts a value and reports whether it was new.
func (s *Server) add(v int64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.values[v]; ok {
		return false
	}
	s.values[v] = struct{}{}
	return true
}

// merge unions a set of values into ours and returns the values that were
// previously unknown.
func (s *Server) merge(vals []int64) []int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	fresh := make([]int64, 0, len(vals))
	for _, v := range vals {
		if _, ok := s.values[v]; ok {
			continue
		}
		s.values[v] = struct{}{}
		fresh = append(fresh, v)
	}
	return fresh
}

func (s *Server) snapshot() []int64 {
	s.mu.RLock()
	out := make([]int64, 0, len(s.values))
	for v := range s.values {
		out = append(out, v)
	}
	s.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// sendValues sends a batch of values to one peer.
func (s *Server) sendValues(dest string, values []int64) {
	if len(values) == 0 {
		return
	}
	if err := s.node.Send(dest, struct {
		Type     string  `json:"type"`
		Messages []int64 `json:"messages"`
	}{
		Type:     "replicate",
		Messages: values,
	}); err != nil {
		log.Printf("send to %s failed: %v", dest, err)
	}
}

// sendToPeers sends values to every known peer, optionally skipping one.
func (s *Server) sendToPeers(values []int64, skip string) {
	for _, p := range s.peerList() {
		if p == skip {
			continue
		}
		s.sendValues(p, values)
	}
}

// pushFullState sends our complete state to every peer.  This is the
// anti-entropy half of the protocol: it repairs anything lost while the
// network was partitioned.
func (s *Server) pushFullState() {
	all := s.snapshot()
	s.sendToPeers(all, "")
}

// handleBroadcast accepts a client broadcast.  The value is made durable in
// the local set before we acknowledge it, then gossiped to the other nodes.
func (s *Server) handleBroadcast(msg maelstrom.Message) error {
	var body struct {
		Message int64 `json:"message"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	s.add(body.Message)

	if err := s.node.Reply(msg, struct {
		Type string `json:"type"`
	}{Type: "broadcast_ok"}); err != nil {
		return err
	}

	// Immediately push the new value so it spreads quickly in a healthy
	// network.  Periodic full-state exchanges cover losses and partitions.
	s.sendToPeers([]int64{body.Message}, msg.Src)
	return nil
}

// handleReplicate merges a batch of values received from a peer.  Any newly
// learned values are forwarded to the remaining peers, giving epidemic
// propagation, while the periodic full-state exchange guarantees eventual
// convergence after partitions heal.
func (s *Server) handleReplicate(msg maelstrom.Message) error {
	var body struct {
		Messages []int64 `json:"messages"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	fresh := s.merge(body.Messages)
	if len(fresh) > 0 {
		s.sendToPeers(fresh, msg.Src)
	}
	return nil
}

// handleRead returns every value currently known by this node.
func (s *Server) handleRead(msg maelstrom.Message) error {
	messages := s.snapshot()
	return s.node.Reply(msg, struct {
		Type     string  `json:"type"`
		Messages []int64 `json:"messages"`
	}{Type: "read_ok", Messages: messages})
}

// handleTopology accepts Maelstrom's suggested topology.  We use a complete
// mesh instead, which makes convergence after a partition heal independent of
// the suggested links, but we still acknowledge the request.
func (s *Server) handleTopology(msg maelstrom.Message) error {
	return s.node.Reply(msg, struct {
		Type string `json:"type"`
	}{Type: "topology_ok"})
}

// startGossip runs the periodic anti-entropy loop.
func (s *Server) startGossip() {
	s.startOnce.Do(func() {
		go func() {
			ticker := time.NewTicker(100 * time.Millisecond)
			defer ticker.Stop()
			for range ticker.C {
				s.pushFullState()
			}
		}()
	})
}

func main() {
	s := newServer()

	s.node.Handle("broadcast", s.handleBroadcast)
	s.node.Handle("read", s.handleRead)
	s.node.Handle("topology", s.handleTopology)
	s.node.Handle("replicate", s.handleReplicate)
	// Never crash on an unexpected error message from the network.
	s.node.Handle("error", func(msg maelstrom.Message) error { return nil })
	s.node.Handle("init", func(msg maelstrom.Message) error {
		// The library has already recorded our identity by the time this
		// handler runs.
		s.setPeers(s.node.NodeIDs())
		s.startGossip()
		return nil
	})

	if err := s.node.Run(); err != nil {
		log.Printf("node exited: %v", err)
		os.Exit(1)
	}
}
