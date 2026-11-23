package main

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// server holds the state of a single gossip node. Each node keeps the set of
// every rumor it has heard of and repeatedly pushes that whole set to every
// other node in the cluster. Full-set anti-entropy is what repairs the state
// after the network has been split: messages sent across a partition are lost
// silently, but the ticker keeps re-sending the complete set, so as soon as
// the partition heals the next round trip converges everything.
type server struct {
	n      *maelstrom.Node
	ready  chan struct{}
	mu     sync.Mutex
	rumors map[int]struct{}
}

const syncInterval = 200 * time.Millisecond

func main() {
	n := maelstrom.NewNode()
	s := &server{
		n:      n,
		ready:  make(chan struct{}),
		rumors: make(map[int]struct{}),
	}

	n.Handle("tell", s.handleBroadcast)
	n.Handle("recall", s.handleRead)
	n.Handle("neighbors", s.handleTopology)
	n.Handle("gossip", s.handleGossip)
	n.Handle("gossip_ok", func(msg maelstrom.Message) error { return nil })
	// The library answers "init" itself; this handler just lets us know when
	// the node ID and the peer list are available.
	n.Handle("init", func(msg maelstrom.Message) error {
		close(s.ready)
		return nil
	})

	go s.syncLoop()

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

// handleBroadcast records a new rumor and acknowledges it before spreading it
// around. The client's "told" reply must not wait on the network.
func (s *server) handleBroadcast(msg maelstrom.Message) error {
	var body struct {
		Message int `json:"rumor"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	s.mu.Lock()
	_, known := s.rumors[body.Message]
	s.rumors[body.Message] = struct{}{}
	s.mu.Unlock()

	if err := s.n.Reply(msg, map[string]any{"type": "told"}); err != nil {
		return err
	}

	if !known {
		// Fast path: push the fresh rumor out immediately instead of waiting
		// for the next full sync.
		s.sendAll([]int{body.Message})
	}
	return nil
}

func (s *server) handleRead(msg maelstrom.Message) error {
	s.mu.Lock()
	rumors := make([]int, 0, len(s.rumors))
	for r := range s.rumors {
		rumors = append(rumors, r)
	}
	s.mu.Unlock()

	return s.n.Reply(msg, map[string]any{"type": "recalled", "rumors": rumors})
}

// handleTopology accepts the suggested neighbors but ignores them: every node
// gossips with every other node, which keeps the protocol correct no matter
// how the network is partitioned.
func (s *server) handleTopology(msg maelstrom.Message) error {
	return s.n.Reply(msg, map[string]any{"type": "neighbors_noted"})
}

// handleGossip merges a peer's rumors into our own set and, if anything was
// new, forwards it at once so rumors spread epidemically.
func (s *server) handleGossip(msg maelstrom.Message) error {
	var body struct {
		Rumors []int `json:"rumors"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return nil
	}

	var fresh []int
	s.mu.Lock()
	for _, r := range body.Rumors {
		if _, ok := s.rumors[r]; !ok {
			s.rumors[r] = struct{}{}
			fresh = append(fresh, r)
		}
	}
	s.mu.Unlock()

	if len(fresh) > 0 {
		s.sendAll(fresh)
	}
	return nil
}

// syncLoop periodically sends the entire rumor set to every peer. This is the
// safety net that guarantees eventual convergence after partitions heal.
func (s *server) syncLoop() {
	<-s.ready
	ticker := time.NewTicker(syncInterval)
	defer ticker.Stop()

	for range ticker.C {
		s.mu.Lock()
		if len(s.rumors) == 0 {
			s.mu.Unlock()
			continue
		}
		rumors := make([]int, 0, len(s.rumors))
		for r := range s.rumors {
			rumors = append(rumors, r)
		}
		s.mu.Unlock()

		s.sendAll(rumors)
	}
}

// sendAll sends one gossip message carrying the given rumors to every peer.
func (s *server) sendAll(rumors []int) {
	if len(rumors) == 0 {
		return
	}
	self := s.n.ID()
	body := map[string]any{"type": "gossip", "rumors": rumors}
	for _, peer := range s.n.NodeIDs() {
		if peer == self {
			continue
		}
		if err := s.n.Send(peer, body); err != nil {
			log.Printf("send to %s failed: %v", peer, err)
		}
	}
}
