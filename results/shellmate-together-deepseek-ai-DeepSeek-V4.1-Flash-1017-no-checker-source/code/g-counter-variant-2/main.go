package main

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// server implements a grow-only counter (a G-counter).  Each node owns one
// entry in counters: its own turnstile.  Entries are exchanged by gossip and
// merged by taking the maximum value seen for each node.  Since each entry is
// only ever incremented by its owner, the maximum is exactly the number of
// visitors acknowledged by that turnstile, so nobody is counted twice.
type server struct {
	node *maelstrom.Node

	mu       sync.Mutex
	counters map[string]int64
	once     sync.Once
}

func main() {
	n := maelstrom.NewNode()
	s := &server{node: n, counters: make(map[string]int64)}

	n.Handle("init", s.handleInit)
	n.Handle("count", s.handleCount)
	n.Handle("total", s.handleTotal)
	n.Handle("replicate", s.handleReplicate)

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

func (s *server) handleInit(msg maelstrom.Message) error {
	s.mu.Lock()
	for _, id := range s.node.NodeIDs() {
		if _, ok := s.counters[id]; !ok {
			s.counters[id] = 0
		}
	}
	s.mu.Unlock()

	s.once.Do(func() { go s.gossipLoop() })
	return nil
}

func (s *server) handleCount(msg maelstrom.Message) error {
	var body struct {
		Visitors int64 `json:"visitors"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	s.mu.Lock()
	s.counters[s.node.ID()] += body.Visitors
	s.mu.Unlock()

	// Acknowledge only after the local counter has been updated.  Replication
	// happens asynchronously; after a partition heals the periodic gossip makes
	// every node's copy converge to the same value.
	if err := s.node.Reply(msg, maelstrom.MessageBody{Type: "counted"}); err != nil {
		return err
	}

	// Send an immediate update as well, so convergence does not have to wait
	// for the next gossip tick.
	go s.broadcast()
	return nil
}

func (s *server) handleTotal(msg maelstrom.Message) error {
	s.mu.Lock()
	var total int64
	for _, v := range s.counters {
		total += v
	}
	s.mu.Unlock()

	return s.node.Reply(msg, struct {
		Type          string `json:"type"`
		VisitorsSoFar int64  `json:"visitors_so_far"`
	}{Type: "total_is", VisitorsSoFar: total})
}

func (s *server) handleReplicate(msg maelstrom.Message) error {
	var body struct {
		Counters map[string]int64 `json:"counters"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	s.mu.Lock()
	for id, v := range body.Counters {
		if v > s.counters[id] {
			s.counters[id] = v
		}
	}
	s.mu.Unlock()
	return nil
}

func (s *server) gossipLoop() {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for range ticker.C {
		s.broadcast()
	}
}

func (s *server) broadcast() {
	s.mu.Lock()
	snapshot := make(map[string]int64, len(s.counters))
	for id, v := range s.counters {
		snapshot[id] = v
	}
	s.mu.Unlock()

	body := struct {
		Type     string           `json:"type"`
		Counters map[string]int64 `json:"counters"`
	}{Type: "replicate", Counters: snapshot}

	for _, id := range s.node.NodeIDs() {
		if id == s.node.ID() {
			continue
		}
		// If a link is partitioned this send is silently dropped by Maelstrom.
		_ = s.node.Send(id, body)
	}
}
