// Visitor tally for four museum turnstiles, implemented as a grow-only
// (G-)counter.
//
// Each turnstile keeps its own private count, which only ever grows. Nodes
// gossip their entire view of the cluster's counts to each other, merging
// per-node entries with max. Because entries only ever increase, merging is
// idempotent, no visitor is ever counted twice, and a node's total is
// monotonically non-decreasing.
//
// While the network is split, gossip messages crossing the split are dropped
// and totals may be stale; once the network heals, the gossip rounds (both a
// periodic ticker and a poke whenever state changes) make every node converge
// on the same total within a few tens of milliseconds.
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// tally is the last known count for every node in the cluster. Our own entry
// is incremented by `count` requests; other entries arrive via gossip.
type tally struct {
	mu     sync.Mutex
	counts map[string]int64
}

func newTally() *tally {
	return &tally{counts: make(map[string]int64)}
}

// add applies a delta to the local turnstile's count.
func (t *tally) add(nodeID string, delta int64) {
	t.mu.Lock()
	t.counts[nodeID] += delta
	t.mu.Unlock()
}

// total returns the sum of every node's known count.
func (t *tally) total() int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	var sum int64
	for _, v := range t.counts {
		sum += v
	}
	return sum
}

// snapshot copies the whole table, for gossiping.
func (t *tally) snapshot() map[string]int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	s := make(map[string]int64, len(t.counts))
	for k, v := range t.counts {
		s[k] = v
	}
	return s
}

// merge folds another node's table into ours, taking the max per entry.
// Reports whether we learned anything new.
func (t *tally) merge(other map[string]int64) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	learned := false
	for k, v := range other {
		if v > t.counts[k] {
			t.counts[k] = v
			learned = true
		}
	}
	return learned
}

type server struct {
	node  *maelstrom.Node
	tally *tally

	// ready is closed once the init message has told us our node ID.
	ready chan struct{}

	// notify wakes the gossip loop early; poke() never blocks.
	notify chan struct{}
}

func main() {
	n := maelstrom.NewNode()
	s := &server{
		node:   n,
		tally:  newTally(),
		ready:  make(chan struct{}),
		notify: make(chan struct{}, 1),
	}

	var readyOnce sync.Once
	n.Handle("init", func(maelstrom.Message) error {
		readyOnce.Do(func() { close(s.ready) })
		return nil
	})

	n.Handle("count", s.handleCount)
	n.Handle("total", s.handleTotal)

	// Inter-node gossip. A gossip message carries the sender's whole table;
	// it is fire-and-forget, so we never reply to it.
	n.Handle("tally", s.handleTally)
	// Registered for safety only: gossip is unacknowledged, but if some
	// message of this type ever showed up we must not choke on it.
	n.Handle("tally_ok", func(maelstrom.Message) error { return nil })

	// Broadcast our state on every change, plus periodically, forever.
	go s.gossip()

	if err := n.Run(); err != nil {
		log.Fatalf("node %s exiting: %v", n.ID(), err)
	}
}

// handleCount records visitors entering through this turnstile. The delta is
// applied before replying, so every acknowledged count is accounted for.
func (s *server) handleCount(msg maelstrom.Message) error {
	var body struct {
		Visitors int64 `json:"visitors"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	if body.Visitors < 0 {
		// The g-counter workload never sends these. Refuse the request
		// without applying it.
		return fmt.Errorf("tally only grows: refusing negative count %d", body.Visitors)
	}
	s.tally.add(s.node.ID(), body.Visitors)
	s.poke()
	return s.node.Reply(msg, map[string]any{"type": "counted"})
}

// handleTotal reports the tally so far, over all turnstiles.
func (s *server) handleTotal(msg maelstrom.Message) error {
	return s.node.Reply(msg, map[string]any{
		"type":            "total_is",
		"visitors_so_far": s.tally.total(),
	})
}

// handleTally merges a peer's table into ours.
func (s *server) handleTally(msg maelstrom.Message) error {
	var body struct {
		Counts map[string]int64 `json:"counts"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	if s.tally.merge(body.Counts) {
		// We learned something; pass it on promptly.
		s.poke()
	}
	return nil
}

// gossip is the background anti-entropy loop. It fires immediately whenever
// our state changes, and at least every few tens of milliseconds, so that a
// healed partition converges almost at once.
func (s *server) gossip() {
	// Don't talk to anyone before we know our own identity.
	<-s.ready

	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()

	last := time.Now()
	minInterval := 10 * time.Millisecond
	for {
		select {
		case <-tick.C:
		case <-s.notify:
			// Don't let a flood of counts turn into a flood of gossip.
			if time.Since(last) < minInterval {
				continue
			}
		}
		last = time.Now()
		s.broadcast()
	}
}

// broadcast sends our whole table to every other node.
func (s *server) broadcast() {
	counts := s.tally.snapshot()
	for _, peer := range s.node.NodeIDs() {
		if peer == s.node.ID() {
			continue
		}
		if err := s.node.Send(peer, map[string]any{"type": "tally", "counts": counts}); err != nil {
			log.Printf("gossip to %s failed: %v", peer, err)
		}
	}
}

// poke requests a gossip round soon.
func (s *server) poke() {
	select {
	case s.notify <- struct{}{}:
	default:
	}
}
