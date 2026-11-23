// Gossip-based broadcast that survives network partitions.
//
// Every node keeps the full set of values it has seen. For each peer it also
// keeps the set of values that peer has confirmed it knows (a value is only
// marked confirmed when the peer replies to a gossip message carrying it).
// A goroutine per peer repeatedly sends everything the peer has not yet
// confirmed and retries until the ack arrives. Retrying forever is what makes
// the system converge once a partition heals; sending only unconfirmed values
// keeps the volume of messages small.
package main

import (
	"encoding/json"
	"log"
	"sort"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const (
	// How long we wait for a gossip ack before assuming the message was lost.
	rpcTimeout = 500 * time.Millisecond
	// Poll interval when there is nothing outstanding for a peer.
	idlePoll = 50 * time.Millisecond
	// Pause after a failed gossip attempt before trying again.
	retryDelay = 100 * time.Millisecond
)

type server struct {
	n *maelstrom.Node

	mu    sync.Mutex
	vals  map[int]struct{}            // all values we know
	known map[string]map[int]struct{} // peer -> values that peer has acked
	peers []string

	wake chan struct{}
}

func newServer(n *maelstrom.Node) *server {
	return &server{
		n:     n,
		vals:  make(map[int]struct{}),
		known: make(map[string]map[int]struct{}),
		wake:  make(chan struct{}, 16),
	}
}

func main() {
	log.SetFlags(log.Ltime | log.Lmicroseconds)

	n := maelstrom.NewNode()
	s := newServer(n)

	n.Handle("init", s.onInit)
	n.Handle("broadcast", s.onBroadcast)
	n.Handle("read", s.onRead)
	n.Handle("topology", s.onTopology)
	n.Handle("gossip", s.onGossip)

	if err := n.Run(); err != nil {
		log.Fatalf("node %s exited: %v", n.ID(), err)
	}
}

// ---- helpers -------------------------------------------------------------

func toInt(v any) (int, bool) {
	switch x := v.(type) {
	case float64:
		return int(x), true
	case int:
		return x, true
	case int64:
		return int(x), true
	case json.Number:
		if i, err := x.Int64(); err == nil {
			return int(i), true
		}
	}
	return 0, false
}

func parseBody(msg maelstrom.Message) (map[string]any, error) {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return nil, err
	}
	return body, nil
}

// addValue stores v and reports whether it was new.
func (s *server) addValue(v int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.vals[v]; ok {
		return false
	}
	s.vals[v] = struct{}{}
	return true
}

func (s *server) values() []int {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]int, 0, len(s.vals))
	for v := range s.vals {
		out = append(out, v)
	}
	sort.Ints(out)
	return out
}

// pendingFor returns the values the peer has not confirmed yet.
func (s *server) pendingFor(peer string) []int {
	s.mu.Lock()
	defer s.mu.Unlock()
	acked := s.known[peer]
	out := make([]int, 0, len(s.vals))
	for v := range s.vals {
		if _, ok := acked[v]; !ok {
			out = append(out, v)
		}
	}
	sort.Ints(out)
	return out
}

// markKnown records that a peer has acked the given batch.
func (s *server) markKnown(peer string, batch []int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, ok := s.known[peer]
	if !ok {
		m = make(map[int]struct{})
		s.known[peer] = m
	}
	for _, v := range batch {
		m[v] = struct{}{}
	}
}

func (s *server) poke() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

func (s *server) wait() {
	select {
	case <-s.wake:
	case <-time.After(idlePoll):
	}
}

// ---- handlers -----------------------------------------------------------

// onInit fires once the node knows its own ID and the cluster membership; it
// starts the gossip loops. We gossip with every other node rather than only
// the suggested topology neighbours: more links mean more ways around a
// partition, and with five nodes the extra traffic is negligible.
func (s *server) onInit(msg maelstrom.Message) error {
	self := s.n.ID()
	s.mu.Lock()
	for _, id := range s.n.NodeIDs() {
		if id == self {
			continue
		}
		s.peers = append(s.peers, id)
		s.known[id] = make(map[int]struct{})
	}
	peers := append([]string(nil), s.peers...)
	s.mu.Unlock()

	for _, p := range peers {
		go s.peerLoop(p)
	}
	return nil
}

func (s *server) onBroadcast(msg maelstrom.Message) error {
	body, err := parseBody(msg)
	if err != nil {
		return err
	}
	if v, ok := toInt(body["message"]); ok && s.addValue(v) {
		s.poke()
	}
	return s.n.Reply(msg, map[string]any{"type": "broadcast_ok"})
}

func (s *server) onRead(msg maelstrom.Message) error {
	return s.n.Reply(msg, map[string]any{"type": "read_ok", "messages": s.values()})
}

func (s *server) onTopology(msg maelstrom.Message) error {
	// The topology is only a hint; we talk to everyone anyway.
	return s.n.Reply(msg, map[string]any{"type": "topology_ok"})
}

// onGossip receives a batch of values from a peer, stores them, and acks. The
// ack tells the sender that we now hold the batch, which is safe because we
// have already written it to our set by the time we reply.
func (s *server) onGossip(msg maelstrom.Message) error {
	body, err := parseBody(msg)
	if err != nil {
		return err
	}
	fresh := 0
	if raw, ok := body["messages"].([]any); ok {
		for _, m := range raw {
			if v, ok := toInt(m); ok && s.addValue(v) {
				fresh++
			}
		}
	}
	if err := s.n.Reply(msg, map[string]any{"type": "gossip_ok"}); err != nil {
		return err
	}
	if fresh > 0 {
		// We learned something new; let our own peers know about it soon.
		s.poke()
	}
	return nil
}

// ---- gossip -------------------------------------------------------------

// peerLoop keeps one peer up to date. It never gives up on a batch: while a
// partition stands every attempt times out, and once the partition heals the
// next attempt gets through.
func (s *server) peerLoop(peer string) {
	for {
		batch := s.pendingFor(peer)
		if len(batch) == 0 {
			s.wait()
			continue
		}
		if s.gossipTo(peer, batch) {
			s.markKnown(peer, batch)
		} else {
			time.Sleep(retryDelay)
		}
	}
}

// gossipTo sends one batch and waits for its ack.
func (s *server) gossipTo(peer string, batch []int) bool {
	done := make(chan struct{}, 1) // buffered so a late ack can't leak a goroutine
	err := s.n.RPC(peer, map[string]any{
		"type":     "gossip",
		"messages": batch,
	}, func(m maelstrom.Message) error {
		done <- struct{}{}
		return nil
	})
	if err != nil {
		log.Printf("gossip to %s failed to send: %v", peer, err)
		return false
	}
	select {
	case <-done:
		return true
	case <-time.After(rpcTimeout):
		return false
	}
}
