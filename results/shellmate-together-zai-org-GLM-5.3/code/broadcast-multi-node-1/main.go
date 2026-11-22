// Maelstrom "broadcast" workload node.
//
// Protocol: eager gossip with acknowledgements plus periodic anti-entropy.
//   - Every value a node learns about (from a client or from a peer) is stored
//     and queued for every other node.
//   - A background goroutine flushes queued values to peers as "gossip"
//     messages; peers acknowledge with "gossip_ok", which clears the queue.
//     Unacknowledged values are retransmitted, so lost messages or restarted
//     peers eventually catch up.
//   - Every couple of seconds each node re-offers its whole set to every peer
//     (anti-entropy), so a node that crashes and restarts with an empty set is
//     repopulated, and nothing can be permanently lost.
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
	// flushInterval is how often a peer's queue is offered even if nothing
	// new showed up, so no value can sit unnoticed.
	flushInterval = 50 * time.Millisecond
	// resyncInterval is how often the whole set is re-offered to every peer:
	// a safety net against dropped messages and restarted peers.
	resyncInterval = 2 * time.Second
	// resendDelay is the minimum gap between two gossip messages to the same
	// peer: small enough to keep propagation fast, large enough to let an
	// acknowledgement arrive before we consider retransmitting.
	resendDelay = 5 * time.Millisecond
	// maxBatch caps how many values fit in one gossip message, keeping every
	// line comfortably below stdin's line-size limit.
	maxBatch = 500
)

// peerState tracks which values we still owe a given peer.
type peerState struct {
	pending  map[int]struct{}
	lastSent time.Time
}

type server struct {
	n      *maelstrom.Node
	mu     sync.Mutex
	values map[int]struct{}
	peers  map[string]*peerState
	wake   chan struct{}
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	n := maelstrom.NewNode()
	s := &server{
		n:      n,
		values: make(map[int]struct{}),
		peers:  make(map[string]*peerState),
		wake:   make(chan struct{}, 1),
	}

	n.Handle("broadcast", s.handleBroadcast)
	n.Handle("read", s.handleRead)
	n.Handle("topology", s.handleTopology)
	n.Handle("gossip", s.handleGossip)

	go s.run()

	if err := n.Run(); err != nil {
		log.Fatalf("node %s: %v", n.ID(), err)
	}
}

// ensurePeersLocked makes sure we have state for every other node. Called with
// s.mu held. The topology is only a suggestion; with a five node cluster we
// simply gossip to everyone, which is trivially connected.
func (s *server) ensurePeersLocked() {
	for _, id := range s.n.NodeIDs() {
		if id == s.n.ID() {
			continue
		}
		if _, ok := s.peers[id]; !ok {
			s.peers[id] = &peerState{pending: make(map[int]struct{})}
		}
	}
}

// addValueLocked stores v if new, returning true when it was new.
func (s *server) addValueLocked(v int) bool {
	if _, ok := s.values[v]; ok {
		return false
	}
	s.values[v] = struct{}{}
	return true
}

// queueLocked schedules vals for delivery to every peer except "from".
func (s *server) queueLocked(vals []int, from string) {
	if len(vals) == 0 {
		return
	}
	for id, p := range s.peers {
		if id == from {
			continue
		}
		for _, v := range vals {
			p.pending[v] = struct{}{}
		}
	}
}

// wake nudges the sender goroutine without blocking.
func (s *server) wakeSender() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

func (s *server) handleBroadcast(msg maelstrom.Message) error {
	var body struct {
		Message int `json:"message"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	s.mu.Lock()
	s.ensurePeersLocked()
	fresh := s.addValueLocked(body.Message)
	if fresh {
		s.queueLocked([]int{body.Message}, "")
	}
	s.mu.Unlock()
	if fresh {
		s.wakeSender()
	}
	return s.n.Reply(msg, map[string]any{"type": "broadcast_ok"})
}

func (s *server) handleRead(msg maelstrom.Message) error {
	s.mu.Lock()
	vals := make([]int, 0, len(s.values))
	for v := range s.values {
		vals = append(vals, v)
	}
	s.mu.Unlock()
	sort.Ints(vals)
	return s.n.Reply(msg, map[string]any{"type": "read_ok", "messages": vals})
}

func (s *server) handleTopology(msg maelstrom.Message) error {
	// We accept the topology but keep talking to every node: the cluster has
	// five members, so a full mesh is cheap and always connected.
	var body struct {
		Topology map[string][]string `json:"topology"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	s.mu.Lock()
	s.ensurePeersLocked()
	s.mu.Unlock()
	return s.n.Reply(msg, map[string]any{"type": "topology_ok"})
}

// handleGossip merges the offered values, acknowledges them and, if any were
// new, passes them on to its own peers.
func (s *server) handleGossip(msg maelstrom.Message) error {
	var body struct {
		Messages []int `json:"messages"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	s.mu.Lock()
	s.ensurePeersLocked()
	var fresh []int
	for _, v := range body.Messages {
		if s.addValueLocked(v) {
			fresh = append(fresh, v)
		}
	}
	if len(fresh) > 0 {
		s.queueLocked(fresh, msg.Src)
	}
	s.mu.Unlock()

	// Acknowledge (drops the values from the sender's retry queue).
	if err := s.n.Reply(msg, map[string]any{"type": "gossip_ok"}); err != nil {
		return err
	}
	if len(fresh) > 0 {
		s.wakeSender()
	}
	return nil
}

// run is the sender loop: it pushes queued values out promptly and, now and
// then, re-offers everything it knows as a safety net.
func (s *server) run() {
	flush := time.NewTicker(flushInterval)
	defer flush.Stop()
	resync := time.NewTicker(resyncInterval)
	defer resync.Stop()

	for {
		select {
		case <-s.wake:
		case <-flush.C:
		case <-resync.C:
			s.mu.Lock()
			for _, p := range s.peers {
				for v := range s.values {
					p.pending[v] = struct{}{}
				}
			}
			s.mu.Unlock()
		}
		s.flushPending()
	}
}

// flushPending sends each peer the values it is still missing, throttled so a
// peer is not spammed while an acknowledgement is in flight.
func (s *server) flushPending() {
	type batch struct {
		dest string
		vals []int
	}
	now := time.Now()
	var batches []batch

	s.mu.Lock()
	for dest, p := range s.peers {
		if len(p.pending) == 0 || now.Sub(p.lastSent) < resendDelay {
			continue
		}
		vals := make([]int, 0, len(p.pending))
		for v := range p.pending {
			vals = append(vals, v)
		}
		sort.Ints(vals)
		p.lastSent = now
		for start := 0; start < len(vals); start += maxBatch {
			end := start + maxBatch
			if end > len(vals) {
				end = len(vals)
			}
			batches = append(batches, batch{dest: dest, vals: vals[start:end]})
		}
	}
	s.mu.Unlock()

	for _, b := range batches {
		s.sendBatch(b.dest, b.vals)
	}
}

// sendBatch offers vals to one peer and clears them from the queue once the
// peer acknowledges. If the acknowledgement never shows up the values stay
// queued and are sent again later.
func (s *server) sendBatch(dest string, vals []int) {
	if len(vals) == 0 {
		return
	}
	body := map[string]any{"type": "gossip", "messages": vals}
	err := s.n.RPC(dest, body, func(m maelstrom.Message) error {
		s.mu.Lock()
		if p, ok := s.peers[dest]; ok {
			for _, v := range vals {
				delete(p.pending, v)
			}
		}
		s.mu.Unlock()
		return nil
	})
	if err != nil {
		// Nothing fatal: the values remain queued and are retried.
		log.Printf("gossip to %s failed: %v", dest, err)
	}
}
