// Broadcast with eventual delivery under network partitions.
//
// Strategy: every node keeps the full set of values it has seen. Values are
// propagated with periodic anti-entropy gossip: for each peer, a node tracks
// which values that peer is known to already have (from acked gossip rounds,
// or from values received in gossip originating at that peer). Anything not
// yet confirmed at a peer is sent to it, and re-sent after a timeout if no
// ack comes back. Since messages lost during a partition produce no ack, they
// are retried once the partition heals, so every acknowledged broadcast
// eventually reaches every node.
package main

import (
	"encoding/json"
	"io"
	"log"
	"os"
	"sort"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const (
	// gossipInterval is how often each peer's outstanding set is examined.
	gossipInterval = 300 * time.Millisecond
	// retryAfter is how long we wait for a gossip ack before re-sending.
	retryAfter = 2 * gossipInterval
)

type peerState struct {
	// confirmed holds values the peer is known to have.
	confirmed map[int]struct{}
	// inFlight holds values we've sent and are waiting to be acked.
	inFlight map[int]struct{}
	sentAt   time.Time
}

type server struct {
	n *maelstrom.Node

	mu sync.Mutex
	// values is the set of all messages this node knows about.
	values map[int]struct{}
	// peers tracks per-peer gossip state.
	peerStates map[string]*peerState

	// notify wakes the gossip loop early when a new value shows up.
	notify chan struct{}
}

func main() {
	// Keep stderr quiet unless debugging: the library logs every message.
	if os.Getenv("MAELSTROM_VERBOSE") != "" {
		log.SetOutput(os.Stderr)
	} else {
		log.SetOutput(io.Discard)
	}

	n := maelstrom.NewNode()
	s := &server{
		n:          n,
		values:     make(map[int]struct{}),
		peerStates: make(map[string]*peerState),
		notify:     make(chan struct{}, 1),
	}

	n.Handle("broadcast", s.handleBroadcast)
	n.Handle("read", s.handleRead)
	n.Handle("topology", s.handleTopology)
	n.Handle("gossip", s.handleGossip)

	go s.gossipLoop()

	if err := n.Run(); err != nil {
		log.Fatalf("run: %v", err)
	}
}

// stateFor returns (creating if needed) the gossip state for a peer.
func (s *server) stateFor(peer string) *peerState {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, ok := s.peerStates[peer]
	if !ok {
		st = &peerState{
			confirmed: make(map[int]struct{}),
			inFlight:  make(map[int]struct{}),
		}
		s.peerStates[peer] = st
	}
	return st
}

// add records a value, returning true if it was new.
func (s *server) add(v int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.values[v]; ok {
		return false
	}
	s.values[v] = struct{}{}
	return true
}

// all returns the values known locally, sorted ascending.
func (s *server) all() []int {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]int, 0, len(s.values))
	for v := range s.values {
		out = append(out, v)
	}
	sort.Ints(out)
	return out
}

// peers returns the IDs of the other nodes in the cluster.
func (s *server) peers() []string {
	var out []string
	self := s.n.ID()
	for _, id := range s.n.NodeIDs() {
		if id != self {
			out = append(out, id)
		}
	}
	return out
}

// ---- client-facing handlers ----

func (s *server) handleBroadcast(msg maelstrom.Message) error {
	var body struct {
		Message int `json:"message"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	if s.add(body.Message) {
		s.wake()
	}
	return s.n.Reply(msg, map[string]any{"type": "broadcast_ok"})
}

func (s *server) handleRead(msg maelstrom.Message) error {
	return s.n.Reply(msg, map[string]any{
		"type":     "read_ok",
		"messages": s.all(),
	})
}

func (s *server) handleTopology(msg maelstrom.Message) error {
	// We gossip with every other node, so the suggested topology isn't needed
	// for correctness. Acknowledge it right away.
	return s.n.Reply(msg, map[string]any{"type": "topology_ok"})
}

// ---- node-to-node gossip ----

type gossipBody struct {
	Type     string `json:"type"`
	Messages []int  `json:"messages"`
}

func (s *server) handleGossip(msg maelstrom.Message) error {
	var body gossipBody
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	// Anything a peer sends us is something the peer already has.
	if isNode(msg.Src, s.n.NodeIDs()) {
		st := s.stateFor(msg.Src)
		s.mu.Lock()
		for _, v := range body.Messages {
			st.confirmed[v] = struct{}{}
		}
		s.mu.Unlock()
	}

	newVal := false
	for _, v := range body.Messages {
		if s.add(v) {
			newVal = true
		}
	}
	if newVal {
		s.wake()
	}
	return s.n.Reply(msg, map[string]any{"type": "gossip_ok"})
}

// gossipLoop periodically pushes outstanding values to every peer until they
// are all confirmed.
func (s *server) gossipLoop() {
	ticker := time.NewTicker(gossipInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.notify:
		case <-ticker.C:
		}

		if len(s.n.NodeIDs()) == 0 {
			continue // not initialized yet
		}

		for _, peer := range s.peers() {
			s.gossipRound(peer)
		}
	}
}

// gossipRound sends one batch of outstanding values to a peer.
func (s *server) gossipRound(peer string) {
	s.mu.Lock()
	st, ok := s.peerStates[peer]
	if !ok {
		st = &peerState{
			confirmed: make(map[int]struct{}),
			inFlight:  make(map[int]struct{}),
		}
		s.peerStates[peer] = st
	}
	if len(st.inFlight) > 0 {
		if time.Since(st.sentAt) < retryAfter {
			// Still waiting on an ack for the previous batch.
			s.mu.Unlock()
			return
		}
		// Timed out: assume the batch was lost and retry it.
		st.inFlight = make(map[int]struct{})
	}
	vals := make([]int, 0)
	for v := range s.values {
		if _, have := st.confirmed[v]; have {
			continue
		}
		if _, sent := st.inFlight[v]; sent {
			continue
		}
		vals = append(vals, v)
		st.inFlight[v] = struct{}{}
	}
	st.sentAt = time.Now()
	s.mu.Unlock()

	if len(vals) == 0 {
		return
	}

	sort.Ints(vals)
	body := gossipBody{Type: "gossip", Messages: vals}
	if err := s.n.RPC(peer, body, func(reply maelstrom.Message) error {
		s.confirm(peer, vals)
		return nil
	}); err != nil {
		s.forgetInFlight(peer, vals)
	}
}

// confirm marks values as safely received by a peer.
func (s *server) confirm(peer string, vals []int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, ok := s.peerStates[peer]
	if !ok {
		return
	}
	for _, v := range vals {
		st.confirmed[v] = struct{}{}
		delete(st.inFlight, v)
	}
}

// forgetInFlight puts values back into the pending set for a peer.
func (s *server) forgetInFlight(peer string, vals []int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if st, ok := s.peerStates[peer]; ok {
		for _, v := range vals {
			delete(st.inFlight, v)
		}
	}
}

// wake nudges the gossip loop to run a round soon.
func (s *server) wake() {
	select {
	case s.notify <- struct{}{}:
	default:
	}
}

func isNode(id string, nodeIDs []string) bool {
	for _, n := range nodeIDs {
		if n == id {
			return true
		}
	}
	return false
}
