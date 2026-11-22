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

const (
	// Every node periodically performs an anti-entropy exchange with the next
	// node in the ring.  The interval is deliberately short so that a rumor
	// which was created during a partition is still delivered after the
	// partition heals.
	syncInterval = 100 * time.Millisecond
)

type tellBody struct {
	Rumor int64 `json:"rumor"`
}

type rumorBody struct {
	Rumor int64 `json:"item"`
}

type syncBody struct {
	Rumors    []int64 `json:"known"`
	WantReply bool    `json:"want_reply"`
}

type nodeState struct {
	mu     sync.RWMutex
	rumors map[int64]struct{}

	peerMu sync.Mutex
	peers  []string
	next   int

	startOnce sync.Once
}

func newNodeState() *nodeState {
	return &nodeState{rumors: make(map[int64]struct{})}
}

func (s *nodeState) add(r int64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.rumors[r]; exists {
		return false
	}
	s.rumors[r] = struct{}{}
	return true
}

func (s *nodeState) addAll(values []int64) bool {
	changed := false
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range values {
		if _, exists := s.rumors[r]; !exists {
			s.rumors[r] = struct{}{}
			changed = true
		}
	}
	return changed
}

func (s *nodeState) snapshot() []int64 {
	s.mu.RLock()
	values := make([]int64, 0, len(s.rumors))
	for r := range s.rumors {
		values = append(values, r)
	}
	s.mu.RUnlock()
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	return values
}

func main() {
	log.SetFlags(0)
	log.SetPrefix("village: ")

	n := maelstrom.NewNode()
	state := newNodeState()

	// The library handles the init reply itself.  We use its init callback to
	// learn the cluster membership and start the anti-entropy loop.
	n.Handle("init", func(msg maelstrom.Message) error {
		state.peerMu.Lock()
		state.peers = append([]string(nil), n.NodeIDs()...)
		state.peerMu.Unlock()
		state.startOnce.Do(func() { go state.syncLoop(n) })
		return nil
	})

	// Accept a rumor from a client, remember it, and immediately push it to
	// every other node.  The acknowledgement is deliberately sent after the
	// rumor is durable in the local in-memory set.
	n.Handle("tell", func(msg maelstrom.Message) error {
		var body tellBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		state.add(body.Rumor)
		state.pushRumor(n, body.Rumor, "")
		return n.Reply(msg, map[string]any{"type": "told"})
	})

	// Return all rumors currently known by this node.
	n.Handle("recall", func(msg maelstrom.Message) error {
		return n.Reply(msg, map[string]any{
			"type":   "recalled",
			"rumors": state.snapshot(),
		})
	})

	// The suggested topology is optional.  We use a full-mesh anti-entropy
	// protocol instead, but must still acknowledge the request.
	n.Handle("neighbors", func(msg maelstrom.Message) error {
		return n.Reply(msg, map[string]any{"type": "neighbors_noted"})
	})

	// A one-rumor push message used by the epidemic part of the protocol.
	n.Handle("gossip_rumor", func(msg maelstrom.Message) error {
		var body rumorBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		if state.add(body.Rumor) {
			// Forward a newly learned rumor, but not back to the node from
			// which it arrived.
			state.pushRumor(n, body.Rumor, msg.Src)
		}
		return nil
	})

	// A full-state anti-entropy message.  Replies carry want_reply=false, so
	// they cannot cause a reply storm.
	n.Handle("gossip_sync", func(msg maelstrom.Message) error {
		var body syncBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		state.addAll(body.Rumors)
		if body.WantReply {
			return n.Send(msg.Src, map[string]any{
				"type":       "gossip_sync",
				"known":      state.snapshot(),
				"want_reply": false,
			})
		}
		return nil
	})

	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}
}

// pushRumor sends a newly learned rumor to every peer except skip.  Full mesh
// push makes normal convergence very fast; periodic state reconciliation is
// what repairs messages lost while the network was partitioned.
func (s *nodeState) pushRumor(n *maelstrom.Node, rumor int64, skip string) {
	s.peerMu.Lock()
	peers := append([]string(nil), s.peers...)
	s.peerMu.Unlock()

	for _, peer := range peers {
		if peer == n.ID() || peer == skip {
			continue
		}
		if err := n.Send(peer, map[string]any{
			"type": "gossip_rumor",
			"item": rumor,
		}); err != nil {
			log.Printf("push rumor %d to %s: %s", rumor, peer, err)
		}
	}
}

// syncLoop periodically exchanges the complete local state with one peer.
// Because every node advances around the same ring, a full state traverses
// the entire cluster even if some individual packets are lost.
func (s *nodeState) syncLoop(n *maelstrom.Node) {
	ticker := time.NewTicker(syncInterval)
	defer ticker.Stop()

	for range ticker.C {
		s.peerMu.Lock()
		if len(s.peers) < 2 {
			s.peerMu.Unlock()
			continue
		}
		peer := s.peers[s.next%len(s.peers)]
		s.next = (s.next + 1) % len(s.peers)
		s.peerMu.Unlock()

		if peer == n.ID() {
			continue
		}
		if err := n.Send(peer, map[string]any{
			"type":       "gossip_sync",
			"known":      s.snapshot(),
			"want_reply": true,
		}); err != nil {
			log.Printf("sync with %s: %s", peer, err)
		}
	}
}
