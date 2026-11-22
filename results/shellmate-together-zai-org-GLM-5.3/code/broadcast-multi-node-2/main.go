// Maelstrom broadcast workload node.
//
// Strategy: each node keeps a set of messages and gossips them to every other
// node directly (the cluster is small, so a full mesh beats routing through a
// suggested topology). Dissemination is delta-based with acknowledgements: a
// node re-sends a value to a peer until the peer acknowledges it, so
// partitions and restarts heal automatically.
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const (
	// How often we retry values a peer has not acknowledged yet.
	syncInterval = 200 * time.Millisecond
	// How often we re-send our entire set to a peer, ignoring what it has
	// acknowledged. Belt and braces for lost acknowledgements.
	fullSyncInterval = 5 * time.Second
)

type server struct {
	n  *maelstrom.Node
	mu sync.Mutex

	// The set of messages this node knows about.
	values map[int]bool
	// peer -> values the peer has acknowledged knowing.
	confirmed map[string]map[int]bool
	// Every other node in the cluster.
	peers   []string
	nodes   map[string]bool
	started bool
}

func main() {
	n := maelstrom.NewNode()
	s := &server{
		n:         n,
		values:    make(map[int]bool),
		confirmed: make(map[string]map[int]bool),
		nodes:     make(map[string]bool),
	}

	// init: remember the cluster membership and start gossiping.
	n.Handle("init", func(msg maelstrom.Message) error {
		s.mu.Lock()
		if s.started {
			s.mu.Unlock()
			return nil
		}
		s.started = true
		s.peers = s.peers[:0]
		s.nodes = make(map[string]bool)
		for _, id := range n.NodeIDs() {
			s.nodes[id] = true
			if id == n.ID() {
				continue
			}
			s.peers = append(s.peers, id)
			if s.confirmed[id] == nil {
				s.confirmed[id] = make(map[int]bool)
			}
		}
		s.mu.Unlock()

		go s.loop()

		// In case this is a fresh process (e.g. after a crash), pull the
		// current state from our peers.
		s.requestState()
		return nil
	})

	// broadcast: a client gives us a new value.
	n.Handle("broadcast", s.handleBroadcast)

	// read: a client wants everything we know.
	n.Handle("read", s.handleRead)

	// topology: acknowledged, but we prefer talking to everyone directly.
	n.Handle("topology", s.handleTopology)

	// gossip: another node tells us about values it has.
	n.Handle("gossip", s.handleGossip)

	// state_request: another node (usually a restarting one) wants our whole set.
	n.Handle("state_request", s.handleStateRequest)

	// state_ok / gossip_ok arrive as replies (in_reply_to != 0) and are
	// routed to RPC callbacks, but register handlers so an unexpected reply
	// never kills the process.
	n.Handle("gossip_ok", func(msg maelstrom.Message) error { return nil })
	n.Handle("state_ok", func(msg maelstrom.Message) error { return nil })

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

// handleBroadcast stores the value, tells the other nodes about it, and only
// then acknowledges the client, so an acknowledged value survives this node
// going away.
func (s *server) handleBroadcast(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	value, err := toInt(body["message"])
	if err != nil {
		return err
	}

	fresh := s.addValue(value)
	if fresh {
		s.gossipValue(value)
	}
	return s.n.Reply(msg, map[string]any{"type": "broadcast_ok"})
}

func (s *server) handleRead(msg maelstrom.Message) error {
	return s.n.Reply(msg, map[string]any{
		"type":     "read_ok",
		"messages": s.snapshot(),
	})
}

func (s *server) handleTopology(msg maelstrom.Message) error {
	// We use our own full-mesh topology, so there is nothing to store.
	return s.n.Reply(msg, map[string]any{"type": "topology_ok"})
}

func (s *server) handleGossip(msg maelstrom.Message) error {
	var body struct {
		Messages []int `json:"messages"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	var fresh []int
	s.mu.Lock()
	for _, v := range body.Messages {
		if !s.values[v] {
			s.values[v] = true
			fresh = append(fresh, v)
		}
		// The sender obviously has everything it told us about.
		s.markConfirmedLocked(msg.Src, v)
	}
	s.mu.Unlock()

	// Acknowledge, so the sender can stop re-sending this batch.
	if err := s.n.Reply(msg, map[string]any{"type": "gossip_ok"}); err != nil {
		return err
	}

	// Keep values flowing towards nodes the sender may not have reached.
	s.gossipValues(fresh, msg.Src)
	return nil
}

func (s *server) handleStateRequest(msg maelstrom.Message) error {
	return s.n.Reply(msg, map[string]any{
		"type":     "state_ok",
		"messages": s.snapshot(),
	})
}

// loop periodically re-sends anything our peers have not confirmed, and every
// so often our full set, regardless.
func (s *server) loop() {
	syncTicker := time.NewTicker(syncInterval)
	fullTicker := time.NewTicker(fullSyncInterval)
	defer syncTicker.Stop()
	defer fullTicker.Stop()

	for range syncTicker.C {
		s.syncPeers(false)
		select {
		case <-fullTicker.C:
			s.syncPeers(true)
		default:
		}
	}
}

// syncPeers sends every value that the given peer has not acknowledged. With
// full set, it ignores what has been acknowledged.
func (s *server) syncPeers(full bool) {
	type batch struct {
		peer string
		vals []int
	}

	s.mu.Lock()
	batches := make([]batch, 0, len(s.peers))
	for _, p := range s.peers {
		known := s.confirmed[p]
		vals := make([]int, 0, len(s.values))
		for v := range s.values {
			if full || !known[v] {
				vals = append(vals, v)
			}
		}
		if len(vals) > 0 {
			sort.Ints(vals)
			batches = append(batches, batch{p, vals})
		}
	}
	s.mu.Unlock()

	for _, b := range batches {
		s.gossipTo(b.peer, b.vals)
	}
}

// gossipValue pushes a single new value to every peer.
func (s *server) gossipValue(v int) {
	s.mu.Lock()
	peers := append([]string(nil), s.peers...)
	s.mu.Unlock()
	for _, p := range peers {
		s.gossipTo(p, []int{v})
	}
}

// gossipValues pushes a set of values to every peer except exclude.
func (s *server) gossipValues(vals []int, exclude string) {
	if len(vals) == 0 {
		return
	}
	s.mu.Lock()
	peers := make([]string, 0, len(s.peers))
	for _, p := range s.peers {
		if p != exclude {
			peers = append(peers, p)
		}
	}
	s.mu.Unlock()
	for _, p := range peers {
		valsCopy := append([]int(nil), vals...)
		s.gossipTo(p, valsCopy)
	}
}

// gossipTo sends a batch of values to one peer and records them as confirmed
// when the peer acknowledges.
func (s *server) gossipTo(peer string, vals []int) {
	if len(vals) == 0 || peer == s.n.ID() || !s.isClusterNode(peer) {
		return
	}
	err := s.n.RPC(peer, map[string]any{
		"type":     "gossip",
		"messages": vals,
	}, func(reply maelstrom.Message) error {
		if reply.RPCError() != nil {
			return nil // do not mark as confirmed; we'll retry
		}
		s.mu.Lock()
		for _, v := range vals {
			s.markConfirmedLocked(peer, v)
		}
		s.mu.Unlock()
		return nil
	})
	if err != nil {
		log.Printf("gossip to %s failed: %v", peer, err)
	}
}

// requestState asks every peer for its full set of values. Used at startup so a
// restarted node catches up immediately.
func (s *server) requestState() {
	s.mu.Lock()
	peers := append([]string(nil), s.peers...)
	s.mu.Unlock()
	for _, p := range peers {
		peer := p
		err := s.n.RPC(peer, map[string]any{"type": "state_request"}, func(reply maelstrom.Message) error {
			var body struct {
				Messages []int `json:"messages"`
			}
			if err := json.Unmarshal(reply.Body, &body); err != nil {
				return err
			}
			s.mu.Lock()
			for _, v := range body.Messages {
				s.values[v] = true
				s.markConfirmedLocked(peer, v)
			}
			s.mu.Unlock()
			return nil
		})
		if err != nil {
			log.Printf("state request to %s failed: %v", peer, err)
		}
	}
}

// toInt converts a JSON-decoded number into an int, tolerating either of the
// encodings the JSON decoder may produce.
func toInt(x any) (int, error) {
	switch v := x.(type) {
	case float64:
		return int(v), nil
	case int:
		return v, nil
	case json.Number:
		i, err := v.Int64()
		return int(i), err
	case string:
		return 0, fmt.Errorf("not a number: %q", v)
	default:
		return 0, fmt.Errorf("missing or invalid message: %#v", x)
	}
}

// addValue records a value, returning true if it was new.
func (s *server) addValue(v int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.values[v] {
		return false
	}
	s.values[v] = true
	return true
}

// snapshot returns all known values, sorted.
func (s *server) snapshot() []int {
	s.mu.Lock()
	vals := make([]int, 0, len(s.values))
	for v := range s.values {
		vals = append(vals, v)
	}
	s.mu.Unlock()
	sort.Ints(vals)
	return vals
}

func (s *server) isClusterNode(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.nodes[id]
}

// markConfirmedLocked notes that peer has acknowledged v. Caller holds s.mu.
func (s *server) markConfirmedLocked(peer string, v int) {
	known := s.confirmed[peer]
	if known == nil {
		if !s.nodes[peer] {
			return // not a cluster node; don't track it
		}
		known = make(map[int]bool)
		s.confirmed[peer] = known
	}
	known[v] = true
}
