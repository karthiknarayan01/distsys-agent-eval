package main

import (
	"encoding/json"
	"log"
	"sort"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// node is a gossip server: every rumor is an integer, and nodes periodically
// exchange everything they know. The protocol is deliberately simple and
// redundant, since the network may be partitioned arbitrarily and messages
// sent across a partition are silently dropped.
type node struct {
	n *maelstrom.Node

	mu     sync.Mutex
	rumors map[int]struct{}
	// sent tracks, per peer, which rumors we have already pushed to it. It is
	// only an optimisation: full-state anti-entropy syncs repair anything
	// that was lost (e.g. because of a partition).
	sent map[string]map[int]struct{}

	peers []string
	wake  chan struct{}
	once  sync.Once
}

const (
	tickInterval  = 100 * time.Millisecond
	fullSyncEvery = 5 // ticks between full state pushes
	chunkSize     = 512
)

func main() {
	srv := &node{
		n:      maelstrom.NewNode(),
		rumors: make(map[int]struct{}),
		sent:   make(map[string]map[int]struct{}),
		wake:   make(chan struct{}, 1),
	}

	srv.n.Handle("tell", srv.handleTell)
	srv.n.Handle("recall", srv.handleRecall)
	srv.n.Handle("neighbors", srv.handleNeighbors)
	srv.n.Handle("gossip", srv.handleGossip)
	srv.n.Handle("init", func(msg maelstrom.Message) error {
		srv.start()
		return nil
	})

	if err := srv.n.Run(); err != nil {
		log.Fatal(err)
	}
}

// start launches the background gossip loop once we know the cluster layout.
func (s *node) start() {
	s.once.Do(func() {
		for _, id := range s.n.NodeIDs() {
			if id != s.n.ID() {
				s.peers = append(s.peers, id)
			}
		}
		go s.gossipLoop()
	})
}

// add records a rumor and reports whether it was new.
func (s *node) add(v int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.rumors[v]; ok {
		return false
	}
	s.rumors[v] = struct{}{}
	return true
}

func (s *node) handleTell(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		log.Printf("bad tell message: %s", err)
		return s.n.Reply(msg, map[string]any{"type": "told"})
	}
	if v, ok := toInt(body["rumor"]); ok {
		if s.add(v) {
			select {
			case s.wake <- struct{}{}:
			default:
			}
		}
	}
	return s.n.Reply(msg, map[string]any{"type": "told"})
}

// toInt converts a JSON-decoded number to an int. Numbers arrive as float64 or
// json.Number depending on how the message was encoded.
func toInt(v any) (int, bool) {
	switch x := v.(type) {
	case float64:
		return int(x), true
	case json.Number:
		i, err := x.Int64()
		if err != nil {
			f, err := x.Float64()
			if err != nil {
				return 0, false
			}
			return int(f), true
		}
		return int(i), true
	case int:
		return x, true
	case int64:
		return int(x), true
	}
	return 0, false
}

func (s *node) handleRecall(msg maelstrom.Message) error {
	s.mu.Lock()
	vals := make([]int, 0, len(s.rumors))
	for v := range s.rumors {
		vals = append(vals, v)
	}
	s.mu.Unlock()
	sort.Ints(vals)
	return s.n.Reply(msg, map[string]any{
		"type":   "recalled",
		"rumors": vals,
	})
}

// handleNeighbors accepts the suggested topology but uses a full mesh of all
// nodes instead: it converges faster and is more resilient to partitions.
func (s *node) handleNeighbors(msg maelstrom.Message) error {
	s.start()
	return s.n.Reply(msg, map[string]any{"type": "neighbors_noted"})
}

func (s *node) handleGossip(msg maelstrom.Message) error {
	var body struct {
		Vals []any `json:"vals"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		log.Printf("bad gossip message: %s", err)
		return nil
	}
	fresh := false
	for _, raw := range body.Vals {
		if v, ok := toInt(raw); ok && s.add(v) {
			fresh = true
		}
	}
	if fresh {
		select {
		case s.wake <- struct{}{}:
		default:
		}
	}
	// Gossip is fire-and-forget: no ack needed, anti-entropy repairs losses.
	return nil
}

// gossipLoop pushes newly learned rumors to every peer as quickly as possible,
// and periodically re-sends the entire set so that anything dropped by a
// partition eventually makes it through after the heal.
func (s *node) gossipLoop() {
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()
	tick := 0
	for {
		select {
		case <-ticker.C:
		case <-s.wake:
		}
		tick++
		full := tick%fullSyncEvery == 0
		s.push(full)
	}
}

func (s *node) push(full bool) {
	s.mu.Lock()
	all := make([]int, 0, len(s.rumors))
	for v := range s.rumors {
		all = append(all, v)
	}
	sent := s.sent
	s.mu.Unlock()
	sort.Ints(all)

	peers := s.peers
	if len(peers) == 0 {
		return
	}

	for _, p := range peers {
		var out []int
		if full {
			out = all
		} else {
			known := sent[p]
			for _, v := range all {
				if _, ok := known[v]; !ok {
					out = append(out, v)
				}
			}
		}
		if len(out) == 0 {
			continue
		}
		for start := 0; start < len(out); start += chunkSize {
			end := start + chunkSize
			if end > len(out) {
				end = len(out)
			}
			chunk := out[start:end]
			s.n.Send(p, map[string]any{"type": "gossip", "vals": chunk})
		}
		if !full {
			s.mu.Lock()
			known := sent[p]
			if known == nil {
				known = make(map[int]struct{})
				sent[p] = known
			}
			for _, v := range out {
				known[v] = struct{}{}
			}
			s.mu.Unlock()
		} else {
			s.mu.Lock()
			known := sent[p]
			if known == nil {
				known = make(map[int]struct{})
				sent[p] = known
			}
			for _, v := range all {
				known[v] = struct{}{}
			}
			s.mu.Unlock()
		}
	}
}
