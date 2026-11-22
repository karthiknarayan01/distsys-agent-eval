package main

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type server struct {
	node *maelstrom.Node

	mu     sync.Mutex
	rumors map[int]struct{}
	unsent []int
	peers  []string
}

func (s *server) addRumor(r int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.rumors[r]; ok {
		return
	}
	s.rumors[r] = struct{}{}
	s.unsent = append(s.unsent, r)
}

func (s *server) merge(rs []int) {
	s.addRumorSlice(rs)
}

func (s *server) addRumorSlice(rs []int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range rs {
		if _, ok := s.rumors[r]; ok {
			continue
		}
		s.rumors[r] = struct{}{}
		s.unsent = append(s.unsent, r)
	}
}

func (s *server) takeUnsent() []int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.unsent) == 0 {
		return nil
	}
	out := s.unsent
	s.unsent = nil
	return out
}

func (s *server) snapshot() []int {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]int, 0, len(s.rumors))
	for r := range s.rumors {
		out = append(out, r)
	}
	return out
}

func (s *server) sendAll(rumors []int) {
	if len(rumors) == 0 {
		return
	}
	for _, p := range s.peers {
		if p == s.node.ID() {
			continue
		}
		body := map[string]any{"type": "gossip", "rumors": rumors}
		if err := s.node.Send(p, body); err != nil {
			log.Printf("send to %s: %s", p, err)
		}
	}
}

// gossipLoop pushes new rumors eagerly and re-sends the full rumor set
// periodically so that anything lost to a partition is eventually delivered.
func (s *server) gossipLoop() {
	deltaTicker := time.NewTicker(120 * time.Millisecond)
	fullTicker := time.NewTicker(700 * time.Millisecond)
	for {
		select {
		case <-deltaTicker.C:
			if batch := s.takeUnsent(); batch != nil {
				s.sendAll(batch)
			}
		case <-fullTicker.C:
			s.sendAll(s.snapshot())
		}
	}
}

func main() {
	n := maelstrom.NewNode()
	s := &server{node: n, rumors: make(map[int]struct{})}

	n.Handle("init", func(msg maelstrom.Message) error {
		s.peers = n.NodeIDs()
		go s.gossipLoop()
		return nil
	})

	n.Handle("tell", func(msg maelstrom.Message) error {
		var body struct {
			Rumor *int `json:"rumor"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		if body.Rumor != nil {
			s.addRumor(*body.Rumor)
		}
		return n.Reply(msg, map[string]any{"type": "told"})
	})

	n.Handle("recall", func(msg maelstrom.Message) error {
		return n.Reply(msg, map[string]any{
			"type":   "recalled",
			"rumors": s.snapshot(),
		})
	})

	n.Handle("neighbors", func(msg maelstrom.Message) error {
		var body struct {
			Neighbors map[string][]string `json:"neighbors"`
		}
		if err := json.Unmarshal(msg.Body, &body); err == nil && len(body.Neighbors) > 0 {
			// Remember the suggested topology; we gossip to everyone, but keep
			// track of it so we have the full membership even without init.
			seen := make(map[string]struct{})
			for _, v := range body.Neighbors {
				for _, id := range v {
					seen[id] = struct{}{}
				}
			}
			for id := range body.Neighbors {
				seen[id] = struct{}{}
			}
			s.mu.Lock()
			if len(seen) >= len(s.peers) {
				peers := make([]string, 0, len(seen))
				for id := range seen {
					peers = append(peers, id)
				}
				s.peers = peers
			}
			s.mu.Unlock()
		}
		return n.Reply(msg, map[string]any{"type": "neighbors_noted"})
	})

	n.Handle("gossip", func(msg maelstrom.Message) error {
		var body struct {
			Rumors []int `json:"rumors"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		s.addRumorSlice(body.Rumors)
		return nil
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
