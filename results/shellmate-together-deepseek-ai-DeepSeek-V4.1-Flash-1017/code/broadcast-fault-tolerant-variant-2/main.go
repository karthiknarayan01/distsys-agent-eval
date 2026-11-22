package main

import (
	"encoding/json"
	"io"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// server holds the gossip state of one village member.
type server struct {
	node *maelstrom.Node

	mu     sync.Mutex
	rumors map[int]struct{}
	hash   uint64 // order-independent hash of the rumor set
	peers  []string
	begun  bool
}

func splitmix64(x uint64) uint64 {
	x += 0x9e3779b97f4a7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}

// addAll inserts rumors and returns the subset that was actually new.
func (s *server) addAll(ids []int) []int {
	var added []int
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, id := range ids {
		if _, ok := s.rumors[id]; ok {
			continue
		}
		s.rumors[id] = struct{}{}
		s.hash ^= splitmix64(uint64(int64(id)))
		added = append(added, id)
	}
	return added
}

func (s *server) snapshot() (uint64, int, []int, []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := make([]int, 0, len(s.rumors))
	for id := range s.rumors {
		ids = append(ids, id)
	}
	peers := make([]string, len(s.peers))
	copy(peers, s.peers)
	return s.hash, len(s.rumors), ids, peers
}

// push sends a batch of rumors to every peer except the given one.
func (s *server) push(ids []int, except string) {
	if len(ids) == 0 {
		return
	}
	s.mu.Lock()
	peers := make([]string, len(s.peers))
	copy(peers, s.peers)
	s.mu.Unlock()
	for _, p := range peers {
		if p == except {
			continue
		}
		if err := s.node.Send(p, map[string]any{"type": "rumor_push", "rumors": ids}); err != nil {
			log.Printf("push to %s: %v", p, err)
		}
	}
}

// gossip periodically exchanges a compact digest with every peer so that
// rumors lost to a network partition are eventually retransmitted.
func (s *server) gossip() {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for range ticker.C {
		h, count, _, peers := s.snapshot()
		for _, p := range peers {
			if err := s.node.Send(p, map[string]any{
				"type":  "rumor_digest",
				"hash":  h,
				"count": count,
			}); err != nil {
				log.Printf("digest to %s: %v", p, err)
			}
		}
	}
}

func (s *server) handleInit(msg maelstrom.Message) error {
	s.mu.Lock()
	self := s.node.ID()
	s.peers = s.peers[:0]
	for _, id := range s.node.NodeIDs() {
		if id != self {
			s.peers = append(s.peers, id)
		}
	}
	first := !s.begun
	s.begun = true
	s.mu.Unlock()
	if first {
		go s.gossip()
	}
	return nil
}

func (s *server) handleNeighbors(msg maelstrom.Message) error {
	return s.node.Reply(msg, map[string]any{"type": "neighbors_noted"})
}

func (s *server) handleTell(msg maelstrom.Message) error {
	var body struct {
		Rumor int `json:"rumor"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	s.addAll([]int{body.Rumor})
	if err := s.node.Reply(msg, map[string]any{"type": "told"}); err != nil {
		return err
	}
	s.push([]int{body.Rumor}, "")
	return nil
}

func (s *server) handleRecall(msg maelstrom.Message) error {
	_, _, ids, _ := s.snapshot()
	if ids == nil {
		ids = []int{}
	}
	return s.node.Reply(msg, map[string]any{"type": "recalled", "rumors": ids})
}

func (s *server) handlePush(msg maelstrom.Message) error {
	var body struct {
		Rumors []int `json:"rumors"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	added := s.addAll(body.Rumors)
	if len(added) > 0 {
		s.push(added, msg.Src)
	}
	return nil
}

func (s *server) handleDigest(msg maelstrom.Message) error {
	var body struct {
		Hash  uint64 `json:"hash"`
		Count int    `json:"count"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	h, count, ids, _ := s.snapshot()
	if body.Hash == h && body.Count == count {
		return nil
	}
	if ids == nil {
		ids = []int{}
	}
	return s.node.Send(msg.Src, map[string]any{"type": "rumor_have", "rumors": ids})
}

func (s *server) handleHave(msg maelstrom.Message) error {
	var body struct {
		Rumors []int `json:"rumors"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	theirs := make(map[int]struct{}, len(body.Rumors))
	for _, id := range body.Rumors {
		theirs[id] = struct{}{}
	}
	_, _, ids, _ := s.snapshot()
	ours := make(map[int]struct{}, len(ids))
	var missingForPeer []int
	for _, id := range ids {
		ours[id] = struct{}{}
		if _, ok := theirs[id]; !ok {
			missingForPeer = append(missingForPeer, id)
		}
	}
	var missingForUs bool
	for _, id := range body.Rumors {
		if _, ok := ours[id]; !ok {
			missingForUs = true
			break
		}
	}
	if len(missingForPeer) > 0 {
		if err := s.node.Send(msg.Src, map[string]any{"type": "rumor_push", "rumors": missingForPeer}); err != nil {
			return err
		}
	}
	if missingForUs {
		// Ask them for their state as well; our answer to that request will
		// prompt them to push what we are missing.
		if err := s.node.Send(msg.Src, map[string]any{"type": "rumor_have", "rumors": ids}); err != nil {
			return err
		}
	}
	return nil
}

func main() {
	log.SetOutput(io.Discard)

	n := maelstrom.NewNode()
	s := &server{node: n, rumors: make(map[int]struct{})}

	n.Handle("init", s.handleInit)
	n.Handle("neighbors", s.handleNeighbors)
	n.Handle("tell", s.handleTell)
	n.Handle("recall", s.handleRecall)
	n.Handle("rumor_push", s.handlePush)
	n.Handle("rumor_digest", s.handleDigest)
	n.Handle("rumor_have", s.handleHave)

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
