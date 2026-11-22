package main

import (
	"encoding/json"
	"log"
	"math/rand"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// node keeps a set of all values it has heard about, and gossips new values to
// every other node in the cluster.
type server struct {
	mu       sync.Mutex
	n        *maelstrom.Node
	messages map[int]struct{}
}

func main() {
	s := &server{
		n:        maelstrom.NewNode(),
		messages: make(map[int]struct{}),
	}

	s.n.Handle("broadcast", s.handleBroadcast)
	s.n.Handle("gossip", s.handleGossip)
	s.n.Handle("read", s.handleRead)
	s.n.Handle("topology", s.handleTopology)

	// Anti-entropy: periodically push the full set of known values to a
	// random peer so that anything missed is eventually picked up.
	go func() {
		for {
			time.Sleep(500 * time.Millisecond)
			s.antiEntropy()
		}
	}()

	if err := s.n.Run(); err != nil {
		log.Fatal(err)
	}
}

// add inserts v into the set, reporting whether it was new.
func (s *server) add(v int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.messages[v]; ok {
		return false
	}
	s.messages[v] = struct{}{}
	return true
}

func (s *server) snapshot() []int {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]int, 0, len(s.messages))
	for v := range s.messages {
		out = append(out, v)
	}
	return out
}

// broadcastToPeers sends all values in vals to every node except skip.
func (s *server) broadcastToPeers(vals []int, skip string) {
	if len(vals) == 0 {
		return
	}
	for _, id := range s.n.NodeIDs() {
		if id == s.n.ID() || id == skip {
			continue
		}
		if err := s.n.Send(id, map[string]any{
			"type":     "gossip",
			"messages": vals,
		}); err != nil {
			log.Printf("send to %s: %s", id, err)
		}
	}
}

func (s *server) handleBroadcast(msg maelstrom.Message) error {
	var body struct {
		Message int `json:"message"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	if err := s.n.Reply(msg, map[string]any{"type": "broadcast_ok"}); err != nil {
		return err
	}

	if s.add(body.Message) {
		go s.broadcastToPeers([]int{body.Message}, "")
	}
	return nil
}

func (s *server) handleGossip(msg maelstrom.Message) error {
	var body struct {
		Messages []int `json:"messages"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	fresh := make([]int, 0, len(body.Messages))
	for _, v := range body.Messages {
		if s.add(v) {
			fresh = append(fresh, v)
		}
	}

	// Propagate only newly-learned values onwards, so the whole cluster
	// converges without flooding.
	if len(fresh) > 0 {
		go s.broadcastToPeers(fresh, msg.Src)
	}
	return nil
}

func (s *server) handleRead(msg maelstrom.Message) error {
	return s.n.Reply(msg, map[string]any{
		"type":     "read_ok",
		"messages": s.snapshot(),
	})
}

func (s *server) handleTopology(msg maelstrom.Message) error {
	// Ignore the suggested topology: a full mesh is simple and reliable.
	return s.n.Reply(msg, map[string]any{"type": "topology_ok"})
}

func (s *server) antiEntropy() {
	ids := s.n.NodeIDs()
	if len(ids) < 2 {
		return
	}
	peers := make([]string, 0, len(ids)-1)
	for _, id := range ids {
		if id != s.n.ID() {
			peers = append(peers, id)
		}
	}
	if len(peers) == 0 {
		return
	}
	dest := peers[rand.Intn(len(peers))]
	_ = s.n.Send(dest, map[string]any{
		"type":     "gossip",
		"messages": s.snapshot(),
	})
}
