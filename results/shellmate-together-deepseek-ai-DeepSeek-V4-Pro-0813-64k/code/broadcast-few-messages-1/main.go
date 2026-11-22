package main

import (
	"encoding/json"
	"log"
	"sort"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const flushInterval = 100 * time.Millisecond

type server struct {
	n *maelstrom.Node

	mu        sync.Mutex
	values    map[int]struct{}
	pending   []int
	neighbors []string
	isRoot    bool
	ready     bool
}

func newServer() *server {
	return &server{
		n:      maelstrom.NewNode(),
		values: make(map[int]struct{}),
	}
}

// handleInit is invoked after the library has recorded our ID and the full
// node list. We arrange the cluster into a star: the lexicographically first
// node is the hub, everyone else is a spoke.
func (s *server) handleInit(msg maelstrom.Message) error {
	ids := append([]string(nil), s.n.NodeIDs()...)
	sort.Strings(ids)
	root := ids[0]

	s.mu.Lock()
	if s.n.ID() == root {
		s.isRoot = true
		for _, id := range ids {
			if id != root {
				s.neighbors = append(s.neighbors, id)
			}
		}
	} else {
		s.neighbors = []string{root}
	}
	s.ready = true
	s.mu.Unlock()
	return nil
}

func (s *server) handleBroadcast(msg maelstrom.Message) error {
	var body struct {
		Message int `json:"message"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	s.mu.Lock()
	if _, ok := s.values[body.Message]; !ok {
		s.values[body.Message] = struct{}{}
		s.pending = append(s.pending, body.Message)
	}
	s.mu.Unlock()

	return s.n.Reply(msg, map[string]any{"type": "broadcast_ok"})
}

func (s *server) handleRead(msg maelstrom.Message) error {
	s.mu.Lock()
	msgs := make([]int, 0, len(s.values))
	for v := range s.values {
		msgs = append(msgs, v)
	}
	s.mu.Unlock()

	return s.n.Reply(msg, map[string]any{"type": "read_ok", "messages": msgs})
}

func (s *server) handleTopology(msg maelstrom.Message) error {
	// We arrange the cluster ourselves, so the suggested topology is ignored.
	return s.n.Reply(msg, map[string]any{"type": "topology_ok"})
}

// handleBatch receives a batch of values from a neighbor. Spokes only ever
// receive batches from the hub, and must not echo them back; the hub receives
// batches from spokes and must fan them out to every other spoke.
func (s *server) handleBatch(msg maelstrom.Message) error {
	var body struct {
		Messages []int `json:"messages"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	s.mu.Lock()
	for _, v := range body.Messages {
		if _, ok := s.values[v]; !ok {
			s.values[v] = struct{}{}
			if s.isRoot {
				s.pending = append(s.pending, v)
			}
		}
	}
	s.mu.Unlock()

	return nil
}

func (s *server) flushLoop() {
	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()
	for range ticker.C {
		s.mu.Lock()
		if !s.ready || len(s.pending) == 0 {
			s.mu.Unlock()
			continue
		}
		batch := s.pending
		s.pending = nil
		neighbors := append([]string(nil), s.neighbors...)
		s.mu.Unlock()

		body := map[string]any{"type": "batch", "messages": batch}
		for _, nb := range neighbors {
			if err := s.n.Send(nb, body); err != nil {
				log.Printf("send error: %v", err)
			}
		}
	}
}

func main() {
	s := newServer()

	s.n.Handle("init", s.handleInit)
	s.n.Handle("broadcast", s.handleBroadcast)
	s.n.Handle("read", s.handleRead)
	s.n.Handle("topology", s.handleTopology)
	s.n.Handle("batch", s.handleBatch)

	go s.flushLoop()

	if err := s.n.Run(); err != nil {
		log.Fatal(err)
	}
}
