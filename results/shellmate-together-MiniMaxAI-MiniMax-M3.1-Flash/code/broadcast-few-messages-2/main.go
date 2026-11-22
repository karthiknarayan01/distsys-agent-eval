package main

import (
	"encoding/json"
	"log"
	"sort"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type valueBody struct {
	Type    string `json:"type"`
	Message int    `json:"message"`
}

type gossipBody struct {
	Type    string `json:"type"`
	Message int    `json:"message"`
}

type replyBody struct {
	Type string `json:"type"`
}

type readReply struct {
	Type     string `json:"type"`
	Messages []int  `json:"messages"`
}

type server struct {
	node *maelstrom.Node

	mu     sync.RWMutex
	values map[int]struct{}
}

func newServer() *server {
	return &server{
		node:   maelstrom.NewNode(),
		values: make(map[int]struct{}),
	}
}

func (s *server) add(value int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.values[value]; ok {
		return false
	}
	s.values[value] = struct{}{}
	return true
}

func (s *server) snapshot() []int {
	s.mu.RLock()
	values := make([]int, 0, len(s.values))
	for value := range s.values {
		values = append(values, value)
	}
	s.mu.RUnlock()

	// The workload accepts any order. Sorting makes replies deterministic and
	// costs very little for the number of values in a test.
	sort.Ints(values)
	return values
}

func (s *server) handleBroadcast(msg maelstrom.Message) error {
	var body valueBody
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	// A client broadcast is the only event which starts dissemination.  An
	// internal gossip message is deliberately a different type, so recipients
	// only store the value and never amplify it.
	if s.add(body.Message) {
		// There is no need to relay through a topology: a direct message to
		// every other node uses exactly N-1 server messages per value (the
		// minimum for a point-to-point network), and gives every recipient the
		// value after just one network hop.
		for _, id := range s.node.NodeIDs() {
			if id == s.node.ID() {
				continue
			}
			if err := s.node.Send(id, gossipBody{
				Type:    "gossip",
				Message: body.Message,
			}); err != nil {
				return err
			}
		}
	}

	return s.node.Reply(msg, replyBody{Type: "broadcast_ok"})
}

func (s *server) handleGossip(msg maelstrom.Message) error {
	var body gossipBody
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	s.add(body.Message)
	return nil
}

func (s *server) handleRead(msg maelstrom.Message) error {
	return s.node.Reply(msg, readReply{
		Type:     "read_ok",
		Messages: s.snapshot(),
	})
}

func (s *server) handleTopology(msg maelstrom.Message) error {
	// The suggested topology is optional.  Direct dissemination gives a
	// better latency/message tradeoff for this workload.
	return s.node.Reply(msg, replyBody{Type: "topology_ok"})
}

func main() {
	s := newServer()
	s.node.Handle("broadcast", s.handleBroadcast)
	s.node.Handle("gossip", s.handleGossip)
	s.node.Handle("read", s.handleRead)
	s.node.Handle("topology", s.handleTopology)

	if err := s.node.Run(); err != nil {
		log.Fatalf("node failed: %v", err)
	}
}
