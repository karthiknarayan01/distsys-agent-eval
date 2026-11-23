package main

import (
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const gossipType = "broadcast_gossip"

type server struct {
	node *maelstrom.Node

	mu     sync.RWMutex
	values map[int64]struct{}
	peers  map[string]struct{}
}

type broadcastBody struct {
	Message int64 `json:"message"`
}

type gossipBody struct {
	Type    string `json:"type"`
	Message int64  `json:"message"`
}

type topologyBody struct {
	Topology map[string][]string `json:"topology"`
}

func newServer(node *maelstrom.Node) *server {
	return &server{
		node:   node,
		values: make(map[int64]struct{}),
		peers:  make(map[string]struct{}),
	}
}

// remember adds a value to the local set. It returns true only when the value
// was not already known.
func (s *server) remember(value int64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.values[value]; ok {
		return false
	}
	s.values[value] = struct{}{}
	return true
}

func (s *server) peerSnapshot() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	peers := make([]string, 0, len(s.peers))
	for peer := range s.peers {
		peers = append(peers, peer)
	}
	return peers
}

func (s *server) valueSnapshot() []int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	values := make([]int64, 0, len(s.values))
	for value := range s.values {
		values = append(values, value)
	}
	return values
}

// disseminate sends a newly learned value to all topology neighbors. Peers
// forward values they have not seen before, so one copy is enough to flood the
// connected cluster without producing an infinite gossip loop.
func (s *server) disseminate(value int64, except string) error {
	for _, peer := range s.peerSnapshot() {
		if peer == except {
			continue
		}
		if err := s.node.Send(peer, gossipBody{Type: gossipType, Message: value}); err != nil {
			return fmt.Errorf("send gossip to %s: %w", peer, err)
		}
	}
	return nil
}

func (s *server) handleBroadcast(msg maelstrom.Message) error {
	var body broadcastBody
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return fmt.Errorf("decode broadcast: %w", err)
	}

	if s.remember(body.Message) {
		// Store and propagate before acknowledging the client. Thus every
		// acknowledged value has already started its journey through the
		// cluster.
		if err := s.disseminate(body.Message, ""); err != nil {
			return err
		}
	}

	return s.node.Reply(msg, map[string]any{"type": "broadcast_ok"})
}

func (s *server) handleGossip(msg maelstrom.Message) error {
	var body gossipBody
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return fmt.Errorf("decode gossip: %w", err)
	}

	if s.remember(body.Message) {
		if err := s.disseminate(body.Message, msg.Src); err != nil {
			return err
		}
	}
	return nil
}

func (s *server) handleTopology(msg maelstrom.Message) error {
	var body topologyBody
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return fmt.Errorf("decode topology: %w", err)
	}

	neighbors := body.Topology[s.node.ID()]
	peers := make(map[string]struct{}, len(neighbors))
	for _, peer := range neighbors {
		if peer != "" && peer != s.node.ID() {
			peers[peer] = struct{}{}
		}
	}

	s.mu.Lock()
	s.peers = peers
	s.mu.Unlock()

	if err := s.node.Reply(msg, map[string]any{"type": "topology_ok"}); err != nil {
		return err
	}

	// A broadcast can race with the initial topology message. Re-send every
	// value we already know so such values are not stranded.
	for _, value := range s.valueSnapshot() {
		if err := s.disseminate(value, ""); err != nil {
			return err
		}
	}
	return nil
}

func (s *server) handleRead(msg maelstrom.Message) error {
	values := s.valueSnapshot()
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })

	return s.node.Reply(msg, map[string]any{
		"type":     "read_ok",
		"messages": values,
	})
}

func main() {
	node := maelstrom.NewNode()
	server := newServer(node)

	node.Handle("broadcast", server.handleBroadcast)
	node.Handle("read", server.handleRead)
	node.Handle("topology", server.handleTopology)
	node.Handle(gossipType, server.handleGossip)

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}
}
