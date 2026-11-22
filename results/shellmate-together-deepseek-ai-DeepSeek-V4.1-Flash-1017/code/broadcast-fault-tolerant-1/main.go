package main

import (
	"encoding/json"
	"log"
	"sort"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// gossipInterval controls how often each node sends its complete set of
// values to every other node. Periodic full-state exchange makes the
// protocol robust against arbitrary, temporary network partitions: a value
// only has to be present on one side of a healed partition for it to spread
// to the whole cluster.
const gossipInterval = 100 * time.Millisecond

type broadcastMessage struct {
	Type     string `json:"type"`
	Message  int    `json:"message"`
	Messages []int  `json:"messages,omitempty"`
}

type readResponse struct {
	Type     string `json:"type"`
	Messages []int  `json:"messages"`
}

type server struct {
	node *maelstrom.Node

	mu       sync.RWMutex
	messages map[int]struct{}

	startOnce sync.Once
}

func main() {
	n := maelstrom.NewNode()
	s := &server{
		node:     n,
		messages: make(map[int]struct{}),
	}

	n.Handle("broadcast", s.handleBroadcast)
	n.Handle("read", s.handleRead)
	n.Handle("topology", s.handleTopology)
	n.Handle("gossip", s.handleGossip)

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

func (s *server) handleBroadcast(msg maelstrom.Message) error {
	var body broadcastMessage
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	s.mu.Lock()
	s.messages[body.Message] = struct{}{}
	s.mu.Unlock()

	s.startGossip()

	return s.node.Reply(msg, map[string]any{"type": "broadcast_ok"})
}

func (s *server) handleRead(msg maelstrom.Message) error {
	s.mu.RLock()
	messages := make([]int, 0, len(s.messages))
	for value := range s.messages {
		messages = append(messages, value)
	}
	s.mu.RUnlock()

	sort.Ints(messages)
	return s.node.Reply(msg, readResponse{Type: "read_ok", Messages: messages})
}

func (s *server) handleTopology(msg maelstrom.Message) error {
	s.startGossip()
	return s.node.Reply(msg, map[string]any{"type": "topology_ok"})
}

func (s *server) handleGossip(msg maelstrom.Message) error {
	var body broadcastMessage
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	s.mu.Lock()
	for _, value := range body.Messages {
		s.messages[value] = struct{}{}
	}
	s.mu.Unlock()
	// Make sure the newly learned state is passed on promptly even if this
	// node was started without having seen a client request yet.
	s.startGossip()
	return nil
}

func (s *server) startGossip() {
	s.startOnce.Do(func() {
		go s.gossipLoop()
	})
}

func (s *server) gossipLoop() {
	ticker := time.NewTicker(gossipInterval)
	defer ticker.Stop()

	for range ticker.C {
		s.gossip()
	}
}

func (s *server) gossip() {
	s.mu.RLock()
	messages := make([]int, 0, len(s.messages))
	for value := range s.messages {
		messages = append(messages, value)
	}
	s.mu.RUnlock()

	body := broadcastMessage{Type: "gossip", Messages: messages}
	for _, dest := range s.node.NodeIDs() {
		if dest == s.node.ID() {
			continue
		}
		// Errors here are connection-level and are expected during
		// partitions. The next periodic round will retry.
		_ = s.node.Send(dest, body)
	}
}
