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

// The gossip interval is deliberately short.  Sending the complete local set
// periodically is a simple form of anti-entropy: a message which was sent
// while the network was partitioned is sent again after the partition heals.
const gossipInterval = 100 * time.Millisecond

type broadcastRequest struct {
	maelstrom.MessageBody
	Message int64 `json:"message"`
}

type readRequest struct {
	maelstrom.MessageBody
}

type topologyRequest struct {
	maelstrom.MessageBody
	Topology map[string][]string `json:"topology"`
}

type gossipMessage struct {
	Type     string  `json:"type"`
	Messages []int64 `json:"messages"`
}

type server struct {
	node *maelstrom.Node

	mu        sync.Mutex
	nodeID    string
	values    map[int64]struct{}
	neighbors []string
	topology  map[string][]string

	// wake asks the sender goroutine to publish our state immediately.
	wake chan struct{}
}

func newServer() *server {
	return &server{
		node:   maelstrom.NewNode(),
		values: make(map[int64]struct{}),
		wake:   make(chan struct{}, 1),
	}
}

func (s *server) handleInit(msg maelstrom.Message) error {
	s.mu.Lock()
	s.nodeID = s.node.ID()
	if s.topology != nil {
		s.setNeighborsLocked(s.topology[s.nodeID])
	}
	s.mu.Unlock()

	s.kick()
	return nil
}

func (s *server) handleTopology(msg maelstrom.Message) error {
	var body topologyRequest
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	s.mu.Lock()
	s.topology = body.Topology
	if s.nodeID != "" {
		s.setNeighborsLocked(body.Topology[s.nodeID])
	}
	s.mu.Unlock()

	// The topology request itself is not a value, so this does not require a
	// client-visible acknowledgement.  Publish our current state to the new
	// neighbors as soon as possible.
	s.kick()
	return s.node.Reply(msg, map[string]string{"type": "topology_ok"})
}

func (s *server) handleBroadcast(msg maelstrom.Message) error {
	var body broadcastRequest
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	s.mu.Lock()
	_, existed := s.values[body.Message]
	s.values[body.Message] = struct{}{}
	s.mu.Unlock()

	if !existed {
		s.kick()
	}

	// A value is retained locally before it is acknowledged.  It is retried
	// to every neighbor until the network heals.
	return s.node.Reply(msg, map[string]string{"type": "broadcast_ok"})
}

func (s *server) handleRead(msg maelstrom.Message) error {
	s.mu.Lock()
	messages := make([]int64, 0, len(s.values))
	for value := range s.values {
		messages = append(messages, value)
	}
	s.mu.Unlock()

	// The order is unspecified, but a stable order makes the output easier to
	// inspect and still contains no duplicates.
	sort.Slice(messages, func(i, j int) bool { return messages[i] < messages[j] })
	return s.node.Reply(msg, struct {
		Type     string  `json:"type"`
		Messages []int64 `json:"messages"`
	}{
		Type:     "read_ok",
		Messages: messages,
	})
}

func (s *server) handleGossip(msg maelstrom.Message) error {
	var body gossipMessage
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	changed := false
	s.mu.Lock()
	for _, value := range body.Messages {
		if _, exists := s.values[value]; !exists {
			s.values[value] = struct{}{}
			changed = true
		}
	}
	s.mu.Unlock()

	if changed {
		s.kick()
	}
	return nil
}

// setNeighborsLocked installs a de-duplicated copy of the neighbor list.
// s.mu must be held by the caller.
func (s *server) setNeighborsLocked(neighbors []string) {
	seen := make(map[string]struct{}, len(neighbors))
	result := make([]string, 0, len(neighbors))
	for _, neighbor := range neighbors {
		if neighbor == s.nodeID {
			continue
		}
		if _, exists := seen[neighbor]; exists {
			continue
		}
		seen[neighbor] = struct{}{}
		result = append(result, neighbor)
	}
	s.neighbors = result
}

func (s *server) kick() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// publish sends our entire value set to every topology neighbor.  Sending the
// full set (rather than only deltas) makes this protocol repair both lost
// messages and messages whose acknowledgement was lost.
func (s *server) publish() {
	s.mu.Lock()
	if len(s.values) == 0 || len(s.neighbors) == 0 {
		s.mu.Unlock()
		return
	}
	messages := make([]int64, 0, len(s.values))
	for value := range s.values {
		messages = append(messages, value)
	}
	neighbors := append([]string(nil), s.neighbors...)
	s.mu.Unlock()

	sort.Slice(messages, func(i, j int) bool { return messages[i] < messages[j] })
	for _, neighbor := range neighbors {
		if err := s.node.Send(neighbor, gossipMessage{
			Type:     "gossip",
			Messages: messages,
		}); err != nil {
			log.Printf("gossip to %s failed: %v", neighbor, err)
		}
	}
}

func (s *server) sender() {
	ticker := time.NewTicker(gossipInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.publish()
		case <-s.wake:
			s.publish()
		}
	}
}

func main() {
	s := newServer()

	s.node.Handle("init", s.handleInit)
	s.node.Handle("topology", s.handleTopology)
	s.node.Handle("broadcast", s.handleBroadcast)
	s.node.Handle("read", s.handleRead)
	s.node.Handle("gossip", s.handleGossip)

	go s.sender()

	if err := s.node.Run(); err != nil {
		log.Printf("node failed: %v", err)
		os.Exit(1)
	}
}
