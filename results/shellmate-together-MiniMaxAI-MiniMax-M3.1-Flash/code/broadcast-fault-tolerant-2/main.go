package main

import (
	"encoding/json"
	"log"
	"sort"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const (
	// Periodic full-state exchanges are anti-entropy: they repair messages
	// lost while the network was partitioned without requiring per-value
	// acknowledgements.
	antiEntropyInterval = 250 * time.Millisecond
)

type server struct {
	node *maelstrom.Node

	mu     sync.Mutex
	values map[int64]struct{}

	startOnce sync.Once
}

type broadcastRequest struct {
	Message int64 `json:"message"`
}

type topologyRequest struct {
	Topology map[string][]string `json:"topology"`
}

type gossipMessage struct {
	Type    string `json:"type"`
	Message int64  `json:"message"`
}

type syncMessage struct {
	Type     string  `json:"type"`
	Messages []int64 `json:"messages"`
}

func main() {
	n := maelstrom.NewNode()
	s := &server{
		node:   n,
		values: make(map[int64]struct{}),
	}

	n.Handle("init", s.handleInit)
	n.Handle("topology", s.handleTopology)
	n.Handle("broadcast", s.handleBroadcast)
	n.Handle("read", s.handleRead)
	n.Handle("gossip", s.handleGossip)
	n.Handle("sync", s.handleSync)

	if err := n.Run(); err != nil {
		log.Fatalf("node failed: %v", err)
	}
}

func (s *server) handleInit(maelstrom.Message) error {
	// The init handler is invoked after the library has recorded our ID and
	// the complete membership. All-to-all anti-entropy is deliberately used:
	// it converges in one exchange after a partition heals, and messages sent
	// across a partition are harmlessly retried by later exchanges.
	s.startOnce.Do(func() {
		go s.runAntiEntropy()
	})
	return nil
}

func (s *server) handleTopology(msg maelstrom.Message) error {
	var req topologyRequest
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		return err
	}
	// The suggested topology is optional. We use the known membership and
	// replicate to every peer, which is both simpler and faster to converge.
	_ = req.Topology
	return s.node.Reply(msg, map[string]any{"type": "topology_ok"})
}

func (s *server) handleBroadcast(msg maelstrom.Message) error {
	var req broadcastRequest
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		return err
	}

	s.mu.Lock()
	_, alreadyKnown := s.values[req.Message]
	s.values[req.Message] = struct{}{}
	s.mu.Unlock()

	// The acknowledgement means that this node has durably (in memory)
	// accepted the value. Replication is asynchronous and is retried by
	// anti-entropy, so a partition cannot make the acknowledgement unsafe.
	if err := s.node.Reply(msg, map[string]any{"type": "broadcast_ok"}); err != nil {
		return err
	}

	if !alreadyKnown {
		s.sendToPeers(gossipMessage{Type: "gossip", Message: req.Message})
	}
	return nil
}

func (s *server) handleRead(msg maelstrom.Message) error {
	values := s.snapshot()
	return s.node.Reply(msg, map[string]any{
		"type":     "read_ok",
		"messages": values,
	})
}

func (s *server) handleGossip(msg maelstrom.Message) error {
	var req gossipMessage
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		return err
	}

	s.mu.Lock()
	s.values[req.Message] = struct{}{}
	s.mu.Unlock()
	return nil
}

func (s *server) handleSync(msg maelstrom.Message) error {
	var req syncMessage
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		return err
	}

	s.mu.Lock()
	for _, value := range req.Messages {
		s.values[value] = struct{}{}
	}
	s.mu.Unlock()
	return nil
}

// snapshot returns a sorted copy of the local grow-only set.
func (s *server) snapshot() []int64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	values := make([]int64, 0, len(s.values))
	for value := range s.values {
		values = append(values, value)
	}
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	return values
}

// sendToPeers sends one message to every other member of the cluster.
func (s *server) sendToPeers(body any) {
	for _, peer := range s.node.NodeIDs() {
		if peer == s.node.ID() {
			continue
		}
		if err := s.node.Send(peer, body); err != nil {
			log.Printf("failed to send to %s: %v", peer, err)
		}
	}
}

// runAntiEntropy periodically exchanges the complete local state. Union is
// idempotent and commutative, so repeated and out-of-order exchanges are safe.
func (s *server) runAntiEntropy() {
	ticker := time.NewTicker(antiEntropyInterval)
	defer ticker.Stop()

	for range ticker.C {
		messages := s.snapshot()
		if len(messages) == 0 {
			continue
		}
		s.sendToPeers(syncMessage{Type: "sync", Messages: messages})
	}
}
