package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sort"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type broadcastRequest struct {
	Type    string `json:"type"`
	Message *int64 `json:"message,omitempty"`
	MsgID   *int64 `json:"msg_id,omitempty"`
}

type readRequest struct {
	Type  string `json:"type"`
	MsgID *int64 `json:"msg_id,omitempty"`
}

type topologyRequest struct {
	Type     string              `json:"type"`
	Topology map[string][]string `json:"topology"`
	MsgID    *int64              `json:"msg_id,omitempty"`
}

type broadcastManyRequest struct {
	Type     string  `json:"type"`
	Messages []int64 `json:"messages"`
	MsgID    *int64  `json:"msg_id,omitempty"`
}

// server owns the grow-only set of values known by this node.
type server struct {
	node *maelstrom.Node

	mu      sync.RWMutex
	values  map[int64]struct{}
	peers   []string
	peerSet map[string]struct{}
}

func newServer(node *maelstrom.Node) *server {
	return &server{
		node:    node,
		values:  make(map[int64]struct{}),
		peerSet: make(map[string]struct{}),
	}
}

// setPeers chooses a complete mesh. Maelstrom's topology is advisory, and a
// complete mesh lets a value reach every node in one hop.
func (s *server) setPeers(ids []string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.peers = s.peers[:0]
	s.peerSet = make(map[string]struct{})
	for _, id := range ids {
		if id == s.node.ID() {
			continue
		}
		if _, exists := s.peerSet[id]; exists {
			continue
		}
		s.peerSet[id] = struct{}{}
		s.peers = append(s.peers, id)
	}
	sort.Strings(s.peers)
}

func (s *server) add(value int64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.values[value]; exists {
		return false
	}
	s.values[value] = struct{}{}
	return true
}

func (s *server) addMany(values []int64) []int64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	added := make([]int64, 0, len(values))
	for _, value := range values {
		if _, exists := s.values[value]; exists {
			continue
		}
		s.values[value] = struct{}{}
		added = append(added, value)
	}
	return added
}

func (s *server) snapshot() []int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	values := make([]int64, 0, len(s.values))
	for value := range s.values {
		values = append(values, value)
	}
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	return values
}

func (s *server) peersCopy() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	peers := make([]string, len(s.peers))
	copy(peers, s.peers)
	return peers
}

// disseminate sends a newly learned value directly to every other node.
// A receiver does not forward values it already has, so this cannot loop.
func (s *server) disseminate(value int64) {
	for _, peer := range s.peersCopy() {
		body := broadcastRequest{Type: "broadcast"}
		v := value
		body.Message = &v
		if err := s.node.Send(peer, body); err != nil {
			log.Printf("failed to send value %d to %s: %v", value, peer, err)
		}
	}
}

func decodeBody(msg maelstrom.Message, dst any) error {
	if err := json.Unmarshal(msg.Body, dst); err != nil {
		return fmt.Errorf("decode request body: %w", err)
	}
	return nil
}

func (s *server) handleBroadcast(msg maelstrom.Message) error {
	var req broadcastRequest
	if err := decodeBody(msg, &req); err != nil {
		return err
	}
	if req.Message == nil {
		return fmt.Errorf("broadcast request is missing message")
	}

	value := *req.Message
	if s.add(value) {
		s.disseminate(value)
	}

	// Requests from clients have a msg_id and need a response. Inter-node
	// messages deliberately do not have one.
	if req.MsgID != nil {
		return s.node.Reply(msg, map[string]any{"type": "broadcast_ok"})
	}
	return nil
}

func (s *server) handleBroadcastMany(msg maelstrom.Message) error {
	var req broadcastManyRequest
	if err := decodeBody(msg, &req); err != nil {
		return err
	}

	for _, value := range s.addMany(req.Messages) {
		s.disseminate(value)
	}
	if req.MsgID != nil {
		return s.node.Reply(msg, map[string]any{"type": "broadcast_many_ok"})
	}
	return nil
}

func (s *server) handleRead(msg maelstrom.Message) error {
	var req readRequest
	if err := decodeBody(msg, &req); err != nil {
		return err
	}
	return s.node.Reply(msg, map[string]any{
		"type":     "read_ok",
		"messages": s.snapshot(),
	})
}

func (s *server) handleTopology(msg maelstrom.Message) error {
	var req topologyRequest
	if err := decodeBody(msg, &req); err != nil {
		return err
	}

	// Keep using a complete mesh. The suggested topology is only a hint, and
	// direct delivery gives predictable convergence in this small cluster.
	return s.node.Reply(msg, map[string]any{"type": "topology_ok"})
}

func main() {
	log.SetFlags(0)
	log.SetOutput(os.Stderr)

	node := maelstrom.NewNode()
	s := newServer(node)

	node.Handle("init", func(msg maelstrom.Message) error {
		// The library has already recorded the node and cluster IDs.
		s.setPeers(node.NodeIDs())
		return nil
	})
	node.Handle("broadcast", s.handleBroadcast)
	node.Handle("broadcast_many", s.handleBroadcastMany)
	node.Handle("read", s.handleRead)
	node.Handle("topology", s.handleTopology)

	if err := node.Run(); err != nil {
		log.Fatalf("node stopped: %v", err)
	}
}
