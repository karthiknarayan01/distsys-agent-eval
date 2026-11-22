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

const (
	gossipType = "gossip"
	syncType   = "sync"
)

type server struct {
	node *maelstrom.Node

	mu       sync.RWMutex
	messages map[int64]struct{}
	peers    map[string]struct{}
}

// broadcastRequest is a request sent by a client.
type broadcastRequest struct {
	Type    string `json:"type"`
	MsgID   int    `json:"msg_id"`
	Message int64  `json:"message"`
}

// gossipMessage carries one newly learned value between nodes.
type gossipMessage struct {
	Type    string `json:"type"`
	Message int64  `json:"message"`
}

// syncMessage carries an anti-entropy snapshot between nodes.
type syncMessage struct {
	Type     string  `json:"type"`
	Messages []int64 `json:"messages"`
}

// topologyRequest contains the topology supplied by Maelstrom.
type topologyRequest struct {
	Type     string              `json:"type"`
	MsgID    int                 `json:"msg_id"`
	Topology map[string][]string `json:"topology"`
}

func main() {
	log.SetOutput(os.Stderr)
	log.SetFlags(0)

	s := &server{
		node:     maelstrom.NewNode(),
		messages: make(map[int64]struct{}),
		peers:    make(map[string]struct{}),
	}

	s.node.Handle("init", s.onInit)
	s.node.Handle("topology", s.onTopology)
	s.node.Handle("broadcast", s.onBroadcast)
	s.node.Handle("read", s.onRead)
	s.node.Handle(gossipType, s.onGossip)
	s.node.Handle(syncType, s.onSync)

	if err := s.node.Run(); err != nil {
		log.Printf("node stopped: %v", err)
		os.Exit(1)
	}
}

func (s *server) onInit(msg maelstrom.Message) error {
	// Until the topology arrives, use every other known node as a peer. This
	// also makes the implementation independent of the particular topology.
	for _, id := range s.node.NodeIDs() {
		if id != s.node.ID() {
			s.mu.Lock()
			s.peers[id] = struct{}{}
			s.mu.Unlock()
		}
	}

	go s.antiEntropyLoop()
	return nil
}

func (s *server) onTopology(msg maelstrom.Message) error {
	var body topologyRequest
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	neighbors := body.Topology[s.node.ID()]
	if len(neighbors) > 0 {
		s.mu.Lock()
		s.peers = make(map[string]struct{}, len(neighbors))
		for _, id := range neighbors {
			if id != s.node.ID() {
				s.peers[id] = struct{}{}
			}
		}
		s.mu.Unlock()
	}

	return s.node.Reply(msg, map[string]any{"type": "topology_ok"})
}

func (s *server) onBroadcast(msg maelstrom.Message) error {
	var body broadcastRequest
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	s.add(body.Message)
	s.gossipValue(body.Message, "")

	return s.node.Reply(msg, map[string]any{"type": "broadcast_ok"})
}

func (s *server) onRead(msg maelstrom.Message) error {
	s.mu.RLock()
	values := make([]int64, 0, len(s.messages))
	for value := range s.messages {
		values = append(values, value)
	}
	s.mu.RUnlock()

	// Sorting is not required, but makes responses deterministic and easier
	// to inspect.
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	return s.node.Reply(msg, map[string]any{
		"type":     "read_ok",
		"messages": values,
	})
}

func (s *server) onGossip(msg maelstrom.Message) error {
	var body gossipMessage
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	if s.add(body.Message) {
		// Forward only values that were new to us. Together with the source
		// exclusion, this prevents a value from circulating forever.
		s.gossipValue(body.Message, msg.Src)
	}
	return nil
}

func (s *server) onSync(msg maelstrom.Message) error {
	var body syncMessage
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	for _, value := range body.Messages {
		s.add(value)
	}
	return nil
}

// add inserts value into the local grow-only set. It returns true only when
// the value was not already present.
func (s *server) add(value int64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.messages[value]; exists {
		return false
	}
	s.messages[value] = struct{}{}
	return true
}

// gossipValue sends a value to all current peers. If exclude is non-empty,
// that peer is skipped (it is the node from which we learned the value).
func (s *server) gossipValue(value int64, exclude string) {
	s.mu.RLock()
	peers := make([]string, 0, len(s.peers))
	for peer := range s.peers {
		if peer != exclude {
			peers = append(peers, peer)
		}
	}
	s.mu.RUnlock()

	for _, peer := range peers {
		if err := s.node.Send(peer, gossipMessage{
			Type:    gossipType,
			Message: value,
		}); err != nil {
			log.Printf("gossip to %s failed: %v", peer, err)
		}
	}
}

// antiEntropyLoop periodically exchanges snapshots with peers. Gossip gives
// low latency, while this periodic exchange ensures convergence even if an
// individual gossip message was not delivered.
func (s *server) antiEntropyLoop() {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()

	for range ticker.C {
		s.mu.RLock()
		values := make([]int64, 0, len(s.messages))
		for value := range s.messages {
			values = append(values, value)
		}
		peers := make([]string, 0, len(s.peers))
		for peer := range s.peers {
			peers = append(peers, peer)
		}
		s.mu.RUnlock()

		sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
		for _, peer := range peers {
			if err := s.node.Send(peer, syncMessage{
				Type:     syncType,
				Messages: values,
			}); err != nil {
				log.Printf("sync to %s failed: %v", peer, err)
			}
		}
	}
}
