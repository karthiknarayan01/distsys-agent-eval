package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const (
	// The public protocol used by the museum workload.
	countType   = "count"
	countedType = "counted"
	totalType   = "total"
	totalIsType = "total_is"

	// Internal anti-entropy messages. These names are deliberately different
	// from all of the public (and native Maelstrom) protocol names.
	gossipType  = "museum_counter_gossip"
	stateType   = "museum_counter_state"
	stateOKType = "museum_counter_state_ok"

	gossipInterval = 100 * time.Millisecond
	pullTimeout    = 250 * time.Millisecond
)

// counter is a grow-only counter represented as a G-counter. Each turnstile
// (node) owns one component and may only increase it. Merging two states is
// therefore the component-wise maximum.
type counter struct {
	mu         sync.Mutex
	components map[string]int64
}

func newCounter() *counter {
	return &counter{components: make(map[string]int64)}
}

func (c *counter) add(nodeID string, visitors int64) {
	c.mu.Lock()
	c.components[nodeID] += visitors
	c.mu.Unlock()
}

func (c *counter) merge(other map[string]int64) {
	c.mu.Lock()
	for nodeID, value := range other {
		// The counter is grow-only, so negative and stale values are ignored.
		if value > c.components[nodeID] {
			c.components[nodeID] = value
		}
	}
	c.mu.Unlock()
}

func (c *counter) snapshot() map[string]int64 {
	c.mu.Lock()
	result := make(map[string]int64, len(c.components))
	for nodeID, value := range c.components {
		result[nodeID] = value
	}
	c.mu.Unlock()
	return result
}

func (c *counter) total() int64 {
	c.mu.Lock()
	var result int64
	for _, value := range c.components {
		result += value
	}
	c.mu.Unlock()
	return result
}

type server struct {
	node    *maelstrom.Node
	counter *counter

	peersMu sync.RWMutex
	peers   []string

	// A client message ID identifies one logical operation. Remembering IDs
	// makes count handling idempotent if a request is ever delivered twice.
	seenMu sync.Mutex
	seen   map[string]struct{}
}

func newServer() *server {
	return &server{
		node:    maelstrom.NewNode(),
		counter: newCounter(),
		seen:    make(map[string]struct{}),
	}
}

type countRequest struct {
	Type     string `json:"type"`
	MsgID    int    `json:"msg_id"`
	Visitors int64  `json:"visitors"`
	Delta    int64  `json:"delta"`
}

type totalRequest struct {
	Type  string `json:"type"`
	MsgID int    `json:"msg_id"`
}

type countResponse struct {
	Type string `json:"type"`
}

type totalIsResponse struct {
	Type          string `json:"type"`
	VisitorsSoFar int64  `json:"visitors_so_far"`
}

type readOKResponse struct {
	Type  string `json:"type"`
	Value int64  `json:"value"`
}

type gossipMessage struct {
	Type       string           `json:"type"`
	Components map[string]int64 `json:"components"`
}

type stateResponse struct {
	Type       string           `json:"type"`
	Components map[string]int64 `json:"components"`
}

func operationKey(src string, msgID int) string {
	return fmt.Sprintf("%s/%d", src, msgID)
}

// handleCount records visitors in the local node's G-counter component and
// acknowledges the request. It does not wait for the network: counts remain
// available during a partition.
func (s *server) handleCount(msg maelstrom.Message) error {
	var request countRequest
	if err := json.Unmarshal(msg.Body, &request); err != nil {
		return fmt.Errorf("decode count request: %w", err)
	}

	visitors := request.Visitors
	if request.Type == "add" {
		// Compatibility with Maelstrom's native g-counter spelling. The
		// rename proxy normally translates this to "count"/"visitors".
		visitors = request.Delta
	}
	if visitors < 0 {
		return fmt.Errorf("visitor count must be non-negative")
	}

	// msg_id is unique for each client operation. Treat a repeated delivery
	// as the same operation so that it cannot count visitors twice.
	key := ""
	duplicate := false
	if request.MsgID != 0 {
		key = operationKey(msg.Src, request.MsgID)
		s.seenMu.Lock()
		_, duplicate = s.seen[key]
		if !duplicate {
			s.seen[key] = struct{}{}
		}
		s.seenMu.Unlock()
	}

	if !duplicateRequest(key, duplicate) {
		s.counter.add(s.node.ID(), visitors)
	}

	responseType := countedType
	if request.Type == "add" {
		responseType = "add_ok"
	}
	return s.node.Reply(msg, countResponse{Type: responseType})
}

// duplicateRequest reports whether key identifies an operation already seen.
// An empty key means that no usable message ID was supplied.
func duplicateRequest(key string, duplicate bool) bool {
	return key != "" && duplicate
}

func (s *server) handleTotal(msg maelstrom.Message) error {
	var request totalRequest
	if err := json.Unmarshal(msg.Body, &request); err != nil {
		return fmt.Errorf("decode total request: %w", err)
	}

	// Pull the newest component state directly from the other turnstiles as
	// well as using anti-entropy. This makes a read just after a partition is
	// healed converge immediately rather than waiting for the next gossip.
	s.pullPeerStates()

	value := s.counter.total()
	if request.Type == "read" {
		// Compatibility with Maelstrom's native g-counter spelling.
		return s.node.Reply(msg, readOKResponse{Type: "read_ok", Value: value})
	}
	return s.node.Reply(msg, totalIsResponse{Type: totalIsType, VisitorsSoFar: value})
}

func (s *server) handleGossip(msg maelstrom.Message) error {
	var request gossipMessage
	if err := json.Unmarshal(msg.Body, &request); err != nil {
		return fmt.Errorf("decode gossip message: %w", err)
	}
	s.counter.merge(request.Components)
	return nil
}

func (s *server) handleStateRequest(msg maelstrom.Message) error {
	return s.node.Reply(msg, stateResponse{
		Type:       stateOKType,
		Components: s.counter.snapshot(),
	})
}

func (s *server) setPeers(peers []string) {
	copyOfPeers := append([]string(nil), peers...)
	s.peersMu.Lock()
	s.peers = copyOfPeers
	s.peersMu.Unlock()
}

func (s *server) peerList() []string {
	s.peersMu.RLock()
	result := append([]string(nil), s.peers...)
	s.peersMu.RUnlock()
	return result
}

// pullPeerStates asks every peer for its current state in parallel. Requests
// which cross a partition are dropped by the network; in that case the local
// state is still returned after the short timeout.
func (s *server) pullPeerStates() {
	peers := s.peerList()
	if len(peers) == 0 {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), pullTimeout)
	defer cancel()

	var wg sync.WaitGroup
	for _, peer := range peers {
		if peer == s.node.ID() {
			continue
		}
		wg.Add(1)
		go func(peer string) {
			defer wg.Done()
			result := make(chan map[string]int64, 1)
			err := s.node.RPC(peer, gossipMessage{Type: stateType}, func(msg maelstrom.Message) error {
				var response stateResponse
				if err := json.Unmarshal(msg.Body, &response); err != nil {
					return err
				}
				select {
				case result <- response.Components:
				default:
				}
				return nil
			})
			if err != nil {
				return
			}
			select {
			case components := <-result:
				s.counter.merge(components)
			case <-ctx.Done():
			}
		}(peer)
	}
	wg.Wait()
}

// gossipLoop sends full G-counter states to all peers. Full-state anti-entropy
// plus component-wise maximum makes healing and repeated partitions safe.
func (s *server) gossipLoop() {
	ticker := time.NewTicker(gossipInterval)
	defer ticker.Stop()

	for range ticker.C {
		components := s.counter.snapshot()
		message := gossipMessage{Type: gossipType, Components: components}
		for _, peer := range s.peerList() {
			if peer == s.node.ID() {
				continue
			}
			if err := s.node.Send(peer, message); err != nil {
				log.Printf("gossip send to %s failed: %v", peer, err)
			}
		}
	}
}

func (s *server) handleInit(msg maelstrom.Message) error {
	s.setPeers(s.node.NodeIDs())
	go s.gossipLoop()
	return nil
}

func (s *server) run() error {
	s.node.Handle("init", s.handleInit)
	s.node.Handle(countType, s.handleCount)
	s.node.Handle(totalType, s.handleTotal)
	s.node.Handle(gossipType, s.handleGossip)
	s.node.Handle(stateType, s.handleStateRequest)

	// These two handlers make the program usable directly with Maelstrom's
	// native g-counter workload as well as with the renamed museum protocol.
	s.node.Handle("add", s.handleCount)
	s.node.Handle("read", s.handleTotal)

	return s.node.Run()
}

func main() {
	log.SetOutput(os.Stderr)
	log.SetFlags(0)
	if err := newServer().run(); err != nil {
		log.Printf("node failed: %v", err)
		os.Exit(1)
	}
}
