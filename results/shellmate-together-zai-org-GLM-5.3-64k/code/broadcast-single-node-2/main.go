// Command node implements Maelstrom's "broadcast" workload for a single-node
// cluster: it keeps every value broadcast to it and reports them back on read.
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// broadcastBody is the body of a client "broadcast" request.
type broadcastBody struct {
	Type    string `json:"type"`
	MsgID   int    `json:"msg_id"`
	Message int64  `json:"message"`
}

// readBody is the body of a client "read" request.
type readBody struct {
	Type  string `json:"type"`
	MsgID int    `json:"msg_id"`
}

// topologyBody is the body of a client "topology" request.
type topologyBody struct {
	Type     string              `json:"type"`
	MsgID    int                 `json:"msg_id"`
	Topology map[string][]string `json:"topology"`
}

// server holds the state shared between message handlers.
type server struct {
	n *maelstrom.Node

	mu        sync.Mutex
	messages  map[int64]struct{} // set of broadcast values
	neighbors []string           // neighbours from the topology message
}

func newServer(n *maelstrom.Node) *server {
	return &server{
		n:        n,
		messages: make(map[int64]struct{}),
	}
}

// add records a value. Returns false if the value was already known.
func (s *server) add(v int64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.messages[v]; ok {
		return false
	}
	s.messages[v] = struct{}{}
	return true
}

// values returns all known values, sorted, in a fresh slice.
func (s *server) values() []int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]int64, 0, len(s.messages))
	for v := range s.messages {
		out = append(out, v)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// handleBroadcast acknowledges a value, keeping it.
func (s *server) handleBroadcast(msg maelstrom.Message) error {
	var body broadcastBody
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return fmt.Errorf("unmarshal broadcast body: %w", err)
	}
	s.add(body.Message)
	return s.n.Reply(msg, map[string]any{"type": "broadcast_ok"})
}

// handleRead returns every value that has been broadcast to this node.
func (s *server) handleRead(msg maelstrom.Message) error {
	var body readBody
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return fmt.Errorf("unmarshal read body: %w", err)
	}
	return s.n.Reply(msg, map[string]any{
		"type":     "read_ok",
		"messages": s.values(),
	})
}

// handleTopology records the neighbour list and acknowledges.
func (s *server) handleTopology(msg maelstrom.Message) error {
	var body topologyBody
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return fmt.Errorf("unmarshal topology body: %w", err)
	}

	s.mu.Lock()
	if neighbors, ok := body.Topology[s.n.ID()]; ok {
		s.neighbors = append([]string(nil), neighbors...)
	}
	s.mu.Unlock()

	return s.n.Reply(msg, map[string]any{"type": "topology_ok"})
}

func main() {
	n := maelstrom.NewNode()
	s := newServer(n)

	n.Handle("broadcast", s.handleBroadcast)
	n.Handle("read", s.handleRead)
	n.Handle("topology", s.handleTopology)

	if err := n.Run(); err != nil {
		log.Fatalf("node %s exited: %v", n.ID(), err)
	}
}
