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

type broadcastKey struct {
	value  int64
	origin string
}

type pendingBroadcast struct {
	origin string
	value  int64
}

// Server implements a dissemination-tree broadcast.
//
// For each possible origin, we build a breadth-first spanning tree of the
// supplied topology.  A value is sent once along every edge of that tree:
// exactly N-1 server-to-server messages reach all N nodes.  BFS also gives
// the shortest possible tree depth for the origin, which keeps propagation
// latency low (on Maelstrom's 5x5 grid, at most eight network hops).
type Server struct {
	node *maelstrom.Node

	mu       sync.RWMutex
	messages map[int64]struct{}
	seen     map[broadcastKey]struct{}

	// trees[origin][node] is the list of children of node in the BFS tree
	// rooted at origin.
	trees map[string]map[string][]string

	pending []pendingBroadcast
}

func NewServer(node *maelstrom.Node) *Server {
	return &Server{
		node:     node,
		messages: make(map[int64]struct{}),
		seen:     make(map[broadcastKey]struct{}),
		trees:    make(map[string]map[string][]string),
	}
}

func (s *Server) Run() error {
	s.node.Handle("topology", s.handleTopology)
	s.node.Handle("broadcast", s.handleBroadcast)
	s.node.Handle("read", s.handleRead)
	s.node.Handle("disseminate", s.handleDisseminate)
	return s.node.Run()
}

func (s *Server) handleTopology(msg maelstrom.Message) error {
	var body struct {
		Type     string              `json:"type"`
		Topology map[string][]string `json:"topology"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return fmt.Errorf("decode topology: %w", err)
	}

	trees := buildTrees(body.Topology, s.node.NodeIDs())

	s.mu.Lock()
	s.trees = trees
	pending := s.pending
	s.pending = nil
	s.mu.Unlock()

	// A client broadcast normally arrives after topology, but flushing this
	// small queue also makes startup ordering harmless.
	for _, p := range pending {
		s.forward(p.origin, p.value)
	}

	return s.node.Reply(msg, map[string]any{"type": "topology_ok"})
}

func (s *Server) handleBroadcast(msg maelstrom.Message) error {
	var body struct {
		Type    string `json:"type"`
		MsgID   int    `json:"msg_id"`
		Message int64  `json:"message"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return fmt.Errorf("decode broadcast: %w", err)
	}

	origin := s.node.ID()
	key := broadcastKey{value: body.Message, origin: origin}

	s.mu.Lock()
	_, alreadySeen := s.seen[key]
	if !alreadySeen {
		s.seen[key] = struct{}{}
		s.messages[body.Message] = struct{}{}
	}
	children, haveTree := s.trees[origin][origin]
	if !haveTree {
		s.pending = append(s.pending, pendingBroadcast{origin: origin, value: body.Message})
	}
	s.mu.Unlock()

	// Start dissemination before acknowledging the client.  This also makes
	// the broadcast acknowledgement a useful durability point.
	if !alreadySeen && haveTree {
		s.sendChildren(origin, body.Message, children)
	}

	return s.node.Reply(msg, map[string]any{"type": "broadcast_ok"})
}

func (s *Server) handleDisseminate(msg maelstrom.Message) error {
	var body struct {
		Type    string `json:"type"`
		Origin  string `json:"origin"`
		Message int64  `json:"message"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return fmt.Errorf("decode dissemination: %w", err)
	}

	key := broadcastKey{value: body.Message, origin: body.Origin}

	s.mu.Lock()
	_, alreadySeen := s.seen[key]
	if !alreadySeen {
		s.seen[key] = struct{}{}
		s.messages[body.Message] = struct{}{}
	}
	children := s.trees[body.Origin][s.node.ID()]
	s.mu.Unlock()

	if !alreadySeen {
		s.sendChildren(body.Origin, body.Message, children)
	}
	return nil
}

func (s *Server) forward(origin string, value int64) {
	s.mu.RLock()
	children := s.trees[origin][origin]
	s.mu.RUnlock()
	s.sendChildren(origin, value, children)
}

func (s *Server) sendChildren(origin string, value int64, children []string) {
	for _, child := range children {
		if err := s.node.Send(child, map[string]any{
			"type":    "disseminate",
			"origin":  origin,
			"message": value,
		}); err != nil {
			log.Printf("send value %d to %s: %v", value, child, err)
		}
	}
}

func (s *Server) handleRead(msg maelstrom.Message) error {
	s.mu.RLock()
	values := make([]int64, 0, len(s.messages))
	for value := range s.messages {
		values = append(values, value)
	}
	s.mu.RUnlock()

	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	return s.node.Reply(msg, map[string]any{
		"type":     "read_ok",
		"messages": values,
	})
}

// buildTrees returns, for every origin, a breadth-first spanning tree of the
// topology.  Only tree edges are ever used, so each value costs exactly N-1
// inter-node messages.
func buildTrees(topology map[string][]string, nodeIDs []string) map[string]map[string][]string {
	ids := make([]string, 0, len(topology)+len(nodeIDs))
	seenID := make(map[string]struct{})
	for _, id := range nodeIDs {
		if _, ok := seenID[id]; !ok {
			seenID[id] = struct{}{}
			ids = append(ids, id)
		}
	}
	for id := range topology {
		if _, ok := seenID[id]; !ok {
			seenID[id] = struct{}{}
			ids = append(ids, id)
		}
	}

	trees := make(map[string]map[string][]string, len(ids))
	for _, origin := range ids {
		children := make(map[string][]string, len(ids))
		distance := make(map[string]int, len(ids))
		for _, id := range ids {
			distance[id] = -1
		}

		queue := make([]string, 0, len(ids))
		queue = append(queue, origin)
		distance[origin] = 0
		for head := 0; head < len(queue); head++ {
			u := queue[head]
			for _, v := range topology[u] {
				if _, known := distance[v]; !known {
					continue
				}
				if distance[v] == -1 {
					distance[v] = distance[u] + 1
					children[u] = append(children[u], v)
					queue = append(queue, v)
				}
			}
		}
		trees[origin] = children
	}
	return trees
}

func main() {
	log.SetOutput(os.Stderr)
	server := NewServer(maelstrom.NewNode())
	if err := server.Run(); err != nil {
		log.Fatal(err)
	}
}
