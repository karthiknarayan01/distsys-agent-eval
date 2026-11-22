package main

import (
	"encoding/json"
	"io"
	"log"
	"sort"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// server holds the broadcast state for one node.
//
// Values are flooded over a deterministic tree that every node computes for
// itself from the (identically ordered) node ID list handed out by the init
// message. A value is relayed exactly once by each node, to every tree
// neighbour except the one it arrived from, so the whole cluster sees a value
// after at most tree-depth hops and each value costs at most (nodes-1)
// server-to-server messages in total.
type server struct {
	node *maelstrom.Node

	mu     sync.Mutex
	values map[int]struct{}

	peers []string
}

// branchFactor controls the width of the tree. With 25 nodes a factor of 5
// gives a depth-2 tree: root -> 5 children -> up to 19 grandchildren.
const branchFactor = 5

func newServer(n *maelstrom.Node) *server {
	return &server{
		node:   n,
		values: make(map[int]struct{}),
	}
}

// buildTree computes this node's neighbours from the cluster membership.
func (s *server) buildTree() {
	ids := s.node.NodeIDs()
	if len(ids) == 0 {
		return
	}
	sorted := make([]string, len(ids))
	copy(sorted, ids)
	sort.Strings(sorted)

	idx := -1
	for i, id := range sorted {
		if id == s.node.ID() {
			idx = i
			break
		}
	}
	if idx < 0 {
		return
	}

	peers := make([]string, 0, branchFactor+1)
	if idx > 0 {
		peers = append(peers, sorted[(idx-1)/branchFactor])
	}
	for c := idx*branchFactor + 1; c <= idx*branchFactor+branchFactor && c < len(sorted); c++ {
		peers = append(peers, sorted[c])
	}
	s.mu.Lock()
	s.peers = peers
	s.mu.Unlock()
}

// forward relays values to every tree neighbour except the one we got them
// from.
func (s *server) forward(values []int, except string) {
	if len(values) == 0 {
		return
	}
	s.mu.Lock()
	peers := s.peers
	s.mu.Unlock()

	body := map[string]any{"type": "relay", "values": values}
	for _, p := range peers {
		if p == except {
			continue
		}
		if err := s.node.Send(p, body); err != nil {
			log.Printf("relay to %s: %s", p, err)
		}
	}
}

// ensureTree builds the tree if a message arrives before init did.
func (s *server) ensureTree() {
	s.mu.Lock()
	have := len(s.peers) > 0
	s.mu.Unlock()
	if !have {
		s.buildTree()
	}
}

// insert records values, returning the ones that were not already known.
func (s *server) insert(values []int) []int {
	s.mu.Lock()
	defer s.mu.Unlock()
	var fresh []int
	for _, v := range values {
		if _, ok := s.values[v]; ok {
			continue
		}
		s.values[v] = struct{}{}
		fresh = append(fresh, v)
	}
	return fresh
}

func (s *server) handleInit(msg maelstrom.Message) error {
	s.buildTree()
	return nil
}

func (s *server) handleTopology(msg maelstrom.Message) error {
	// We arrange the nodes ourselves; the suggested topology is unused.
	s.buildTree()
	return s.node.Reply(msg, map[string]any{"type": "topology_ok"})
}

func (s *server) handleBroadcast(msg maelstrom.Message) error {
	var body struct {
		Message int `json:"message"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	s.ensureTree()
	fresh := s.insert([]int{body.Message})
	if err := s.node.Reply(msg, map[string]any{"type": "broadcast_ok"}); err != nil {
		return err
	}
	s.forward(fresh, "")
	return nil
}

func (s *server) handleRelay(msg maelstrom.Message) error {
	var body struct {
		Values []int `json:"values"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	s.ensureTree()
	s.forward(s.insert(body.Values), msg.Src)
	return nil
}

func (s *server) handleRead(msg maelstrom.Message) error {
	s.mu.Lock()
	values := make([]int, 0, len(s.values))
	for v := range s.values {
		values = append(values, v)
	}
	s.mu.Unlock()

	return s.node.Reply(msg, map[string]any{
		"type":     "read_ok",
		"messages": values,
	})
}

func main() {
	// The library logs every message it sends and receives; that is a lot of
	// STDOUT-sized JSON for a run this chatty, so keep stderr quiet.
	log.SetOutput(io.Discard)

	n := maelstrom.NewNode()
	s := newServer(n)

	n.Handle("init", s.handleInit)
	n.Handle("topology", s.handleTopology)
	n.Handle("broadcast", s.handleBroadcast)
	n.Handle("relay", s.handleRelay)
	n.Handle("read", s.handleRead)

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
