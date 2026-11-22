package main

import (
	"encoding/json"
	"log"
	"os"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type NodeState struct {
	node     *maelstrom.Node
	mu       sync.Mutex
	rumors   map[int]bool
	nodeIDs  []string
}

func main() {
	n := maelstrom.NewNode()
	state := &NodeState{
		node:    n,
		rumors:  make(map[int]bool),
	}

	n.Handle("init", func(msg maelstrom.Message) error {
		var body map[string]interface{}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		state.nodeIDs = n.NodeIDs()
		// Start gossip loop after init
		go state.gossipLoop()
		return nil
	})

	n.Handle("tell", func(msg maelstrom.Message) error {
		var body struct {
			Rumor int `json:"rumor"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		state.mu.Lock()
		state.rumors[body.Rumor] = true
		state.mu.Unlock()

		// Reply told
		reply := map[string]interface{}{
			"type": "told",
		}
		if err := n.Reply(msg, reply); err != nil {
			return err
		}

		// Immediately gossip to others
		go state.gossipRumor(body.Rumor)
		return nil
	})

	n.Handle("recall", func(msg maelstrom.Message) error {
		state.mu.Lock()
		rumors := make([]int, 0, len(state.rumors))
		for r := range state.rumors {
			rumors = append(rumors, r)
		}
		state.mu.Unlock()

		reply := map[string]interface{}{
			"type":    "recalled",
			"rumors": rumors,
		}
		return n.Reply(msg, reply)
	})

	n.Handle("neighbors", func(msg maelstrom.Message) error {
		reply := map[string]interface{}{
			"type": "neighbors_noted",
		}
		return n.Reply(msg, reply)
	})

	n.Handle("rumor_msg", func(msg maelstrom.Message) error {
		var body struct {
			Rumors []int `json:"rumors"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		state.mu.Lock()
		for _, r := range body.Rumors {
			state.rumors[r] = true
		}
		state.mu.Unlock()
		return nil
	})

	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}
}

func (s *NodeState) gossipLoop() {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for range ticker.C {
		s.gossipAll()
	}
}

func (s *NodeState) gossipAll() {
	s.mu.Lock()
	if len(s.nodeIDs) == 0 {
		s.mu.Unlock()
		return
	}
	rumors := make([]int, 0, len(s.rumors))
	for r := range s.rumors {
		rumors = append(rumors, r)
	}
	nodeIDs := make([]string, len(s.nodeIDs))
	copy(nodeIDs, s.nodeIDs)
	s.mu.Unlock()

	if len(rumors) == 0 {
		return
	}

	myID := s.node.ID()
	for _, dest := range nodeIDs {
		if dest == myID {
			continue
		}
		msg := map[string]interface{}{
			"type":    "rumor_msg",
			"rumors": rumors,
		}
		// Send asynchronously; ignore errors (messages across split disappear)
		s.node.Send(dest, msg)
	}
}

func (s *NodeState) gossipRumor(r int) {
	s.gossipAll()
}
