package main

import (
	"encoding/json"
	"log"
	"os"
	"sort"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type NodeState struct {
	mu        sync.Mutex
	values    map[int]bool
	neighbors []string
	root      bool
}

func main() {
	n := maelstrom.NewNode()
	state := &NodeState{
		values: make(map[int]bool),
	}

	n.Handle("init", func(msg maelstrom.Message) error {
		ids := n.NodeIDs()
		sort.Strings(ids)
		state.root = (ids[0] == n.ID())
		if state.root {
			state.neighbors = ids[1:]
		} else {
			state.neighbors = []string{ids[0]}
		}
		return nil
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		return n.Reply(msg, map[string]interface{}{
			"type": "topology_ok",
		})
	})

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body map[string]interface{}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		valFloat, ok := body["message"].(float64)
		if !ok {
			return errBadValue
		}
		val := int(valFloat)

		// Acknowledge immediately
		if err := n.Reply(msg, map[string]interface{}{
			"type": "broadcast_ok",
		}); err != nil {
			return err
		}

		state.propagate(n, val, "")
		return nil
	})

	n.Handle("replicate", func(msg maelstrom.Message) error {
		var body map[string]interface{}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		valFloat, ok := body["message"].(float64)
		if !ok {
			return errBadValue
		}
		val := int(valFloat)
		state.propagate(n, val, msg.Src)
		return nil
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		state.mu.Lock()
		vals := make([]int, 0, len(state.values))
		for v := range state.values {
			vals = append(vals, v)
		}
		state.mu.Unlock()
		return n.Reply(msg, map[string]interface{}{
			"type":      "read_ok",
			"messages": vals,
		})
	})

	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}
}

var errBadValue = maelstrom.NewRPCError(maelstrom.Crash, "invalid message value")

func (s *NodeState) propagate(node *maelstrom.Node, val int, exclude string) {
	s.mu.Lock()
	if s.values[val] {
		s.mu.Unlock()
		return
	}
	s.values[val] = true
	neighbors := make([]string, len(s.neighbors))
	copy(neighbors, s.neighbors)
	s.mu.Unlock()

	for _, neighbor := range neighbors {
		if neighbor == exclude {
			continue
		}
		node.Send(neighbor, map[string]interface{}{
			"type":    "replicate",
			"message": val,
		})
	}
}
