package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type CounterState struct {
	mu     sync.RWMutex
	Counts map[string]int
}

func main() {
	n := maelstrom.NewNode()
	state := &CounterState{
		Counts: make(map[string]int),
	}

	n.Handle("init", func(msg maelstrom.Message) error {
		return nil
	})

	n.Handle("count", func(msg maelstrom.Message) error {
		var body struct {
			Type     string `json:"type"`
			MsgID    int    `json:"msg_id"`
			Visitors int    `json:"visitors"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		state.mu.Lock()
		state.Counts[n.ID()] = state.Counts[n.ID()] + body.Visitors
		state.mu.Unlock()

		go replicate(n, state)

		return n.Reply(msg, map[string]interface{}{
			"type": "counted",
		})
	})

	n.Handle("total", func(msg maelstrom.Message) error {
		state.mu.RLock()
		total := 0
		for _, v := range state.Counts {
			total += v
		}
		state.mu.RUnlock()

		return n.Reply(msg, map[string]interface{}{
			"type":             "total_is",
			"visitors_so_far": total,
		})
	})

	n.Handle("merge", func(msg maelstrom.Message) error {
		var body struct {
			Type  string         `json:"type"`
			Value map[string]int `json:"value"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		state.mu.Lock()
		for node, val := range body.Value {
			if val > state.Counts[node] {
				state.Counts[node] = val
			}
		}
		state.mu.Unlock()
		return nil
	})

	if err := n.Run(); err != nil {
		log.Printf("Run error: %v", err)
	}
}

func replicate(n *maelstrom.Node, state *CounterState) {
	state.mu.RLock()
	copyState := make(map[string]int)
	for k, v := range state.Counts {
		copyState[k] = v
	}
	state.mu.RUnlock()

	for _, nodeID := range n.NodeIDs() {
		if nodeID == n.ID() {
			continue
		}
		n.Send(nodeID, map[string]interface{}{
			"type":  "merge",
			"value": copyState,
		})
	}
}
