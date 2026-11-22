package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type broadcastRequest struct {
	Message int `json:"message"`
}

type topologyRequest struct {
	Topology map[string][]string `json:"topology"`
}

func main() {
	node := maelstrom.NewNode()

	var mu sync.Mutex
	var messages []int

	node.Handle("broadcast", func(msg maelstrom.Message) error {
		var req broadcastRequest
		if err := json.Unmarshal(msg.Body, &req); err != nil {
			return err
		}

		// Store the value before acknowledging it, so every acknowledged
		// broadcast is guaranteed to be visible to subsequent reads.
		mu.Lock()
		messages = append(messages, req.Message)
		mu.Unlock()

		return node.Reply(msg, map[string]any{
			"type": "broadcast_ok",
		})
	})

	node.Handle("read", func(msg maelstrom.Message) error {
		mu.Lock()
		snapshot := make([]int, len(messages))
		copy(snapshot, messages)
		mu.Unlock()

		return node.Reply(msg, map[string]any{
			"type":     "read_ok",
			"messages": snapshot,
		})
	})

	node.Handle("topology", func(msg maelstrom.Message) error {
		var req topologyRequest
		if err := json.Unmarshal(msg.Body, &req); err != nil {
			return err
		}

		return node.Reply(msg, map[string]any{
			"type": "topology_ok",
		})
	})

	if err := node.Run(); err != nil {
		log.Fatalf("node failed: %v", err)
	}
}
