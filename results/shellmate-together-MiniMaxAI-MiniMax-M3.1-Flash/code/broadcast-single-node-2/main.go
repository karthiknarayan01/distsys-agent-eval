package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type broadcastRequest struct {
	Message int64 `json:"message"`
}

func main() {
	node := maelstrom.NewNode()

	// The mutex makes broadcasts and reads linearizable with respect to one
	// another. In particular, a read cannot reply after a broadcast's
	// acknowledgement without also including that broadcast's value.
	var mu sync.Mutex
	messages := make([]int64, 0)

	node.Handle("broadcast", func(msg maelstrom.Message) error {
		var request broadcastRequest
		if err := json.Unmarshal(msg.Body, &request); err != nil {
			return err
		}

		mu.Lock()
		defer mu.Unlock()

		messages = append(messages, request.Message)
		return node.Reply(msg, map[string]any{"type": "broadcast_ok"})
	})

	node.Handle("read", func(msg maelstrom.Message) error {
		mu.Lock()
		defer mu.Unlock()

		// Reply while holding the lock so the response order agrees with the
		// order in which broadcasts become visible to reads.
		return node.Reply(msg, map[string]any{
			"type":     "read_ok",
			"messages": messages,
		})
	})

	node.Handle("topology", func(msg maelstrom.Message) error {
		return node.Reply(msg, map[string]any{"type": "topology_ok"})
	})

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}
}
