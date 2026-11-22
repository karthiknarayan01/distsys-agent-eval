package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()

	// All values broadcast to this node. A single node holds the whole
	// cluster's state, so a mutex-protected set is enough.
	var mu sync.Mutex
	messages := make(map[int]struct{})

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body struct {
			Message int `json:"message"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		mu.Lock()
		messages[body.Message] = struct{}{}
		mu.Unlock()

		return n.Reply(msg, map[string]any{"type": "broadcast_ok"})
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		mu.Lock()
		values := make([]int, 0, len(messages))
		for v := range messages {
			values = append(values, v)
		}
		mu.Unlock()

		return n.Reply(msg, map[string]any{
			"type":     "read_ok",
			"messages": values,
		})
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		return n.Reply(msg, map[string]any{"type": "topology_ok"})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
