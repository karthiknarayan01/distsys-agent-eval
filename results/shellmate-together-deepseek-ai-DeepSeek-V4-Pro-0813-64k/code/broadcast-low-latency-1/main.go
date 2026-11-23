package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()

	var mu sync.Mutex
	values := make(map[int]struct{})

	// A value broadcast by a client: store it, acknowledge immediately, and
	// fan it out to every other node in one hop. With 25 nodes that is 24
	// messages per broadcast and every node has the value after a single
	// 100ms hop.
	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body struct {
			Message int `json:"message"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		mu.Lock()
		if _, seen := values[body.Message]; !seen {
			values[body.Message] = struct{}{}
			for _, id := range n.NodeIDs() {
				if id != n.ID() {
					n.Send(id, map[string]any{
						"type":    "spread",
						"message": body.Message,
					})
				}
			}
		}
		mu.Unlock()

		return n.Reply(msg, map[string]any{"type": "broadcast_ok"})
	})

	// A value forwarded from another node: just store it.
	n.Handle("spread", func(msg maelstrom.Message) error {
		var body struct {
			Message int `json:"message"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		mu.Lock()
		values[body.Message] = struct{}{}
		mu.Unlock()

		return nil
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		mu.Lock()
		msgs := make([]int, 0, len(values))
		for v := range values {
			msgs = append(msgs, v)
		}
		mu.Unlock()

		return n.Reply(msg, map[string]any{
			"type":     "read_ok",
			"messages": msgs,
		})
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		return n.Reply(msg, map[string]any{"type": "topology_ok"})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
