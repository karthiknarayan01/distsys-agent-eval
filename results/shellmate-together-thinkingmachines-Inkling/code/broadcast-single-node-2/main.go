package main

import (
	"encoding/json"
	"fmt"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()
	values := make([]int, 0)

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body struct {
			Message int `json:"message"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return fmt.Errorf("unmarshal broadcast body: %w", err)
		}
		values = append(values, body.Message)
		return n.Reply(msg, map[string]interface{}{"type": "broadcast_ok"})
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		return n.Reply(msg, map[string]interface{}{
			"type":     "read_ok",
			"messages": values,
		})
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		return n.Reply(msg, map[string]interface{}{"type": "topology_ok"})
	})

	if err := n.Run(); err != nil {
		panic(err)
	}
}
