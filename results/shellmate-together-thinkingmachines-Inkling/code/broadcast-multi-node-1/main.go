package main

import (
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()

	values := make(map[int]bool)

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body struct {
			maelstrom.MessageBody
			Message int `json:"message"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		values[body.Message] = true
		// propagate to all nodes except source? Send to all other nodes.
		for _, id := range n.NodeIDs() {
			if id == msg.Src {
				continue
			}
			n.Send(id, map[string]any{
				"type": "gossip",
				"message": body.Message,
			})
		}
		return n.Reply(msg, map[string]any{"type": "broadcast_ok"})
	})

	n.Handle("gossip", func(msg maelstrom.Message) error {
		var body struct {
			maelstrom.MessageBody
			Message int `json:"message"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		if !values[body.Message] {
			values[body.Message] = true
			for _, id := range n.NodeIDs() {
				if id == msg.Src {
					continue
				}
				n.Send(id, map[string]any{
					"type": "gossip",
					"message": body.Message,
				})
			}
		}
		return nil
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		msgs := make([]int, 0, len(values))
		for v := range values {
			msgs = append(msgs, v)
		}
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
