package main

import (
	"encoding/json"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()

	state := make(map[string]int)

	n.Handle("init", func(msg maelstrom.Message) error {
		var body maelstrom.InitMessageBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		n.Init(body.NodeID, body.NodeIDs)

		// Initialize local counter entry.
		state[n.ID()] = 0

		// Start periodic replication.
		go func() {
			ticker := time.NewTicker(500 * time.Millisecond)
			defer ticker.Stop()
			for range ticker.C {
				for _, peer := range n.NodeIDs() {
					if peer == n.ID() {
						continue
					}
					_ = n.Send(peer, map[string]any{
						"type":  "replicate",
						"value": copyState(state),
					})
				}
			}
		}()

		return n.Reply(msg, maelstrom.MessageBody{Type: "init_ok"})
	})

	n.Handle("add", func(msg maelstrom.Message) error {
		var body struct {
			maelstrom.MessageBody
			Delta int `json:"delta"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		state[n.ID()] += body.Delta

		// Replicate to peers immediately.
		for _, peer := range n.NodeIDs() {
			if peer == n.ID() {
				continue
			}
			_ = n.Send(peer, map[string]any{
				"type":  "replicate",
				"value": copyState(state),
			})
		}

		return n.Reply(msg, maelstrom.MessageBody{Type: "add_ok"})
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		total := 0
		for _, v := range state {
			total += v
		}
		return n.Reply(msg, map[string]any{
			"type":  "read_ok",
			"value": total,
		})
	})

	n.Handle("replicate", func(msg maelstrom.Message) error {
		var body struct {
			maelstrom.MessageBody
			Value map[string]int `json:"value"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		for k, v := range body.Value {
			if v > state[k] {
				state[k] = v
			}
		}
		return nil
	})

	_ = n.Run()
}

func copyState(s map[string]int) map[string]int {
	c := make(map[string]int, len(s))
	for k, v := range s {
		c[k] = v
	}
	return c
}
