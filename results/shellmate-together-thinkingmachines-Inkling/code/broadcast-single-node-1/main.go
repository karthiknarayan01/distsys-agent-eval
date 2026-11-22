package main

import (
	"encoding/json"
	"log"
	"os"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()

	var mu sync.Mutex
	values := make(map[int]bool)

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body map[string]interface{}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		if msgVal, ok := body["message"].(float64); ok {
			mu.Lock()
			values[int(msgVal)] = true
			mu.Unlock()
		}
		return n.Reply(msg, map[string]interface{}{
			"type": "broadcast_ok",
		})
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		mu.Lock()
		msgs := make([]int, 0, len(values))
		for v := range values {
			msgs = append(msgs, v)
		}
		mu.Unlock()
		return n.Reply(msg, map[string]interface{}{
			"type":      "read_ok",
			"messages": msgs,
		})
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		return n.Reply(msg, map[string]interface{}{
			"type": "topology_ok",
		})
	})

	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}
}
