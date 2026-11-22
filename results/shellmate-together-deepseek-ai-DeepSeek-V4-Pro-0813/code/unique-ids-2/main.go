package main

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()

	var mu sync.Mutex
	var counter int64

	n.Handle("generate", func(msg maelstrom.Message) error {
		var body struct {
			Type  string `json:"type"`
			MsgID int    `json:"msg_id"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		mu.Lock()
		counter++
		id := counter
		mu.Unlock()

		return n.Reply(msg, map[string]any{
			"type": "generate_ok",
			"id":   fmt.Sprintf("%s-%d", n.ID(), id),
		})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
