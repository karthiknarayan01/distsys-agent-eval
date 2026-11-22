package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	node := maelstrom.NewNode()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	msgCh := node.Handle(ctx, nil)

	var values []int
	var valuesMutex sync.Mutex

	// Broadcast handler - store the value and reply
	node.Handle("broadcast", func(msg maelstrom.Message) error {
		var body struct {
			Message int `json:"message"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			log.Printf("Failed to unmarshal broadcast: %v", err)
			return err
		}

		valuesMutex.Lock()
		values = append(values, body.Message)
		valuesMutex.Unlock()

		msg_id := msg.Get("msg_id")
		node.Reply(msg, maelstrom.BroadcastOK{InReplyTo: msg_id})
		return nil
	})

	// Read handler - return all stored values
	node.Handle("read", func(msg maelstrom.Message) error {
		var body struct{}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			log.Printf("Failed to unmarshal read: %v", err)
			return err
		}

		valuesMutex.Lock()
		var messages []int
		for _, v := range values {
			messages = append(messages, v)
		}
		valuesMutex.Unlock()

		node.Reply(msg, maelstrom.ReadOK{InReplyTo: msg.Get("msg_id"), Messages: messages})
		return nil
	})

	// Topology handler
	node.Handle("topology", func(msg maelstrom.Message) error {
		node.Reply(msg, maelstrom.TopologyOK{InReplyTo: msg.Get("msg_id")})
		return nil
	})

	if err := node.Run(); err != nil {
		os.Exit(1)
	}
}
