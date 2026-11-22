package main

import (
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()

	// Register a handler for the "echo" message type.
	n.Handle("echo", func(msg maelstrom.Message) error {
		// Unmarshal the message body as a loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// Build the echo_ok response carrying the same "echo" value.
		resp := map[string]any{
			"type":         "echo_ok",
			"echo":         body["echo"],
			"in_reply_to":  body["msg_id"],
		}

		return n.Reply(msg, resp)
	})

	// Execute the node's message loop. This runs until STDIN is closed.
	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
	}
}
