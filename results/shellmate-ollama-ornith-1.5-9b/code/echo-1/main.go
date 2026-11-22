package main

import (
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()

	// Register a handler for the "echo" message that responds with an "echo_ok".
	n.Handle("echo", func(msg maelstrom.Message) error {
		// Unmarshal the message body as a loosely-typed map so that the
		// echo value (which may be any JSON value) is preserved.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// Set the response type, keeping the original echo value intact.
		body["type"] = "echo_ok"

		// Reply carries in_reply_to (set by the library) and the echoed body.
		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
	}
}
