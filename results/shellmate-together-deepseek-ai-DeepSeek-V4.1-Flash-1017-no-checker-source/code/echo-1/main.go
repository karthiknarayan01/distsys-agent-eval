package main

import (
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()

	n.Handle("echo", func(msg maelstrom.Message) error {
		// Decode the incoming body as untyped JSON so any JSON value for
		// "echo" is preserved exactly.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		reply := map[string]any{
			"type": "echo_ok",
		}
		if v, ok := body["echo"]; ok {
			reply["echo"] = v
		}
		return n.Reply(msg, reply)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
