package main

import (
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()

	// echo: reply with the same value under the "echo" key.
	n.Handle("echo", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		reply := map[string]any{"type": "echo_ok"}
		if v, ok := body["echo"]; ok {
			reply["echo"] = v
		}

		return n.Reply(msg, reply)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
