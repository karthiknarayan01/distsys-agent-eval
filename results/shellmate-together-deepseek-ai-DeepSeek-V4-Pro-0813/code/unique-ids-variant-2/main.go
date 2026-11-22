package main

import (
	"encoding/json"
	"fmt"
	"sync/atomic"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()
	var counter int64

	n.Handle("issue", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		body["type"] = "issued"
		body["ticket"] = fmt.Sprintf("%s-%d", n.ID(), atomic.AddInt64(&counter, 1))

		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		panic(err)
	}
}
