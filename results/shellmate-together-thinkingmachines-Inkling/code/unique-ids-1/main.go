package main

import (
	"fmt"
	"sync/atomic"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()

	var counter int64

	n.Handle("generate", func(msg maelstrom.Message) error {
		id := fmt.Sprintf("%s-%d", n.ID(), atomic.AddInt64(&counter, 1))
		return n.Reply(msg, map[string]any{
			"type": "generate_ok",
			"id":   id,
		})
	})

	n.Run()
}
