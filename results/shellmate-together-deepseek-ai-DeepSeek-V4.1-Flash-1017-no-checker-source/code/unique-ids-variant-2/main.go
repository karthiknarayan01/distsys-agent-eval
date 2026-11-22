package main

import (
	"fmt"
	"log"
	"sync/atomic"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()
	var counter uint64

	n.Handle("issue", func(msg maelstrom.Message) error {
		id := atomic.AddUint64(&counter, 1)
		return n.Reply(msg, map[string]any{
			"type":   "issued",
			"ticket": fmt.Sprintf("%s-%d", n.ID(), id),
		})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
