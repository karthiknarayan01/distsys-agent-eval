package main

import (
	"fmt"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()

	var mu sync.Mutex
	var counter uint64

	n.Handle("issue", func(msg maelstrom.Message) error {
		mu.Lock()
		counter++
		c := counter
		mu.Unlock()

		ticket := fmt.Sprintf("%s-%d", n.ID(), c)

		return n.Reply(msg, map[string]any{
			"type":   "issued",
			"ticket": ticket,
		})
	})

	if err := n.Run(); err != nil {
		panic(err)
	}
}
