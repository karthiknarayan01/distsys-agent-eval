package main

import (
	"fmt"
	"sync/atomic"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()

	var counter atomic.Uint64
	n.Handle("issue", func(msg maelstrom.Message) error {
		// Node IDs are distinct, and the counter is atomic and local to this
		// node. Therefore no two tickets can ever collide, even while the
		// cluster is partitioned.
		ticket := fmt.Sprintf("%s-%d", n.ID(), counter.Add(1))
		return n.Reply(msg, map[string]any{
			"type":   "issued",
			"ticket": ticket,
		})
	})

	if err := n.Run(); err != nil {
		panic(err)
	}
}
