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

	// Every node assigns IDs of the form "<node-id>-<counter>". Node IDs are
	// unique across the cluster and counters are local, so no two nodes can
	// ever hand out the same ID -- even when the cluster is partitioned and
	// nodes cannot talk to each other. This keeps availability total.
	n.Handle("generate", func(msg maelstrom.Message) error {
		id := fmt.Sprintf("%s-%d", n.ID(), atomic.AddUint64(&counter, 1))
		return n.Reply(msg, map[string]any{
			"type": "generate_ok",
			"id":   id,
		})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
