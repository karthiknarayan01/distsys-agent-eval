package main

import (
	"fmt"
	"log"
	"sync/atomic"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	node := maelstrom.NewNode()

	// Each node has a globally unique node ID.  Combining that ID with a
	// strictly increasing local counter makes an ID unique without requiring
	// any communication with other nodes.  In particular, IDs remain unique
	// while the network is partitioned.
	var counter atomic.Uint64

	node.Handle("generate", func(msg maelstrom.Message) error {
		id := fmt.Sprintf("%s-%d", node.ID(), counter.Add(1))
		return node.Reply(msg, map[string]any{
			"type": "generate_ok",
			"id":   id,
		})
	})

	if err := node.Run(); err != nil {
		log.Fatalf("node failed: %v", err)
	}
}
