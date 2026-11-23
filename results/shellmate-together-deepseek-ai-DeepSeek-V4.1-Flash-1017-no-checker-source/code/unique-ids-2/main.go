package main

import (
	"fmt"
	"sync/atomic"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type idNode struct {
	node    *maelstrom.Node
	counter atomic.Uint64
}

func main() {
	n := maelstrom.NewNode()
	idn := &idNode{node: n}

	n.Handle("generate", func(msg maelstrom.Message) error {
		// A node ID is unique within the cluster, and the counter is unique
		// within this node. Together they form an ID unique across the whole
		// cluster without requiring any communication between nodes. This
		// keeps generate available even while the network is partitioned.
		id := fmt.Sprintf("%s-%d", idn.node.ID(), idn.counter.Add(1))
		return idn.node.Reply(msg, map[string]any{
			"type": "generate_ok",
			"id":   id,
		})
	})

	if err := n.Run(); err != nil {
		panic(err)
	}
}
