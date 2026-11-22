package main

import (
	"log"
	"strconv"
	"sync/atomic"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// A node's ID is unique within its cluster. Combining that immutable ID with
// a process-local, monotonically increasing counter therefore gives us a
// globally unique value without communicating with any other node.
//
// In particular, this remains correct while the network is partitioned: ID
// generation needs no peer, and no request has to wait for a quorum.
type idGenerator struct {
	counter atomic.Uint64
}

func (g *idGenerator) next(nodeID string) string {
	seq := g.counter.Add(1)
	return nodeID + "-" + strconv.FormatUint(seq, 10)
}

func main() {
	log.SetFlags(0)

	node := maelstrom.NewNode()
	generator := new(idGenerator)

	node.Handle("generate", func(req maelstrom.Message) error {
		// Maelstrom supplies the node's unique ID in the init message, which
		// always precedes workload requests.
		return node.Reply(req, struct {
			Type string `json:"type"`
			ID   string `json:"id"`
		}{
			Type: "generate_ok",
			ID:   generator.next(node.ID()),
		})
	})

	if err := node.Run(); err != nil {
		log.Fatalf("node failed: %v", err)
	}
}
