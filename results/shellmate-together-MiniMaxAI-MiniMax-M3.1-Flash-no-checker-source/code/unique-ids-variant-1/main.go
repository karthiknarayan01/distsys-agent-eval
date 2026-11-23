// Command node implements a partition-tolerant, globally unique ticket issuer.
//
// Every node owns a disjoint namespace: tickets are made from the node's
// cluster-unique ID and a monotonically increasing local counter.  Because a
// node can issue tickets without talking to any other node, issuance remains
// available while the network is partitioned.
package main

import (
	"log"
	"strconv"
	"sync/atomic"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type ticketService struct {
	node    *maelstrom.Node
	counter atomic.Uint64
}

func main() {
	node := maelstrom.NewNode()
	service := &ticketService{node: node}

	node.Handle("issue", service.issue)

	if err := node.Run(); err != nil {
		log.Fatalf("node failed: %v", err)
	}
}

// issue allocates one ticket and immediately replies to the request.
func (s *ticketService) issue(request maelstrom.Message) error {
	sequence := s.counter.Add(1)

	// Node IDs are unique within a Maelstrom cluster, and the local sequence
	// never repeats, so these ticket strings are globally unique without any
	// inter-node communication.
	ticket := s.node.ID() + "-" + strconv.FormatUint(sequence, 10)

	return s.node.Reply(request, map[string]any{
		"type":   "issued",
		"ticket": ticket,
	})
}
