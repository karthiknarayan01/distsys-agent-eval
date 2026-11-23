package main

import (
	"fmt"
	"log"
	"sync/atomic"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type issuedBody struct {
	Type   string `json:"type"`
	Ticket string `json:"ticket"`
}

func main() {
	node := maelstrom.NewNode()

	// Maelstrom gives every node in the cluster a distinct ID.  Prefixing a
	// per-node counter with that ID gives every ticket a distinct owner and
	// number, so no coordination (and hence no partition-sensitive waiting)
	// is needed to mint a ticket.
	var prefix atomic.Value // string, installed by the init handler
	var counter atomic.Uint64

	node.Handle("init", func(maelstrom.Message) error {
		prefix.Store(node.ID() + "-")
		return nil
	})

	node.Handle("issue", func(msg maelstrom.Message) error {
		p, _ := prefix.Load().(string)
		if p == "" {
			// This should not happen: Maelstrom initializes nodes before
			// sending client requests.  Keeping the fallback makes the node
			// robust if it is driven manually.
			p = node.ID() + "-"
		}

		ticket := fmt.Sprintf("%s%d", p, counter.Add(1))
		return node.Reply(msg, issuedBody{
			Type:   "issued",
			Ticket: ticket,
		})
	})

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}
}
