package main

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type ticketGenerator struct {
	sequence atomic.Uint64
	prefix   atomic.Value // string
	nonce    string
}

func newTicketGenerator() *ticketGenerator {
	g := &ticketGenerator{}
	g.prefix.Store("uninitialized")

	// The random process nonce also makes tickets unique if a node process is
	// restarted during a test. The node ID keeps concurrently running nodes
	// in disjoint namespaces, so ticket generation never requires the network.
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		// crypto/rand should not fail, but retain a uniqueness fallback.
		g.nonce = fmt.Sprintf("%d", time.Now().UnixNano())
	} else {
		g.nonce = hex.EncodeToString(nonce[:])
	}
	return g
}

func (g *ticketGenerator) initialize(nodeID string) {
	// Keep a stable, human-readable node prefix and add a per-process nonce.
	// For example: "n3-93a0...-42".
	g.prefix.Store(fmt.Sprintf("%s-%s", nodeID, g.nonce))
}

func (g *ticketGenerator) next() string {
	n := g.sequence.Add(1)
	return fmt.Sprintf("%s-%d", g.prefix.Load().(string), n)
}

func main() {
	node := maelstrom.NewNode()
	generator := newTicketGenerator()

	node.Handle("init", func(msg maelstrom.Message) error {
		generator.initialize(node.ID())
		return nil
	})

	issue := func(replyType string) maelstrom.HandlerFunc {
		return func(msg maelstrom.Message) error {
			return node.Reply(msg, map[string]any{
				"type":   replyType,
				"ticket": generator.next(),
			})
		}
	}

	node.Handle("issue", issue("issued"))
	// Also accept Maelstrom's original name when the program is run without
	// the rename wrapper.
	node.Handle("generate", issue("generate_ok"))

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}
}
