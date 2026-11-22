// Command node implements one machine of a box office that issues tickets
// carrying globally unique numbers.
//
// Every ticket must be unique across the whole cluster, and every request must
// be answered immediately, even by a node that is partitioned away from all of
// its peers. Those two requirements together rule out any kind of
// coordination, so each node mints ticket numbers locally out of three parts
// that cannot collide:
//
//   - the node's own ID, which Maelstrom guarantees is distinct per machine
//     (n1, n2, ...),
//   - a random per-process token, so that even a process that was restarted
//     and whose counter starts over cannot re-issue a ticket it handed out
//     earlier,
//   - a monotonically increasing local counter.
//
// The ticket is an opaque string such as "n3-9f2c1a7b-204".
package main

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// maxInitWait bounds how long a request is willing to wait for the node's own
// ID to become known. Maelstrom always delivers "init" as the very first
// message, so in practice this gate opens within microseconds; the bound only
// exists so that a request can never hang forever.
const maxInitWait = 5 * time.Second

func main() {
	n := maelstrom.NewNode()

	// Random token identifying this incarnation of the process.
	var seed [8]byte
	if _, err := rand.Read(seed[:]); err != nil {
		panic(fmt.Sprintf("node: cannot read random seed: %v", err))
	}
	incarnation := hex.EncodeToString(seed[:])

	var counter atomic.Uint64

	// The library runs every handler in its own goroutine, so a request could
	// in principle be handled before the "init" message has been processed and
	// the node's ID (and the "src" of our replies) is known. Gate request
	// handling on initialization to make that impossible.
	var (
		initOnce sync.Once
		ready    = make(chan struct{})
		nodeID   atomic.Value // string
	)
	markReady := func() {
		id := n.ID()
		if id != "" {
			nodeID.Store(id)
		}
		initOnce.Do(func() { close(ready) })
	}

	// Called by the library from handleInitMessage, after it has recorded our
	// ID and the cluster membership.
	n.Handle("init", func(_ maelstrom.Message) error {
		markReady()
		return nil
	})

	n.Handle("issue", func(msg maelstrom.Message) error {
		select {
		case <-ready:
		case <-time.After(maxInitWait):
			// Fall through and answer anyway: an answered request is better
			// than a hung one, and init always precedes requests in practice.
		}

		id, _ := nodeID.Load().(string)
		ticket := fmt.Sprintf("%s-%s-%d", id, incarnation, counter.Add(1))
		return n.Reply(msg, map[string]any{
			"type":   "issued",
			"ticket": ticket,
		})
	})

	if err := n.Run(); err != nil {
		panic(err)
	}
}
