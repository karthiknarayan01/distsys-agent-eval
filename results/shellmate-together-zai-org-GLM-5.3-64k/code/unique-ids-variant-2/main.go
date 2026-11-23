// Ticket numbers: a box office spread over several machines. Every machine
// issues tickets on its own, and no two tickets may ever share a number, no
// matter which machine issued them and no matter how the network is
// partitioned.
package main

import (
	"fmt"
	"io"
	"log"
	"math/rand/v2"
	"os"
	"sync"
	"sync/atomic"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	// The library logs every message it reads and writes; at ~1000 requests
	// per second that is a lot of noise on stderr, so keep it quiet.
	log.SetOutput(io.Discard)

	n := maelstrom.NewNode()

	// A local, monotonically increasing counter. Combined with the node ID it
	// is unique across the cluster without any coordination at all, so a node
	// that is cut off from every other node still answers at once.
	//
	// A per-process random session tag is added as well, so that even a node
	// that restarts (and starts its counter over) can never re-issue a ticket.
	var counter atomic.Uint64
	session := rand.Uint64()

	// Closed once the init message has told us who we are. Requests normally
	// only start arriving after that, but if one shows up early we wait for
	// our own node ID rather than minting a ticket without it.
	var readyOnce sync.Once
	ready := make(chan struct{})
	n.Handle("init", func(msg maelstrom.Message) error {
		readyOnce.Do(func() { close(ready) })
		return nil
	})

	ticket := func() string {
		<-ready
		return fmt.Sprintf("%s-%d-%016x", n.ID(), counter.Add(1), session)
	}

	// The names the clients of this task use.
	n.Handle("issue", func(msg maelstrom.Message) error {
		return n.Reply(msg, map[string]any{
			"type":   "issued",
			"ticket": ticket(),
		})
	})

	// The same thing under Maelstrom's own names, in case the renaming shim
	// is not in front of us.
	n.Handle("generate", func(msg maelstrom.Message) error {
		return n.Reply(msg, map[string]any{
			"type": "generate_ok",
			"id":   ticket(),
		})
	})

	if err := n.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "fatal: %v\n", err)
		os.Exit(1)
	}
}
