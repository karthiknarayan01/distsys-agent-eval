package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"sync/atomic"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	// The Maelstrom library logs every message it receives and sends. At the
	// workload's request rate that produces a large amount of unnecessary I/O.
	log.SetOutput(io.Discard)

	n := maelstrom.NewNode()

	// The node ID is unique within a Maelstrom cluster. Combining it with a
	// process-local counter therefore gives every ticket a globally unique
	// value without requiring any communication between nodes.
	var prefix atomic.Value // string
	var counter atomic.Uint64
	initialized := make(chan struct{})

	n.Handle("init", func(msg maelstrom.Message) error {
		prefix.Store(n.ID())
		close(initialized)
		return nil
	})

	n.Handle("issue", func(msg maelstrom.Message) error {
		// Maelstrom sends requests only after initialization. Waiting here as
		// well also makes the handler safe if messages are delivered together.
		<-initialized

		nodeID, ok := prefix.Load().(string)
		if !ok || nodeID == "" {
			return fmt.Errorf("node received a ticket request before initialization")
		}

		seq := counter.Add(1)
		ticket := nodeID + "-" + strconv.FormatUint(seq, 10)

		return n.Reply(msg, map[string]any{
			"type":   "issued",
			"ticket": ticket,
		})
	})

	if err := n.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "node failed: %v\n", err)
		os.Exit(1)
	}
}
