package main

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"os"
	"sync/atomic"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// node hands out IDs that are unique across the whole cluster without any
// coordination. Each node just combines three things that no other node can
// ever repeat:
//
//   - its own node ID, which is unique within the cluster,
//   - a random 64-bit value chosen once at process start (so that even a
//     restarted process with the same node ID cannot reissue old IDs),
//   - a counter that is strictly increasing for the lifetime of the process.
//
// Because no network round trip is required, the handler works even when the
// node is completely partitioned from the rest of the cluster.
func main() {
	// Keep stderr quiet: log lines are not needed and cost syscalls at 1000
	// requests per second.
	log.SetOutput(io.Discard)

	n := maelstrom.NewNode()

	var counter uint64

	// A process-lifetime random nonce. Fall back to something still-unlikely
	// if the entropy source is unavailable.
	var nonceBytes [8]byte
	if _, err := rand.Read(nonceBytes[:]); err != nil {
		fmt.Fprintf(os.Stderr, "read random nonce: %s\n", err)
		for i := range nonceBytes {
			nonceBytes[i] = byte(i * 37)
		}
	}
	nonce := hex.EncodeToString(nonceBytes[:])

	n.Handle("generate", func(msg maelstrom.Message) error {
		// msg.Dest is this node's ID; use it (rather than the library's
		// n.ID()) so the handler is correct even before init is processed.
		prefix := msg.Dest
		if prefix == "" {
			prefix = n.ID()
		}

		c := atomic.AddUint64(&counter, 1)

		return n.Reply(msg, map[string]any{
			"type": "generate_ok",
			"id":   fmt.Sprintf("%s-%s-%d", prefix, nonce, c),
		})
	})

	if err := n.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "node error: %s\n", err)
		os.Exit(1)
	}
}
