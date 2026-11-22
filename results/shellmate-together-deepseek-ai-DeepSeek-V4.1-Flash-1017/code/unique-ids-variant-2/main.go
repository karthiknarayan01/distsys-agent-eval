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

func main() {
	// Maelstrom pumps lots of messages through this process; its Go client
	// logs every one of them to stderr by default.  Keep stderr quiet so the
	// test rate is not limited by logging.
	log.SetOutput(io.Discard)

	n := maelstrom.NewNode()

	// A process gets its ID from Maelstrom before any client request arrives.
	// That ID is unique within the cluster, and the counter is unique within
	// this process, so their combination is globally unique.  This requires
	// no cooperation between nodes and therefore keeps working when the
	// cluster is partitioned.  A random process nonce additionally makes the
	// IDs distinct if a process were ever restarted during the same test.
	var seq atomic.Uint64
	var nonce [8]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		// crypto/rand is available in the test environment, but retain a
		// deterministic fallback rather than failing to start.
		copy(nonce[:], "fallback")
	}
	nonceHex := hex.EncodeToString(nonce[:])

	n.Handle("issue", func(msg maelstrom.Message) error {
		id := seq.Add(1)
		return n.Reply(msg, map[string]any{
			"type":   "issued",
			"ticket": fmt.Sprintf("%s-%s-%d", n.ID(), nonceHex, id),
		})
	})

	if err := n.Run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
