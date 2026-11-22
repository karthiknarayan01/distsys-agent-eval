package main

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"log"
	"sync/atomic"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// randHex returns n random bytes, hex encoded. It is used to make IDs unique
// even across process restarts (where the local counter would restart at 0).
func randHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		// crypto/rand should never fail; fall back to a constant-ish value
		// rather than crashing and dropping requests.
		log.Printf("rand read failed: %v", err)
		binary.BigEndian.PutUint64(b, uint64(atomic.AddUint64(&fallback, 1)))
	}
	return hex.EncodeToString(b)
}

var fallback uint64

func main() {
	n := maelstrom.NewNode()

	// IDs are generated purely locally: every node has a distinct node ID, so
	// prefixing a per-node monotonically increasing counter with the node ID
	// is already unique across the cluster. A random suffix additionally
	// disambiguates IDs issued before and after a process restart. No
	// coordination between nodes is required, so every request is answered
	// immediately, even when the node is completely partitioned.
	var counter uint64

	n.Handle("generate", func(msg maelstrom.Message) error {
		c := atomic.AddUint64(&counter, 1)
		id := fmt.Sprintf("%s-%d-%s", n.ID(), c, randHex(8))
		return n.Reply(msg, map[string]any{
			"type": "generate_ok",
			"id":   id,
		})
	})

	if err := n.Run(); err != nil {
		log.Fatalf("node exited with error: %v", err)
	}
}
