// Unique ID generator.
//
// Each node hands out IDs of the form "<nodeID>-<session>-<counter>", where:
//   - nodeID is unique per node in the cluster (assigned by Maelstrom at init),
//   - session is a random 64-bit value generated once per process start, so a
//     restarted process (whose counter would restart at 0) can never collide
//     with IDs it handed out in a previous life,
//   - counter is a locally incrementing integer.
//
// No coordination between nodes is required, so every request is answered even
// when the node is completely partitioned from the rest of the cluster, and
// IDs are trivially unique across the whole cluster for the whole test.
package main

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"log"
	"sync/atomic"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()

	// Random per-process session tag, guarding against restarts.
	var session uint64
	buf := make([]byte, 8)
	if _, err := rand.Read(buf); err != nil {
		// crypto/rand should never fail on Linux, but don't die over it.
		log.Printf("falling back to zero session tag: %v", err)
	} else {
		session = binary.BigEndian.Uint64(buf)
	}

	var counter uint64

	n.Handle("generate", func(msg maelstrom.Message) error {
		next := atomic.AddUint64(&counter, 1)
		id := fmt.Sprintf("%s-%016x-%d", n.ID(), session, next)
		return n.Reply(msg, map[string]any{
			"type": "generate_ok",
			"id":   id,
		})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
