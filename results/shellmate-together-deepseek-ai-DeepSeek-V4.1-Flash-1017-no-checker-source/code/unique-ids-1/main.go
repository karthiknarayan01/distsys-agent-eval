package main

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"sync/atomic"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()

	// A per-process random prefix makes IDs unique even if a node were ever
	// restarted with a fresh counter. Within one process the atomic counter
	// ensures that no value is handed out twice.
	var salt [16]byte
	if _, err := rand.Read(salt[:]); err != nil {
		log.Fatalf("generate random prefix: %v", err)
	}
	prefix := hex.EncodeToString(salt[:])

	var counter atomic.Uint64

	n.Handle("generate", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		id := fmt.Sprintf("%s-%s-%d", n.ID(), prefix, counter.Add(1))
		return n.Reply(msg, map[string]any{
			"type": "generate_ok",
			"id":   id,
		})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
