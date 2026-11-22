package main

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"sync/atomic"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()

	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		log.Fatalf("random read failed: %v", err)
	}
	token := hex.EncodeToString(b)

	var counter int64

	n.Handle("issue", func(msg maelstrom.Message) error {
		seq := atomic.AddInt64(&counter, 1)
		ticket := fmt.Sprintf("%s-%d", token, seq)
		return n.Reply(msg, map[string]interface{}{
			"type":    "issued",
			"ticket":  ticket,
		})
	})

	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}
}
