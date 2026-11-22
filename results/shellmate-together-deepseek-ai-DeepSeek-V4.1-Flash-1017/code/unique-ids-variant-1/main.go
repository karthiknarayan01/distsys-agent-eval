package main

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log"
	"sync/atomic"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()

	// A random per-process token makes tickets unique even if a process is
	// restarted and its counter starts again from zero.  Node IDs alone are
	// already distinct within a Maelstrom run, but this extra component costs
	// nothing and protects against restarts.
	var tokenBytes [12]byte
	if _, err := rand.Read(tokenBytes[:]); err != nil {
		log.Fatal(err)
	}
	token := hex.EncodeToString(tokenBytes[:])

	var counter atomic.Uint64

	// Handle the workload names used by maelstrom-rename for this exercise,
	// as well as the stock unique-ids names so the program is also usable
	// without the rename wrapper.
	issue := func(msg maelstrom.Message, replyType string) error {
		seq := counter.Add(1)
		ticket := fmt.Sprintf("%s-%s-%d", n.ID(), token, seq)
		return n.Reply(msg, map[string]any{
			"type":   replyType,
			"ticket": ticket,
		})
	}

	n.Handle("issue", func(msg maelstrom.Message) error {
		return issue(msg, "issued")
	})

	n.Handle("generate", func(msg maelstrom.Message) error {
		seq := counter.Add(1)
		ticket := fmt.Sprintf("%s-%s-%d", n.ID(), token, seq)
		return n.Reply(msg, map[string]any{
			"type": "generate_ok",
			"id":   ticket,
		})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
