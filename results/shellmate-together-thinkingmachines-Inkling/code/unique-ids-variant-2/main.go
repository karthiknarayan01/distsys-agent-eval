package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync/atomic"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()

	var counter int64 = 0

	n.Handle("issue", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		val := atomic.AddInt64(&counter, 1)
		body["type"] = "issued"
		body["ticket"] = fmt.Sprintf("%s-%d", n.ID(), val)
		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}
}
