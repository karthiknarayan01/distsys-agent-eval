package main

import (
	"fmt"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()

	ready := make(chan struct{})
	var nodeID string
	var mu sync.Mutex
	var counter int

	n.Handle("init", func(msg maelstrom.Message) error {
		nodeID = n.ID()
		close(ready)
		return nil
	})

	n.Handle("generate", func(msg maelstrom.Message) error {
		<-ready
		mu.Lock()
		counter++
		id := fmt.Sprintf("%s-%d", nodeID, counter)
		mu.Unlock()

		return n.Reply(msg, map[string]interface{}{
			"type": "generate_ok",
			"id": id,
		})
	})

	n.Run()
}
