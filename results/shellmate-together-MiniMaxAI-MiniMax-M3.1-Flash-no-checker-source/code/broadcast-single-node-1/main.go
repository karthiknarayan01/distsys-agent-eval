package main

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type broadcastRequest struct {
	maelstrom.MessageBody
	Message int `json:"message"`
}

type readRequest struct {
	maelstrom.MessageBody
}

type topologyRequest struct {
	maelstrom.MessageBody
	Topology map[string][]string `json:"topology"`
}

type broadcastOK struct {
	maelstrom.MessageBody
}

type readOK struct {
	maelstrom.MessageBody
	Messages []int `json:"messages"`
}

type topologyOK struct {
	maelstrom.MessageBody
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	node := maelstrom.NewNode()

	// Requests are handled concurrently by the Maelstrom library, so the local
	// broadcast log needs a mutex. Keeping the mutex locked while replying also
	// makes sure that every broadcast acknowledged before a read completes is
	// present in that read.
	var mu sync.Mutex
	messages := make([]int, 0)

	node.Handle("broadcast", func(msg maelstrom.Message) error {
		var req broadcastRequest
		if err := json.Unmarshal(msg.Body, &req); err != nil {
			return fmt.Errorf("decode broadcast request: %w", err)
		}

		mu.Lock()
		defer mu.Unlock()

		messages = append(messages, req.Message)
		return node.Reply(msg, broadcastOK{MessageBody: maelstrom.MessageBody{Type: "broadcast_ok"}})
	})

	node.Handle("read", func(msg maelstrom.Message) error {
		var req readRequest
		if err := json.Unmarshal(msg.Body, &req); err != nil {
			return fmt.Errorf("decode read request: %w", err)
		}

		mu.Lock()
		defer mu.Unlock()

		// Copy the slice so the reply does not expose the backing array while a
		// later broadcast appends to it.
		snapshot := make([]int, len(messages))
		copy(snapshot, messages)
		return node.Reply(msg, readOK{
			MessageBody: maelstrom.MessageBody{Type: "read_ok"},
			Messages:    snapshot,
		})
	})

	node.Handle("topology", func(msg maelstrom.Message) error {
		var req topologyRequest
		if err := json.Unmarshal(msg.Body, &req); err != nil {
			return fmt.Errorf("decode topology request: %w", err)
		}

		return node.Reply(msg, topologyOK{MessageBody: maelstrom.MessageBody{Type: "topology_ok"}})
	})

	if err := node.Run(); err != nil {
		return fmt.Errorf("run node: %w", err)
	}
	return nil
}
