package main

import (
	"encoding/json"
	"log"
	"os"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type server struct {
	node *maelstrom.Node

	mu     sync.RWMutex
	values map[int]struct{}
}

func main() {
	s := &server{
		node:   maelstrom.NewNode(),
		values: make(map[int]struct{}),
	}

	s.node.Handle("broadcast", func(msg maelstrom.Message) error {
		var body struct {
			Message int `json:"message"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		s.mu.Lock()
		_, exists := s.values[body.Message]
		if !exists {
			s.values[body.Message] = struct{}{}
		}
		s.mu.Unlock()

		// The cluster is small enough that a single direct hop reaches every
		// other node. Values are only forwarded by the node that originally
		// acknowledged them, so each value costs at most nodeCount-1 messages.
		if !exists {
			for _, id := range s.node.NodeIDs() {
				if id == s.node.ID() {
					continue
				}
				s.node.Send(id, map[string]any{
					"type":    "broadcast_value",
					"message": body.Message,
				})
			}
		}

		return s.node.Reply(msg, map[string]any{
			"type": "broadcast_ok",
		})
	})

	s.node.Handle("broadcast_value", func(msg maelstrom.Message) error {
		var body struct {
			Message int `json:"message"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		s.mu.Lock()
		s.values[body.Message] = struct{}{}
		s.mu.Unlock()
		return nil
	})

	s.node.Handle("read", func(msg maelstrom.Message) error {
		s.mu.RLock()
		messages := make([]int, 0, len(s.values))
		for value := range s.values {
			messages = append(messages, value)
		}
		s.mu.RUnlock()

		return s.node.Reply(msg, map[string]any{
			"type":     "read_ok",
			"messages": messages,
		})
	})

	s.node.Handle("topology", func(msg maelstrom.Message) error {
		return s.node.Reply(msg, map[string]any{
			"type": "topology_ok",
		})
	})

	if err := s.node.Run(); err != nil {
		log.Println(err)
		os.Exit(1)
	}
}
