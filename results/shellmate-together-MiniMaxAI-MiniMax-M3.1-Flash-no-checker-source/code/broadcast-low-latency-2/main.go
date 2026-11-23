package main

import (
	"encoding/json"
	"log"
	"os"
	"sort"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type broadcastBody struct {
	Type    string `json:"type"`
	Message int    `json:"message"`
}

type readBody struct {
	Type string `json:"type"`
}

type state struct {
	mu       sync.RWMutex
	messages map[int]struct{}
}

func main() {
	n := maelstrom.NewNode()
	s := &state{messages: make(map[int]struct{})}

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body broadcastBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		s.mu.Lock()
		s.messages[body.Message] = struct{}{}
		s.mu.Unlock()

		// A one-hop fanout reaches every node after a single network delay.
		// There are only N-1 destinations, so this remains below the traffic
		// budget while avoiding all multi-hop forwarding.
		for _, peer := range n.NodeIDs() {
			if peer == n.ID() {
				continue
			}
			if err := n.Send(peer, broadcastBody{
				Type:    "gossip",
				Message: body.Message,
			}); err != nil {
				return err
			}
		}

		return n.Reply(msg, map[string]any{"type": "broadcast_ok"})
	})

	n.Handle("gossip", func(msg maelstrom.Message) error {
		var body broadcastBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		s.mu.Lock()
		s.messages[body.Message] = struct{}{}
		s.mu.Unlock()
		return nil
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		s.mu.RLock()
		messages := make([]int, 0, len(s.messages))
		for value := range s.messages {
			messages = append(messages, value)
		}
		s.mu.RUnlock()
		sort.Ints(messages)
		return n.Reply(msg, map[string]any{
			"type":     "read_ok",
			"messages": messages,
		})
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		return n.Reply(msg, map[string]any{"type": "topology_ok"})
	})

	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}
}
