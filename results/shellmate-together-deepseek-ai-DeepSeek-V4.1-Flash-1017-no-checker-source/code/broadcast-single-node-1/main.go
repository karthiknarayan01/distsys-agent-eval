package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// nodeState holds everything broadcast to this node.
type nodeState struct {
	mu       sync.Mutex
	messages map[int]struct{}
}

func main() {
	n := maelstrom.NewNode()
	st := &nodeState{messages: make(map[int]struct{})}

	// broadcast: remember the value, then acknowledge.
	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body struct {
			Message int `json:"message"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		st.mu.Lock()
		st.messages[body.Message] = struct{}{}
		st.mu.Unlock()

		return n.Reply(msg, map[string]any{"type": "broadcast_ok"})
	})

	// read: return every value stored so far.
	n.Handle("read", func(msg maelstrom.Message) error {
		st.mu.Lock()
		messages := make([]int, 0, len(st.messages))
		for m := range st.messages {
			messages = append(messages, m)
		}
		st.mu.Unlock()

		return n.Reply(msg, map[string]any{
			"type":     "read_ok",
			"messages": messages,
		})
	})

	// topology: accepted and acknowledged (no neighbours matter for one node).
	n.Handle("topology", func(msg maelstrom.Message) error {
		return n.Reply(msg, map[string]any{"type": "topology_ok"})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
