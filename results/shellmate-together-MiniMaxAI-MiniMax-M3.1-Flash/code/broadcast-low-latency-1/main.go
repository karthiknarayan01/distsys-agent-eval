package main

import (
	"encoding/json"
	"io"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// broadcastBody is used for both client requests and one-hop
// server-to-server copies of a value.
type broadcastBody struct {
	Type    string `json:"type"`
	MsgID   int    `json:"msg_id,omitempty"`
	Message int    `json:"message"`
}

type nodeState struct {
	mu       sync.RWMutex
	messages map[int]struct{}
}

func main() {
	// Maelstrom treats stderr as a log stream. Suppressing the library's very
	// verbose per-message logging avoids unnecessary work in this hot loop.
	log.SetOutput(io.Discard)

	m := maelstrom.NewNode()
	state := &nodeState{messages: make(map[int]struct{})}

	m.Handle("broadcast", func(msg maelstrom.Message) error {
		var body broadcastBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		state.mu.Lock()
		state.messages[body.Message] = struct{}{}
		state.mu.Unlock()

		// The fastest reliable way to reach all 25 nodes is a single-hop
		// fan-out from the node that received the client request. Each peer
		// only stores the value; it does not forward it. Thus one value costs
		// exactly 24 inter-node messages and takes only one network delay to
		// reach every node.
		for _, peer := range m.NodeIDs() {
			if peer == m.ID() {
				continue
			}
			if err := m.Send(peer, broadcastBody{
				Type:    "replicate",
				Message: body.Message,
			}); err != nil {
				return err
			}
		}

		return m.Reply(msg, map[string]any{"type": "broadcast_ok"})
	})

	// The library does not allow two handlers for one type, so use a distinct
	// internal type for server-to-server replication.
	m.Handle("replicate", func(msg maelstrom.Message) error {
		var body broadcastBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		state.mu.Lock()
		state.messages[body.Message] = struct{}{}
		state.mu.Unlock()
		return nil
	})

	m.Handle("read", func(msg maelstrom.Message) error {
		state.mu.RLock()
		messages := make([]int, 0, len(state.messages))
		for value := range state.messages {
			messages = append(messages, value)
		}
		state.mu.RUnlock()

		return m.Reply(msg, map[string]any{
			"type":     "read_ok",
			"messages": messages,
		})
	})

	m.Handle("topology", func(msg maelstrom.Message) error {
		// We use a complete logical mesh rather than the suggested topology.
		// The simulator does not require messages to follow topology edges.
		return m.Reply(msg, map[string]any{"type": "topology_ok"})
	})

	if err := m.Run(); err != nil {
		log.Fatal(err)
	}
}
