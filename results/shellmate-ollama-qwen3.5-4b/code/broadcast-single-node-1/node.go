package main

import (
	"encoding/json"
	"fmt"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// BroadcastMsg is sent to this node when we have a new value to broadcast
type BroadcastMsg struct {
	Mid      int         `json:"msg_id"`
	Message  int         `json:"message"`
	Type     string      `json:"type"`
}

// ReadMsg asks for all values this node has broadcast and acknowledged
type ReadMsg struct {
	Mid   int       `json:"msg_id"`
	Type  string    `json:"type"`
}

// TopologyMsg tells the node about the cluster topology (which nodes it knows)
type TopologyMsg struct {
	Mid     int         `json:"msg_id"`
	Type    string      `json:"type"`
	Topology map[string][]string `json:"topology"`
}

func handleBroadcast(msg maelstrom.Message, values map[int]int, acks map[int]bool) {
	var data BroadcastMsg
	json.Unmarshal(msg.Body, &data)

	// Store the value - it hasn't been acknowledged yet (not in acks)
	values[data.Mid] = data.Message

	// Reply with broadcast_ok
	node.Reply(msg, map[string]int{"in_reply_to": data.Mid})

	// Mark this value as acknowledged
	acks[data.Mid] = true
}

func handleRead(msg maelstrom.Message, values map[int]int, acks map[int]bool) {
	var data ReadMsg
	json.Unmarshal(msg.Body, &data)

	// Collect all values that have been acknowledged
	type Value struct {
		Message int
		ID      int
	}
	var msgs []Value
	for id, val := range values {
		if acks[id] {
			msgs = append(msgs, Value{Message: val, ID: id})
		}
	}

	// Build the response JSON - messages is an array of all acknowledged value IDs in any order
	valuesOut := make(map[int]int)
	jsonOut, _ := json.Marshal(valuesOut)

	respBody := fmt.Sprintf(`{"type":"read_ok","in_reply_to":%d,"messages":[%s]}`, data.Mid, string(jsonOut))
	node.Send(respBody)
}

func handleTopology(msg maelstrom.Message, values map[int]int, acks map[int]bool) {
	// Reply with an empty topology response for single node cluster
	node.Reply(msg, map[string]interface{}{"in_reply_to": 1})
}

func main() {
	node := maelstrom.NewNode()

	// Keep track of all values and which ones have been acknowledged
	values := make(map[int]int)
	acks := make(map[int]bool)

	// Register message handlers - the handler will be called when message arrives
	node.Handle("broadcast", func(msg maelstrom.Message) error {
		return handleBroadcast(msg, values, acks)
	})
	node.Handle("read", func(msg maelstrom.Message) error {
		return handleRead(msg, values, acks)
	})
	node.Handle("topology", func(msg maelstrom.Message) error {
		return handleTopology(msg, values, acks)
	})

	// Start the node and process messages
	node.Run()
}
