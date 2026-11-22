package main

import (
	"encoding/json"
	"log"

	goprotobuf "github.com/jepsen-io/maelstrom/demo/go"
)

// NodeState tracks the broadcast values seen by this node
type NodeState struct {
	BroadcastMap map[int]int // msg_id -> value
}

func NewNodeState() *NodeState {
	return &NodeState{
		BroadcastMap: make(map[int]int),
	}
}

// BroadcastMsg represents a broadcast request
type BroadcastMsg struct {
	MsgID   int     `json:"msg_id"`
	Message int     `json:"message"`
}

// ReadMsg represents a read request
type ReadMsg struct {
	MsgID int `json:"msg_id"`
}

// ReadOk represents the response to a read request
type ReadOk struct {
	InReplyTo  int    `json:"in_reply_to"`
	Messages   []int  `json:"messages"`
}

// TopologyMsg represents a topology request
type TopologyMsg struct {
	MsgID     int       `json:"msg_id"`
	Topology  map[string][]string `json:"topology"`
}

// TopologyOk represents acknowledgment of a topology request
type TopologyOk struct {
	InReplyTo int `json:"in_reply_to"`
}

func main() {
	node := goprotobuf.NewNode()
	state := NewNodeState()

	// Handle init message - stores node ID and node IDs
	node.Handle("init", func(msg goprotobuf.Message) error {
		var body struct {
			NodeID  string `json:"node_id"`
			NodeIDs []string `json:"node_ids"`
		}
		err := json.Unmarshal([]byte(msg.Body), &body)
		if err != nil {
			return nil // Ignore errors for init
		}
		log.Printf("Initialized node %s with node IDs %v", body.NodeID, body.NodeIDs)
		return nil
	})

	// Handle broadcast requests - store the message and value
	node.Handle("broadcast", func(msg goprotobuf.Message) error {
		var body BroadcastMsg
		err := json.Unmarshal([]byte(msg.Body), &body)
		if err != nil {
			return nil // Ignore errors
		}
		state.BroadcastMap[body.MsgID] = body.Message
		log.Printf("Received broadcast for msg_id %d with value %d", body.MsgID, body.Message)
		return nil
	})

	// Handle read requests - return all stored values using node.Reply
	node.Handle("read", func(msg goprotobuf.Message) error {
		var body ReadMsg
		err := json.Unmarshal([]byte(msg.Body), &body)
		if err != nil {
			return nil // Ignore errors
		}

		// Return all values that have been broadcast and acknowledged
		result := []int(nil)
		for _, v := range state.BroadcastMap {
			result = append(result, v)
		}

		resp := ReadOk{
			InReplyTo:  body.MsgID,
			Messages:   result,
		}
		return node.Reply(msg, resp)
	})

	// Handle topology requests - acknowledge
	node.Handle("topology", func(msg goprotobuf.Message) error {
		var body TopologyMsg
		err := json.Unmarshal([]byte(msg.Body), &body)
		if err != nil {
			return nil // Ignore errors
		}
		log.Printf("Received topology for msg_id %d: %v", body.MsgID, body.Topology)
		return nil // Acknowledgment sent automatically by Handle/Reply
	})

	// Run the node and wait for termination signal
	return node.Run()
}
