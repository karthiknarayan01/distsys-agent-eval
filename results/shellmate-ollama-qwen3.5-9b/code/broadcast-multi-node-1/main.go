package main

import (
	"encoding/json"
	"sync"

	"github.com/jepsen-io/maelstrom/demo/go"
)

// NodeState holds state for a single node in the cluster
type NodeState struct {
	myID         string           // This node's ID
	allNodeIDs   []string         // List of all node IDs in cluster  
	values       map[int64]int64  // local msg_id -> broadcast value we've seen
	mu           sync.RWMutex
}

// Global state - will be initialized by first init message
var state *NodeState = makeState()

func makeState() *NodeState {
	return &NodeState{
		values: make(map[int64]int64),
	}
}

// initHandler handles the init message and initializes node state
func initHandler(node *go.Node, msg go.Message) error {
	if msg.BodyType != "init" {
		return nil
	}

	var body struct {
		NodeID  string   `json:"node_id"`
		NodeIDs []string `json:"node_ids"`
	}
	json.Unmarshal(msg.BodyBytes, &body)

	state.myID = body.NodeID
	state.allNodeIDs = body.NodeIDs

	initOK := map[string]interface{}{
		"type":            "init_ok",
		"in_reply_to":     msg.MsgID,
		"node_id":        state.myID,
		"all_node_ids":   state.allNodeIDs,
	}
	initOKBytes, _ := json.Marshal(initOK)
	node.Reply(msg, initOKBytes)

	return nil
}

// topologyHandler handles the topology message  
func topologyHandler(node *go.Node, msg go.Message) error {
	if msg.BodyType != "topology" {
		return nil
	}

	topologyOK := map[string]interface{}{
		"type":            "topology_ok",
		"in_reply_to":     msg.MsgID,
		"node_id":         state.myID,
		"all_node_ids":    state.allNodeIDs,
	}
	topologyOKBytes, _ := json.Marshal(topologyOK)
	node.Reply(msg, topologyOKBytes)

	return nil
}

// broadcastHandler handles broadcast from client
func broadcastHandler(node *go.Node, msg go.Message) error {
	if msg.BodyType != "broadcast" {
		return nil
	}

	var body struct {
		Message int64 `json:"message"` // The value being broadcasted (msg_id in the spec)
	}
	json.Unmarshal(msg.BodyBytes, &body)

	msgID := msg.MsgID  // Use incoming msg_id from client

	state.mu.Lock()
	state.values[msgID] = body.Message
	state.mu.Unlock()

	broadcastOK := map[string]interface{}{
		"type":            "broadcast_ok",
		"in_reply_to":     msg.MsgID,
		"node_id":         state.myID,
		"value_source_id": msgID,
	}
	broadcastOKBytes, _ := json.Marshal(broadcastOK)

	// Send propagate messages to all other nodes (simple flooding for reliable delivery)
	for _, dest := range state.allNodeIDs {
		if dest != state.myID {
			propMsg := map[string]interface{}{
				"type":              "propagate",
				"msg_id":            msgID,
				"value_source_id":   msgID,
				"value_from_node_id": state.myID,
			}
			propBytes, _ := json.Marshal(propMsg)
			node.Send(dest, propBytes)
		}
	}

	node.Reply(msg, broadcastOKBytes)

	return nil
}

// readHandler handles read requests from clients
func readHandler(node *go.Node, msg go.Message) error {
	if msg.BodyType != "read" {
		return nil
	}

	state.mu.RLock()
	values := make([]int64, 0, len(state.values))
	for _, v := range state.values {
		values = append(values, v)
	}
	state.mu.RUnlock()

	readOK := map[string]interface{}{
		"type":       "read_ok",
		"in_reply_to": msg.MsgID,
		"messages":   values,
	}
	readOKBytes, _ := json.Marshal(readOK)
	node.Reply(msg, readOKBytes)

	return nil
}

func main() {
	node := go.NewNode("node")

	node.Handle("init", func(msg go.Message) error {
		return initHandler(node, msg)
	})

	node.Handle("topology", topologyHandler)
	node.Handle("broadcast", broadcastHandler)
	node.Handle("read", readHandler)

	node.Run()
}
