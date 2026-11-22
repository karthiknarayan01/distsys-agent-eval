package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
)

// TicketGen handles ticket generation across nodes
var nodeNum   int // our node ID from init message
var tickets  map[int]string // key: msg_id, value: ticket string
var mu       sync.Mutex

func main() {
	nodeNum = myID
	tickets = make(map[int]string)
	mu.Lock()
	mu.Unlock()

	// Register handlers
	RegisterHandleInit(HandleInit)
	RegisterHandleIssue(HandleIssue)

	// Start the node - wait forever for requests
	Run()
}

// HandleInit is called when we receive an init message
func HandleInit(msg Message) error {
	var body struct{ NodeID string `json:"node_id,omitempty"` }
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	nodeNum = parseInt(body.NodeID)
	tickets = make(map[int]string)
	mu.Lock()
	mu.Unlock()
	return nil
}

// HandleIssue handles incoming ticket requests
func HandleIssue(msg Message) error {
	var req struct{ MsgID json.Number `json:"msg_id"` }
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		return err
	}

	msgID64, _ := req.MsgID.Int64()
	msgID := int(msgID64)

	mu.Lock()
	ticketStr := fmt.Sprintf("n%d-%d", nodeNum, msgID)
	tickets[msgID] = ticketStr
	mu.Unlock()

	resp := map[string]interface{}{
		"type":        "issued",
		"in_reply_to": int(req.MsgID.Int64()),
		"ticket":      ticketStr,
	}
	return nil
}
