package main

import (
	"encoding/json"
	"log"
	"sync"

	"github.com/jepsen-io/maelstrom/demo/go"
)

type InitMessageBody struct {
	go.MessageBody
	NodeID  string   `json:"node_id"`
	NodeIDs []string `json:"node_ids"`
}

type TxnOp struct {
	Key   int      `json:"key,omitempty"`
	Value interface{} `json:"value,omitempty"`
	Type  string   `json:"type,omitempty"`
}

type InitDoneMessageBody struct {
	go.MessageBody
	NodeID   string     `json:"node_id,omitempty"`
	NodeIDs  []string   `json:"node_ids,omitempty"`
	TxnLog   [][]byte   `json:"txn_log,omitempty"`
}

// In-memory store for read-committed isolation
var (
	storeMu sync.RWMutex
	store   = make(map[int]interface{})
)

func main() {
	n := go.NewNode()

	// Handle init message to get our node ID and list of all nodes
	n.Handle("init", func(msg go.Message) error {
		body := InitMessageBody{}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			log.Printf("failed to unmarshal init: %v", err)
			return go.NewRPCError(go.MalformedRequest, "bad body")
		}
		log.Printf("init: node_id=%q, node_ids=%v", body.NodeID, body.NodeIDs)
		
		// Save the node ID for potential use later
		currentNodeID = body.NodeID
		if currentNodeID == "" {
			// Init message might not have our ID directly
			return nil
		}

		// Save all node IDs for topology information
		nodeIDs = body.NodeIDs

		// If we have a txn_log, restore previous writes from it
		if len(body.TxnLog) > 0 {
			for _, logBytes := range body.TxnLog {
				var op TxnOp
				if err := json.Unmarshal(logBytes, &op); err != nil {
					log.Printf("failed to unmarshal txn_log entry: %v", err)
					continue
				}
				store[op.Key] = op.Value
			}
		}

		return nil
	})

	n.Handle("txn", func(msg go.Message) error {
		body := TxnOp{} // placeholder - need to parse differently
		
		var txnList []json.RawMessage
		if err := json.Unmarshal(msg.Body, &map[string]interface{}{
			"type": "txn",
			"msg_id": 0,
			"txn":   &txnList,
		}); err != nil {
			log.Printf("failed to unmarshal txn: %v", err)
			return go.NewRPCError(go.MalformedRequest, "bad body")
		}

		// Parse incoming transaction operations
		var ops []TxnOp
		for _, opRaw := range txnList {
			var op TxnOp
			if err := json.Unmarshal(opRaw, &op); err != nil {
				log.Printf("failed to unmarshal txn op: %v", err)
				continue
			}
			ops = append(ops, op)
		}

		// Process transaction - for each operation:
		// - Writes are logged and stored immediately
		// - Reads see the latest committed value (from store)
		
		results := make([]TxnOp, len(ops))
		for i, op := range ops {
			if op.Type == "r" {
				// Read returns the latest value in store (read-committed)
				storeMu.RLock()
				val, ok := store[op.Key]
				storeMu.RUnlock()
				
				results[i] = TxnOp{
					Key:   op.Key,
					Type:  "r",
					Value: val,
				}
			} else if op.Type == "w" {
				// Write is stored immediately (write is visible to other reads in this txn)
				storeMu.Lock()
				store[op.Key] = op.Value
				storeMu.Unlock()
				
				results[i] = TxnOp{
					Key:   op.Key,
					Type:  "w",
					Value: op.Value,
				}
			}
		}

		response := map[string]interface{}{
			"type":       "txn_ok",
			"in_reply_to": body.MsgID,
			"txn":        results,
		}
		
		respBytes, err := json.Marshal(response)
		if err != nil {
			log.Printf("failed to marshal response: %v", err)
			return go.NewRPCError(go.TemporarilyUnavailable, "internal error")
		}

		msg.Reply(string(respBytes))
		return nil
	})

	n.Run()
}

var (
	currentNodeID string
	nodeIDs       []string
)
