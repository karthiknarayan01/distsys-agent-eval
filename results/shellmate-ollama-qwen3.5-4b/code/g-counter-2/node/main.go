package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/jepsen-io/maelstrom/demo/go"
)

// node represents a single node in the cluster
type node struct {
	nodeHandle maelstrom.NodeHandle
	seqKV      *maelstrom.KV
	nodeID     int
	nodeIDs    []int
}

func main() {
	err := go.Run(func(ctx context.Context, nodeHandle maelstrom.NodeHandle) error {
		var n node
		n.nodeHandle = nodeHandle
		
		// Get init message to learn about all nodes
		if msg, err := nodeHandle.Recv(); err != nil {
			return err
		} else if msg.Type == "init" {
			n.nodeIDs = make([]int, 0)
			for _, nidStr := range msg.NodeIDs() {
				id, _ := strconv.Atoi(nidStr.(string))
				n.nodeIDs = append(n.nodeIDs, id)
			}
			n.nodeID = int(msg.ID().(float64))
		}
		
		// Create seq-kv client for this node
		n.seqKV, _ = maelstrom.NewSeqKV(ctx, n.nodeID, nil)
		
		// Register handlers for add and read requests
		nodeHandle.Handle("add", func(msg maelstrom.Message) error {
			var body map[string]any
			if err := json.Unmarshal(msg.Body, &body); err != nil {
				return err
			}
			
			msgID := int(body["msg_id"].(float64))
			delta := int(body["delta"].(float64))
			
			// Each node maintains its own counter in seq-kv for grow-only semantics
			counterKey := fmt.Sprintf("counter_%d", n.nodeID)
			
			// Read current value from seq-kv
			val, err := n.seqKV.ReadInt(ctx, counterKey)
			if err != nil {
				return err
			}
			
			newVal := val + delta
			
			// CAS to atomically update (try to increment from old value)
			_, ok, err := n.seqKV.CompareAndSwap(ctx, counterKey, int(val), newVal, true)
			if err != nil {
				return err
			}
			
			// Build reply body
			replyBody := map[string]any{
				"type":         "add_ok",
				"in_reply_to":  msgID,
				"value":        newVal,
			}
			
			return json.Marshal(msg)
		})
		
		nodeHandle.Handle("read", func(msg maelstrom.Message) error {
			var body map[string]any
			if err := json.Unmarshal(msg.Body, &body); err != nil {
				return err
			}
			
			msgID := int(body["msg_id"].(float64))
			counterKey := fmt.Sprintf("counter_%d", n.nodeID)
			
			val, err := n.seqKV.ReadInt(ctx, counterKey)
			if err != nil {
				return err
			}
			
			body["in_reply_to"] = msgID
			body["value"] = val
			
			return json.Marshal(msg)
		})
		
		return nil
	})

	if err != nil {
		os.Exit(1)
	}

	return nil
}
