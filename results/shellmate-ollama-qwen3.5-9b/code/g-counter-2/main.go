package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jepsen-io/maelstrom/demo/go"
)

type AddReq struct {
	Type   string `json:"type"`
	MsgID  int    `json:"msg_id"`
	Delta  int64  `json:"delta"`
}

type ReadReq struct {
	Type   string `json:"type"`
	MsgID  int    `json:"msg_id"`
}

type AddOK struct {
	Type       string `json:"type"`
	InReplyTo  int    `json:"in_reply_to"`
	Value      int64  `json:"value"`
}

type ReadOK struct {
	Type       string `json:"type"`
	InReplyTo  int    `json:"in_reply_to"`
	Value      int64  `json:"value"`
}

func main() {
	ctx := context.Background()
	node := go.NewNode()

	// Setup seq-kv for atomic counter operations
	seqkv := go.NewSeqKV(node)

	// Handle init message
	node.Handle(go.InitType, func(msg go.Message) error {
		var initMsg go.InitMessageBody
		if err := msg.Body.Unmarshal(&initMsg); err != nil {
			return fmt.Errorf("parse init: %v", err)
		}
		
		node.Init(initMsg.NodeID, initMsg.NodeIDs)

		// Initialize seq-kv counter to 0 if it doesn't exist
		if _, err := seqkv.ReadInt(ctx, "counter"); err != nil {
			// Create the key with value 0
			if go.ErrorCode(err) == go.KeyDoesNotExist {
				seqkv.Write(ctx, "counter", int64(0))
			} else {
				return fmt.Errorf("init seq-kv: %v", err)
			}
		}

		return nil
	})

	// Handle add requests
	node.Handle(go.AddType, func(msg go.Message) error {
		var req AddReq
		if err := msg.Body.Unmarshal(&req); err != nil {
			return fmt.Errorf("parse add: %v", err)
		}

		// Use CompareAndSwap to atomically increment the counter
		current, _ := seqkv.ReadInt(ctx, "counter") // Get current value (0 if doesn't exist)
		
		// Atomically CAS: increment from current to current + delta
		err := seqkv.CompareAndSwap(ctx, "counter", int64(current), int64(current)+req.Delta, true)

		// Read the new value
		newValue, err2 := seqkv.ReadInt(ctx, "counter")
		if err != nil && err2 == nil {
			return err
		}

		// Build response body
		respBody := AddOK{
			Type:       "add_ok",
			InReplyTo:  req.MsgID,
			Value:      int64(newValue),
		}

		return node.Reply(msg, respBody)
	})

	// Handle read requests
	node.Handle(go.ReadType, func(msg go.Message) error {
		var req ReadReq
		if err := msg.Body.Unmarshal(&req); err != nil {
			return fmt.Errorf("parse read: %v", err)
		}

		current, err := seqkv.ReadInt(ctx, "counter")
		if err != nil {
			return fmt.Errorf("read counter: %v", err)
		}

		respBody := ReadOK{
			Type:       "read_ok",
			InReplyTo:  req.MsgID,
			Value:      int64(current),
		}

		return node.Reply(msg, respBody)
	})

	return node.Run()
}
