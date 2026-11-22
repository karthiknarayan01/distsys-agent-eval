package main

import (
	"context"
	"log"

	"github.com/jepsen-io/maelstrom/demo/go"
)

func handleInit(ctx context.Context, node go.Node, body map[string]interface{}) {
	node.Reply(ctx, go.Body{Type: "init_ok"})
}

type addReq struct {
	Type      string `json:"type"`
	InReplyTo int64  `json:"in_reply_to,omitempty"`
	Delta     int64  `json:"delta"`
}

func handleAdd(ctx context.Context, node go.Node, body map[string]interface{}) {
	var req addReq
	
	// Extract message ID from the request
	msgID := -1
	if val, ok := body["msg_id"]; ok {
		switch v := val.(type) {
		case float64: // json unmarshals numbers as float64
			msgID = int64(v)
		case int64:
			msgID = v
		case int:
			msgID = int64(v)
		}
	}

	delta := req.Delta
	if val, ok := body["delta"]; ok {
		switch d := val.(type) {
		case float64:
			delta = int64(d)
		case int64:
			delta = d
		case int:
			delta = int64(d)
		}
	}

	seqKV := node.NewSeqKV(node.ID())
	key := "counter"
	
	// Try to read current value first, then CAS to new value
	currentVal := int64(0)
	var err error
	currentVal, err = seqKV.ReadInt(ctx, key)
	if err != nil {
		log.Printf("Read error: %v", err)
	}
	
	newVal := currentVal + delta
	
	// Use CompareAndSwap for atomicity
	_, err = seqKV.CompareAndSwap(ctx, key, newVal, newVal, true)
	if err != nil {
		log.Printf("CAS error: %v", err)
	}

	// Write the new value as the source of truth
	_, err = seqKV.Write(ctx, key, newVal)
	if err != nil {
		log.Printf("Write error: %v", err)
		node.Reply(ctx, go.Body{Type: "err", Body: map[string]interface{}{"error": err.Error()}})
		return
	}

	node.Reply(ctx, go.Body{
		Type:   "add_ok",
		MsgID:  msgID,
		Delta:  delta,
	})
}

func handleRead(ctx context.Context, node go.Node, body map[string]interface{}) {
	seqKV := node.NewSeqKV(node.ID())
	
	key := "counter"
	value, err := seqKV.ReadInt(ctx, key)
	if err != nil {
		log.Printf("Read error: %v", err)
		node.Reply(ctx, go.Body{Type: "err", Body: map[string]interface{}{"error": err.Error()}})
		return
	}

	msgID := -1
	if val, ok := body["msg_id"]; ok {
		switch v := val.(type) {
		case float64:
			msgID = int64(v)
		case int64:
			msgID = v
		case int:
			msgID = int64(v)
		}
	}

	node.Reply(ctx, go.Body{
		Type:      "read_ok",
		MsgID:     msgID,
		InReplyTo: -1,
		Value:     value,
	})
}

func main() {
	ctx := context.Background()
	
	node := maelstrom.NewNode()

	maelstrom.Handle(node, "init", handleInit)
	maelstrom.Handle(node, "add", handleAdd)
	maelstrom.Handle(node, "read", handleRead)

	if err := node.Run(); err != nil {
		log.Fatalf("node.Run() failed: %v", err)
	}
}
