package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	node := demogo.NewNode()

	node.Init(node.ID(), node.NodeIDs())

	// Handle init message - just acknowledge it
	node.Handle("init", func(msg demogo.Message) error {
		resp := map[string]any{
			"type":           "ok",
			"msg_id":         msg.ID(),
			"in_reply_to":    msg.Source(),
		}
		return node.Reply(msg, resp)
	})

	// Create a context for seq-kv operations
	ctx := context.Background()
	seqKV := demogo.NewSeqKV(node)

	// Handle count: increment the global counter in seq-kv
	node.Handle("count", func(msg demogo.Message) error {
		var req struct {
			Type      string `json:"type"`
			MsgID     int64  `json:"msg_id"`
			Visitors  int    `json:"visitors"`
		}
		if err := json.Unmarshal(msg.Body, &req); err != nil {
			return fmt.Errorf("unmarshal: %v", err)
		}

		currentVal, err := seqKV.ReadInt(ctx, "total")
		if err != nil {
			return fmt.Errorf("seq-kv read: %v", err)
		}
		newVal := currentVal + req.Visitors
		if err := seqKV.Write(ctx, "total", newVal); err != nil {
			return fmt.Errorf("seq-kv write: %v", err)
		}

		resp := map[string]any{
			"type":          "counted",
			"msg_id":        req.MsgID,
			"in_reply_to":   msg.Source(),
		}
		return node.Reply(msg, resp)
	})

	// Handle total: read the global counter from seq-kv
	node.Handle("total", func(msg demogo.Message) error {
		var req struct {
			Type  string `json:"type"`
			MsgID int64  `json:"msg_id"`
		}
		if err := json.Unmarshal(msg.Body, &req); err != nil {
			return fmt.Errorf("unmarshal: %v", err)
		}

		val, err := seqKV.ReadInt(ctx, "total")
		if err != nil {
			return fmt.Errorf("seq-kv read: %v", err)
		}

		resp := map[string]any{
			"type":            "total_is",
			"msg_id":          req.MsgID,
			"in_reply_to":     msg.Source(),
			"visitors_so_far": val,
		}
		return node.Reply(msg, resp)
	})

	if err := node.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "node.Run: %v\n", err)
		os.Exit(1)
	}
}
