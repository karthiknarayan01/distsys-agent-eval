package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	
	"github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	node := maelstrom.NewNode("node")
	defer node.Close()
	
	middleware, cleanup := maelstrom.Handle(node, HandleInitMsg, HandleGenerateMsg)
	cleanup()
	
	if err := middleware.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "middleware run error: %v\n", err)
		os.Exit(1)
	}
}

func HandleInitMsg(ctx *maelstrom.Context) bool {
	body := ctx.Body()
	if body == nil {
		ctx.Reply(nil)
		return false
	}
	
	var id interface{}
	json.Unmarshal(body, &id)
	
	nodeID := node.GetNodeID()
	
	err := Reply(ctx, nil, map[string]interface{}{
		"type":  "init_ok",
		"node":  nodeID,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "reply init error: %v\n", err)
	}
	
	return true
}

func HandleGenerateMsg(ctx *maelstrom.Context) bool {
	var msg struct {
		Type        string `json:"type"`
		MessageID   int64  `json:"msg_id"`
		Middleware  interface{} `json:"middleware,omitempty"`
	}
	
	json.Unmarshal(ctx.Body(), &msg)
	
	node := ctx.Node()
	if node == nil {
		ctx.Reply(nil)
		return false
	}
	
	var inReplyTo int64 = -1
	if msg.MessageID > 0 {
		inReplyTo = msg.MessageID
	}
	
	counterFile, err := os.OpenFile(filepath.Join("/tmp", fmt.Sprintf("node_%s_counter", node.ID())), 
		os.O_CREATE|os.O_WRONLY|os.O_SYNC, 0600)
	if err != nil {
		ctx.Reply(nil)
		return false
	}
	defer counterFile.Close()
	
	var currentCount int64
	counterData := make([]byte, 10)
	_, _ = io.ReadFull(counterFile, counterData)
	fmt.Sscanf(string(counterData), "%d", &currentCount)
	
	currentCount++
	newCount := currentCount
	
	_, _ = counterFile.WriteString(fmt.Sprintf("%d\n", newCount))
	
	id := fmt.Sprintf("%s-%d", node.ID(), newCount)
	
	err = Reply(ctx, int(inReplyTo), map[string]interface{}{
		"type":   "generate_ok",
		"node":   node.ID(),
		"id":     id,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "reply generate error: %v\n", err)
	}
	
	return true
}
