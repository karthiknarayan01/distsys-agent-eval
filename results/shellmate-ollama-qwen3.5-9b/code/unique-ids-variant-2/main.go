package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/jepsen-io/maelstrom/demo/go"
)

var counter int64
var mu sync.Mutex

func handleInit(ctx *go.Node, msg go.Message) error {
	log.Printf("Node %s initialized", ctx.ID())
	// No special reply needed for init in this simple version
	return nil
}

func handleIssue(ctx *go.Node, msg go.Message) error {
	var body struct {
		Type      string `json:"type"`
		MsgID     int64  `json:"msg_id"`
		InReplyTo int64 `json:"in_reply_to,omitempty"`
	}

	if err := json.Unmarshal(msg.Body, &body); err != nil {
		log.Printf("Error parsing issue message: %v", err)
		return err
	}

	mu.Lock()
	counter++
	ticket := fmt.Sprintf("%s-%d", ctx.ID(), counter)
	mu.Unlock()

	replyBody := map[string]any{
		"type":      "issued",
		"in_reply_to": body.InReplyTo,
		"ticket":    ticket,
	}

	ctx.Reply(msg, replyBody)
	return nil
}

func main() {
	node := go.NewNode()
	node.Handle("init", handleInit)
	node.Handle("issue", handleIssue)
	os.Exit(node.Run())
}
