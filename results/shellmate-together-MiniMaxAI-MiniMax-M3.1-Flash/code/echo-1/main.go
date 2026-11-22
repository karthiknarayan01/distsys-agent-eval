package main

import (
	"encoding/json"
	"fmt"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// echoRequest is deliberately decoded field by field so that the echo value can
// be retained as raw JSON. RawMessage preserves arbitrary JSON values (including
// large numbers) without converting them through float64.
type echoRequest struct {
	Type  string          `json:"type"`
	MsgID int             `json:"msg_id"`
	Echo  json.RawMessage `json:"echo"`
}

type echoReply struct {
	Type      string          `json:"type"`
	InReplyTo int             `json:"in_reply_to"`
	Echo      json.RawMessage `json:"echo"`
}

func main() {
	node := maelstrom.NewNode()

	node.Handle("echo", func(msg maelstrom.Message) error {
		var req echoRequest
		if err := json.Unmarshal(msg.Body, &req); err != nil {
			return fmt.Errorf("decode echo request: %w", err)
		}
		if req.Type != "echo" {
			return fmt.Errorf("unexpected message type %q", req.Type)
		}
		if len(req.Echo) == 0 {
			return fmt.Errorf("echo request has no echo value")
		}

		// Send directly so json.RawMessage is embedded in the response without
		// a round trip through map[string]any (which could change numbers).
		return node.Send(msg.Src, echoReply{
			Type:      "echo_ok",
			InReplyTo: req.MsgID,
			Echo:      req.Echo,
		})
	})

	if err := node.Run(); err != nil {
		log.Fatalf("node stopped: %v", err)
	}
}
