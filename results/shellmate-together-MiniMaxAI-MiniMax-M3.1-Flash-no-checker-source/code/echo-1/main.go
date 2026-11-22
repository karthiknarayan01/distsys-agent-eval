package main

import (
	"encoding/json"
	"fmt"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type echoRequest struct {
	Type  string          `json:"type"`
	MsgID int             `json:"msg_id"`
	Echo  json.RawMessage `json:"echo"`
}

type echoResponse struct {
	Type      string          `json:"type"`
	InReplyTo int             `json:"in_reply_to"`
	Echo      json.RawMessage `json:"echo"`
}

func main() {
	node := maelstrom.NewNode()

	node.Handle("echo", func(msg maelstrom.Message) error {
		var request echoRequest
		if err := json.Unmarshal(msg.Body, &request); err != nil {
			return fmt.Errorf("decode echo request: %w", err)
		}
		if len(request.Echo) == 0 {
			return fmt.Errorf("echo request is missing the echo value")
		}

		// Send directly rather than using Node.Reply so RawMessage preserves the
		// exact JSON value (including large numbers) supplied by the client.
		return node.Send(msg.Src, echoResponse{
			Type:      "echo_ok",
			InReplyTo: request.MsgID,
			Echo:      request.Echo,
		})
	})

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}
}
