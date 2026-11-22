package main

import (
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type echoRequest struct {
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
			return err
		}

		// Send the response directly so RawMessage preserves every JSON value
		// exactly, including large numbers.
		return node.Send(msg.Src, echoResponse{
			Type:      "echo_ok",
			InReplyTo: request.MsgID,
			Echo:      request.Echo,
		})
	})

	if err := node.Run(); err != nil {
		log.Fatalf("node failed: %v", err)
	}
}
