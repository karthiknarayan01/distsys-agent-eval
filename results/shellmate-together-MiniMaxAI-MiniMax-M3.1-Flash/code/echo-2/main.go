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
	Type string          `json:"type"`
	Echo json.RawMessage `json:"echo"`
}

func main() {
	node := maelstrom.NewNode()

	node.Handle("echo", func(msg maelstrom.Message) error {
		var req echoRequest
		if err := json.Unmarshal(msg.Body, &req); err != nil {
			return fmt.Errorf("decode echo request: %w", err)
		}

		return node.Reply(msg, echoResponse{
			Type: "echo_ok",
			Echo: req.Echo,
		})
	})

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}
}
