package main

import (
	"encoding/json"
	"log"
	"os"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// echoReply is an "echo_ok" response. Every field that comes from the request
// is kept as raw JSON so that any JSON value (numbers, strings, arrays,
// objects, booleans, null) is echoed back byte-for-byte, without being
// round-tripped through a float64/interface{} representation.
type echoReply struct {
	Type      string          `json:"type"`
	Echo      json.RawMessage `json:"echo,omitempty"`
	InReplyTo json.RawMessage `json:"in_reply_to,omitempty"`
}

func main() {
	n := maelstrom.NewNode()

	n.Handle("echo", func(msg maelstrom.Message) error {
		var req map[string]json.RawMessage
		if err := json.Unmarshal(msg.Body, &req); err != nil {
			return err
		}

		reply := echoReply{Type: "echo_ok"}
		if v, ok := req["echo"]; ok {
			reply.Echo = v
		}
		if id, ok := req["msg_id"]; ok {
			reply.InReplyTo = id
		}

		return n.Send(msg.Src, reply)
	})

	if err := n.Run(); err != nil {
		log.Printf("node error: %v", err)
		os.Exit(1)
	}
}
