// Command node implements Maelstrom's "echo" workload: it answers every
// "echo" request with an "echo_ok" carrying the very same "echo" value.
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// echoRequestBody is the body of a client "echo" request. The echo value is
// kept as raw JSON so that it is echoed back byte-for-byte, whatever kind of
// JSON value it is (string, number, object, array, ...).
type echoRequestBody struct {
	Type  string          `json:"type"`
	MsgID int             `json:"msg_id"`
	Echo  json.RawMessage `json:"echo"`
}

// echoResponseBody is the body we send back. Field order matches the format
// shown in the task description.
type echoResponseBody struct {
	Type      string          `json:"type"`
	InReplyTo int             `json:"in_reply_to,omitempty"`
	Echo      json.RawMessage `json:"echo,omitempty"`
}

func main() {
	n := maelstrom.NewNode()

	n.Handle("echo", func(msg maelstrom.Message) error {
		var body echoRequestBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return fmt.Errorf("unmarshal echo message body: %w", err)
		}

		reply := echoResponseBody{
			Type:      "echo_ok",
			InReplyTo: body.MsgID,
		}

		// Only include "echo" if the request actually had one, and pass it
		// through untouched.
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(msg.Body, &fields); err == nil {
			if raw, ok := fields["echo"]; ok {
				reply.Echo = append(json.RawMessage(nil), raw...)
			}
		}

		return n.Send(msg.Src, reply)
	})

	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}
}
