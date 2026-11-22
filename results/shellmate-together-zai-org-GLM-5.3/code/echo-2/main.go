// Command node implements Maelstrom's "echo" workload: it answers every
// "echo" request with an "echo_ok" carrying the same "echo" value.
package main

import (
	"encoding/json"
	"fmt"
	"os"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// echoBody is the request body of an "echo" message. The "echo" field is kept
// as raw JSON so that any JSON value (string, number, object, ...) can be
// echoed back byte-for-byte.
type echoBody struct {
	MsgID int             `json:"msg_id"`
	Echo  json.RawMessage `json:"echo"`
}

// echoOKBody is the reply body sent back for an "echo" request.
type echoOKBody struct {
	Type      string          `json:"type"`
	InReplyTo int             `json:"in_reply_to"`
	Echo      json.RawMessage `json:"echo"`
}

func main() {
	n := maelstrom.NewNode()

	// Register a handler for the "echo" message that responds with an
	// "echo_ok" carrying the same "echo" value.
	n.Handle("echo", func(msg maelstrom.Message) error {
		var body echoBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return fmt.Errorf("unmarshal echo message body: %w", err)
		}

		// Build the reply body ourselves (instead of Node.Reply) so the
		// "echo" value is never re-parsed and its exact JSON is preserved.
		resp, err := json.Marshal(echoOKBody{
			Type:      "echo_ok",
			InReplyTo: body.MsgID,
			Echo:      body.Echo,
		})
		if err != nil {
			return fmt.Errorf("marshal echo_ok body: %w", err)
		}

		return n.Send(msg.Src, json.RawMessage(resp))
	})

	// Execute the node's message loop. This will run until STDIN is closed.
	if err := n.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %s\n", err)
		os.Exit(1)
	}
}
