package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// echoRequest is the body of an "echo" request. The echo value is kept as raw
// JSON so that any JSON value (including big numbers) is echoed byte-for-byte
// without losing precision or changing its representation.
type echoRequest struct {
	Type  string          `json:"type"`
	MsgID int             `json:"msg_id"`
	Echo  json.RawMessage `json:"echo"`
}

func main() {
	// Keep the simulation fast: the library logs every message to stderr by
	// default, which is unnecessary noise here.
	log.SetOutput(io.Discard)

	n := maelstrom.NewNode()

	n.Handle("echo", func(msg maelstrom.Message) error {
		var req echoRequest
		if err := json.Unmarshal(msg.Body, &req); err != nil {
			return err
		}

		echo := req.Echo
		if len(echo) == 0 {
			echo = json.RawMessage("null")
		}

		// Build the reply body by hand so the echo value is passed through
		// verbatim.
		body := json.RawMessage(fmt.Sprintf(
			`{"type":"echo_ok","in_reply_to":%d,"echo":%s}`,
			req.MsgID, string(echo),
		))

		return n.Send(msg.Src, body)
	})

	if err := n.Run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
