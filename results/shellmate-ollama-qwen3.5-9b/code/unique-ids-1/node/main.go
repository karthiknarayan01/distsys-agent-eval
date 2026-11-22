package main

import (
	"encoding/json"
	"fmt"
	"os"

	m "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	node := m.NewNode()

	var nextSeq int64 = 0

	// Handle init message - the network sends this to tell us our ID and all node IDs
	node.Handle("init", func(msg m.Message) error {
		var req struct {
			ID interface{} `json:"id"`
			m.MessageBody
		}
		if err := json.Unmarshal(msg.Body, &req); err != nil {
			return fmt.Errorf("invalid init message: %w", err)
		}

		// Get our node ID from the id field in init message
		switch v := req.ID.(type) {
		case string:
			_ = v // we don't use this directly, but the source is our own node when local
		default:
			_, _ = json.Marshal(req.ID)
		}

		return nil
	})

	// Handle generate requests
	node.Handle("generate", func(msg m.Message) error {
		var req struct {
			m.MessageBody
		}
		if err := json.Unmarshal(msg.Body, &req); err != nil {
			return fmt.Errorf("invalid generate message: %w", err)
		}

		nextSeq++
		seqNum := nextSeq

		// For uniqueness: each node uses its own source ID as prefix + local sequence
		srcID := msg.Src
		idStr := fmt.Sprintf("%s-%020d", srcID, seqNum)

		body := map[string]interface{}{
			"type":        "generate_ok",
			"in_reply_to": req.InReplyTo,
			"id":          idStr,
		}

		return node.Reply(msg, body)
	})

	if err := node.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
	}
}
