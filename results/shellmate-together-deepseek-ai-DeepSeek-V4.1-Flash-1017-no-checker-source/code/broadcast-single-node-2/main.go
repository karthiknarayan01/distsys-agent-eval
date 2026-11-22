package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type body struct {
	Type     string   `json:"type"`
	MsgID    int      `json:"msg_id"`
	Message  int      `json:"message"`
	NodeID   string   `json:"node_id"`
	NodeIDs  []string `json:"node_ids"`
	Messages []int    `json:"messages,omitempty"`
}

func main() {
	n := maelstrom.NewNode()

	// Values broadcast to this node, in the order they arrived.
	messages := make([]int, 0, 1024)
	seen := make(map[int]struct{})

	// Process messages strictly one at a time, in the order they arrive on
	// stdin. This guarantees that a value is stored before its broadcast_ok
	// is written, and that any read processed afterwards contains it.
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		var msg maelstrom.Message
		if err := json.Unmarshal(line, &msg); err != nil {
			log.Printf("unmarshal message: %s", err)
			continue
		}

		var b body
		if err := json.Unmarshal(msg.Body, &b); err != nil {
			log.Printf("unmarshal body: %s", err)
			continue
		}

		switch b.Type {
		case "init":
			n.Init(b.NodeID, b.NodeIDs)
			if err := n.Reply(msg, map[string]any{"type": "init_ok"}); err != nil {
				log.Printf("reply init: %s", err)
			}

		case "topology":
			// Single node: no neighbours to track, just acknowledge.
			if err := n.Reply(msg, map[string]any{"type": "topology_ok"}); err != nil {
				log.Printf("reply topology: %s", err)
			}

		case "broadcast":
			if _, ok := seen[b.Message]; !ok {
				seen[b.Message] = struct{}{}
				messages = append(messages, b.Message)
			}
			if err := n.Reply(msg, map[string]any{"type": "broadcast_ok"}); err != nil {
				log.Printf("reply broadcast: %s", err)
			}

		case "read":
			out := make([]int, len(messages))
			copy(out, messages)
			if err := n.Reply(msg, map[string]any{
				"type":     "read_ok",
				"messages": out,
			}); err != nil {
				log.Printf("reply read: %s", err)
			}

		default:
			// Ignore anything else (e.g. replies we don't care about).
			log.Printf("ignoring message of type %q", b.Type)
		}
	}
	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "scan stdin: %s\n", err)
		os.Exit(1)
	}
}
