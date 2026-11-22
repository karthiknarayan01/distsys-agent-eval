package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// broadcastValue is a single broadcast message body, as sent by the client.
type broadcastValue struct {
	MessageBody
	Message int `json:"message"`
}

// topologyRequest is a topology message body, as sent by the client.
type topologyRequest struct {
	MessageBody
	Topology map[string][]string `json:"topology"`
}

// topologyResponse is a topology_ok message body, as sent by the client.
type topologyResponse struct {
	MessageBody
}

// readRequest is a read message body, as sent by the client.
type readRequest struct {
	MessageBody
}

// readResponse is a read_ok message body, as sent by the client.
type readResponse struct {
	MessageBody
	Messages []any `json:"messages"`
}

var (
	node   *maelstrom.Node
	mu     sync.Mutex
	values map[int]int // msg_id -> broadcast value
)

func main() {
	node = maelstrom.NewNode()

	node.Handle("broadcast", func(msg maelstrom.Message) error {
		var body broadcastValue
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		mu.Lock()
		if values == nil {
			values = make(map[int]int)
		}
		values[body.MsgID] = body.Message
		mu.Unlock()

		return node.Reply(msg, map[string]any{
			"type":     "broadcast_ok",
			"in_reply_to": body.MsgID,
		})
	})

	node.Handle("topology", func(msg maelstrom.Message) error {
		var body topologyRequest
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		return node.Reply(msg, map[string]any{
			"type":     "topology_ok",
			"in_reply_to": body.MsgID,
		})
	})

	node.Handle("read", func(msg maelstrom.Message) error {
		var body readRequest
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		mu.Lock()
		type kv struct {
			ID   int
			Val  int
		}
		var out []kv
		for id, val := range values {
			out = append(out, kv{id, val})
		}
		mu.Unlock()

		// Sort by msg_id for deterministic output.
		for i := 1; i < len(out); i++ {
			j := i
			for j > 0 {
				if out[j].ID < out[j-1].ID {
					out[j], out[j-1] = out[j-1], out[j]
					j--
				} else {
					break
				}
			}
		}

		msgs := make([]any, len(out))
		for i, kv := range out {
			msgs[i] = kv.Val
		}

		return node.Reply(msg, map[string]any{
			"type":      "read_ok",
			"in_reply_to": msg.MsgID,
			"messages": msgs,
		})
	})

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}
}