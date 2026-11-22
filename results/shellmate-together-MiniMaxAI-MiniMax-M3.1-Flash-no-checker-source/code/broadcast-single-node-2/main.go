package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type broadcastBody struct {
	Type    string `json:"type"`
	MsgID   int    `json:"msg_id"`
	Message int    `json:"message"`
}

type readBody struct {
	Type  string `json:"type"`
	MsgID int    `json:"msg_id"`
}

type topologyBody struct {
	Type     string              `json:"type"`
	MsgID    int                 `json:"msg_id"`
	Topology map[string][]string `json:"topology"`
}

type okBody struct {
	Type string `json:"type"`
}

type readOKBody struct {
	Type     string `json:"type"`
	Messages []int  `json:"messages"`
}

func main() {
	node := maelstrom.NewNode()

	// The lock protects the message set and also orders read responses with
	// respect to broadcast acknowledgements. Thus, whenever a read is sent,
	// every broadcast acknowledged by that point is present in its snapshot.
	var stateMu sync.Mutex
	messages := make(map[int]struct{})

	node.Handle("broadcast", func(msg maelstrom.Message) error {
		var body broadcastBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		stateMu.Lock()
		defer stateMu.Unlock()

		messages[body.Message] = struct{}{}
		return node.Reply(msg, okBody{Type: "broadcast_ok"})
	})

	node.Handle("read", func(msg maelstrom.Message) error {
		var body readBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		stateMu.Lock()
		defer stateMu.Unlock()

		snapshot := make([]int, 0, len(messages))
		for value := range messages {
			snapshot = append(snapshot, value)
		}
		return node.Reply(msg, readOKBody{Type: "read_ok", Messages: snapshot})
	})

	node.Handle("topology", func(msg maelstrom.Message) error {
		var body topologyBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		return node.Reply(msg, okBody{Type: "topology_ok"})
	})

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}
}
