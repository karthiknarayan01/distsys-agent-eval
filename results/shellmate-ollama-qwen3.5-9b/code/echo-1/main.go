package main

import (
	"encoding/json"

	"github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	node := maelstrom.NewNode()

	node.Init("127.0.0.1")

	node.Handle("init", func(msg go.Message) error {
		body := struct {
			ID   string `json:"id"`
			Nodes []struct {
				ID     string   `json:"id"`
				Address string   `json:"address"`
			} `json:"nodes"`
		}{}

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		node.Init(body.ID, body.Nodes)
		for _, n := range body.Nodes {
			println(n.Address, n.ID)
		}
		return nil
	})

	node.Handle("echo", func(msg go.Message) error {
		var body struct {
			Type   string      `json:"type"`
			MsgID  int64       `json:"msg_id"`
			Echo   json.RawMessage `json:"echo"`
		}

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		reply := map[string]interface{}{
			"type":         "echo_ok",
			"in_reply_to": body.MsgID,
			"echo":         body.Echo,
		}

		return node.Reply(msg, reply)
	})

	if err := node.Run(); err != nil {
		panic(err)
	}
}
