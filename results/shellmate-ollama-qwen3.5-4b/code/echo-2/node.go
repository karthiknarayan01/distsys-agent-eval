package main

import (
	"encoding/json"
	"fmt"

	m "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	m.Run(func(node *m.Node, init m.InitMessage) error {
		return nil
	})

	node := m.NewNode()

	msgID := int64(1)

	for {
		msgData, err := node.StdIn.ReadString('\n')
		if err != nil {
			break
		}

		var msg map[string]interface{}
		json.Unmarshal([]byte(msgData), &msg)

		msgType, _ := msg["type"].(string)

		switch msgType {
		case "echo":
			body := msg["body"].(map[string]interface{})
			echoVal := body["echo"]

			replyBody := map[string]interface{}{
				"echo": echoVal,
			}

			jsonData, _ := json.Marshal(replyBody)
			node.Stdout.WriteString(string(jsonData))
			node.Stdout.WriteString("\n")
		}
	}

	return nil
}
