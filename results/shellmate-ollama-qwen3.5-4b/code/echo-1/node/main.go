package main

import (
	"encoding/json"
	"log"

	node "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := node.NewNode()
	defer n.Run()

	var echoHandler func(node.Message, any) error = func(msg node.Message, body any) error {
		switch msg.Type {
		case "echo":
			var req map[string]any
			if err := json.Unmarshal(msg.Body, &req); err != nil {
				log.Printf("Error parsing echo request: %v", err)
				return err
			}

			var resp node.Message
			resp.Type = "echo_ok"
			resp.InReplyTo = int(req["msg_id"].(float64))
			resp.Echo = string(msg.Body) // Same echo value

			return n.Reply(&resp, nil)
		default:
			log.Printf("Unhandled message type: %s", msg.Type)
		}
		return nil
	}

	n.Init("node", node.NodeIDs())
	n.Handle("echo", echoHandler)
}
