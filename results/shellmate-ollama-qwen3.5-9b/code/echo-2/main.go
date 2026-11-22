package main

import (
	"encoding/json"
	"fmt"

	"github.com/jepsen-io/maelstrom/demo/go"
)

func handleEcho(n *go.Node, msg go.Message) error {
	var body map[string]interface{}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return fmt.Errorf("failed to parse body: %v", err)
	}

	echoValue := body["echo"]
	inReplyTo := int64(0)
	if msg.InReplyTo != "" {
		var id int64
		fmt.Sscanf(msg.InReplyTo, "%d", &id)
		inReplyTo = id
	}

	replyBody := map[string]interface{}{
		"in_reply_to": inReplyTo,
		"type":         "echo_ok",
		"echo":         echoValue,
	}

	return n.Reply(msg.Dest, msg.Type, replyBody)
}

func handleInit(n *go.Node, msg go.Message) error {
	fmt.Printf("[%s] init: %s\n", msg.Src, msg.Body)
	var body map[string]interface{}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return fmt.Errorf("failed to parse body: %v", err)
	}

	nodes := make([]string, 0, len(body["nodes"].([]interface{})))
	for _, node := range body["nodes"].([]interface{}) {
		nodes = append(nodes, node.(string))
	}
	fmt.Printf("[%s] nodes: %v\n", msg.Src, nodes)

	return n.Reply(msg.InReplyTo, msg.Type, nil)
}

func main() {
	n := go.NewNode()

	n.Handle("init", handleInit)
	n.Handle("echo", handleEcho)

	if err := n.Run(); err != nil {
		fmt.Printf("error running: %v\n", err)
	}
}
