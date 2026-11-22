package main

import (
	"context"
	"encoding/json"
	"time"

	"github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	node := maelstrom.NewNode()
	defer node.Run()

	// Handle init message - not needed, framework handles it

	// Handle count messages - add visitors to counter
	node.Handle("count", func(msg maelstrom.Message) error {
		var body map[string]interface{}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		visitors, ok := body["visitors"].(float64)
		if !ok {
			return fmt.Errorf("invalid visitors field")
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		kv := go.NewSeqKV(node)

		nodeID := node.ID()
		key := "counters:" + nodeID

		currentVal, err := kv.ReadInt(ctx, key)
		if err != nil {
			return err
		}

		newVal := currentVal + int(visitors)
		kv.Write(ctx, key, newVal)

		return nil
	})

	// Handle total messages - return sum of all counts
	node.Handle("total", func(msg maelstrom.Message) error {
		var body map[string]interface{}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		kv := go.NewSeqKV(node)

		total := 0
		for _, nid := range node.NodeIDs() {
			val, err := kv.ReadInt(ctx, "counters:"+nid)
			if err != nil {
				return err
			}
			total += val
		}

		return maelstrom.Reply(msg, map[string]interface{}{
			"type":            "total_is",
			"in_reply_to":     body["msg_id"],
			"visitors_so_far": total,
		})
	})
}
