package main

import (
	"encoding/json"
	"log"
	"os"
	"sort"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()

	var mu sync.Mutex
	messages := make(map[int]bool)

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body map[string]interface{}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		valFloat, ok := body["message"].(float64)
		if !ok {
			return maelstrom.NewRPCError(maelstrom.MalformedRequest, "message not int")
		}
		val := int(valFloat)

		mu.Lock()
		messages[val] = true
		mu.Unlock()

		src := msg.Src
		nodeIDs := n.NodeIDs()
		nodeID := n.ID()

		isNode := false
		for _, id := range nodeIDs {
			if id == src {
				isNode = true
				break
			}
		}

		if !isNode && nodeID != "" && len(nodeIDs) > 0 {
			for _, id := range nodeIDs {
				if id == nodeID {
					continue
				}
				n.Send(id, map[string]interface{}{
					"type":    "broadcast",
					"message": val,
				})
			}
		}

		if _, hasMsgID := body["msg_id"]; hasMsgID {
			return n.Reply(msg, map[string]interface{}{"type": "broadcast_ok"})
		}
		return nil
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		mu.Lock()
		vals := make([]int, 0, len(messages))
		for v := range messages {
			vals = append(vals, v)
		}
		mu.Unlock()

		sort.Ints(vals)

		return n.Reply(msg, map[string]interface{}{
			"type":     "read_ok",
			"messages": vals,
		})
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		return n.Reply(msg, map[string]interface{}{"type": "topology_ok"})
	})

	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}
}
