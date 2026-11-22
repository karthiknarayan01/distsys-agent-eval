package main

import (
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"

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
			return fmt.Errorf("bad broadcast message")
		}
		val := int(valFloat)
		mu.Lock()
		messages[val] = true
		mu.Unlock()
		return n.Reply(msg, map[string]interface{}{
			"type": "broadcast_ok",
		})
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
			"type":      "read_ok",
			"messages": vals,
		})
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		return n.Reply(msg, map[string]interface{}{
			"type": "topology_ok",
		})
	})

	n.Handle("init", func(msg maelstrom.Message) error {
		go func() {
			ticker := time.NewTicker(100 * time.Millisecond)
			defer ticker.Stop()
			for {
				<-ticker.C
				nodeIDs := n.NodeIDs()
				if len(nodeIDs) == 0 {
					continue
				}
				mu.Lock()
				vals := make([]int, 0, len(messages))
				for v := range messages {
					vals = append(vals, v)
				}
				mu.Unlock()
				sort.Ints(vals)
				for _, id := range nodeIDs {
					if id == n.ID() {
						continue
					}
					_ = n.Send(id, map[string]interface{}{
						"type":      "gossip",
						"messages": vals,
					})
				}
			}
		}()
		return nil
	})

	n.Handle("gossip", func(msg maelstrom.Message) error {
		var body map[string]interface{}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		arr, ok := body["messages"].([]interface{})
		if !ok {
			return fmt.Errorf("bad gossip messages")
		}
		mu.Lock()
		for _, item := range arr {
			valFloat, ok := item.(float64)
			if !ok {
				continue
			}
			val := int(valFloat)
			messages[val] = true
		}
		mu.Unlock()
		return nil
	})

	if err := n.Run(); err != nil {
		fmt.Printf("ERROR: %v\n", err)
	}
}
