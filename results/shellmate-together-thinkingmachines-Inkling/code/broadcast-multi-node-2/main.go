package main

import (
	"encoding/json"
	"log"
	"os"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type NodeState struct {
	mu       sync.Mutex
	values   map[int]bool
	nodeIDs  []string
	topology map[string][]string
}

func main() {
	n := maelstrom.NewNode()
	state := &NodeState{
		values:   make(map[int]bool),
		topology: make(map[string][]string),
	}

	n.Handle("init", func(msg maelstrom.Message) error {
		var body maelstrom.InitMessageBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		state.nodeIDs = body.NodeIDs
		return nil
	})

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body map[string]interface{}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		val := int(body["message"].(float64))

		state.mu.Lock()
		state.values[val] = true
		state.mu.Unlock()

		if err := n.Reply(msg, map[string]interface{}{"type": "broadcast_ok"}); err != nil {
			return err
		}

		for _, id := range state.nodeIDs {
			if id == n.ID() {
				continue
			}
			n.Send(id, map[string]interface{}{
				"type":    "broadcast_value",
				"message": val,
			})
		}

		return nil
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		state.mu.Lock()
		vals := make([]int, 0, len(state.values))
		for v := range state.values {
			vals = append(vals, v)
		}
		state.mu.Unlock()

		return n.Reply(msg, map[string]interface{}{
			"type":      "read_ok",
			"messages":  vals,
		})
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		var body map[string]interface{}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		topo := body["topology"].(map[string]interface{})
		state.mu.Lock()
		for k, v := range topo {
			arr := make([]string, 0)
			for _, item := range v.([]interface{}) {
				arr = append(arr, item.(string))
			}
			state.topology[k] = arr
		}
		state.mu.Unlock()

		return n.Reply(msg, map[string]interface{}{"type": "topology_ok"})
	})

	n.Handle("broadcast_value", func(msg maelstrom.Message) error {
		var body map[string]interface{}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		val := int(body["message"].(float64))
		state.mu.Lock()
		state.values[val] = true
		state.mu.Unlock()
		return nil
	})

	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}
}
