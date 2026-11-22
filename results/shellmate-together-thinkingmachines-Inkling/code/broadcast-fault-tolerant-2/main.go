package main

import (
	"encoding/json"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

var (
	node    *maelstrom.Node
	mu      sync.Mutex
	values  map[int]struct{}
	nodeIDs []string
	nodeID  string
)

func main() {
	node = maelstrom.NewNode()
	values = make(map[int]struct{})

	node.Handle("init", func(msg maelstrom.Message) error {
		nodeID = node.ID()
		nodeIDs = node.NodeIDs()
		return nil
	})

	node.Handle("topology", func(msg maelstrom.Message) error {
		return node.Reply(msg, map[string]interface{}{"type": "topology_ok"})
	})

	node.Handle("broadcast", func(msg maelstrom.Message) error {
		var body map[string]interface{}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		val := int(body["message"].(float64))
		mu.Lock()
		values[val] = struct{}{}
		mu.Unlock()
		broadcastSync()
		return node.Reply(msg, map[string]interface{}{"type": "broadcast_ok"})
	})

	node.Handle("read", func(msg maelstrom.Message) error {
		mu.Lock()
		arr := make([]int, 0, len(values))
		for v := range values {
			arr = append(arr, v)
		}
		mu.Unlock()
		return node.Reply(msg, map[string]interface{}{"type": "read_ok", "messages": arr})
	})

	node.Handle("sync", func(msg maelstrom.Message) error {
		var body map[string]interface{}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		vals, ok := body["values"].([]interface{})
		if !ok {
			return nil
		}
		mu.Lock()
		added := false
		for _, v := range vals {
			val := int(v.(float64))
			if _, exists := values[val]; !exists {
				values[val] = struct{}{}
				added = true
			}
		}
		mu.Unlock()
		if added {
			broadcastSync()
		}
		return nil
	})

	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for range ticker.C {
			broadcastSync()
		}
	}()

	if err := node.Run(); err != nil {
		panic(err)
	}
}

func broadcastSync() {
	mu.Lock()
	arr := make([]int, 0, len(values))
	for v := range values {
		arr = append(arr, v)
	}
	ids := nodeIDs
	id := nodeID
	mu.Unlock()

	for _, nid := range ids {
		if nid == id {
			continue
		}
		node.Send(nid, map[string]interface{}{
			"type":    "sync",
			"values": arr,
		})
	}
}
