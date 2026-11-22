package main

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	node := maelstrom.NewNode()

	var mu sync.Mutex
	counter := make(map[string]int)

	node.Handle("init", func(msg maelstrom.Message) error {
		var body maelstrom.InitMessageBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		mu.Lock()
		counter[body.NodeID] = 0
		mu.Unlock()
		return node.Reply(msg, maelstrom.MessageBody{Type: "init_ok"})
	})

	node.Handle("add", func(msg maelstrom.Message) error {
		var body struct {
			maelstrom.MessageBody
			Delta int `json:"delta"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		mu.Lock()
		nodeID := node.ID()
		counter[nodeID] += body.Delta
		mu.Unlock()
		return node.Reply(msg, maelstrom.MessageBody{Type: "add_ok"})
	})

	node.Handle("replicate", func(msg maelstrom.Message) error {
		var body struct {
			maelstrom.MessageBody
			Value map[string]int `json:"value"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		mu.Lock()
		for k, v := range body.Value {
			if v > counter[k] {
				counter[k] = v
			}
		}
		mu.Unlock()
		return nil
	})

	node.Handle("read", func(msg maelstrom.Message) error {
		mu.Lock()
		sum := 0
		for _, v := range counter {
			sum += v
		}
		val := sum
		mu.Unlock()
		return node.Reply(msg, map[string]interface{}{"type":"read_ok","value":val})
	})

	go func() {
		for {
			time.Sleep(200 * time.Millisecond)
			if node.ID() == "" {
				continue
			}
			mu.Lock()
			copyMap := make(map[string]int)
			for k, v := range counter {
				copyMap[k] = v
			}
			nodeIDs := node.NodeIDs()
			mu.Unlock()

			for _, nid := range nodeIDs {
				if nid == node.ID() {
					continue
				}
				node.Send(nid, map[string]interface{}{
					"type":  "replicate",
					"value": copyMap,
				})
			}
		}
	}()

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}
}
