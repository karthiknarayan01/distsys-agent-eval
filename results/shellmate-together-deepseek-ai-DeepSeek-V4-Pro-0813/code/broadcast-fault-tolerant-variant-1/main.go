package main

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()

	var mu sync.Mutex
	rumors := make(map[int]bool)

	n.Handle("tell", func(msg maelstrom.Message) error {
		var body struct {
			Rumor int `json:"rumor"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		mu.Lock()
		rumors[body.Rumor] = true
		mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "told"})
	})

	n.Handle("recall", func(msg maelstrom.Message) error {
		mu.Lock()
		list := make([]int, 0, len(rumors))
		for r := range rumors {
			list = append(list, r)
		}
		mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "recalled", "rumors": list})
	})

	n.Handle("neighbors", func(msg maelstrom.Message) error {
		return n.Reply(msg, map[string]any{"type": "neighbors_noted"})
	})

	n.Handle("gossip", func(msg maelstrom.Message) error {
		var body struct {
			Rumors []int `json:"rumors"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		mu.Lock()
		for _, r := range body.Rumors {
			rumors[r] = true
		}
		mu.Unlock()
		return nil
	})

	// Periodically gossip the full set of known rumors to every other node.
	// Messages lost during a partition are simply retried on the next tick,
	// so once the network heals every rumor eventually reaches every node.
	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for range ticker.C {
			mu.Lock()
			list := make([]int, 0, len(rumors))
			for r := range rumors {
				list = append(list, r)
			}
			mu.Unlock()

			for _, id := range n.NodeIDs() {
				if id == n.ID() {
					continue
				}
				n.Send(id, map[string]any{"type": "gossip", "rumors": list})
			}
		}
	}()

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
