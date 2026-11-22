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
	rumors := make(map[int]struct{})

	n.Handle("tell", func(msg maelstrom.Message) error {
		var body struct {
			Rumor int `json:"rumor"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		mu.Lock()
		rumors[body.Rumor] = struct{}{}
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
			Items []int `json:"items"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		mu.Lock()
		for _, r := range body.Items {
			rumors[r] = struct{}{}
		}
		mu.Unlock()
		return nil
	})

	// Periodically re-broadcast everything we know to every other node.
	// Messages lost during a partition are retried forever, so once the
	// network heals every rumor eventually reaches every node.
	go func() {
		var others []string
		for {
			time.Sleep(100 * time.Millisecond)

			// Node IDs are only known after the init message has been
			// processed, so resolve them lazily on the first tick.
			if others == nil {
				for _, id := range n.NodeIDs() {
					if id != n.ID() {
						others = append(others, id)
					}
				}
			}

			mu.Lock()
			list := make([]int, 0, len(rumors))
			for r := range rumors {
				list = append(list, r)
			}
			mu.Unlock()

			if len(list) == 0 {
				continue
			}
			for _, dst := range others {
				if err := n.Send(dst, map[string]any{"type": "gossip", "items": list}); err != nil {
					log.Printf("send gossip to %s: %v", dst, err)
				}
			}
		}
	}()

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
