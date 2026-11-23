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
	counters := make(map[string]int)

	// gossip sends our full grow-only counter map to every other node.
	gossip := func() {
		if n.ID() == "" {
			return
		}
		mu.Lock()
		value := make(map[string]int, len(counters))
		for k, v := range counters {
			value[k] = v
		}
		mu.Unlock()

		for _, id := range n.NodeIDs() {
			if id == n.ID() {
				continue
			}
			if err := n.Send(id, map[string]any{
				"type":  "replicate",
				"value": value,
			}); err != nil {
				log.Printf("gossip send error: %v", err)
			}
		}
	}

	n.Handle("count", func(msg maelstrom.Message) error {
		var body struct {
			Visitors int `json:"visitors"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		mu.Lock()
		counters[n.ID()] += body.Visitors
		mu.Unlock()

		if err := n.Reply(msg, map[string]any{"type": "counted"}); err != nil {
			return err
		}

		// Push our state out right away so reads converge quickly.
		gossip()
		return nil
	})

	n.Handle("replicate", func(msg maelstrom.Message) error {
		var body struct {
			Value map[string]int `json:"value"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		mu.Lock()
		for k, v := range body.Value {
			if v > counters[k] {
				counters[k] = v
			}
		}
		mu.Unlock()
		return nil
	})

	n.Handle("total", func(msg maelstrom.Message) error {
		mu.Lock()
		total := 0
		for _, v := range counters {
			total += v
		}
		mu.Unlock()

		return n.Reply(msg, map[string]any{
			"type":           "total_is",
			"visitors_so_far": total,
		})
	})

	// Safety net: keep gossiping periodically so that state dropped during a
	// partition is eventually re-sent once the network heals.
	go func() {
		for {
			time.Sleep(time.Second)
			gossip()
		}
	}()

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
