package main

import (
	"encoding/json"
	"log"
	"os"
	"sort"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()

	var mu sync.Mutex
	rumors := make(map[int]bool)

	n.Handle("init", func(msg maelstrom.Message) error {
		return nil
	})

	n.Handle("neighbors", func(msg maelstrom.Message) error {
		var body struct {
			maelstrom.MessageBody
			Neighbors map[string][]string `json:"neighbors"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		_ = body.Neighbors
		return n.Reply(msg, map[string]interface{}{
			"type": "neighbors_noted",
		})
	})

	n.Handle("tell", func(msg maelstrom.Message) error {
		var body struct {
			maelstrom.MessageBody
			Rumor int `json:"rumor"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		mu.Lock()
		rumors[body.Rumor] = true
		vals := make([]int, 0, len(rumors))
		for v := range rumors {
			vals = append(vals, v)
		}
		mu.Unlock()

		go gossipAll(n, vals)
		return n.Reply(msg, map[string]interface{}{
			"type": "told",
		})
	})

	n.Handle("recall", func(msg maelstrom.Message) error {
		mu.Lock()
		vals := make([]int, 0, len(rumors))
		for v := range rumors {
			vals = append(vals, v)
		}
		sort.Ints(vals)
		mu.Unlock()
		return n.Reply(msg, map[string]interface{}{
			"type": "recalled",
			"rumors": vals,
		})
	})

	n.Handle("gossip", func(msg maelstrom.Message) error {
		var body struct {
			maelstrom.MessageBody
			Values []int `json:"values"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		mu.Lock()
		changed := false
		for _, v := range body.Values {
			if !rumors[v] {
				rumors[v] = true
				changed = true
			}
		}
		vals := make([]int, 0, len(rumors))
		for v := range rumors {
			vals = append(vals, v)
		}
		mu.Unlock()

		if changed {
			go gossipAll(n, vals)
		}
		// Reply with our current set to help anti-entropy.
		go gossipBack(n, msg.Src, vals)
		return nil
	})

	// Start periodic gossip in background.
	go func() {
		for {
			time.Sleep(200 * time.Millisecond)
			mu.Lock()
			vals := make([]int, 0, len(rumors))
			for v := range rumors {
				vals = append(vals, v)
			}
			mu.Unlock()
			gossipAll(n, vals)
		}
	}()

	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}
}

func gossipAll(n *maelstrom.Node, vals []int) {
	if len(vals) == 0 {
		return
	}
	self := n.ID()
	if self == "" {
		return
	}
	nodes := n.NodeIDs()
	body := map[string]interface{}{
		"type": "gossip",
		"values": vals,
	}
	for _, node := range nodes {
		if node == self {
			continue
		}
		if err := n.Send(node, body); err != nil {
			log.Printf("Send error: %v", err)
		}
	}
}

func gossipBack(n *maelstrom.Node, dest string, vals []int) {
	if len(vals) == 0 || dest == "" {
		return
	}
	body := map[string]interface{}{
		"type": "gossip",
		"values": vals,
	}
	if err := n.Send(dest, body); err != nil {
		log.Printf("Send error: %v", err)
	}
}
