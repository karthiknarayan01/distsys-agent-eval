package main

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// counterNode keeps a grow-only counter as a map from node ID to that node's
// contribution. Counts are merged by taking the maximum for each node, which
// makes replication idempotent and safe to retry.
type counterNode struct {
	node   *maelstrom.Node
	mu     sync.Mutex
	counts map[string]int
}

type addRequest struct {
	Delta int `json:"delta"`
}

type replicateRequest struct {
	Counters map[string]int `json:"counters"`
}

func (c *counterNode) merge(counters map[string]int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for id, value := range counters {
		if value > c.counts[id] {
			c.counts[id] = value
		}
	}
}

func (c *counterNode) snapshot() map[string]int {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[string]int, len(c.counts))
	for id, value := range c.counts {
		out[id] = value
	}
	return out
}

func (c *counterNode) value() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	total := 0
	for _, value := range c.counts {
		total += value
	}
	return total
}

// replicateLoop periodically pushes the complete counter state to every peer.
// Lost messages during a partition are simply retried on the next tick.
func (c *counterNode) replicateLoop() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for range ticker.C {
		state := c.snapshot()
		for _, peer := range c.node.NodeIDs() {
			if peer == c.node.ID() {
				continue
			}
			body := map[string]any{
				"type":     "replicate",
				"counters": state,
			}
			if err := c.node.Send(peer, body); err != nil {
				log.Printf("replicate to %s: %v", peer, err)
			}
		}
	}
}

func main() {
	n := maelstrom.NewNode()
	c := &counterNode{
		node:   n,
		counts: make(map[string]int),
	}

	n.Handle("init", func(msg maelstrom.Message) error {
		go c.replicateLoop()
		return nil
	})

	n.Handle("add", func(msg maelstrom.Message) error {
		var req addRequest
		if err := json.Unmarshal(msg.Body, &req); err != nil {
			return err
		}
		if req.Delta < 0 {
			// The workload only sends non-negative deltas. Be defensive rather
			// than corrupting a grow-only counter if one ever arrives.
			req.Delta = 0
		}

		c.mu.Lock()
		c.counts[n.ID()] += req.Delta
		c.mu.Unlock()

		return n.Reply(msg, map[string]any{"type": "add_ok"})
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		return n.Reply(msg, map[string]any{
			"type":  "read_ok",
			"value": c.value(),
		})
	})

	n.Handle("replicate", func(msg maelstrom.Message) error {
		var req replicateRequest
		if err := json.Unmarshal(msg.Body, &req); err != nil {
			return err
		}
		c.merge(req.Counters)
		return nil
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
