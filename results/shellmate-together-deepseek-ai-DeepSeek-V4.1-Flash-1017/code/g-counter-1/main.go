package main

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// A grow-only counter replicated as a vector of per-node totals.  An add is
// applied to the local node's component before it is acknowledged, and nodes
// periodically exchange the whole vector, merging it component-wise by max.
// This makes adds idempotent under retransmission: a state message can be
// delivered any number of times without counting an add twice.
type counter struct {
	mu     sync.Mutex
	counts map[string]int64
}

func newCounter() *counter {
	return &counter{counts: make(map[string]int64)}
}

func (c *counter) add(node string, delta int64) {
	c.mu.Lock()
	c.counts[node] += delta
	c.mu.Unlock()
}

func (c *counter) merge(other map[string]int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for node, value := range other {
		if value > c.counts[node] {
			c.counts[node] = value
		}
	}
}

func (c *counter) snapshot() map[string]int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[string]int64, len(c.counts))
	for node, value := range c.counts {
		out[node] = value
	}
	return out
}

func (c *counter) value() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	var total int64
	for _, value := range c.counts {
		total += value
	}
	return total
}

func main() {
	n := maelstrom.NewNode()
	c := newCounter()

	broadcast := func(state map[string]int64) {
		if state == nil {
			state = c.snapshot()
		}
		for _, peer := range n.NodeIDs() {
			if peer == n.ID() {
				continue
			}
			if err := n.Send(peer, map[string]any{
				"type":   "replicate",
				"counts": state,
			}); err != nil {
				log.Printf("send replicate to %s: %v", peer, err)
			}
		}
	}

	n.Handle("init", func(msg maelstrom.Message) error {
		// Start periodic anti-entropy after the node knows its ID and peers.
		go func() {
			ticker := time.NewTicker(500 * time.Millisecond)
			defer ticker.Stop()
			for range ticker.C {
				broadcast(nil)
			}
		}()
		return nil
	})

	n.Handle("add", func(msg maelstrom.Message) error {
		var body struct {
			Delta int64 `json:"delta"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		if body.Delta < 0 {
			body.Delta = 0
		}
		c.add(n.ID(), body.Delta)
		state := c.snapshot()
		if err := n.Reply(msg, map[string]any{"type": "add_ok"}); err != nil {
			return err
		}
		// Replicate promptly as well as periodically, so a final quiet period
		// does not have to wait for the next tick.
		broadcast(state)
		return nil
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		return n.Reply(msg, map[string]any{
			"type":  "read_ok",
			"value": c.value(),
		})
	})

	n.Handle("replicate", func(msg maelstrom.Message) error {
		var body struct {
			Counts map[string]int64 `json:"counts"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		c.merge(body.Counts)
		return nil
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
