package main

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// A counter keeps, for every turnstile, the number of visitors which that
// turnstile has acknowledged.  The nodes periodically exchange these small
// per-node totals.  Taking the maximum of each entry makes the exchange
// idempotent, commutative and associative, so the nodes converge as soon as
// messages can flow again.  The overall tally is simply the sum of all of the
// per-turnstile totals.
type counter struct {
	node *maelstrom.Node

	mu     sync.Mutex
	counts map[string]int // turnstile (node) id -> visitors counted there
	seen   map[string]struct{}
}

type countRequest struct {
	Type     string `json:"type"`
	Visitors int    `json:"visitors"`
	MsgID    int    `json:"msg_id"`
}

type gossip struct {
	Type   string         `json:"type"`
	Counts map[string]int `json:"counts"`
}

func newCounter(n *maelstrom.Node) *counter {
	return &counter{
		node:   n,
		counts: make(map[string]int),
		seen:   make(map[string]struct{}),
	}
}

func (c *counter) snapshot() map[string]int {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[string]int, len(c.counts))
	for k, v := range c.counts {
		out[k] = v
	}
	return out
}

func (c *counter) total() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	var sum int
	for _, v := range c.counts {
		sum += v
	}
	return sum
}

func (c *counter) handleCount(msg maelstrom.Message) error {
	var req countRequest
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		return err
	}

	c.mu.Lock()
	// Client requests are not retried by the workload, but guarding against
	// a repeated delivery makes the operation safe anyway.
	key := msg.Src + ":" + itoa(req.MsgID)
	if _, ok := c.seen[key]; !ok {
		c.seen[key] = struct{}{}
		c.counts[c.node.ID()] += req.Visitors
	}
	c.mu.Unlock()

	// Acknowledge only after the count is safely in this node's state.
	if err := c.node.Reply(msg, map[string]any{"type": "counted"}); err != nil {
		return err
	}

	// Push the update to peers immediately; ignore errors, because another
	// periodic exchange will retry the information.
	c.broadcast()
	return nil
}

func (c *counter) handleTotal(msg maelstrom.Message) error {
	return c.node.Reply(msg, map[string]any{
		"type":            "total_is",
		"visitors_so_far": c.total(),
	})
}

func (c *counter) handleGossip(msg maelstrom.Message) error {
	var g gossip
	if err := json.Unmarshal(msg.Body, &g); err != nil {
		return err
	}

	c.mu.Lock()
	for id, n := range g.Counts {
		if n > c.counts[id] {
			c.counts[id] = n
		}
	}
	c.mu.Unlock()
	return nil
}

func (c *counter) broadcast() {
	g := gossip{Type: "state", Counts: c.snapshot()}
	for _, id := range c.node.NodeIDs() {
		if id == c.node.ID() {
			continue
		}
		if err := c.node.Send(id, g); err != nil {
			log.Printf("send state to %s: %v", id, err)
		}
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [24]byte
	i := len(buf)
	neg := n < 0
	if neg {
		n = -n
	}
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

func main() {
	n := maelstrom.NewNode()
	c := newCounter(n)

	n.Handle("count", c.handleCount)
	n.Handle("total", c.handleTotal)
	n.Handle("state", c.handleGossip)

	go func() {
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		for range ticker.C {
			c.broadcast()
		}
	}()

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
