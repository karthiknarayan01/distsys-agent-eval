package main

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// counter is a state-based grow-only counter CRDT: a map of node id ->
// number of increments applied on that node. Merging takes the element-wise
// maximum, which is idempotent, commutative and associative, so the state is
// monotone no matter in which order (or how often) updates arrive.
type counter struct {
	mu     sync.Mutex
	counts map[string]int
}

func newCounter() *counter {
	return &counter{counts: make(map[string]int)}
}

// add increments the local node's entry. Only this node ever writes its own
// entry, so a delta can never be counted twice.
func (c *counter) add(nodeID string, delta int) {
	c.mu.Lock()
	c.counts[nodeID] += delta
	c.mu.Unlock()
}

// merge takes the element-wise max of other into c. Returns true when c
// gained information (i.e. some entry grew).
func (c *counter) merge(other map[string]int) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	changed := false
	for k, v := range other {
		if v > c.counts[k] {
			c.counts[k] = v
			changed = true
		}
	}
	return changed
}

func (c *counter) value() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	sum := 0
	for _, v := range c.counts {
		sum += v
	}
	return sum
}

func (c *counter) snapshot() map[string]int {
	c.mu.Lock()
	defer c.mu.Unlock()
	m := make(map[string]int, len(c.counts))
	for k, v := range c.counts {
		m[k] = v
	}
	return m
}

type stateBody struct {
	Type   string         `json:"type"`
	Counts map[string]int `json:"counts"`
}

// gossipInterval is how often each node pushes its full state to its peers.
const gossipInterval = 40 * time.Millisecond

// gatherTimeout bounds how long a read waits for the other nodes' state.
const gatherTimeout = 150 * time.Millisecond

func main() {
	n := maelstrom.NewNode()
	c := newCounter()

	peers := func() []string {
		out := make([]string, 0, len(n.NodeIDs()))
		for _, id := range n.NodeIDs() {
			if id != n.ID() {
				out = append(out, id)
			}
		}
		return out
	}

	// Push our full state to every peer. Fire and forget: if the network is
	// split the message is simply lost, and the next round will carry the
	// (now larger) state.
	gossip := func() {
		snap := c.snapshot()
		for _, p := range peers() {
			_ = n.Send(p, stateBody{Type: "state", Counts: snap})
		}
	}

	// Pull every peer's state, concurrently, bounded by gatherTimeout.
	gather := func() {
		ctx, cancel := context.WithTimeout(context.Background(), gatherTimeout)
		defer cancel()

		var wg sync.WaitGroup
		for _, p := range peers() {
			wg.Add(1)
			go func(p string) {
				defer wg.Done()
				ch := make(chan maelstrom.Message, 1)
				err := n.RPC(p, map[string]any{"type": "gather"}, func(m maelstrom.Message) error {
					select {
					case ch <- m:
					default:
					}
					return nil
				})
				if err != nil {
					return
				}
				select {
				case m := <-ch:
					var body stateBody
					if err := json.Unmarshal(m.Body, &body); err == nil && body.Counts != nil {
						c.merge(body.Counts)
					}
				case <-ctx.Done():
				}
			}(p)
		}
		wg.Wait()
	}

	n.Handle("add", func(msg maelstrom.Message) error {
		var body struct {
			Delta int `json:"delta"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		c.add(n.ID(), body.Delta)
		// Tell the others right away; the add is already durable in our own
		// state, so it is safe to acknowledge immediately.
		snap := c.snapshot()
		for _, p := range peers() {
			_ = n.Send(p, stateBody{Type: "state", Counts: snap})
		}
		return n.Reply(msg, map[string]any{"type": "add_ok"})
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		// Refresh from peers so that (once the network has healed) a read
		// reflects every acknowledged add.
		gather()
		return n.Reply(msg, map[string]any{"type": "read_ok", "value": c.value()})
	})

	// Gossip push from a peer.
	n.Handle("state", func(msg maelstrom.Message) error {
		var body stateBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return nil
		}
		if c.merge(body.Counts) {
			// We know something the sender may not: push it straight back.
			snap := c.snapshot()
			_ = n.Send(msg.Src, stateBody{Type: "state", Counts: snap})
		}
		return nil
	})

	// Gossip pull from a peer (or from ourselves, never).
	n.Handle("gather", func(msg maelstrom.Message) error {
		return n.Reply(msg, map[string]any{
			"type":   "gather_ok",
			"counts": c.snapshot(),
		})
	})

	n.Handle("init", func(msg maelstrom.Message) error {
		go func() {
			ticker := time.NewTicker(gossipInterval)
			defer ticker.Stop()
			for range ticker.C {
				gossip()
			}
		}()
		return nil
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
