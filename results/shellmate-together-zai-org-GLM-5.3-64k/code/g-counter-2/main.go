// A Maelstrom node that implements an eventually consistent grow-only
// counter.
//
// Each node owns one monotonic component of the counter: it adds every delta
// it acknowledges to its own component and never forgets it. The global
// counter value is the sum of all components. Nodes periodically push their
// whole view of the components (a map node-id -> component) to every peer,
// which merges it by taking the maximum per node. Because components are
// monotonic and merge by max, gossip is idempotent, commutative and can never
// count an add twice. Once the network is healed, gossip converges in a couple
// of ticks, so every node's read returns the same total.
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// gossipInterval is how often a node pushes its view of all counter
// components to its peers.
const gossipInterval = 100 * time.Millisecond

// counter holds this node's view of every node's counter component.
type counter struct {
	node *maelstrom.Node

	mu     sync.Mutex
	values map[string]int64

	startOnce sync.Once
}

func newCounter(node *maelstrom.Node) *counter {
	return &counter{node: node, values: make(map[string]int64)}
}

// total returns the sum of all known components. Callers must hold c.mu.
func (c *counter) total() int64 {
	var sum int64
	for _, v := range c.values {
		sum += v
	}
	return sum
}

// merge folds another node's view into ours. Callers must hold c.mu.
func (c *counter) merge(other map[string]int64) {
	for id, v := range other {
		if cur, ok := c.values[id]; !ok || v > cur {
			c.values[id] = v
		}
	}
}

// gossip pushes our current view of all components to every other node.
// Messages sent across a partition are simply lost, which is fine: the next
// tick resends everything.
func (c *counter) gossip() {
	c.mu.Lock()
	snapshot := make(map[string]int64, len(c.values))
	for id, v := range c.values {
		snapshot[id] = v
	}
	c.mu.Unlock()

	body := map[string]any{"type": "state", "values": snapshot}
	for _, id := range c.node.NodeIDs() {
		if id == c.node.ID() {
			continue
		}
		if err := c.node.Send(id, body); err != nil {
			log.Printf("gossip to %s failed: %v", id, err)
		}
	}
}

// start launches the periodic gossip loop. Called once the node knows its own
// ID and its peers.
func (c *counter) start() {
	c.startOnce.Do(func() {
		go func() {
			ticker := time.NewTicker(gossipInterval)
			defer ticker.Stop()
			for range ticker.C {
				c.gossip()
			}
		}()
	})
}

func main() {
	log.SetOutput(os.Stderr)

	n := maelstrom.NewNode()
	c := newCounter(n)

	// init: remember who we are and start gossiping.
	n.Handle("init", func(msg maelstrom.Message) error {
		c.mu.Lock()
		c.values[n.ID()] = 0
		c.mu.Unlock()
		c.start()
		return nil
	})

	// add: bump our own component, then ack. The add is only ever applied
	// here, so it can never be counted twice.
	n.Handle("add", func(msg maelstrom.Message) error {
		var body struct {
			Delta json.Number `json:"delta"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return fmt.Errorf("unmarshal add body: %w", err)
		}
		delta, err := body.Delta.Int64()
		if err != nil {
			return fmt.Errorf("bad delta %q: %w", body.Delta.String(), err)
		}
		if delta < 0 {
			// Not part of this workload, but be explicit rather than silently
			// acknowledging an add we didn't perform.
			return fmt.Errorf("negative delta %d", delta)
		}

		c.mu.Lock()
		c.values[n.ID()] += delta
		c.mu.Unlock()

		go c.gossip()
		return n.Reply(msg, map[string]any{"type": "add_ok"})
	})

	// read: return the sum of every component we know about.
	n.Handle("read", func(msg maelstrom.Message) error {
		go c.gossip()
		c.mu.Lock()
		total := c.total()
		c.mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "read_ok", "value": total})
	})

	// state: a peer's view of all components. Merge by max; no reply needed.
	n.Handle("state", func(msg maelstrom.Message) error {
		var body struct {
			Values map[string]int64 `json:"values"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			log.Printf("ignoring malformed gossip: %v", err)
			return nil
		}
		c.mu.Lock()
		c.merge(body.Values)
		c.mu.Unlock()
		return nil
	})

	if err := n.Run(); err != nil {
		log.Fatalf("node %s exited: %v", n.ID(), err)
	}
}
