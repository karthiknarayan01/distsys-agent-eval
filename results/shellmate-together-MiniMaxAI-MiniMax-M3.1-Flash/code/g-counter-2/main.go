package main

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// counter is a grow-only counter represented as a vector clock.  Each node
// only ever changes the component belonging to itself.  Merging two vectors is
// the component-wise maximum, so replicas can be exchanged in any order and
// messages can safely be lost or duplicated.
type counter struct {
	mu      sync.Mutex
	parts   map[string]int64
	started sync.Once
}

func newCounter() *counter {
	return &counter{parts: make(map[string]int64)}
}

func (c *counter) add(nodeID string, delta int64) map[string]int64 {
	c.mu.Lock()
	c.parts[nodeID] += delta
	snapshot := c.snapshotLocked()
	c.mu.Unlock()
	return snapshot
}

func (c *counter) merge(other map[string]int64) {
	c.mu.Lock()
	for nodeID, value := range other {
		if value > c.parts[nodeID] {
			c.parts[nodeID] = value
		}
	}
	c.mu.Unlock()
}

func (c *counter) value() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()

	var total int64
	for _, value := range c.parts {
		total += value
	}
	return total
}

func (c *counter) snapshotLocked() map[string]int64 {
	result := make(map[string]int64, len(c.parts))
	for nodeID, value := range c.parts {
		result[nodeID] = value
	}
	return result
}

func (c *counter) snapshot() map[string]int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.snapshotLocked()
}

type addRequest struct {
	maelstrom.MessageBody
	Delta int64 `json:"delta"`
}

type readResponse struct {
	Type  string `json:"type"`
	Value int64  `json:"value"`
}

type addResponse struct {
	Type string `json:"type"`
}

type replicateMessage struct {
	Type  string           `json:"type"`
	Value map[string]int64 `json:"value"`
}

// broadcast sends a complete vector to every other node.  Full-vector
// anti-entropy is simple and, because merge is a maximum, repeated sends
// cannot double-count an increment.
func broadcast(n *maelstrom.Node, parts map[string]int64) {
	for _, peer := range n.NodeIDs() {
		if peer == n.ID() {
			continue
		}
		if err := n.Send(peer, replicateMessage{
			Type:  "g_counter_replicate",
			Value: parts,
		}); err != nil {
			log.Printf("replication to %s failed: %v", peer, err)
		}
	}
}

func main() {
	n := maelstrom.NewNode()
	c := newCounter()

	// Once initialization has supplied the node IDs, periodically exchange
	// full vectors.  This eventually repairs any updates made while peers
	// were separated by a partition.
	n.Handle("init", func(msg maelstrom.Message) error {
		c.started.Do(func() {
			go func() {
				ticker := time.NewTicker(100 * time.Millisecond)
				defer ticker.Stop()
				for range ticker.C {
					broadcast(n, c.snapshot())
				}
			}()
		})
		return nil
	})

	n.Handle("add", func(msg maelstrom.Message) error {
		var request addRequest
		if err := json.Unmarshal(msg.Body, &request); err != nil {
			return fmt.Errorf("decode add request: %w", err)
		}
		if request.Delta < 0 {
			return fmt.Errorf("counter only supports non-negative increments")
		}

		// Update the local component before acknowledging the request.  Thus
		// every acknowledged add is represented in at least one vector.
		parts := c.add(n.ID(), request.Delta)
		if err := n.Reply(msg, addResponse{Type: "add_ok"}); err != nil {
			return err
		}

		// Also try to disseminate the update immediately.  Periodic
		// anti-entropy covers the case where this message is lost in a split.
		broadcast(n, parts)
		return nil
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		return n.Reply(msg, readResponse{
			Type:  "read_ok",
			Value: c.value(),
		})
	})

	n.Handle("g_counter_replicate", func(msg maelstrom.Message) error {
		var update replicateMessage
		if err := json.Unmarshal(msg.Body, &update); err != nil {
			return fmt.Errorf("decode replication message: %w", err)
		}
		c.merge(update.Value)
		return nil
	})

	if err := n.Run(); err != nil {
		log.Fatalf("node failed: %v", err)
	}
}
