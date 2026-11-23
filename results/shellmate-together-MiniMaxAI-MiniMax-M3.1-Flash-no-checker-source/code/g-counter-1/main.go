package main

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// counter is a grow-only counter.  Each node owns one component of the
// counter and only ever increases that component.  Components are merged by
// taking their maximum, which makes replication safe to repeat.
type counter struct {
	mu    sync.Mutex
	parts map[string]int64

	// Requests are normally delivered once.  Remembering requests also makes
	// a repeated request harmless (it must not increment the counter twice).
	seen map[string]struct{}
}

type addRequest struct {
	Type  string `json:"type"`
	MsgID int    `json:"msg_id"`
	Delta int64  `json:"delta"`
}

type readRequest struct {
	Type  string `json:"type"`
	MsgID int    `json:"msg_id"`
}

type addOK struct {
	Type string `json:"type"`
}

type readOK struct {
	Type  string `json:"type"`
	Value int64  `json:"value"`
}

type replicate struct {
	Type  string           `json:"type"`
	Value map[string]int64 `json:"value"`
}

func newCounter() *counter {
	return &counter{
		parts: make(map[string]int64),
		seen:  make(map[string]struct{}),
	}
}

// add records a request exactly once and returns the current state.
func (c *counter) add(nodeID string, requestKey string, delta int64) (map[string]int64, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.seen[requestKey]; !ok {
		c.parts[nodeID] += delta
		c.seen[requestKey] = struct{}{}
	}
	return c.snapshotLocked(), true
}

// merge incorporates a replicated state.  The returned boolean says whether
// the local state changed.
func (c *counter) merge(remote map[string]int64) (map[string]int64, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	changed := false
	for nodeID, value := range remote {
		if value < 0 {
			continue
		}
		if value > c.parts[nodeID] {
			c.parts[nodeID] = value
			changed = true
		}
	}
	return c.snapshotLocked(), changed
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

func (c *counter) value() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()

	var total int64
	for _, part := range c.parts {
		total += part
	}
	return total
}

// gossip sends the complete state to every other node.  Sending the complete
// state makes the protocol self-healing: a lost message is repaired by the
// next round, and duplicate messages have no effect.
func gossip(n *maelstrom.Node, state map[string]int64) {
	for _, peer := range n.NodeIDs() {
		if peer == n.ID() {
			continue
		}
		if err := n.Send(peer, replicate{Type: "replicate", Value: state}); err != nil {
			log.Printf("gossip to %s failed: %v", peer, err)
		}
	}
}

func parseBody[T any](msg maelstrom.Message) (T, error) {
	var body T
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return body, err
	}
	return body, nil
}

func main() {
	n := maelstrom.NewNode()
	c := newCounter()

	n.Handle("init", func(msg maelstrom.Message) error {
		// Periodically exchange full states.  A short interval makes
		// convergence quick after a partition heals, without flooding the
		// three-node cluster.
		go func() {
			ticker := time.NewTicker(50 * time.Millisecond)
			defer ticker.Stop()
			for range ticker.C {
				gossip(n, c.snapshot())
			}
		}()
		return nil
	})

	n.Handle("add", func(msg maelstrom.Message) error {
		body, err := parseBody[addRequest](msg)
		if err != nil {
			return err
		}
		if body.Delta < 0 {
			return fmt.Errorf("negative delta %d", body.Delta)
		}

		// msg_id is unique for a client, so the pair identifies a request
		// even if that request is accidentally delivered more than once.
		requestKey := fmt.Sprintf("%s/%d", msg.Src, body.MsgID)
		state, _ := c.add(n.ID(), requestKey, body.Delta)

		if err := n.Reply(msg, addOK{Type: "add_ok"}); err != nil {
			return err
		}

		// Start propagation immediately; the periodic gossip repairs any
		// messages lost while the network is partitioned.
		go gossip(n, state)
		return nil
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		if _, err := parseBody[readRequest](msg); err != nil {
			return err
		}
		return n.Reply(msg, readOK{Type: "read_ok", Value: c.value()})
	})

	n.Handle("replicate", func(msg maelstrom.Message) error {
		body, err := parseBody[replicate](msg)
		if err != nil {
			return err
		}
		state, changed := c.merge(body.Value)
		if changed {
			// Return the merged state to the sender.  This turns an
			// acknowledgement-free exchange into fast anti-entropy.
			go gossip(n, state)
		}
		return nil
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
