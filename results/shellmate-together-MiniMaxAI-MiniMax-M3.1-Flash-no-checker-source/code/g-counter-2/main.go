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

const (
	stateType       = "g-counter-state"
	stateRequestTyp = "g-counter-state-request"
)

// requestBody contains the fields used by client requests.  Keeping the
// request fields in one type also makes malformed requests harmless.
type requestBody struct {
	Type  string `json:"type"`
	MsgID int    `json:"msg_id"`
	Delta int64  `json:"delta"`
}

type stateBody struct {
	Type   string           `json:"type"`
	Values map[string]int64 `json:"values"`
}

type addOKBody struct {
	Type string `json:"type"`
}

type readOKBody struct {
	Type  string `json:"type"`
	Value int64  `json:"value"`
}

type requestID struct {
	client string
	msgID  int
}

// counter is a grow-only counter.  Each node owns one monotonically
// increasing component.  Components are joined by taking their maximum,
// so anti-entropy messages are idempotent and can safely be repeated.
type counter struct {
	node *maelstrom.Node

	mu     sync.Mutex
	values map[string]int64
	seen   map[requestID]struct{}

	peers []string
}

func newCounter(node *maelstrom.Node) *counter {
	return &counter{
		node:   node,
		values: make(map[string]int64),
		seen:   make(map[requestID]struct{}),
	}
}

func (c *counter) init() {
	c.mu.Lock()
	c.peers = append([]string(nil), c.node.NodeIDs()...)
	c.mu.Unlock()

	// Repeated full-state exchanges make convergence independent of which
	// anti-entropy messages happened to be lost during a partition.
	go c.gossipLoop()
}

func (c *counter) gossipLoop() {
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for range ticker.C {
		c.broadcastState()
	}
}

func (c *counter) snapshot() map[string]int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.copyValuesLocked()
}

func (c *counter) copyValuesLocked() map[string]int64 {
	out := make(map[string]int64, len(c.values))
	for node, value := range c.values {
		out[node] = value
	}
	return out
}

// merge joins a state received from another node.  max is the join operation
// for a grow-only counter.
func (c *counter) merge(in map[string]int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for node, value := range in {
		if value < 0 {
			continue
		}
		if value > c.values[node] {
			c.values[node] = value
		}
	}
}

func (c *counter) broadcastState() {
	values := c.snapshot()
	c.mu.Lock()
	peers := append([]string(nil), c.peers...)
	c.mu.Unlock()

	for _, peer := range peers {
		if peer == c.node.ID() {
			continue
		}
		if err := c.node.Send(peer, stateBody{Type: stateType, Values: values}); err != nil {
			log.Printf("g-counter: send state to %s: %v", peer, err)
		}
	}
}

func (c *counter) handleAdd(msg maelstrom.Message) error {
	var body requestBody
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return fmt.Errorf("decode add request: %w", err)
	}
	if body.Delta < 0 {
		return fmt.Errorf("negative add delta: %d", body.Delta)
	}

	id := requestID{client: msg.Src, msgID: body.MsgID}

	c.mu.Lock()
	if _, duplicate := c.seen[id]; duplicate {
		// A retry of an already accepted operation is idempotent.  In
		// particular, it must not add the delta a second time.
		c.mu.Unlock()
		return c.node.Reply(msg, addOKBody{Type: "add_ok"})
	}
	c.seen[id] = struct{}{}
	owner := c.node.ID()
	c.values[owner] += body.Delta
	c.mu.Unlock()

	// Push the new state immediately.  Periodic anti-entropy handles the
	// case where this message is lost in a partition.
	c.broadcastState()
	return c.node.Reply(msg, addOKBody{Type: "add_ok"})
}

func (c *counter) handleState(msg maelstrom.Message) error {
	var body stateBody
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return fmt.Errorf("decode counter state: %w", err)
	}
	c.merge(body.Values)
	return nil
}

func (c *counter) handleStateRequest(msg maelstrom.Message) error {
	return c.node.Reply(msg, stateBody{Type: stateType, Values: c.snapshot()})
}

// queryPeers asks every peer for its current state.  This is useful for a
// read immediately after a partition heals: even if all earlier gossip was
// lost, the peers return their current components directly.
func (c *counter) queryPeers(timeout time.Duration) {
	c.mu.Lock()
	peers := append([]string(nil), c.peers...)
	c.mu.Unlock()

	peerCount := 0
	for _, peer := range peers {
		if peer != c.node.ID() {
			peerCount++
		}
	}
	if peerCount == 0 {
		return
	}

	// The channel is buffered so a callback which arrives after the timeout
	// cannot strand a library goroutine.
	responses := make(chan map[string]int64, peerCount)
	for _, peer := range peers {
		if peer == c.node.ID() {
			continue
		}
		err := c.node.RPC(peer, map[string]any{"type": stateRequestTyp}, func(reply maelstrom.Message) error {
			var body stateBody
			if err := json.Unmarshal(reply.Body, &body); err != nil {
				return nil
			}
			select {
			case responses <- body.Values:
			default:
			}
			return nil
		})
		if err != nil {
			log.Printf("g-counter: request state from %s: %v", peer, err)
		}
	}

	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for received := 0; received < peerCount; received++ {
		select {
		case values := <-responses:
			c.merge(values)
		case <-deadline.C:
			return
		}
	}
}

func (c *counter) handleRead(msg maelstrom.Message) error {
	// A bounded wait keeps reads responsive during a partition.  When the
	// network is healthy, peers normally answer immediately.
	c.queryPeers(200 * time.Millisecond)

	values := c.snapshot()
	var total int64
	for _, value := range values {
		total += value
	}
	return c.node.Reply(msg, readOKBody{Type: "read_ok", Value: total})
}

func main() {
	node := maelstrom.NewNode()
	c := newCounter(node)

	node.Handle("init", func(maelstrom.Message) error {
		c.init()
		return nil
	})
	node.Handle("add", c.handleAdd)
	node.Handle("read", c.handleRead)
	node.Handle(stateType, c.handleState)
	node.Handle(stateRequestTyp, c.handleStateRequest)

	if err := node.Run(); err != nil {
		log.Printf("g-counter: node failed: %v", err)
		os.Exit(1)
	}
}
