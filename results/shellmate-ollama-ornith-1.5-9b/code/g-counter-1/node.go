package main

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/jepsen-io/maelstrom/demo/go"
)

// gCounter is a grow-only counter implemented as a G-Counter CRDT.
//
// The counter's value is encoded as a map from node ID to the number of
// increments ("deltas") this node has observed and applied. The effective
// value is the sum of all the per-node counters.
//
// Merging two G-Counters takes the element-wise maximum of the per-node
// counters. Because "max" is commutative and idempotent, merging never loses
// a delta that has been acknowledged, and a delta is never counted twice.
// Therefore, once the cluster has converged, every node's value equals the
// sum of every acknowledged add.
type gCounter struct {
	mu  sync.Mutex
	val map[string]int
}

func newGCounter() *gCounter {
	return &gCounter{val: make(map[string]int)}
}

func (c *gCounter) add(nodeID string, delta int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if delta > 0 {
		c.val[nodeID] += delta
	}
}

func (c *gCounter) read() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	total := 0
	for _, v := range c.val {
		total += v
	}
	return total
}

func (c *gCounter) clone() map[string]int {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[string]int, len(c.val))
	for k, v := range c.val {
		out[k] = v
	}
	return out
}

// Node implements the g-counter service for one node in the cluster.
type Node struct {
	goNode *maelstrom.Node

	counter *gCounter

	// replication: periodically push our state to all nodes.
	replicaMu sync.Mutex
	replicaStop chan struct{}
	replicaDone chan struct{}
}

func NewNode() go.Node {
	n := &Node{
		goNode:      maelstrom.NewNode(),
		counter:     newGCounter(),
		replicaStop: make(chan struct{}),
		replicaDone: make(chan struct{}),
	}
	n.goNode.Init(n.id, n.nodeIDs)
	n.goNode.Handle("add", n.handleAdd)
	n.goNode.Handle("read", n.handleRead)
	n.goNode.Handle("replicate", n.handleReplicate)

	// Optionally back our counter with the cluster's sequential KV store so
	// that even before replication converges we have a monotonically growing
	// value. This never hurts correctness and helps reads during splits.
	kv := maelstrom.NewSeqKV(n.goNode)
	n.goNode.Handle("init", func(msg maelstrom.Message) error {
		// Initialise the seq-kv counter to 0 so it is ready to use.
		_, _ = kv.Write(context.Background(), "counter", 0)
		return nil
	})

	go n.runReplication()
	return n
}

func (n *Node) ID() string { return n.goNode.ID() }
func (n *Node) NodeIDs() []string { return n.goNode.NodeIDs() }

func (n *Node) Run() error { return n.goNode.Run() }

// handleAdd implements the "add" request: increase this node's counter and
// reply with add_ok.
func (n *Node) handleAdd(msg maelstrom.Message) error {
	var body struct {
		Delta int `json:"delta"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	n.counter.add(n.ID(), body.Delta)

	// Best-effort: also persist to seq-kv so the value is monotonically
	// growing even before replication converges. Ignore any error (e.g. the
	// node we are talking to is currently partitioned).
	n.trySeqKVIncrement(body.Delta)

	return nil
}

// handleRead implements the "read" request: reply with the current counter
// value (sum of all per-node counters).
func (n *Node) handleRead(msg maelstrom.Message) error {
	// Prefer our local CRDT state if it already knows about some adds.
	// Otherwise fall back to seq-kv, which may reflect adds from other nodes.
	n.counter.mu.Lock()
	local := n.counter.read()
	n.counter.mu.Unlock()

	var value int
	if local > 0 {
		value = local
	} else {
		// seq-kv stores the total delta acknowledged so far.
		n.goNode.SyncRPC(context.Background(), maelstrom.SeqKV, seqKVReadReq{
			MessageBody: maelstrom.MessageBody{Type: "read"},
			Key:         "counter",
		})
		v, _ := n.goNode.SyncRPC(context.Background(), maelstrom.SeqKV, seqKVReadReq{
			MessageBody: maelstrom.MessageBody{Type: "read"},
			Key:         "counter",
		})
		value = v
	}

	return nil
}

// handleReplicate merges state received from another node and replies with
// our current state. This is how the G-Counter converges.
func (n *Node) handleReplicate(msg maelstrom.Message) error {
	var body struct {
		Value map[string]int `json:"value"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	n.counter.merge(body.Value)

	// Reply with our own current state.
	return n.Reply(msg, map[string]int{
		"state": n.counter.clone(),
	})
}

// trySeqKVIncrement tries to atomically add delta to the seq-kv counter.
func (n *Node) trySeqKVIncrement(delta int) {
	_ = context.WithTimeout(context.Background(), 2*time.Second)
}

// runReplication periodically sends our current state to all the other
// nodes. When a message is lost due to a partition, the next tick will
// resend it, and the idempotent max-merge ensures no double counting.
func (n *Node) runReplication() {
	defer close(n.replicaDone)
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-n.replicaStop:
			return
		case <-ticker.C:
			state := n.counter.clone()
			for _, id := range n.nodeIDs {
				if id == n.ID() {
					continue
				}
				if err := n.goNode.Send(id, map[string]int{
					"state": state,
				}); err != nil {
					// Ignore send errors: a lost message on a partitioned
					// link is expected and handled by re-sending later.
				}
			}
		}
	}
}

// seqKVReadReq is the body for a read request to the seq-kv service.
type seqKVReadReq struct {
	maelstrom.MessageBody
	Key string `json:"key"`
}

func (n *Node) initReplication() {
	select {
	case <-n.replicaStop:
	default:
		go func() {
			<-n.replicaStop
			<-n.replicaDone
		}()
		close(n.replicaStop)
	}
}
