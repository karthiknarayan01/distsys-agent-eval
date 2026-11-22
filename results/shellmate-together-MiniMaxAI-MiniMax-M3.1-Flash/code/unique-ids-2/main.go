package main

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// idGenerator produces IDs without communicating with any other node.
//
// An ID is [logical-time, sequence, node-id].  The node ID makes IDs from
// different nodes distinct.  Within one node, logical time never moves
// backwards and sequence is strictly increasing for IDs having the same
// logical time.  Consequently, no two IDs produced by any node can be equal.
// This remains true while the network is partitioned.
type idGenerator struct {
	mu         sync.Mutex
	nodeID     string
	logicalNow int64
	sequence   uint64
}

func (g *idGenerator) initialize(msg maelstrom.Message) error {
	var body maelstrom.InitMessageBody
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	g.mu.Lock()
	g.nodeID = body.NodeID
	g.mu.Unlock()
	return nil
}

// next returns a fresh ID. It must be called after initialization.
func (g *idGenerator) next() []any {
	g.mu.Lock()
	defer g.mu.Unlock()

	now := time.Now().UnixMilli()
	if now > g.logicalNow {
		g.logicalNow = now
		g.sequence = 0
	} else {
		// The wall clock moved backwards (or stayed still).  Keep using the
		// greatest time seen and advance the counter instead.
		g.sequence++
	}

	return []any{g.logicalNow, g.sequence, g.nodeID}
}

func main() {
	node := maelstrom.NewNode()
	generator := &idGenerator{}

	node.Handle("init", generator.initialize)
	node.Handle("generate", func(msg maelstrom.Message) error {
		id := generator.next()
		return node.Reply(msg, map[string]any{
			"type": "generate_ok",
			"id":   id,
		})
	})

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}
}
