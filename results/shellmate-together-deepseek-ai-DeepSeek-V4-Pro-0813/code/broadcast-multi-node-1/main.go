package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type node struct {
	n  *maelstrom.Node
	mu sync.Mutex
	vs map[int]bool
}

func main() {
	nd := &node{
		n:  maelstrom.NewNode(),
		vs: make(map[int]bool),
	}

	nd.n.Handle("broadcast", nd.handleBroadcast)
	nd.n.Handle("gossip", nd.handleGossip)
	nd.n.Handle("read", nd.handleRead)
	nd.n.Handle("topology", nd.handleTopology)

	if err := nd.n.Run(); err != nil {
		log.Fatal(err)
	}
}

func (nd *node) handleBroadcast(msg maelstrom.Message) error {
	var body struct {
		Message int `json:"message"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	nd.add(body.Message)

	// Forward to all other nodes.
	nd.forward(body.Message, msg.Src)

	return nd.n.Reply(msg, map[string]any{"type": "broadcast_ok"})
}

func (nd *node) handleGossip(msg maelstrom.Message) error {
	var body struct {
		Message int `json:"message"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	if nd.add(body.Message) {
		// New value: forward to all nodes except the one we got it from.
		nd.forward(body.Message, msg.Src)
	}
	return nil
}

func (nd *node) handleRead(msg maelstrom.Message) error {
	nd.mu.Lock()
	msgs := make([]int, 0, len(nd.vs))
	for v := range nd.vs {
		msgs = append(msgs, v)
	}
	nd.mu.Unlock()

	return nd.n.Reply(msg, map[string]any{
		"type":     "read_ok",
		"messages": msgs,
	})
}

func (nd *node) handleTopology(msg maelstrom.Message) error {
	return nd.n.Reply(msg, map[string]any{"type": "topology_ok"})
}

// add inserts v into the set, returning true if it was newly added.
func (nd *node) add(v int) bool {
	nd.mu.Lock()
	defer nd.mu.Unlock()
	if nd.vs[v] {
		return false
	}
	nd.vs[v] = true
	return true
}

// forward sends v to every node except skip.
func (nd *node) forward(v int, skip string) {
	for _, id := range nd.n.NodeIDs() {
		if id == nd.n.ID() || id == skip {
			continue
		}
		if err := nd.n.Send(id, map[string]any{
			"type":    "gossip",
			"message": v,
		}); err != nil {
			log.Printf("forward to %s failed: %v", id, err)
		}
	}
}
