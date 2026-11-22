package main

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// gossipInterval controls how often each node runs a full anti-entropy
// exchange with every other node. Maelstrom heals the partition before the
// final reads and gives the cluster some settling time, so a short interval
// like this converges comfortably within that window even if a node was cut
// off for the whole test.
const gossipInterval = 200 * time.Millisecond

// node implements a gossip protocol over the Maelstrom network.
type node struct {
	maelstrom *maelstrom.Node

	// mu guards rumors, a map from a canonical JSON encoding of a rumor to
	// the rumor itself. Using the encoded form as the key means we dedupe
	// cheaply no matter how the numbers were decoded on the wire.
	mu     sync.Mutex
	rumors map[string]any

	loopOnce sync.Once
}

func main() {
	n := &node{
		maelstrom: maelstrom.NewNode(),
		rumors:    make(map[string]any),
	}

	// The library handles "init" itself (it fills in our ID and the cluster
	// membership) and then delegates to our handler, which is where we kick
	// off the background gossip loop.
	n.maelstrom.Handle("init", n.handleInit)
	n.maelstrom.Handle("tell", n.handleTell)
	n.maelstrom.Handle("recall", n.handleRecall)
	n.maelstrom.Handle("neighbors", n.handleNeighbors)

	// Our own node-to-node anti-entropy message. "rs" is deliberately not a
	// name Maelstrom or the renaming proxy rewrites, so it passes through
	// the network untouched.
	n.maelstrom.Handle("gossip", n.handleGossip)

	// Never let an unhandled message type kill the node.
	n.maelstrom.Handle("error", n.handleIgnore)
	n.maelstrom.Handle("gossip_ok", n.handleIgnore)

	if err := n.maelstrom.Run(); err != nil {
		log.Fatalf("fatal: %v", err)
	}
}

// handleInit starts the periodic gossip loop.
func (n *node) handleInit(maelstrom.Message) error {
	n.startLoop()
	return nil
}

// handleTell records a rumor, pushes it straight out to every other node, and
// acknowledges the client immediately.
func (n *node) handleTell(msg maelstrom.Message) error {
	n.startLoop()

	body := bodyMap(msg)
	rumor, ok := body["rumor"]
	if !ok || rumor == nil {
		// Malformed request: still acknowledge it, but store nothing.
		return n.maelstrom.Reply(msg, map[string]any{"type": "told"})
	}

	if n.add(rumor) {
		n.sendToPeers(map[string]any{"type": "gossip", "rs": []any{rumor}})
	}
	return n.maelstrom.Reply(msg, map[string]any{"type": "told"})
}

// handleRecall returns every rumor this node knows about, in any order.
func (n *node) handleRecall(msg maelstrom.Message) error {
	n.startLoop()
	return n.maelstrom.Reply(msg, map[string]any{"type": "recalled", "rumors": n.snapshot()})
}

// handleNeighbors acknowledges the suggested topology. We gossip with the
// whole cluster, so we just note it and carry on.
func (n *node) handleNeighbors(msg maelstrom.Message) error {
	n.startLoop()
	return n.maelstrom.Reply(msg, map[string]any{"type": "neighbors_noted"})
}

// handleGossip merges the incoming rumor set, and if the sender is missing
// anything we know about, pushes the difference back. That makes every
// exchange a full bidirectional anti-entropy step: after one exchange both
// sides hold the union of their sets, and the reply is empty from then on, so
// the ping-pong dies out on its own.
func (n *node) handleGossip(msg maelstrom.Message) error {
	n.startLoop()

	incoming := asRumors(bodyMap(msg)["rs"])
	n.merge(incoming)
	if missing := n.diff(incoming); len(missing) > 0 {
		_ = n.maelstrom.Send(msg.Src, map[string]any{"type": "gossip", "rs": missing})
	}
	return nil
}

// handleIgnore swallows messages we don't care about.
func (n *node) handleIgnore(maelstrom.Message) error {
	return nil
}

// startLoop launches the periodic anti-entropy loop exactly once.
func (n *node) startLoop() {
	n.loopOnce.Do(func() {
		go n.loop()
	})
}

// loop repeatedly offers the node's full rumor set to every other node. A
// message lost to a partition simply disappears, so we just keep trying.
func (n *node) loop() {
	ticker := time.NewTicker(gossipInterval)
	defer ticker.Stop()
	for range ticker.C {
		n.sendToPeers(map[string]any{"type": "gossip", "rs": n.snapshot()})
	}
}

// sendToPeers sends the body to every node in the cluster except ourselves.
func (n *node) sendToPeers(body map[string]any) {
	self := n.maelstrom.ID()
	for _, id := range n.maelstrom.NodeIDs() {
		if id == self {
			continue
		}
		if err := n.maelstrom.Send(id, body); err != nil {
			log.Printf("send to %s: %v", id, err)
		}
	}
}

// add stores a rumor, reporting whether it was new.
func (n *node) add(rumor any) bool {
	k := keyOf(rumor)
	n.mu.Lock()
	defer n.mu.Unlock()
	if _, ok := n.rumors[k]; ok {
		return false
	}
	n.rumors[k] = rumor
	return true
}

// merge stores every rumor in vals that we don't already have.
func (n *node) merge(vals []any) {
	for _, v := range vals {
		_ = n.add(v)
	}
}

// snapshot returns all rumors we know, in arbitrary order.
func (n *node) snapshot() []any {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := make([]any, 0, len(n.rumors))
	for _, v := range n.rumors {
		out = append(out, v)
	}
	return out
}

// diff returns the rumors we hold that don't appear in vals.
func (n *node) diff(vals []any) []any {
	if len(vals) == 0 {
		return n.snapshot()
	}
	theirs := make(map[string]struct{}, len(vals))
	for _, v := range vals {
		theirs[keyOf(v)] = struct{}{}
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	var out []any
	for k, v := range n.rumors {
		if _, ok := theirs[k]; !ok {
			out = append(out, v)
		}
	}
	return out
}

// bodyMap decodes a message body as a generic JSON object.
func bodyMap(msg maelstrom.Message) map[string]any {
	var m map[string]any
	if err := json.Unmarshal(msg.Body, &m); err != nil {
		return map[string]any{}
	}
	return m
}

// asRumors coerces a decoded JSON value into a list of rumors.
func asRumors(v any) []any {
	if v == nil {
		return nil
	}
	rs, ok := v.([]any)
	if !ok {
		return nil
	}
	return rs
}

// keyOf builds the canonical key for a rumor.
func keyOf(v any) string {
	buf, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprintf("%v", v)
	}
	return string(buf)
}
