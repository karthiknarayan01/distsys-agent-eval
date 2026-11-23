package main

import (
	"encoding/json"
	"log"
	"sort"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// The client-visible protocol is deliberately small.  Nodes exchange their
// complete rumor sets using rumor_sync messages.  A complete-set exchange is
// an anti-entropy protocol: messages lost during a partition are simply
// retried after the partition heals.
const syncInterval = 50 * time.Millisecond

type tellBody struct {
	Type  string `json:"type"`
	MsgID int    `json:"msg_id"`
	Rumor int64  `json:"rumor"`
}

type recallBody struct {
	Type  string `json:"type"`
	MsgID int    `json:"msg_id"`
}

type recalledBody struct {
	Type   string  `json:"type"`
	Rumors []int64 `json:"rumors"`
}

type neighborsBody struct {
	Type      string              `json:"type"`
	MsgID     int                 `json:"msg_id"`
	Neighbors map[string][]string `json:"neighbors"`
}

type syncBody struct {
	Type   string  `json:"type"`
	Rumors []int64 `json:"rumors"`
}

type gossip struct {
	mu     sync.RWMutex
	rumors map[int64]struct{}
	node   *maelstrom.Node
	peers  []string
}

func newGossip(node *maelstrom.Node) *gossip {
	return &gossip{
		rumors: make(map[int64]struct{}),
		node:   node,
	}
}

// add stores rumors and reports whether at least one was new.
func (g *gossip) add(rumors []int64) bool {
	g.mu.Lock()
	defer g.mu.Unlock()

	changed := false
	for _, rumor := range rumors {
		if _, ok := g.rumors[rumor]; !ok {
			g.rumors[rumor] = struct{}{}
			changed = true
		}
	}
	return changed
}

// snapshot returns a stable, sorted copy of the rumors currently known.
func (g *gossip) snapshot() []int64 {
	g.mu.RLock()
	out := make([]int64, 0, len(g.rumors))
	for rumor := range g.rumors {
		out = append(out, rumor)
	}
	g.mu.RUnlock()

	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// sendAll sends our current state to every other node.  Failed or dropped
// sends are harmless: the periodic anti-entropy round sends the state again.
func (g *gossip) sendAll() {
	rumors := g.snapshot()
	if len(rumors) == 0 {
		return
	}

	body := syncBody{Type: "rumor_sync", Rumors: rumors}
	for _, peer := range g.peers {
		if err := g.node.Send(peer, body); err != nil {
			log.Printf("sync to %s failed: %v", peer, err)
		}
	}
}

func main() {
	node := maelstrom.NewNode()
	g := newGossip(node)

	node.Handle("init", func(msg maelstrom.Message) error {
		// NodeIDs is populated by the library before this handler runs.
		for _, id := range node.NodeIDs() {
			if id != node.ID() {
				g.peers = append(g.peers, id)
			}
		}

		// Keep retrying complete-state exchanges.  This is what makes the
		// protocol tolerate arbitrarily long partitions: once connectivity is
		// restored, the next exchange carries every rumor missed during the
		// split.
		go func() {
			ticker := time.NewTicker(syncInterval)
			defer ticker.Stop()
			for range ticker.C {
				g.sendAll()
			}
		}()
		return nil
	})

	node.Handle("neighbors", func(msg maelstrom.Message) error {
		// The suggested topology is advisory; this implementation talks to
		// every node directly, so no topology state is required.
		return node.Reply(msg, map[string]any{"type": "neighbors_noted"})
	})

	node.Handle("tell", func(msg maelstrom.Message) error {
		var body tellBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// Store before acknowledging, so an acknowledged rumor can never be
		// forgotten by this node.
		g.add([]int64{body.Rumor})

		// Answer the client promptly.  Dissemination happens in parallel and
		// is retried by the anti-entropy loop.
		if err := node.Reply(msg, map[string]any{"type": "told"}); err != nil {
			return err
		}

		g.sendAll()
		return nil
	})

	node.Handle("recall", func(msg maelstrom.Message) error {
		return node.Reply(msg, recalledBody{
			Type:   "recalled",
			Rumors: g.snapshot(),
		})
	})

	node.Handle("rumor_sync", func(msg maelstrom.Message) error {
		var body syncBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		if g.add(body.Rumors) {
			// Return our (now merged) state immediately.  Besides accelerating
			// ordinary gossip, this makes two nodes exchange both directions of
			// a set difference in one round.
			rumors := g.snapshot()
			if len(rumors) > 0 {
				if err := node.Send(msg.Src, syncBody{Type: "rumor_sync", Rumors: rumors}); err != nil {
					log.Printf("reply sync to %s failed: %v", msg.Src, err)
				}
			}
		}
		return nil
	})

	if err := node.Run(); err != nil {
		log.Fatalf("node stopped: %v", err)
	}
}
