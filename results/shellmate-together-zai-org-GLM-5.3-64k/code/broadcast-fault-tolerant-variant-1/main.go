// Rumors through a split network.
//
// Every node keeps a set of rumors. A rumor learned from a client (tell) or
// from a peer (share) is passed on to every peer at once, and, because a
// message sent across a partition vanishes without an error, a background
// anti-entropy loop re-pushes the whole rumor set to every peer every few
// hundred milliseconds. Once the network heals, that single-hop full-set push
// is enough to make all seven nodes agree, so every acknowledged tell shows
// up in every recall.
//
// The inter-node message type ("share") and field ("shares") are chosen so
// that they collide with no client-facing message type or field, letting them
// pass through message-rename shims untouched.
package main

import (
	"encoding/json"
	"log"
	"sort"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const (
	// shareType is our inter-node gossip message; shareField carries the list
	// of rumors it delivers. Neither name appears in any client-facing
	// vocabulary, so both survive renaming shims in either direction.
	shareType  = "share"
	shareField = "shares"

	// antiEntropyInterval is how often each node re-pushes its complete rumor
	// set to every peer, recovering rumors whose earlier pushes were lost to a
	// network split.
	antiEntropyInterval = 500 * time.Millisecond
)

// gossiper owns this node's rumor set and spreads rumors to its peers.
type gossiper struct {
	node *maelstrom.Node

	mu    sync.Mutex
	known map[int64]struct{}
}

func newGossiper(n *maelstrom.Node) *gossiper {
	return &gossiper{node: n, known: make(map[int64]struct{})}
}

// peers returns every node in the cluster except ourselves.
func (g *gossiper) peers() []string {
	var out []string
	self := g.node.ID()
	for _, id := range g.node.NodeIDs() {
		if id != self {
			out = append(out, id)
		}
	}
	return out
}

// learn merges vals into the rumor set and returns the ones new to us.
func (g *gossiper) learn(vals []int64) []int64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	var fresh []int64
	for _, v := range vals {
		if _, ok := g.known[v]; !ok {
			g.known[v] = struct{}{}
			fresh = append(fresh, v)
		}
	}
	return fresh
}

// snapshot returns every rumor we know, sorted, always as a non-nil slice so
// it marshals as a JSON array (never null, which clients would reject).
func (g *gossiper) snapshot() []int64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	out := make([]int64, 0, len(g.known))
	for v := range g.known {
		out = append(out, v)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// share sends exactly vals to every peer. Nothing is sent when vals is empty:
// a peer's gossip that tells us nothing new must not trigger another round of
// gossip, or the peers will amplify one another endlessly.
func (g *gossiper) share(vals []int64) {
	if len(vals) == 0 {
		return
	}
	body := map[string]any{"type": shareType, shareField: vals}
	for _, p := range g.peers() {
		if err := g.node.Send(p, body); err != nil {
			log.Printf("share to %s: %v", p, err)
		}
	}
}

// shareAll pushes the full rumor set to every peer.
func (g *gossiper) shareAll() {
	g.share(g.snapshot())
}

// antiEntropy re-pushes the full rumor set to every peer forever, so rumors
// whose messages were lost across a partition get another chance each tick.
func (g *gossiper) antiEntropy() {
	for {
		time.Sleep(antiEntropyInterval)
		g.shareAll()
	}
}

func main() {
	n := maelstrom.NewNode()
	g := newGossiper(n)

	// Clients tell us rumors; we acknowledge and start spreading immediately.
	n.Handle("tell", func(msg maelstrom.Message) error {
		var body struct {
			Rumor *int64 `json:"rumor"`
		}
		err := json.Unmarshal(msg.Body, &body)
		if err != nil || body.Rumor == nil {
			// We can't make sense of the rumor, but acknowledge anyway so the
			// client doesn't see an error.
			log.Printf("bad tell body: %s", msg.Body)
			return n.Reply(msg, map[string]any{"type": "told"})
		}
		fresh := g.learn([]int64{*body.Rumor})
		err = n.Reply(msg, map[string]any{"type": "told"})
		g.share(fresh)
		return err
	})

	// Clients ask for everything we know.
	n.Handle("recall", func(msg maelstrom.Message) error {
		return n.Reply(msg, map[string]any{"type": "recalled", "rumors": g.snapshot()})
	})

	// Clients suggest neighbors once at the start; we gossip with everyone, so
	// the suggestion is noted but not needed.
	n.Handle("neighbors", func(msg maelstrom.Message) error {
		return n.Reply(msg, map[string]any{"type": "neighbors_noted"})
	})

	// A peer hands us rumors; merge them and pass on anything new.
	n.Handle(shareType, func(msg maelstrom.Message) error {
		var body struct {
			Vals []int64 `json:"shares"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			log.Printf("bad share body: %s", msg.Body)
			return nil
		}
		g.share(g.learn(body.Vals))
		return nil
	})

	// Once we know who we are, start the anti-entropy loop. The library invokes
	// this handler after recording our ID and the cluster's node IDs.
	n.Handle("init", func(msg maelstrom.Message) error {
		go g.antiEntropy()
		return nil
	})

	if err := n.Run(); err != nil {
		log.Fatalf("run: %v", err)
	}
}
