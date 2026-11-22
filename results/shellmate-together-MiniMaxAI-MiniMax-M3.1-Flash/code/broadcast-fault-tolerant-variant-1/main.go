package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const (
	gossipType   = "rumor_gossip"
	gossipPeriod = 50 * time.Millisecond
)

type rumorNode struct {
	node *maelstrom.Node

	mu     sync.RWMutex
	rumors map[int]struct{}
	peers  []string
}

type replyBody struct {
	Type   string `json:"type"`
	Rumors []int  `json:"rumors"`
}

type gossipBody struct {
	Type  string `json:"type"`
	Items []int  `json:"items"`
}

func main() {
	// The Maelstrom library logs every message. Gossip is intentionally
	// frequent, so keep those logs from overwhelming stderr.
	log.SetOutput(io.Discard)

	n := &rumorNode{
		node:   maelstrom.NewNode(),
		rumors: make(map[int]struct{}),
	}

	n.node.Handle("init", func(msg maelstrom.Message) error {
		n.setPeers(n.node.NodeIDs())
		return nil
	})

	n.node.Handle("neighbors", func(msg maelstrom.Message) error {
		// The simulated network permits direct node-to-node messages. We use
		// all nodes as peers, so the suggested topology is not needed.
		return n.node.Reply(msg, map[string]any{"type": "neighbors_noted"})
	})

	n.node.Handle("tell", func(msg maelstrom.Message) error {
		var body struct {
			Rumor int `json:"rumor"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		if n.add(body.Rumor) {
			// Start spreading immediately; periodic anti-entropy below will
			// retry this and every other rumor across future partitions.
			go n.gossipRound()
		}
		return n.node.Reply(msg, map[string]any{"type": "told"})
	})

	n.node.Handle("recall", func(msg maelstrom.Message) error {
		return n.node.Reply(msg, replyBody{Type: "recalled", Rumors: n.snapshot()})
	})

	n.node.Handle(gossipType, func(msg maelstrom.Message) error {
		var body gossipBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		n.merge(body.Items)
		return nil
	})

	go n.gossipLoop()

	if err := n.node.Run(); err != nil {
		fmt.Fprintln(os.Stderr, "node:", err)
		os.Exit(1)
	}
}

func (n *rumorNode) setPeers(ids []string) {
	self := n.node.ID()
	peers := make([]string, 0, len(ids))
	for _, id := range ids {
		if id != self {
			peers = append(peers, id)
		}
	}

	n.mu.Lock()
	n.peers = peers
	n.mu.Unlock()
}

func (n *rumorNode) add(rumor int) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	if _, exists := n.rumors[rumor]; exists {
		return false
	}
	n.rumors[rumor] = struct{}{}
	return true
}

func (n *rumorNode) merge(rumors []int) {
	n.mu.Lock()
	for _, rumor := range rumors {
		n.rumors[rumor] = struct{}{}
	}
	n.mu.Unlock()
}

func (n *rumorNode) snapshot() []int {
	n.mu.RLock()
	rumors := make([]int, 0, len(n.rumors))
	for rumor := range n.rumors {
		rumors = append(rumors, rumor)
	}
	n.mu.RUnlock()

	// Sorting is not required, but makes replies deterministic.
	sort.Ints(rumors)
	return rumors
}

func (n *rumorNode) gossipLoop() {
	ticker := time.NewTicker(gossipPeriod)
	defer ticker.Stop()
	for range ticker.C {
		n.gossipRound()
	}
}

// gossipRound exchanges the complete local rumor set with every peer. Unlike
// one-shot forwarding, this anti-entropy protocol retries continuously, so a
// rumor learned during a partition is delivered once the network heals.
func (n *rumorNode) gossipRound() {
	n.mu.RLock()
	peers := append([]string(nil), n.peers...)
	n.mu.RUnlock()
	if len(peers) == 0 {
		return
	}

	items := n.snapshot()
	body := gossipBody{Type: gossipType, Items: items}
	for _, peer := range peers {
		_ = n.node.Send(peer, body)
	}
}
