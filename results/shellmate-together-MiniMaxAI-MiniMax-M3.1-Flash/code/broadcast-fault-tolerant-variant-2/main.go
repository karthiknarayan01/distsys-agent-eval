package main

import (
	"encoding/json"
	"log"
	"os"
	"sort"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const (
	batchType     = "rumor_batch"
	batchAckType  = "rumor_batch_ack"
	retryInterval = 100 * time.Millisecond
)

// gossipNode stores the rumors known by this node.  A rumor learned directly
// from a client is kept in pending until every other node acknowledges it.
// This is important: messages sent while the network is partitioned vanish
// without an error, so an acknowledgement (rather than a successful write)
// is what tells us that delivery really happened.
type gossipNode struct {
	n *maelstrom.Node

	mu       sync.Mutex
	rumors   map[int]struct{}
	peers    []string
	pending  map[string]map[int]struct{}
	lastSent map[string]time.Time

	startOnce sync.Once
}

func newGossipNode() *gossipNode {
	return &gossipNode{
		n:        maelstrom.NewNode(),
		rumors:   make(map[int]struct{}),
		pending:  make(map[string]map[int]struct{}),
		lastSent: make(map[string]time.Time),
	}
}

func (g *gossipNode) registerHandlers() {
	// The library handles init itself, but this hook lets us learn the peer
	// IDs and start the retry loop.
	g.n.Handle("init", func(msg maelstrom.Message) error {
		g.mu.Lock()
		g.peers = g.peers[:0]
		for _, id := range g.n.NodeIDs() {
			if id != g.n.ID() {
				g.peers = append(g.peers, id)
			}
		}
		g.mu.Unlock()

		g.startOnce.Do(func() { go g.retryLoop() })
		return nil
	})

	g.n.Handle("neighbors", func(msg maelstrom.Message) error {
		// The suggested topology is optional.  We acknowledge it, but use
		// direct links to every node so a split can never strand a rumor
		// behind an unfortunate choice of forwarding path.
		var body struct {
			Neighbors map[string][]string `json:"neighbors"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		return g.n.Reply(msg, map[string]any{"type": "neighbors_noted"})
	})

	g.n.Handle("tell", func(msg maelstrom.Message) error {
		var body struct {
			Rumor int `json:"rumor"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		g.mu.Lock()
		if _, exists := g.rumors[body.Rumor]; !exists {
			g.rumors[body.Rumor] = struct{}{}
			for _, peer := range g.peers {
				if g.pending[peer] == nil {
					g.pending[peer] = make(map[int]struct{})
				}
				g.pending[peer][body.Rumor] = struct{}{}
			}
		}
		g.mu.Unlock()

		// The client request has now been durably accepted by this node.
		return g.n.Reply(msg, map[string]any{"type": "told"})
	})

	g.n.Handle("recall", func(msg maelstrom.Message) error {
		g.mu.Lock()
		rumors := make([]int, 0, len(g.rumors))
		for rumor := range g.rumors {
			rumors = append(rumors, rumor)
		}
		g.mu.Unlock()
		sort.Ints(rumors)

		return g.n.Reply(msg, map[string]any{
			"type":   "recalled",
			"rumors": rumors,
		})
	})

	// A batch can carry many retries in one message, keeping traffic small
	// even when several large partitions persist for a while.
	g.n.Handle(batchType, func(msg maelstrom.Message) error {
		var body struct {
			Values []int `json:"values"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		g.mu.Lock()
		for _, rumor := range body.Values {
			g.rumors[rumor] = struct{}{}
		}
		g.mu.Unlock()

		// Acknowledge only after the values are in our set.  There is
		// deliberately no in_reply_to: this is our own protocol, not an RPC
		// managed by the Maelstrom library.
		return g.n.Send(msg.Src, map[string]any{
			"type":   batchAckType,
			"values": body.Values,
		})
	})

	g.n.Handle(batchAckType, func(msg maelstrom.Message) error {
		var body struct {
			Values []int `json:"values"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		g.mu.Lock()
		if pending := g.pending[msg.Src]; pending != nil {
			for _, rumor := range body.Values {
				delete(pending, rumor)
			}
		}
		g.mu.Unlock()
		return nil
	})
}

func (g *gossipNode) retryLoop() {
	ticker := time.NewTicker(retryInterval)
	defer ticker.Stop()

	for range ticker.C {
		g.flushPending()
	}
}

// flushPending sends one batch to each peer that still has unacknowledged
// rumors.  A failed/deleted packet simply leaves those rumors pending, so the
// next tick tries again.
func (g *gossipNode) flushPending() {
	now := time.Now()

	type outgoing struct {
		peer   string
		values []int
	}
	batches := make([]outgoing, 0, len(g.peers))

	g.mu.Lock()
	for _, peer := range g.peers {
		pending := g.pending[peer]
		if len(pending) == 0 {
			continue
		}
		if last := g.lastSent[peer]; !last.IsZero() && now.Sub(last) < retryInterval {
			continue
		}

		values := make([]int, 0, len(pending))
		for rumor := range pending {
			values = append(values, rumor)
		}
		sort.Ints(values)
		g.lastSent[peer] = now
		batches = append(batches, outgoing{peer: peer, values: values})
	}
	g.mu.Unlock()

	for _, batch := range batches {
		if err := g.n.Send(batch.peer, map[string]any{
			"type":   batchType,
			"values": batch.values,
		}); err != nil {
			log.Printf("send batch to %s: %v", batch.peer, err)
		}
	}
}

func main() {
	g := newGossipNode()
	g.registerHandlers()
	if err := g.n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}
}
