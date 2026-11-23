// Command maelstrom-counter implements a grow-only counter with no shared store.
//
// The approach: a state-based CRDT. Each node keeps how much it has been asked to add, and the
// largest count it has heard for every other node. Merging two such maps takes the larger count
// for each node, so the same news can arrive any number of times, in any order, and never be
// counted twice. Every node sends its whole map to every other node on a timer, forever, so an
// update lost to a network split is sent again once the split heals. A read is the sum of the
// map.
package main

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const gossipEvery = 200 * time.Millisecond

var (
	node   = maelstrom.NewNode()
	mu     sync.Mutex
	counts = map[string]int{} // node id -> the most that node is known to have added
)

func main() {
	node.Handle("add", handleAdd)
	node.Handle("read", handleRead)
	node.Handle("gossip", handleGossip)
	go gossipForever()
	if err := node.Run(); err != nil {
		log.Fatal(err)
	}
}

// handleAdd counts the delta as this node's own, which only this node ever increases.
func handleAdd(msg maelstrom.Message) error {
	var body struct {
		Delta int `json:"delta"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	mu.Lock()
	counts[node.ID()] += body.Delta
	mu.Unlock()
	return node.Reply(msg, map[string]any{"type": "add_ok"})
}

// handleRead answers with the sum of every node's count as this node knows it.
func handleRead(msg maelstrom.Message) error {
	mu.Lock()
	total := 0
	for _, count := range counts {
		total += count
	}
	mu.Unlock()
	return node.Reply(msg, map[string]any{"type": "read_ok", "value": total})
}

// handleGossip merges another node's map into ours, keeping the larger count for each node.
func handleGossip(msg maelstrom.Message) error {
	var body struct {
		Counts map[string]int `json:"counts"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	mu.Lock()
	defer mu.Unlock()
	for id, count := range body.Counts {
		counts[id] = max(counts[id], count)
	}
	return nil
}

// gossipForever sends this node's whole map to every other node on a timer. A send across a
// split is lost; the next one, after the split heals, carries everything it missed.
func gossipForever() {
	for range time.Tick(gossipEvery) {
		if node.ID() == "" {
			continue
		}
		mu.Lock()
		snapshot := make(map[string]int, len(counts))
		for id, count := range counts {
			snapshot[id] = count
		}
		mu.Unlock()
		for _, peer := range node.NodeIDs() {
			if peer != node.ID() {
				node.Send(peer, map[string]any{"type": "gossip", "counts": snapshot})
			}
		}
	}
}
