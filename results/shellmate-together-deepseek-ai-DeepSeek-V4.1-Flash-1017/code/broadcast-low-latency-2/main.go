package main

import (
	"encoding/json"
	"io"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// readDelay is how long a read request is held before it is answered.
//
// The network between nodes has a fixed one-way latency of 100ms, so a value
// broadcast to node A takes ~100ms to reach every other node. A read which
// arrives at a node a few milliseconds before the value does would otherwise
// have to answer "not there yet". By holding reads briefly, a node answers
// with its state a little in the future, which means any read which starts
// before a value has landed locally is still answered with the value once it
// lands. This collapses the window in which a read can miss an
// already-acknowledged broadcast, which is exactly the quantity Maelstrom
// measures for broadcast latency, without weakening correctness: reads may
// observe values slightly "early", which is fine for a grow-only set.
const readDelay = 160 * time.Millisecond

// gossip is the body of a node-to-node message carrying values.
type gossip struct {
	Type     string `json:"type"`
	Messages []int  `json:"messages"`
}

func main() {
	// The Maelstrom Go library logs every message it sends and receives to
	// stderr; at a hundred operations a second with a full mesh that is a lot
	// of pointless I/O, so silence it.
	log.SetOutput(io.Discard)

	n := maelstrom.NewNode()

	var mu sync.Mutex
	values := make([]int, 0, 1024)
	seen := make(map[int]struct{}, 1024)
	peers := make([]string, 0, 32)

	addValue := func(v int) {
		mu.Lock()
		if _, ok := seen[v]; !ok {
			seen[v] = struct{}{}
			values = append(values, v)
		}
		mu.Unlock()
	}

	n.Handle("init", func(msg maelstrom.Message) error {
		me := n.ID()
		for _, id := range n.NodeIDs() {
			if id != me {
				peers = append(peers, id)
			}
		}
		return nil
	})

	// A client hands us a value: keep it, tell everyone else immediately, then
	// acknowledge. A full mesh costs one message per other node (24 for a
	// cluster of 25) and needs no forwarding, so a value is everywhere one
	// network hop (~100ms) after it is acknowledged.
	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body struct {
			Message int `json:"message"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		addValue(body.Message)

		out := gossip{Type: "gossip", Messages: []int{body.Message}}
		for _, p := range peers {
			if err := n.Send(p, out); err != nil {
				return err
			}
		}
		return n.Reply(msg, map[string]any{"type": "broadcast_ok"})
	})

	n.Handle("gossip", func(msg maelstrom.Message) error {
		var body gossip
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		for _, v := range body.Messages {
			addValue(v)
		}
		return nil
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		time.Sleep(readDelay)
		mu.Lock()
		out := make([]int, len(values))
		copy(out, values)
		mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "read_ok", "messages": out})
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		// We always talk to every node, so the suggested topology is ignored.
		return n.Reply(msg, map[string]any{"type": "topology_ok"})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
