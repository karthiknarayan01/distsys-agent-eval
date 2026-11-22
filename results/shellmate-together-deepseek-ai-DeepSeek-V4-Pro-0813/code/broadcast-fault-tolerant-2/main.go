package main

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()

	var mu sync.Mutex
	values := make(map[int]struct{})

	// snapshot returns a copy of all known values.
	snapshot := func() []int {
		mu.Lock()
		defer mu.Unlock()
		msgs := make([]int, 0, len(values))
		for v := range values {
			msgs = append(msgs, v)
		}
		return msgs
	}

	// gossip sends our full set of values to every other node.
	gossip := func() {
		msgs := snapshot()
		for _, dest := range n.NodeIDs() {
			if dest == n.ID() {
				continue
			}
			if err := n.Send(dest, map[string]any{"type": "gossip", "messages": msgs}); err != nil {
				log.Printf("gossip send error: %v", err)
			}
		}
	}

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body struct {
			Message int `json:"message"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		mu.Lock()
		values[body.Message] = struct{}{}
		mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "broadcast_ok"})
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		return n.Reply(msg, map[string]any{"type": "read_ok", "messages": snapshot()})
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		return n.Reply(msg, map[string]any{"type": "topology_ok"})
	})

	n.Handle("gossip", func(msg maelstrom.Message) error {
		var body struct {
			Messages []int `json:"messages"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		mu.Lock()
		added := false
		for _, v := range body.Messages {
			if _, ok := values[v]; !ok {
				values[v] = struct{}{}
				added = true
			}
		}
		mu.Unlock()
		if added {
			// Spread newly learned values right away.
			gossip()
		}
		return nil
	})

	// Start periodic gossip once we know our node ID and peers.
	n.Handle("init", func(msg maelstrom.Message) error {
		go func() {
			for {
				gossip()
				time.Sleep(50 * time.Millisecond)
			}
		}()
		return nil
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
