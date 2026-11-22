package main

import (
	"encoding/json"
	"log"
	"sort"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()

	var mu sync.Mutex
	values := make(map[int]struct{})
	var pending []int
	var neighbors []string

	// We ignore the suggested topology and arrange the cluster as a star:
	// one root node connected to every other node. This gives a tree of depth
	// two, so any value reaches every node in at most two hops. Values are
	// batched and flushed periodically to keep the message count low.
	n.Handle("init", func(msg maelstrom.Message) error {
		ids := append([]string(nil), n.NodeIDs()...)
		sort.Strings(ids)
		root := ids[0]
		mu.Lock()
		if n.ID() == root {
			neighbors = ids[1:]
		} else {
			neighbors = []string{root}
		}
		mu.Unlock()
		return nil
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		return n.Reply(msg, map[string]any{"type": "topology_ok"})
	})

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body struct {
			Message int `json:"message"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		mu.Lock()
		if _, ok := values[body.Message]; !ok {
			values[body.Message] = struct{}{}
			pending = append(pending, body.Message)
		}
		mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "broadcast_ok"})
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		mu.Lock()
		msgs := make([]int, 0, len(values))
		for v := range values {
			msgs = append(msgs, v)
		}
		mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "read_ok", "messages": msgs})
	})

	n.Handle("gossip", func(msg maelstrom.Message) error {
		var body struct {
			Messages []int `json:"messages"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		mu.Lock()
		// Forward a received value only if we have some neighbor other than
		// the sender. On a tree this floods each value across every edge
		// exactly once, and it stops leaves from echoing values back to the
		// root.
		forward := false
		for _, nb := range neighbors {
			if nb != msg.Src {
				forward = true
				break
			}
		}
		for _, v := range body.Messages {
			if _, ok := values[v]; !ok {
				values[v] = struct{}{}
				if forward {
					pending = append(pending, v)
				}
			}
		}
		mu.Unlock()
		return nil
	})

	// Periodically flush newly learned values to all neighbors.
	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for range ticker.C {
			mu.Lock()
			if len(neighbors) == 0 || len(pending) == 0 {
				mu.Unlock()
				continue
			}
			batch := pending
			pending = nil
			nbs := append([]string(nil), neighbors...)
			mu.Unlock()

			body := map[string]any{"type": "gossip", "messages": batch}
			for _, nb := range nbs {
				if err := n.Send(nb, body); err != nil {
					log.Printf("send gossip to %s: %v", nb, err)
				}
			}
		}
	}()

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
