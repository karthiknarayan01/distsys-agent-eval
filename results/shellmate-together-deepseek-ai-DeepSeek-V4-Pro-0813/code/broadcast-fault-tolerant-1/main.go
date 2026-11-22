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

	var (
		mu       sync.Mutex
		messages = make(map[int]bool)
		nodeIDs  []string
		selfID   string
	)

	n.Handle("init", func(msg maelstrom.Message) error {
		selfID = n.ID()
		nodeIDs = n.NodeIDs()
		go gossipLoop(n, &mu, messages, nodeIDs, selfID)
		return nil
	})

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body struct {
			Message int `json:"message"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		mu.Lock()
		messages[body.Message] = true
		mu.Unlock()

		return n.Reply(msg, map[string]any{"type": "broadcast_ok"})
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		mu.Lock()
		msgs := make([]int, 0, len(messages))
		for m := range messages {
			msgs = append(msgs, m)
		}
		mu.Unlock()

		return n.Reply(msg, map[string]any{"type": "read_ok", "messages": msgs})
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
		for _, m := range body.Messages {
			messages[m] = true
		}
		mu.Unlock()

		return nil
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

// gossipLoop periodically sends the full set of known values to every other
// node. Because messages sent across a network partition are silently dropped,
// we keep retransmitting forever; once the partition heals, every value
// eventually reaches every node.
func gossipLoop(n *maelstrom.Node, mu *sync.Mutex, messages map[int]bool, nodeIDs []string, selfID string) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for range ticker.C {
		mu.Lock()
		msgs := make([]int, 0, len(messages))
		for m := range messages {
			msgs = append(msgs, m)
		}
		mu.Unlock()

		if len(msgs) == 0 {
			continue
		}

		body := map[string]any{"type": "gossip", "messages": msgs}
		for _, dest := range nodeIDs {
			if dest == selfID {
				continue
			}
			if err := n.Send(dest, body); err != nil {
				log.Printf("gossip send to %s failed: %v", dest, err)
			}
		}
	}
}
