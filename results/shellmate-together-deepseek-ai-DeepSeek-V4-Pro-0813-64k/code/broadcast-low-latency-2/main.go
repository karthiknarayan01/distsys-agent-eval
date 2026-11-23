package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()

	var (
		mu      sync.Mutex
		values  = make(map[int]bool)
		hubOnce sync.Once
		hub     string
	)

	getHub := func() string {
		hubOnce.Do(func() {
			hub = n.NodeIDs()[0]
		})
		return hub
	}

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body struct {
			Message int `json:"message"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		mu.Lock()
		values[body.Message] = true
		mu.Unlock()

		if err := n.Reply(msg, map[string]any{"type": "broadcast_ok"}); err != nil {
			return err
		}

		h := getHub()
		if n.ID() == h {
			for _, id := range n.NodeIDs() {
				if id != n.ID() {
					if err := n.Send(id, map[string]any{"type": "gossip", "message": body.Message}); err != nil {
						return err
					}
				}
			}
		} else {
			if err := n.Send(h, map[string]any{"type": "gossip", "message": body.Message}); err != nil {
				return err
			}
		}
		return nil
	})

	n.Handle("gossip", func(msg maelstrom.Message) error {
		var body struct {
			Message int `json:"message"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		mu.Lock()
		if values[body.Message] {
			mu.Unlock()
			return nil
		}
		values[body.Message] = true
		mu.Unlock()

		h := getHub()
		if n.ID() == h {
			for _, id := range n.NodeIDs() {
				if id != n.ID() && id != msg.Src {
					if err := n.Send(id, map[string]any{"type": "gossip", "message": body.Message}); err != nil {
						return err
					}
				}
			}
		}
		return nil
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

	n.Handle("topology", func(msg maelstrom.Message) error {
		return n.Reply(msg, map[string]any{"type": "topology_ok"})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
