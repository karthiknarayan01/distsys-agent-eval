package main

import (
	"encoding/json"
	"log"
	"os"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type broadcastBody struct {
	Type    string `json:"type"`
	MsgID   int    `json:"msg_id,omitempty"`
	Message int64  `json:"message"`
}

type replicateBody struct {
	Type    string `json:"type"`
	Message int64  `json:"message"`
}

type server struct {
	n *maelstrom.Node

	mu     sync.Mutex
	seen   map[int64]struct{}
	values []int64

	root   string
	peers  []string
	allIDs []string
}

func main() {
	n := maelstrom.NewNode()
	s := &server{
		n:    n,
		seen: make(map[int64]struct{}),
	}

	n.Handle("topology", func(msg maelstrom.Message) error {
		return n.Reply(msg, map[string]any{"type": "topology_ok"})
	})

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body broadcastBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// Remember the value before acknowledging it, and arrange for exactly
		// one forward along each edge of the dissemination tree.
		s.mu.Lock()
		newValue := false
		if _, ok := s.seen[body.Message]; !ok {
			s.seen[body.Message] = struct{}{}
			s.values = append(s.values, body.Message)
			newValue = true
		}
		s.mu.Unlock()

		if newValue {
			for _, peer := range s.peers {
				if peer == msg.Src {
					continue
				}
				if err := n.Send(peer, replicateBody{
					Type:    "replicate",
					Message: body.Message,
				}); err != nil {
					return err
				}
			}
		}

		return n.Reply(msg, map[string]any{"type": "broadcast_ok"})
	})

	// Server-to-server dissemination. This body has no msg_id, so it cannot
	// be mistaken for a client request.
	n.Handle("replicate", func(msg maelstrom.Message) error {
		var body replicateBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		s.mu.Lock()
		newValue := false
		if _, ok := s.seen[body.Message]; !ok {
			s.seen[body.Message] = struct{}{}
			s.values = append(s.values, body.Message)
			newValue = true
		}
		s.mu.Unlock()

		if newValue {
			for _, peer := range s.peers {
				if peer == msg.Src {
					continue
				}
				if err := n.Send(peer, replicateBody{
					Type:    "replicate",
					Message: body.Message,
				}); err != nil {
					return err
				}
			}
		}
		return nil
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		s.mu.Lock()
		values := append([]int64(nil), s.values...)
		s.mu.Unlock()
		return n.Reply(msg, map[string]any{
			"type":     "read_ok",
			"messages": values,
		})
	})

	// The init handler is called by the library after it has recorded the
	// node's identity and the cluster membership.
	n.Handle("init", func(msg maelstrom.Message) error {
		ids := n.NodeIDs()
		if len(ids) == 0 {
			return nil
		}

		s.mu.Lock()
		s.allIDs = append([]string(nil), ids...)
		s.root = ids[0]
		if n.ID() == s.root {
			s.peers = append([]string(nil), ids[1:]...)
		} else {
			s.peers = []string{s.root}
		}
		s.mu.Unlock()
		return nil
	})

	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}
}
