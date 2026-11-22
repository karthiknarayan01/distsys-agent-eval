package main

import (
	"encoding/json"
	"log"
	"sort"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type payload struct {
	Type    string `json:"type"`
	Message int    `json:"message"`
}

type server struct {
	node *maelstrom.Node

	mu     sync.RWMutex
	values map[int]struct{}

	root      string
	children  []string
	ready     bool
	setupOnce sync.Once
}

func (s *server) setup() {
	s.setupOnce.Do(func() {
		ids := append([]string(nil), s.node.NodeIDs()...)
		sort.Strings(ids)
		if len(ids) == 0 {
			return
		}

		// A star is a spanning tree with diameter two. Flooding along its
		// edges uses exactly N-1 server-to-server messages per broadcast,
		// which is the minimum possible, while keeping propagation at two
		// network delays for broadcasts originating at a leaf.
		s.root = ids[0]
		s.children = append([]string(nil), ids[1:]...)
		s.ready = true
	})
}

func (s *server) add(value int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.values[value]; ok {
		return false
	}
	s.values[value] = struct{}{}
	return true
}

func (s *server) sendTo(dest string, value int) {
	if err := s.node.Send(dest, map[string]any{
		"type":    "relay",
		"message": value,
	}); err != nil {
		log.Printf("send to %s: %v", dest, err)
	}
}

func (s *server) run() error {
	n := s.node
	s.values = make(map[int]struct{})

	n.Handle("init", func(msg maelstrom.Message) error {
		s.setup()
		return nil
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		s.setup()
		return n.Reply(msg, maelstrom.MessageBody{Type: "topology_ok"})
	})

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body payload
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		s.setup()
		fresh := s.add(body.Message)

		// Acknowledge as soon as the value is durable in this node.  The
		// one-way relay messages below do not need client-visible replies.
		if err := n.Reply(msg, maelstrom.MessageBody{Type: "broadcast_ok"}); err != nil {
			return err
		}

		if !fresh || !s.ready {
			return nil
		}
		if n.ID() == s.root {
			for _, child := range s.children {
				s.sendTo(child, body.Message)
			}
		} else {
			s.sendTo(s.root, body.Message)
		}
		return nil
	})

	n.Handle("relay", func(msg maelstrom.Message) error {
		var body payload
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		s.setup()
		if !s.add(body.Message) {
			return nil // already seen; do not relay it again
		}

		// Only the root forwards relays.  Leaves receiving a relay from
		// the root are done.  The sender already has this value, so the
		// root skips it; that keeps the traffic at one message per tree
		// edge and avoids echoing a value back to its origin.
		if s.ready && n.ID() == s.root {
			for _, child := range s.children {
				if child != msg.Src {
					s.sendTo(child, body.Message)
				}
			}
		}
		return nil
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		s.mu.RLock()
		values := make([]int, 0, len(s.values))
		for value := range s.values {
			values = append(values, value)
		}
		s.mu.RUnlock()

		return n.Reply(msg, map[string]any{
			"type":     "read_ok",
			"messages": values,
		})
	})

	return n.Run()
}

func main() {
	s := &server{node: maelstrom.NewNode()}
	if err := s.run(); err != nil {
		log.Fatal(err)
	}
}
