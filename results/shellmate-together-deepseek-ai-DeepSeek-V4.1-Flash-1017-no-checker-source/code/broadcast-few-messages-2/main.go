package main

import (
	"encoding/json"
	"sort"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// flushInterval is how often the hub node pushes values it has learned down to
// the leaves.  Every leaf receives a single batched message per interval, so
// the number of inter-node messages stays tiny, while the delay added by
// batching stays well below the latency budget.
const flushInterval = 100 * time.Millisecond

type server struct {
	mu     sync.Mutex
	n      *maelstrom.Node
	known  map[int]bool
	queue  []int    // values waiting to be pushed down to leaves (hub only)
	root   string   // hub node id
	isRoot bool     //
	leaves []string // every node except the hub
}

type valuesBody struct {
	Type   string `json:"type"`
	Values []int  `json:"values"`
}

func main() {
	n := maelstrom.NewNode()
	s := &server{n: n, known: make(map[int]bool)}

	// Node learns its own identity and the cluster membership on init.
	n.Handle("init", func(msg maelstrom.Message) error {
		ids := append([]string(nil), n.NodeIDs()...)
		sort.Strings(ids)
		s.mu.Lock()
		if len(ids) > 0 {
			s.root = ids[0]
			s.isRoot = (s.root == n.ID())
			s.leaves = s.leaves[:0]
			for _, id := range ids {
				if id != s.root {
					s.leaves = append(s.leaves, id)
				}
			}
		}
		s.mu.Unlock()
		return nil
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		// The hub-and-spoke layout we build ourselves is flatter than any
		// topology Maelstrom suggests, so the hint is ignored.
		return n.Reply(msg, map[string]any{"type": "topology_ok"})
	})

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body struct {
			Message int `json:"message"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		s.mu.Lock()
		fresh := !s.known[body.Message]
		if fresh {
			s.known[body.Message] = true
			if s.isRoot {
				s.queue = append(s.queue, body.Message)
			}
			root := s.root
			isRoot := s.isRoot
			s.mu.Unlock()

			// Leaves hand their value straight to the hub: no batching delay
			// on the way up.  Values arrive sparsely enough that this costs
			// roughly one message per broadcast.
			if !isRoot {
				n.Send(root, valuesBody{Type: "fwd", Values: []int{body.Message}})
			}
		} else {
			s.mu.Unlock()
		}

		return n.Reply(msg, map[string]any{"type": "broadcast_ok"})
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		s.mu.Lock()
		msgs := make([]int, 0, len(s.known))
		for v := range s.known {
			msgs = append(msgs, v)
		}
		s.mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "read_ok", "messages": msgs})
	})

	// A leaf handing a batch of values up to the hub.
	n.Handle("fwd", func(msg maelstrom.Message) error {
		var body valuesBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		s.mu.Lock()
		for _, v := range body.Values {
			if !s.known[v] {
				s.known[v] = true
				if s.isRoot {
					s.queue = append(s.queue, v)
				}
			}
		}
		s.mu.Unlock()
		return nil
	})

	// The hub pushing freshly learned values down to a leaf.
	n.Handle("dist", func(msg maelstrom.Message) error {
		var body valuesBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		s.mu.Lock()
		for _, v := range body.Values {
			s.known[v] = true
		}
		s.mu.Unlock()
		return nil
	})

	// Periodic batched push from the hub to every leaf.
	go func() {
		ticker := time.NewTicker(flushInterval)
		defer ticker.Stop()
		for range ticker.C {
			s.mu.Lock()
			if len(s.queue) == 0 {
				s.mu.Unlock()
				continue
			}
			vals := s.queue
			s.queue = nil
			leaves := s.leaves
			s.mu.Unlock()

			body := valuesBody{Type: "dist", Values: vals}
			for _, leaf := range leaves {
				n.Send(leaf, body)
			}
		}
	}()

	if err := n.Run(); err != nil {
		panic(err)
	}
}
