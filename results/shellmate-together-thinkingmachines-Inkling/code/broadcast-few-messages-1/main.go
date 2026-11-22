package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sort"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type State struct {
	node      *maelstrom.Node
	mu        sync.Mutex
	known     map[int]bool
	pending   []int
	parent    string
	children  []string
}

func main() {
	n := maelstrom.NewNode()
	s := &State{
		node:     n,
		known:    make(map[int]bool),
		pending:  make([]int, 0),
	}

	n.Handle("init", func(msg maelstrom.Message) error {
		ids := n.NodeIDs()
		sort.Strings(ids)
		self := n.ID()
		idx := -1
		for i, id := range ids {
			if id == self {
				idx = i
				break
			}
		}
		if idx > 0 {
			s.parent = ids[(idx-1)/2]
		}
		s.children = []string{}
		left := 2*idx + 1
		right := 2*idx + 2
		if left < len(ids) {
			s.children = append(s.children, ids[left])
		}
		if right < len(ids) {
			s.children = append(s.children, ids[right])
		}
		go flushLoop(s)
		return nil
	})

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body map[string]interface{}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		val := int(body["message"].(float64))
		s.mu.Lock()
		if !s.known[val] {
			s.known[val] = true
			s.pending = append(s.pending, val)
		}
		s.mu.Unlock()
		return n.Reply(msg, map[string]interface{}{"type": "broadcast_ok"})
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		s.mu.Lock()
		values := make([]int, 0, len(s.known))
		for v := range s.known {
			values = append(values, v)
		}
		s.mu.Unlock()
		return n.Reply(msg, map[string]interface{}{
			"type":      "read_ok",
			"messages": values,
		})
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		return n.Reply(msg, map[string]interface{}{"type": "topology_ok"})
	})

	n.Handle("broadcast_batch", func(msg maelstrom.Message) error {
		var body map[string]interface{}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		values := body["values"].([]interface{})
		s.mu.Lock()
		for _, v := range values {
			vi := int(v.(float64))
			if !s.known[vi] {
				s.known[vi] = true
				s.pending = append(s.pending, vi)
			}
		}
		s.mu.Unlock()
		return nil
	})

	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}
}

func flushLoop(s *State) {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			flush(s)
		}
	}
}

func flush(s *State) {
	s.mu.Lock()
	batch := s.pending
	s.pending = make([]int, 0)
	s.mu.Unlock()

	if len(batch) == 0 {
		return
	}

	neighbors := make([]string, 0)
	if s.parent != "" {
		neighbors = append(neighbors, s.parent)
	}
	neighbors = append(neighbors, s.children...)

	for _, neighbor := range neighbors {
		body := map[string]interface{}{
			"type":    "broadcast_batch",
			"values": batch,
		}
		if err := s.node.Send(neighbor, body); err != nil {
			fmt.Fprintf(os.Stderr, "send error: %v\n", err)
		}
	}
}
