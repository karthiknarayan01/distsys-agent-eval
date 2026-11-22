package main

import (
	"encoding/json"
	"log"
	"sort"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type version struct {
	Counter uint64 `json:"counter"`
	Node    string `json:"node"`
}

type cell struct {
	Value int64   `json:"value"`
	Ver   version `json:"ver"`
}

type entry struct {
	Key   int64   `json:"key"`
	Value int64   `json:"value"`
	Ver   version `json:"ver"`
}

type node struct {
	n *maelstrom.Node

	mu         sync.Mutex
	store      map[int64]cell
	maxCounter uint64
	cursor     int
}

func newer(a, b version) bool {
	if a.Counter != b.Counter {
		return a.Counter > b.Counter
	}
	return a.Node > b.Node
}

func (s *node) apply(entries []entry, remoteMax uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if remoteMax > s.maxCounter {
		s.maxCounter = remoteMax
	}
	for _, e := range entries {
		if e.Ver.Counter > s.maxCounter {
			s.maxCounter = e.Ver.Counter
		}
		old, ok := s.store[e.Key]
		if !ok || newer(e.Ver, old.Ver) {
			s.store[e.Key] = cell{Value: e.Value, Ver: e.Ver}
		}
	}
}

// nextChunk returns a bounded slice of the current store plus this node's
// current Lamport counter. Keeping every message well below the Maelstrom
// scanner limit is important: the Go demo library uses bufio.Scanner, whose
// default token size is 64 KiB.
func (s *node) nextChunk(limit int) ([]entry, uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.store) == 0 {
		return nil, s.maxCounter
	}
	keys := make([]int64, 0, len(s.store))
	for k := range s.store {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	if s.cursor >= len(keys) {
		s.cursor = 0
	}
	end := s.cursor + limit
	if end > len(keys) {
		end = len(keys)
	}
	out := make([]entry, 0, end-s.cursor)
	for _, k := range keys[s.cursor:end] {
		c := s.store[k]
		out = append(out, entry{Key: k, Value: c.Value, Ver: c.Ver})
	}
	s.cursor = end
	return out, s.maxCounter
}

func (s *node) sendToPeers(body map[string]any) {
	for _, peer := range s.n.NodeIDs() {
		if peer == s.n.ID() {
			continue
		}
		if err := s.n.Send(peer, body); err != nil {
			log.Printf("send to %s failed: %v", peer, err)
		}
	}
}

func (s *node) broadcast(entries []entry) {
	if len(entries) == 0 {
		return
	}
	s.sendToPeers(map[string]any{"type": "replicate", "entries": entries})
}

func (s *node) handleTxn(msg maelstrom.Message) error {
	var body struct {
		Txn []json.RawMessage `json:"txn"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	s.mu.Lock()
	// Each transaction gets a timestamp greater than every timestamp this
	// node has observed. The node ID breaks ties between concurrent txns.
	s.maxCounter++
	ver := version{Counter: s.maxCounter, Node: s.n.ID()}

	// Writes are visible to later operations of this same transaction, but
	// are only installed in the shared store after the whole txn is done.
	local := make(map[int64]int64)
	final := make(map[int64]int64)
	out := make([]any, 0, len(body.Txn))
	for _, raw := range body.Txn {
		var op []any
		if err := json.Unmarshal(raw, &op); err != nil {
			s.mu.Unlock()
			return err
		}
		f := op[0].(string)
		k := int64(op[1].(float64))
		switch f {
		case "r":
			var v any
			if x, ok := local[k]; ok {
				v = x
			} else if c, ok := s.store[k]; ok {
				v = c.Value
			}
			out = append(out, []any{"r", k, v})
		case "w":
			v := int64(op[2].(float64))
			local[k] = v
			final[k] = v
			out = append(out, []any{"w", k, v})
		}
	}
	entries := make([]entry, 0, len(final))
	for k, v := range final {
		s.store[k] = cell{Value: v, Ver: ver}
		entries = append(entries, entry{Key: k, Value: v, Ver: ver})
	}
	s.mu.Unlock()

	// Answer immediately; replication is best-effort and repair is periodic.
	if err := s.n.Reply(msg, map[string]any{"type": "txn_ok", "txn": out}); err != nil {
		return err
	}
	if len(entries) > 0 {
		go s.broadcast(entries)
	}
	return nil
}

func main() {
	s := &node{n: maelstrom.NewNode(), store: make(map[int64]cell)}
	s.n.Handle("txn", s.handleTxn)
	s.n.Handle("replicate", func(msg maelstrom.Message) error {
		var body struct {
			Entries    []entry `json:"entries"`
			MaxCounter uint64  `json:"max_counter"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		s.apply(body.Entries, body.MaxCounter)
		return nil
	})

	// Anti-entropy: periodically exchange a bounded chunk of the complete
	// versioned store, so writes sent during a partition are repaired once
	// the network heals. The round-robin cursor eventually covers every key.
	go func() {
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()
		for range ticker.C {
			if len(s.n.NodeIDs()) == 0 {
				continue
			}
			entries, maxCounter := s.nextChunk(300)
			s.sendToPeers(map[string]any{
				"type":        "replicate",
				"entries":     entries,
				"max_counter": maxCounter,
			})
		}
	}()

	if err := s.n.Run(); err != nil {
		log.Fatal(err)
	}
}
