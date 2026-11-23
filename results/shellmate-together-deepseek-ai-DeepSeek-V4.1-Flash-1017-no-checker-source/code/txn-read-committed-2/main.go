package main

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// Version is one committed value of a key. Counter and Node form a globally
// unique, totally ordered timestamp. We never publish a value before its
// transaction has finished, and all transactions are committed locally, so a
// reader can only observe committed versions.
type Version struct {
	Value any    `json:"v"`
	Ctr   int64  `json:"c"`
	Node  string `json:"n"`
}

func (v Version) newerThan(o Version) bool {
	if v.Ctr != o.Ctr {
		return v.Ctr > o.Ctr
	}
	return v.Node > o.Node
}

type state struct {
	mu sync.Mutex

	id    string
	peers []string

	ctr   int64
	store map[string]Version

	started bool
}

type syncBody struct {
	Type    string             `json:"type"`
	Entries map[string]Version `json:"entries,omitempty"`
	MaxCtr  int64              `json:"max_ctr,omitempty"`
}

type txnRequestBody struct {
	Type  string  `json:"type"`
	Txn   [][]any `json:"txn"`
	MsgID int     `json:"msg_id"`
}

type txnResponseBody struct {
	Type string  `json:"type"`
	Txn  [][]any `json:"txn"`
}

func main() {
	n := maelstrom.NewNode()
	s := &state{store: make(map[string]Version)}

	n.Handle("init", func(msg maelstrom.Message) error {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.id = n.ID()
		s.peers = nil
		for _, id := range n.NodeIDs() {
			if id != s.id {
				s.peers = append(s.peers, id)
			}
		}
		if !s.started {
			s.started = true
			go s.antiEntropy(n)
		}
		return nil
	})

	n.Handle("txn", func(msg maelstrom.Message) error {
		var req txnRequestBody
		if err := json.Unmarshal(msg.Body, &req); err != nil {
			return err
		}

		s.mu.Lock()
		s.ctr++
		myTS := Version{Ctr: s.ctr, Node: s.id}

		// A transaction's writes are private until it has executed all its
		// operations. This makes the whole transaction's writes visible
		// atomically: no other transaction can observe an intermediate value.
		pending := make(map[string]any)
		out := make([][]any, len(req.Txn))
		for i, op := range req.Txn {
			if len(op) < 2 {
				out[i] = op
				continue
			}
			kind, _ := op[0].(string)
			key := keyString(op[1])
			switch kind {
			case "r":
				var val any
				if v, ok := pending[key]; ok {
					val = v
				} else if v, ok := s.store[key]; ok {
					val = v.Value
				}
				out[i] = []any{"r", op[1], val}
			case "w":
				var val any
				if len(op) >= 3 {
					val = op[2]
				}
				pending[key] = val
				out[i] = []any{"w", op[1], val}
			default:
				out[i] = op
			}
		}

		// Commit all writes at one point in time.
		entries := make(map[string]Version, len(pending))
		for k, v := range pending {
			ver := Version{Value: v, Ctr: myTS.Ctr, Node: myTS.Node}
			cur, ok := s.store[k]
			if !ok || ver.newerThan(cur) {
				s.store[k] = ver
				entries[k] = ver
			}
		}
		maxCtr := s.ctr
		s.mu.Unlock()

		// Answer immediately; total availability does not permit waiting for
		// the peer. Replication is best-effort and retried by anti-entropy.
		if err := n.Reply(msg, txnResponseBody{Type: "txn_ok", Txn: out}); err != nil {
			return err
		}
		if len(entries) > 0 {
			for _, p := range s.peerList() {
				_ = n.Send(p, syncBody{Type: "sync", Entries: entries, MaxCtr: maxCtr})
			}
		}
		return nil
	})

	n.Handle("sync", func(msg maelstrom.Message) error {
		var body syncBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		if body.MaxCtr > s.ctr {
			s.ctr = body.MaxCtr
		}
		for k, in := range body.Entries {
			if in.Ctr > s.ctr {
				s.ctr = in.Ctr
			}
			cur, ok := s.store[k]
			if !ok || in.newerThan(cur) {
				s.store[k] = in
			}
		}
		return nil
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

func (s *state) peerList() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.peers...)
}

func (s *state) antiEntropy(n *maelstrom.Node) {
	t := time.NewTicker(50 * time.Millisecond)
	defer t.Stop()
	for range t.C {
		s.mu.Lock()
		if !s.started || len(s.peers) == 0 {
			s.mu.Unlock()
			continue
		}
		entries := make(map[string]Version, len(s.store))
		for k, v := range s.store {
			entries[k] = v
		}
		maxCtr := s.ctr
		peers := append([]string(nil), s.peers...)
		s.mu.Unlock()

		for _, p := range peers {
			_ = n.Send(p, syncBody{Type: "sync", Entries: entries, MaxCtr: maxCtr})
		}
	}
}

func keyString(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case json.Number:
		return x.String()
	case float64:
		// JSON numbers are decoded as float64 by encoding/json.
		return fmt.Sprintf("%v", x)
	default:
		b, _ := json.Marshal(x)
		return string(b)
	}
}
