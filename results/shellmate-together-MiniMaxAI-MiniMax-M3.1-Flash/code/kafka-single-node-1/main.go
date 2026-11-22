package main

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// requestDelay is the amount of time each request spends "in storage" before
// it is answered. It models the cost of making an append durable and of
// fetching a batch of records from the log.
const requestDelay = 5 * time.Millisecond

// A log is append-only. We use contiguous offsets starting at zero, so the
// offset of an entry is also its index in entries.
type logData struct {
	entries []json.RawMessage
}

type store struct {
	mu        sync.Mutex
	logs      map[string]*logData
	committed map[string]int
	node      *maelstrom.Node
}

func newStore(node *maelstrom.Node) *store {
	return &store{
		logs:      make(map[string]*logData),
		committed: make(map[string]int),
		node:      node,
	}
}

type sendRequest struct {
	Key string          `json:"key"`
	Msg json.RawMessage `json:"msg"`
}

type pollRequest struct {
	Offsets map[string]int `json:"offsets"`
}

type commitRequest struct {
	Offsets map[string]int `json:"offsets"`
}

type listRequest struct {
	Keys []string `json:"keys"`
}

func (s *store) handleSend(msg maelstrom.Message) error {
	var req sendRequest
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		return err
	}
	time.Sleep(requestDelay)

	s.mu.Lock()
	l := s.logs[req.Key]
	if l == nil {
		l = new(logData)
		s.logs[req.Key] = l
	}
	offset := len(l.entries)
	l.entries = append(l.entries, req.Msg)
	s.mu.Unlock()

	return s.node.Reply(msg, map[string]any{
		"type":   "send_ok",
		"offset": offset,
	})
}

func (s *store) handlePoll(msg maelstrom.Message) error {
	var req pollRequest
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		return err
	}
	time.Sleep(requestDelay)

	// Take a consistent snapshot of every requested log while holding the
	// store lock. A message appended after this snapshot will have a larger
	// offset and will be returned by a later poll.
	s.mu.Lock()
	msgs := make(map[string][][]any, len(req.Offsets))
	for key, requested := range req.Offsets {
		l := s.logs[key]
		if l == nil || len(l.entries) == 0 {
			continue
		}

		start := requested
		if start < 0 {
			start = 0
		}
		if start >= len(l.entries) {
			continue
		}

		// Return every entry from the requested offset onwards. The pairs are
		// contiguous, so a consumer can never skip an entry.
		pairs := make([][]any, 0, len(l.entries)-start)
		for offset := start; offset < len(l.entries); offset++ {
			pairs = append(pairs, []any{offset, l.entries[offset]})
		}
		msgs[key] = pairs
	}
	s.mu.Unlock()

	return s.node.Reply(msg, map[string]any{
		"type": "poll_ok",
		"msgs": msgs,
	})
}

func (s *store) handleCommit(msg maelstrom.Message) error {
	var req commitRequest
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		return err
	}
	time.Sleep(requestDelay)

	s.mu.Lock()
	for key, offset := range req.Offsets {
		// A committed position only moves forward, even if clients commit
		// concurrently.
		if current, ok := s.committed[key]; !ok || offset > current {
			s.committed[key] = offset
		}
	}
	s.mu.Unlock()

	return s.node.Reply(msg, map[string]any{
		"type": "commit_offsets_ok",
	})
}

func (s *store) handleListCommitted(msg maelstrom.Message) error {
	var req listRequest
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		return err
	}
	time.Sleep(requestDelay)

	s.mu.Lock()
	offsets := make(map[string]int, len(req.Keys))
	for _, key := range req.Keys {
		if offset, ok := s.committed[key]; ok {
			offsets[key] = offset
		}
	}
	s.mu.Unlock()

	return s.node.Reply(msg, map[string]any{
		"type":    "list_committed_offsets_ok",
		"offsets": offsets,
	})
}

func main() {
	log.SetFlags(0)
	node := maelstrom.NewNode()
	s := newStore(node)

	node.Handle("send", s.handleSend)
	node.Handle("poll", s.handlePoll)
	node.Handle("commit_offsets", s.handleCommit)
	node.Handle("list_committed_offsets", s.handleListCommitted)

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}
}
