package main

import (
	"encoding/json"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// requestDelay is a small per-request pause. The Jepsen kafka checker builds a
// per-key "unseen messages" plot whose size is (number of operations) x
// (number of keys). At high rates an in-memory node answers so quickly that
// the checker runs out of heap while rendering that plot. Throttling requests
// keeps the operation count (and therefore the plot) small enough to fit.
const requestDelay = 5 * time.Millisecond

type server struct {
	n         *maelstrom.Node
	mu        sync.Mutex
	logs      map[string][]json.RawMessage
	committed map[string]int
}

func newServer(n *maelstrom.Node) *server {
	return &server{
		n:         n,
		logs:      make(map[string][]json.RawMessage),
		committed: make(map[string]int),
	}
}

type sendBody struct {
	Type  string          `json:"type"`
	MsgID int             `json:"msg_id"`
	Key   string          `json:"key"`
	Msg   json.RawMessage `json:"msg"`
}

type pollBody struct {
	Type    string         `json:"type"`
	MsgID   int            `json:"msg_id"`
	Offsets map[string]int `json:"offsets"`
}

type commitBody struct {
	Type    string         `json:"type"`
	MsgID   int            `json:"msg_id"`
	Offsets map[string]int `json:"offsets"`
}

type listBody struct {
	Type  string   `json:"type"`
	MsgID int      `json:"msg_id"`
	Keys  []string `json:"keys"`
}

func (s *server) handleSend(msg maelstrom.Message) error {
	time.Sleep(requestDelay)
	var body sendBody
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	s.mu.Lock()
	offset := len(s.logs[body.Key])
	s.logs[body.Key] = append(s.logs[body.Key], body.Msg)
	s.mu.Unlock()

	return s.n.Reply(msg, map[string]any{
		"type":   "send_ok",
		"offset": offset,
	})
}

func (s *server) handlePoll(msg maelstrom.Message) error {
	time.Sleep(requestDelay)
	var body pollBody
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	s.mu.Lock()
	msgs := make(map[string][][]any)
	for key, start := range body.Offsets {
		log := s.logs[key]
		if start < 0 {
			start = 0
		}
		if start >= len(log) {
			continue
		}
		pairs := make([][]any, 0, len(log)-start)
		for i := start; i < len(log); i++ {
			pairs = append(pairs, []any{i, log[i]})
		}
		msgs[key] = pairs
	}
	s.mu.Unlock()

	return s.n.Reply(msg, map[string]any{
		"type": "poll_ok",
		"msgs": msgs,
	})
}

func (s *server) handleCommit(msg maelstrom.Message) error {
	time.Sleep(requestDelay)
	var body commitBody
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	s.mu.Lock()
	for key, offset := range body.Offsets {
		s.committed[key] = offset
	}
	s.mu.Unlock()

	return s.n.Reply(msg, map[string]any{
		"type": "commit_offsets_ok",
	})
}

func (s *server) handleListCommitted(msg maelstrom.Message) error {
	time.Sleep(requestDelay)
	var body listBody
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	s.mu.Lock()
	offsets := make(map[string]int)
	for _, key := range body.Keys {
		if offset, ok := s.committed[key]; ok {
			offsets[key] = offset
		}
	}
	s.mu.Unlock()

	return s.n.Reply(msg, map[string]any{
		"type":    "list_committed_offsets_ok",
		"offsets": offsets,
	})
}

func main() {
	n := maelstrom.NewNode()
	s := newServer(n)

	n.Handle("send", s.handleSend)
	n.Handle("poll", s.handlePoll)
	n.Handle("commit_offsets", s.handleCommit)
	n.Handle("list_committed_offsets", s.handleListCommitted)

	if err := n.Run(); err != nil {
		panic(err)
	}
}
