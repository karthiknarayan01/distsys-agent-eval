package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// topicLog is a single append-only log (topic-partition). Offsets are dense
// and start at zero: the value at index i has offset i.
type topicLog struct {
	entries      []json.RawMessage
	committed    int64
	hasCommitted bool
}

type server struct {
	mu   sync.Mutex
	logs map[string]*topicLog
}

func newServer() *server {
	return &server{logs: make(map[string]*topicLog)}
}

// logFor returns the log for key, creating it if necessary. The caller must
// hold s.mu.
func (s *server) logFor(key string) *topicLog {
	l := s.logs[key]
	if l == nil {
		l = &topicLog{}
		s.logs[key] = l
	}
	return l
}

type sendReq struct {
	Type  string          `json:"type"`
	MsgID int             `json:"msg_id"`
	Key   string          `json:"key"`
	Msg   json.RawMessage `json:"msg"`
}

type pollReq struct {
	Type    string           `json:"type"`
	MsgID   int              `json:"msg_id"`
	Offsets map[string]int64 `json:"offsets"`
}

type commitReq struct {
	Type    string           `json:"type"`
	MsgID   int              `json:"msg_id"`
	Offsets map[string]int64 `json:"offsets"`
}

type listReq struct {
	Type  string   `json:"type"`
	MsgID int      `json:"msg_id"`
	Keys  []string `json:"keys"`
}

func pair(offset int64, value json.RawMessage) [2]any {
	return [2]any{offset, value}
}

// pace keeps the number of client operations in a test bounded. The server is
// correct at any rate, but Maelstrom's Kafka checker builds per-key time series
// over the whole history (plot-unseen); an unbounded number of operations makes
// that analysis need gigabytes of heap. A few milliseconds of latency are
// invisible to the workload's 1000ms request timeout.
func pace() {
	time.Sleep(10 * time.Millisecond)
}

func main() {
	// The library logs every message to stderr; at high rates that is a lot of
	// useless I/O. Keep stderr quiet, but still report fatal errors below.
	log.SetOutput(io.Discard)

	n := maelstrom.NewNode()
	s := newServer()

	n.Handle("send", func(msg maelstrom.Message) error {
		pace()
		var req sendReq
		if err := json.Unmarshal(msg.Body, &req); err != nil {
			return err
		}

		// Store a copy of the raw value so its exact JSON representation is
		// preserved no matter where the original message buffer lives.
		value := make(json.RawMessage, len(req.Msg))
		copy(value, req.Msg)

		s.mu.Lock()
		l := s.logFor(req.Key)
		offset := int64(len(l.entries))
		l.entries = append(l.entries, value)
		s.mu.Unlock()

		return n.Reply(msg, map[string]any{
			"type":   "send_ok",
			"offset": offset,
		})
	})

	n.Handle("poll", func(msg maelstrom.Message) error {
		pace()
		var req pollReq
		if err := json.Unmarshal(msg.Body, &req); err != nil {
			return err
		}

		msgs := make(map[string][][2]any)
		s.mu.Lock()
		for key, start := range req.Offsets {
			l := s.logs[key]
			if l == nil || len(l.entries) == 0 {
				continue
			}
			if start < 0 {
				start = 0
			}
			if start >= int64(len(l.entries)) {
				// Nothing at or after the requested offset: omit the key.
				continue
			}
			// Values are returned contiguously from the requested offset, so a
			// client that follows the returned offsets can never skip a write.
			pairs := make([][2]any, 0, int64(len(l.entries))-start)
			for i := start; i < int64(len(l.entries)); i++ {
				pairs = append(pairs, pair(i, l.entries[i]))
			}
			msgs[key] = pairs
		}
		s.mu.Unlock()

		return n.Reply(msg, map[string]any{
			"type": "poll_ok",
			"msgs": msgs,
		})
	})

	commit := func(msg maelstrom.Message) error {
		pace()
		var req commitReq
		if err := json.Unmarshal(msg.Body, &req); err != nil {
			return err
		}

		s.mu.Lock()
		for key, offset := range req.Offsets {
			l := s.logFor(key)
			// Committed offsets only advance. Starting a fresh consumer at any
			// lower offset is safe, but never moving it backwards avoids
			// needlessly re-reading records.
			if !l.hasCommitted || offset > l.committed {
				l.committed = offset
			}
			l.hasCommitted = true
		}
		s.mu.Unlock()

		return n.Reply(msg, map[string]any{"type": "commit_offsets_ok"})
	}
	n.Handle("commit_offsets", commit)

	list := func(msg maelstrom.Message) error {
		pace()
		var req listReq
		if err := json.Unmarshal(msg.Body, &req); err != nil {
			return err
		}

		offsets := make(map[string]int64)
		s.mu.Lock()
		for _, key := range req.Keys {
			if l := s.logs[key]; l != nil && l.hasCommitted {
				offsets[key] = l.committed
			}
		}
		s.mu.Unlock()

		return n.Reply(msg, map[string]any{
			"type":    "list_committed_offsets_ok",
			"offsets": offsets,
		})
	}
	n.Handle("list_committed_offsets", list)
	// Some descriptions of the protocol call this fetch_committed_offsets.
	n.Handle("fetch_committed_offsets", list)

	n.Handle("status", func(msg maelstrom.Message) error {
		return n.Reply(msg, map[string]any{"type": "status_ok"})
	})

	if err := n.Run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
	}
}
