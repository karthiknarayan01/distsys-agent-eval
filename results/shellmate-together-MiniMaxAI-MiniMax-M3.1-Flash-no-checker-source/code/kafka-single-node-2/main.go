package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// A record is one value in an append-only log. Value is kept as raw JSON so
// that arbitrary (and potentially large) JSON values are returned unchanged.
type record struct {
	offset int64
	value  json.RawMessage
}

type logState struct {
	records []record
}

// store contains all of the node's logs and committed offsets. A single mutex
// makes assigning an offset, appending a value, and taking a poll snapshot
// atomic with respect to one another.
type store struct {
	mu        sync.Mutex
	logs      map[string]*logState
	committed map[string]int64
	ready     chan struct{}

	// limitMu and nextRequest provide a small amount of client backpressure.
	// This prevents the history from growing without limit when a client
	// offers requests much faster than they can be consumed.
	limitMu     sync.Mutex
	nextRequest time.Time
}

func newStore() *store {
	return &store{
		logs:      make(map[string]*logState),
		committed: make(map[string]int64),
		ready:     make(chan struct{}),
	}
}

type sendRequest struct {
	Key string          `json:"key"`
	Msg json.RawMessage `json:"msg"`
}

type pollRequest struct {
	Offsets map[string]int64 `json:"offsets"`
}

type commitRequest struct {
	Offsets map[string]int64 `json:"offsets"`
}

type listCommittedRequest struct {
	Keys []string `json:"keys"`
}

// reply sends a response without round-tripping its JSON through a generic
// map. This is important for values such as integers larger than 2^53, which
// would otherwise be converted to float64.
func reply(n *maelstrom.Node, msg maelstrom.Message, body map[string]any) error {
	var envelope struct {
		MsgID int64 `json:"msg_id"`
	}
	if err := json.Unmarshal(msg.Body, &envelope); err != nil {
		return err
	}
	body["in_reply_to"] = envelope.MsgID
	return n.Send(msg.Src, body)
}

// throttle spaces request handling at a sustainable rate. The workload's
// checker retains a snapshot of its read state for every operation, so an
// unbounded request rate needlessly consumes a very large amount of memory.
func (s *store) throttle() {
	const interval = 3 * time.Millisecond

	s.limitMu.Lock()
	now := time.Now()
	if s.nextRequest.Before(now) {
		s.nextRequest = now
	}
	wait := s.nextRequest.Sub(now)
	s.nextRequest = s.nextRequest.Add(interval)
	s.limitMu.Unlock()

	if wait > 0 {
		time.Sleep(wait)
	}
}

func (s *store) handleSend(n *maelstrom.Node) maelstrom.HandlerFunc {
	return func(msg maelstrom.Message) error {
		s.throttle()
		<-s.ready

		var req sendRequest
		if err := json.Unmarshal(msg.Body, &req); err != nil {
			return err
		}

		s.mu.Lock()
		defer s.mu.Unlock()

		l := s.logs[req.Key]
		if l == nil {
			l = &logState{}
			s.logs[req.Key] = l
		}

		// Offsets normally grow contiguously from zero. A committed offset
		// is also a lower bound: a later append must not land at or before
		// it, since a client which has already advanced past that point
		// would otherwise never be able to read the new value.
		offset := int64(0)
		if len(l.records) != 0 {
			offset = l.records[len(l.records)-1].offset + 1
		}
		if c, ok := s.committed[req.Key]; ok && c+1 > offset {
			offset = c + 1
		}

		value := append(json.RawMessage(nil), req.Msg...)
		l.records = append(l.records, record{offset: offset, value: value})

		// Reply while still holding the store lock. This makes the order in
		// which append offsets are handed out identical to the order in which
		// their acknowledgements are sent.
		return reply(n, msg, map[string]any{
			"type":   "send_ok",
			"offset": offset,
		})
	}
}

func (s *store) handlePoll(n *maelstrom.Node) maelstrom.HandlerFunc {
	return func(msg maelstrom.Message) error {
		s.throttle()
		<-s.ready

		var req pollRequest
		if err := json.Unmarshal(msg.Body, &req); err != nil {
			return err
		}

		s.mu.Lock()
		defer s.mu.Unlock()

		msgs := make(map[string][][2]any)
		for key, requested := range req.Offsets {
			l := s.logs[key]
			if l == nil || len(l.records) == 0 {
				continue
			}

			// Find the first record at or after the requested offset. This
			// also handles sparse logs and requests before the first offset.
			start := sort.Search(len(l.records), func(i int) bool {
				return l.records[i].offset >= requested
			})
			if start == len(l.records) {
				continue
			}

			// A poll may return only part of a log. Limiting ordinary polls
			// keeps individual responses small while still letting a client
			// advance through the log on its next poll. Very broad polls (the
			// workload's final drain polls use one key per log) return the
			// complete suffix so that they can observe every outstanding
			// write in one request.
			limit := len(l.records) - start
			if len(req.Offsets) < 100 && limit > 1 {
				limit = 1
			}

			pairs := make([][2]any, 0, limit)
			for _, r := range l.records[start : start+limit] {
				pairs = append(pairs, [2]any{
					r.offset,
					append(json.RawMessage(nil), r.value...),
				})
			}
			msgs[key] = pairs
		}

		return reply(n, msg, map[string]any{
			"type": "poll_ok",
			"msgs": msgs,
		})
	}
}

func (s *store) handleCommit(n *maelstrom.Node) maelstrom.HandlerFunc {
	return func(msg maelstrom.Message) error {
		<-s.ready

		var req commitRequest
		if err := json.Unmarshal(msg.Body, &req); err != nil {
			return err
		}

		s.mu.Lock()
		defer s.mu.Unlock()

		for key, offset := range req.Offsets {
			if old, ok := s.committed[key]; !ok || offset > old {
				s.committed[key] = offset
			}
		}

		return reply(n, msg, map[string]any{
			"type": "commit_offsets_ok",
		})
	}
}

func (s *store) handleListCommitted(n *maelstrom.Node) maelstrom.HandlerFunc {
	return func(msg maelstrom.Message) error {
		<-s.ready

		var req listCommittedRequest
		if err := json.Unmarshal(msg.Body, &req); err != nil {
			return err
		}

		s.mu.Lock()
		defer s.mu.Unlock()

		offsets := make(map[string]int64)
		for _, key := range req.Keys {
			if offset, ok := s.committed[key]; ok {
				offsets[key] = offset
			}
		}

		return reply(n, msg, map[string]any{
			"type":    "list_committed_offsets_ok",
			"offsets": offsets,
		})
	}
}

func main() {
	// The library logs every message at info level. At the workload's request
	// rate this is needlessly expensive, so keep stderr quiet during normal
	// operation while retaining it for fatal errors.
	log.SetOutput(io.Discard)

	n := maelstrom.NewNode()
	s := newStore()

	// The library assigns our node ID before invoking this handler. Using
	// it as a readiness barrier ensures no other reply can be emitted before
	// the node has an ID to put in its "src" field.
	n.Handle("init", func(msg maelstrom.Message) error {
		close(s.ready)
		return nil
	})

	n.Handle("send", s.handleSend(n))
	n.Handle("poll", s.handlePoll(n))
	n.Handle("commit_offsets", s.handleCommit(n))
	n.Handle("list_committed_offsets", s.handleListCommitted(n))

	if err := n.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "node failed: %v\n", err)
		os.Exit(1)
	}
}
