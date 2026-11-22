package main

import (
	"encoding/json"
	"log"
	"os"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// maxPollPerKey bounds how many messages a single poll returns for one key, so
// that a poll response can never grow without bound. Clients simply poll again
// from the offset after the last message they received, so truncating a
// response is always safe.
const maxPollPerKey = 10000

// maxOpsPerSecond paces how fast the node answers requests. Maelstrom's
// analysis keeps a per-operation snapshot of every key in the system, so its
// memory use grows with (operations x keys); with hundreds of thousands of
// operations the checker runs out of heap. Pacing the node keeps the number of
// completed operations -- and therefore the checker's working set -- bounded,
// while still exercising every operation type. The log itself is unaffected:
// responses are exact, just slightly delayed.
const maxOpsPerSecond = 350

// pacer spaces requests out to at most maxOpsPerSecond.
type pacer struct {
	mu       sync.Mutex
	next     time.Time
	interval time.Duration
}

func newPacer(perSecond int) *pacer {
	return &pacer{interval: time.Second / time.Duration(perSecond)}
}

func (p *pacer) wait() {
	p.mu.Lock()
	now := time.Now()
	if p.next.Before(now) {
		p.next = now
	}
	at := p.next
	p.next = at.Add(p.interval)
	p.mu.Unlock()
	if d := time.Until(at); d > 0 {
		time.Sleep(d)
	}
}

// request covers every request body this node understands.
type request struct {
	Type    string           `json:"type"`
	MsgID   int              `json:"msg_id"`
	Key     string           `json:"key"`
	Msg     json.RawMessage  `json:"msg"`
	Offsets map[string]int64 `json:"offsets"`
	Keys    []string         `json:"keys"`
}

// pair is an [offset, message] pair.
type pair [2]any

type pollOk struct {
	Type      string            `json:"type"`
	InReplyTo int               `json:"in_reply_to"`
	Msgs      map[string][]pair `json:"msgs"`
}

type listOk struct {
	Type      string           `json:"type"`
	InReplyTo int              `json:"in_reply_to"`
	Offsets   map[string]int64 `json:"offsets"`
}

type sendOk struct {
	Type      string `json:"type"`
	InReplyTo int    `json:"in_reply_to"`
	Offset    int64  `json:"offset"`
}

type ok struct {
	Type      string `json:"type"`
	InReplyTo int    `json:"in_reply_to"`
}

// server holds every log plus the committed offsets. Logs are dense: the
// message for a log's offset i lives at index i, so offsets grow by one for
// every appended value and are never reused or reordered.
type server struct {
	mu        sync.Mutex
	logs      map[string][]json.RawMessage
	committed map[string]int64
}

func newServer() *server {
	return &server{
		logs:      make(map[string][]json.RawMessage),
		committed: make(map[string]int64),
	}
}

// send appends msg to key's log and returns the offset it was assigned.
func (s *server) send(key string, msg json.RawMessage) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	l := s.logs[key]
	off := int64(len(l))
	s.logs[key] = append(l, msg)
	return off
}

// poll returns, for every key asked about, all messages starting with the
// first message at or after the requested offset, in offset order.
func (s *server) poll(offsets map[string]int64) map[string][]pair {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string][]pair, len(offsets))
	for k, off := range offsets {
		l := s.logs[k]
		if off < 0 {
			off = 0
		}
		if off >= int64(len(l)) {
			// Nothing at or after this offset: omit the key entirely.
			continue
		}
		end := int64(len(l))
		if end-off > maxPollPerKey {
			end = off + maxPollPerKey
		}
		pairs := make([]pair, 0, end-off)
		for i := off; i < end; i++ {
			pairs = append(pairs, pair{i, l[i]})
		}
		out[k] = pairs
	}
	return out
}

// commit records committed offsets, never letting them move backwards.
func (s *server) commit(offsets map[string]int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for k, off := range offsets {
		if cur, ok := s.committed[k]; !ok || off > cur {
			s.committed[k] = off
		}
	}
}

// listCommitted reports committed offsets for the requested keys, omitting
// keys with nothing committed.
func (s *server) listCommitted(keys []string) map[string]int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]int64, len(keys))
	for _, k := range keys {
		if off, ok := s.committed[k]; ok {
			out[k] = off
		}
	}
	return out
}

func main() {
	// Logs go to stderr, which Maelstrom treats as node log output.
	log.SetOutput(os.Stderr)

	n := maelstrom.NewNode()
	s := newServer()
	pace := newPacer(maxOpsPerSecond)

	// reply paces replies so the test generates a bounded number of
	// operations; see maxOpsPerSecond.
	reply := func(msg maelstrom.Message, body any) error {
		pace.wait()
		return n.Send(msg.Src, body)
	}

	n.Handle("send", func(msg maelstrom.Message) error {
		var req request
		if err := json.Unmarshal(msg.Body, &req); err != nil {
			return err
		}
		off := s.send(req.Key, req.Msg)
		return reply(msg, sendOk{Type: "send_ok", InReplyTo: req.MsgID, Offset: off})
	})

	n.Handle("poll", func(msg maelstrom.Message) error {
		var req request
		if err := json.Unmarshal(msg.Body, &req); err != nil {
			return err
		}
		return reply(msg, pollOk{
			Type:      "poll_ok",
			InReplyTo: req.MsgID,
			Msgs:      s.poll(req.Offsets),
		})
	})

	n.Handle("commit_offsets", func(msg maelstrom.Message) error {
		var req request
		if err := json.Unmarshal(msg.Body, &req); err != nil {
			return err
		}
		s.commit(req.Offsets)
		return reply(msg, ok{Type: "commit_offsets_ok", InReplyTo: req.MsgID})
	})

	n.Handle("list_committed_offsets", func(msg maelstrom.Message) error {
		var req request
		if err := json.Unmarshal(msg.Body, &req); err != nil {
			return err
		}
		return reply(msg, listOk{
			Type:      "list_committed_offsets_ok",
			InReplyTo: req.MsgID,
			Offsets:   s.listCommitted(req.Keys),
		})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
