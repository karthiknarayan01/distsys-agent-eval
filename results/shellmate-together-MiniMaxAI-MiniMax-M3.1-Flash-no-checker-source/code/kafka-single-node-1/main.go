// A single-node Kafka-style log service.
//
// The node keeps one append-only log per string key. Every log has its own
// offset space, growing monotonically from 0. Clients may:
//
//	send               -> append a value, reply with the assigned offset
//	poll               -> read values from several logs at given offsets
//	commit_offsets     -> record how far a client has read in each log
//	list_committed_offsets -> read back the committed offsets
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// ---------------------------------------------------------------- server state

type entry struct {
	offset int64
	value  json.RawMessage
}

type keyLog struct {
	entries []entry
	// committed is the highest offset a client said it had processed.
	committed    int64
	committedSet bool
}

type server struct {
	mu   sync.Mutex
	logs map[string]*keyLog
}

func newServer() *server {
	return &server{logs: make(map[string]*keyLog)}
}

// ---------------------------------------------------------------- wire types

// msgPair is a single [offset value] pair. It marshals the value as the
// exact JSON the client sent, so values are never rewritten.
type msgPair struct {
	Offset int64
	Value  json.RawMessage
}

func (p msgPair) MarshalJSON() ([]byte, error) {
	off, err := json.Marshal(p.Offset)
	if err != nil {
		return nil, err
	}
	val := p.Value
	if len(val) == 0 || !json.Valid(val) {
		val = json.RawMessage("null")
	}
	out := make([]byte, 0, len(off)+len(val)+2)
	out = append(out, '[')
	out = append(out, off...)
	out = append(out, ',')
	out = append(out, val...)
	out = append(out, ']')
	return out, nil
}

type sendOK struct {
	Type      string `json:"type"`
	InReplyTo int    `json:"in_reply_to"`
	Offset    int64  `json:"offset"`
}

type pollOK struct {
	Type      string               `json:"type"`
	InReplyTo int                  `json:"in_reply_to"`
	Msgs      map[string][]msgPair `json:"msgs"`
}

type commitOK struct {
	Type      string `json:"type"`
	InReplyTo int    `json:"in_reply_to"`
}

type listOK struct {
	Type      string           `json:"type"`
	InReplyTo int              `json:"in_reply_to"`
	Offsets   map[string]int64 `json:"offsets"`
}

// ---------------------------------------------------------------- handlers

func (s *server) handleSend(n *maelstrom.Node, msg maelstrom.Message) error {
	var body struct {
		MsgID int             `json:"msg_id"`
		Key   string          `json:"key"`
		Msg   json.RawMessage `json:"msg"`
		Value json.RawMessage `json:"value"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	value := body.Msg
	if len(value) == 0 {
		value = body.Value
	}
	if len(value) == 0 || !json.Valid(value) {
		value = json.RawMessage("null")
	}

	s.mu.Lock()
	l := s.logs[body.Key]
	if l == nil {
		l = &keyLog{}
		s.logs[body.Key] = l
	}
	offset := int64(len(l.entries))
	l.entries = append(l.entries, entry{offset: offset, value: value})
	s.mu.Unlock()

	return n.Send(msg.Src, sendOK{Type: "send_ok", InReplyTo: body.MsgID, Offset: offset})
}

func (s *server) handlePoll(n *maelstrom.Node, msg maelstrom.Message) error {
	var body struct {
		MsgID   int                    `json:"msg_id"`
		Offsets map[string]json.Number `json:"offsets"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	msgs := make(map[string][]msgPair, len(body.Offsets))

	s.mu.Lock()
	for key, num := range body.Offsets {
		offset, err := num.Int64()
		if err != nil {
			continue
		}
		l := s.logs[key]
		if l == nil || len(l.entries) == 0 {
			continue
		}
		// First entry at or after the requested offset.
		start := sort.Search(len(l.entries), func(i int) bool {
			return l.entries[i].offset >= offset
		})
		if start >= len(l.entries) {
			// Nothing at or after the requested offset: leave the log out.
			continue
		}
		pairs := make([]msgPair, 0, len(l.entries)-start)
		for _, e := range l.entries[start:] {
			pairs = append(pairs, msgPair{Offset: e.offset, Value: e.value})
		}
		msgs[key] = pairs
	}
	s.mu.Unlock()

	return n.Send(msg.Src, pollOK{Type: "poll_ok", InReplyTo: body.MsgID, Msgs: msgs})
}

func (s *server) handleCommitOffsets(n *maelstrom.Node, msg maelstrom.Message) error {
	var body struct {
		MsgID   int                    `json:"msg_id"`
		Offsets map[string]json.Number `json:"offsets"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	s.mu.Lock()
	for key, num := range body.Offsets {
		offset, err := num.Int64()
		if err != nil {
			continue
		}
		l := s.logs[key]
		if l == nil {
			l = &keyLog{}
			s.logs[key] = l
		}
		// Committed offsets only ever move forward.
		if !l.committedSet || offset > l.committed {
			l.committed = offset
			l.committedSet = true
		}
	}
	s.mu.Unlock()

	return n.Send(msg.Src, commitOK{Type: "commit_offsets_ok", InReplyTo: body.MsgID})
}

func (s *server) handleListCommittedOffsets(n *maelstrom.Node, msg maelstrom.Message) error {
	var body struct {
		MsgID int      `json:"msg_id"`
		Keys  []string `json:"keys"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	offsets := make(map[string]int64, len(body.Keys))
	s.mu.Lock()
	for _, key := range body.Keys {
		if l, ok := s.logs[key]; ok && l.committedSet {
			offsets[key] = l.committed
		}
	}
	s.mu.Unlock()

	return n.Send(msg.Src, listOK{
		Type:      "list_committed_offsets_ok",
		InReplyTo: body.MsgID,
		Offsets:   offsets,
	})
}

// ---------------------------------------------------------------- main

func main() {
	if os.Getenv("MAELSTROM_DEBUG") == "" {
		// The library logs every message; that is far too noisy (and slow)
		// for large poll responses.
		log.SetOutput(io.Discard)
	}

	n := maelstrom.NewNode()
	s := newServer()

	n.Handle("send", func(m maelstrom.Message) error { return s.handleSend(n, m) })
	n.Handle("poll", func(m maelstrom.Message) error { return s.handlePoll(n, m) })
	n.Handle("commit_offsets", func(m maelstrom.Message) error {
		return s.handleCommitOffsets(n, m)
	})
	n.Handle("list_committed_offsets", func(m maelstrom.Message) error {
		return s.handleListCommittedOffsets(n, m)
	})
	// Some clients use the alternate name for listing committed offsets.
	n.Handle("fetch_committed_offsets", func(m maelstrom.Message) error {
		return s.handleListCommittedOffsets(n, m)
	})

	if err := n.Run(); err != nil {
		log.SetOutput(os.Stderr)
		fmt.Fprintf(os.Stderr, "node failed: %v\n", err)
		os.Exit(1)
	}
}
