package main

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"

	"github.com/jepsen-io/maelstrom/demo/go"
)

// A Log is an append-only sequence of values with monotonically growing offsets.
type Log struct {
	mu    sync.Mutex
	values []interface{} // value stored at values[offset]
}

func newLog() *Log { return &Log{} }

// append returns the offset assigned to the new value and stores it.
func (l *Log) append(v interface{}) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.values = append(l.values, v)
	return len(l.values) - 1
}

func (l *Log) tail() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.values)
}

// poll returns pairs [offset, value] for offsets >= from, in increasing offset order.
func (l *Log) poll(from int) [][]interface{} {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out [][]interface{}
	for i := from; i < len(l.values); i++ {
		out = append(out, []interface{}{float64(i), l.values[i]})
	}
	return out
}

func (l *Log) committed() (int, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.values) == 0 {
		return 0, false
	}
	return len(l.values) - 1, true
}

// nodeState holds all logs and committed offsets for the single-node cluster.
type nodeState struct {
	mu sync.Mutex

	logs         map[string]*Log
	committed    map[string]int // committed offset per key (highest committed)
	hasCommitted map[string]bool
}

func newState() *nodeState {
	return &nodeState{
		logs:         make(map[string]*Log),
		committed:    make(map[string]int),
		hasCommitted: make(map[string]bool),
	}
}

func (s *nodeState) getLog(key string) *Log {
	s.mu.Lock()
	defer s.mu.Unlock()
	l, ok := s.logs[key]
	if !ok {
		l = newLog()
		s.logs[key] = l
	}
	return l
}

func (s *nodeState) send(key string, msg interface{}) int {
	l := s.getLog(key)
	return l.append(msg)
}

func (s *nodeState) poll(offsets map[string]int) map[string][][]interface{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string][][]interface{})
	for k, off := range offsets {
		out[k] = s.logs[k].poll(off)
	}
	return out
}

func (s *nodeState) commit(offsets map[string]int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for k, off := range offsets {
		l := s.logs[k]
		if l.tail() == 0 {
			continue
		}
		// committed offset must be <= number of values - 1
		if off >= l.tail() {
			off = l.tail() - 1
		}
		s.committed[k] = off
		s.hasCommitted[k] = true
	}
}

func (s *nodeState) listCommitted(keys []string) map[string]int {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]int)
	for _, k := range keys {
		if s.hasCommitted[k] {
			out[k] = s.committed[k]
		}
	}
	return out
}

func main() {
	node := maelstrom.NewNode()

	st := newState()

	node.Handle("send", func(msg maelstrom.Message) error {
		var body sendBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		off := st.send(body.Key, body.Msg)
		return node.Reply(msg, sendOKBody{Offset: off})
	})

	node.Handle("poll", func(msg maelstrom.Message) error {
		var body pollBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		msgs := st.poll(body.Offsets)
		return node.Reply(msg, pollOKBody{Msgs: msgs})
	})

	node.Handle("commit_offsets", func(msg maelstrom.Message) error {
		var body commitBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		st.commit(body.Offsets)
		return node.Reply(msg, commitOKBody{})
	})

	node.Handle("list_committed_offsets", func(msg maelstrom.Message) error {
		var body listBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		offsets := st.listCommitted(body.Keys)
		return node.Reply(msg, listCommittedOffsetsOKBody{Offsets: offsets})
	})

	// The library handles the "init" message automatically, but registering a
	// handler makes the init handler flow explicit and harmless.
	node.Handle("init", func(msg maelstrom.Message) error {
		return nil
	})

	node.Init(node.ID(), node.NodeIDs())

	fmt.Fprintf(os.Stderr, "node %s started\n", node.ID())

	if err := node.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "node run error: %s\n", err)
		os.Exit(1)
	}
}

// --- message body types ---

type sendBody struct {
	maelstrom.MessageBody
	Key  string      `json:"key"`
	Msg  interface{} `json:"msg"`
}

type sendOKBody struct {
	maelstrom.MessageBody
	Offset int `json:"offset"`
}

type pollBody struct {
	maelstrom.MessageBody
	Offsets map[string]int `json:"offsets"`
}

type pollOKBody struct {
	maelstrom.MessageBody
	Msgs map[string][][]interface{} `json:"msgs"`
}

type commitBody struct {
	maelstrom.MessageBody
	Offsets map[string]int `json:"offsets"`
}

type commitOKBody struct {
	maelstrom.MessageBody
}

type listBody struct {
	maelstrom.MessageBody
	Keys []string `json:"keys"`
}

type listCommittedOffsetsOKBody struct {
	maelstrom.MessageBody
	Offsets map[string]int `json:"offsets"`
}
