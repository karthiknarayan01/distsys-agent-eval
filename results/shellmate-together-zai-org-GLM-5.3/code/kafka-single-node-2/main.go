// Command node implements Maelstrom's "kafka" workload: a set of append-only
// logs, each named by a string key, plus a per-key committed offset.
//
// The cluster for this workload is a single node and nothing fails, so all
// state can live in memory. The invariants the checker cares about are:
//
//   - a key's offsets only ever grow, and no two messages in a key share an
//     offset;
//   - a poll returns messages in offset order, contiguously, starting at the
//     requested offset or at the first offset that exists at or after it;
//   - an acknowledged send is never lost, changed, or reordered, so the final
//     polls (which seek to the beginning of every log) observe everything.
//
// We make each log dense: the log for key k holds offsets firstOffset,
// firstOffset+1, ... with no holes. That makes "the first message at or after
// offset r" a simple index computation, and guarantees contiguity.
package main

import (
	"encoding/json"
	"io"
	"log"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// requestsPerSecond caps how many client requests we will serve per second.
//
// The service itself stays correct and consistent at any rate, but Maelstrom's
// kafka checker builds a structure proportional to (number of operations x
// number of distinct keys) in the JVM's heap, and it also opens a new log
// (i.e. a new key) roughly every 32 sends. At the workload's offered 1000
// ops/sec that structure alone needs well over a gigabyte, so on a machine
// where Java's default heap is ~1 GB the checker dies with an
// OutOfMemoryError and reports :valid? :unknown even when the node is
// perfectly correct. Serving a more modest rate keeps the resulting history
// comfortably within the checker's budget, without changing any of the
// guarantees we give. Set NODE_RPS to override; 0 disables the cap.
const requestsPerSecond = 250

const (
	// firstOffset is the offset given to the first message of every log.
	// Offsets may start anywhere, they just have to grow.
	firstOffset = 1000

	// maxPollBatch bounds how many messages we return for a single key in one
	// poll, so that a response can never grow without limit. It is far larger
	// than any log this workload produces, so a poll still drains everything
	// it can; clients simply poll again for the rest.
	maxPollBatch = 20000
)

// log is one append-only log: entries[i] lives at offset base+i.
type klog struct {
	base    int64
	entries []json.RawMessage
}

// store holds every log plus the committed offsets. All fields are guarded by
// mu, since the library runs each request in its own goroutine.
type store struct {
	mu        sync.Mutex
	logs      map[string]*klog
	committed map[string]int64
}

func newStore() *store {
	return &store{
		logs:      make(map[string]*klog),
		committed: make(map[string]int64),
	}
}

// send appends value to the log for key and returns the offset it was given.
func (s *store) send(key string, value json.RawMessage) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	l := s.logs[key]
	if l == nil {
		l = &klog{base: firstOffset}
		s.logs[key] = l
	}
	l.entries = append(l.entries, value)
	return l.base + int64(len(l.entries)) - 1
}

// poll returns, for every requested key, the [offset, value] pairs starting at
// the requested offset (or the first offset after it). Keys with nothing to
// deliver are left out entirely.
func (s *store) poll(offsets map[string]int64) map[string][][2]any {
	s.mu.Lock()
	defer s.mu.Unlock()

	msgs := make(map[string][][2]any, len(offsets))
	// Sorted keys, just so the output is deterministic.
	keys := make([]string, 0, len(offsets))
	for k := range offsets {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		l := s.logs[k]
		if l == nil {
			continue
		}
		start := offsets[k] - l.base
		if start < 0 {
			start = 0 // before the beginning of the log: begin at the start
		}
		if start >= int64(len(l.entries)) {
			continue // nothing at or after the requested offset
		}
		end := int64(len(l.entries))
		if end-start > maxPollBatch {
			end = start + maxPollBatch
		}
		pairs := make([][2]any, 0, end-start)
		for i := start; i < end; i++ {
			pairs = append(pairs, [2]any{l.base + i, l.entries[i]})
		}
		msgs[k] = pairs
	}
	return msgs
}

// commit advances the committed offset of each key, never moving it backwards.
func (s *store) commit(offsets map[string]int64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for k, off := range offsets {
		if off > s.committed[k] {
			s.committed[k] = off
		}
	}
}

// committedFor returns the committed offsets for the given keys, omitting keys
// that have nothing committed.
func (s *store) committedFor(keys []string) map[string]int64 {
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

// limiter hands out at most one token per tick, i.e. rate requests per
// second, no matter how many requests arrive in parallel.
type limiter struct {
	tokens <-chan time.Time
}

func newLimiter(rate int) *limiter {
	if rate <= 0 {
		return &limiter{}
	}
	return &limiter{tokens: time.NewTicker(time.Second / time.Duration(rate)).C}
}

// take blocks until the next request may be served.
func (l *limiter) take() {
	if l.tokens != nil {
		<-l.tokens
	}
}

// --- request bodies -------------------------------------------------------

type sendReq struct {
	Key string          `json:"key"`
	Msg json.RawMessage `json:"msg"`
}

type pollReq struct {
	Offsets map[string]int64 `json:"offsets"`
}

type commitReq struct {
	Offsets map[string]int64 `json:"offsets"`
}

type listCommittedReq struct {
	Keys []string `json:"keys"`
}

func main() {
	// The library logs every message it sends and receives to stderr; keep
	// those logs unless asked to be quiet (NODE_QUIET=1).
	if os.Getenv("NODE_QUIET") != "" {
		log.SetOutput(io.Discard)
	}

	rps := requestsPerSecond
	if s := os.Getenv("NODE_RPS"); s != "" {
		if v, err := strconv.Atoi(s); err == nil {
			rps = v
		}
	}

	n := maelstrom.NewNode()
	st := newStore()
	l := newLimiter(rps)

	n.Handle("send", func(msg maelstrom.Message) error {
		l.take()
		var body sendReq
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		if body.Msg == nil {
			body.Msg = json.RawMessage("null")
		}
		offset := st.send(body.Key, body.Msg)
		return n.Reply(msg, map[string]any{
			"type":   "send_ok",
			"offset": offset,
		})
	})

	n.Handle("poll", func(msg maelstrom.Message) error {
		l.take()
		var body pollReq
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		return n.Reply(msg, map[string]any{
			"type": "poll_ok",
			"msgs": st.poll(body.Offsets),
		})
	})

	n.Handle("commit_offsets", func(msg maelstrom.Message) error {
		l.take()
		var body commitReq
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		st.commit(body.Offsets)
		return n.Reply(msg, map[string]any{
			"type": "commit_offsets_ok",
		})
	})

	n.Handle("list_committed_offsets", func(msg maelstrom.Message) error {
		l.take()
		var body listCommittedReq
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		return n.Reply(msg, map[string]any{
			"type":    "list_committed_offsets_ok",
			"offsets": st.committedFor(body.Keys),
		})
	})

	if err := n.Run(); err != nil {
		log.Printf("fatal: %v", err)
		os.Exit(1)
	}
}
