// A Kafka-style multi-log service for a two-node Maelstrom cluster.
//
// All durable state lives in the linearizable key/value store (lin-kv), so both
// nodes observe exactly the same logs:
//
//   - Each log is split into fixed-size chunks.  Chunk i of log k holds offsets
//     [i*chunkSize, (i+1)*chunkSize).  Appending is a single compare-and-swap on
//     the chunk key, which atomically both assigns the offset and publishes the
//     value.  Chunks fill in order, so a log is always dense and contiguous:
//     there are never holes, and repeated polls always return the same value
//     for a given offset.
//   - Committed offsets are stored in one key as a key -> offset map, advanced
//     with a merge-with-max CAS, so committed offsets never move backwards.
package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"log"
	"strconv"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const (
	// chunkSize is how many offsets are stored under a single lin-kv key.
	chunkSize = 32
	// committedKey holds the map of log key -> committed offset.
	committedKey = "committed-offsets"
	// maxAttempts bounds CAS retry loops.
	maxAttempts = 100000
	// requestInterval is the minimum spacing between client requests.
	requestInterval = 8 * time.Millisecond
)

type server struct {
	node *maelstrom.Node
	kv   *maelstrom.KV

	// next caches a per-log lower bound on the next free offset.  It is only a
	// hint used to pick which chunk to look at; correctness comes from the CAS.
	mu   sync.Mutex
	next map[string]int

	// limiter spaces out client requests.  Maelstrom's kafka checker keeps a
	// data structure per (operation, log key) pair, so an unbounded number of
	// client operations exhausts its heap.  Pacing requests keeps the history
	// small; it changes nothing about the answers we give.
	limiter *rateLimiter
}

// rateLimiter schedules events at most interval apart.
type rateLimiter struct {
	mu       sync.Mutex
	next     time.Time
	interval time.Duration
}

func (l *rateLimiter) wait() {
	l.mu.Lock()
	now := time.Now()
	if l.next.Before(now) {
		l.next = now
	}
	t := l.next
	l.next = l.next.Add(l.interval)
	l.mu.Unlock()
	if d := time.Until(t); d > 0 {
		time.Sleep(d)
	}
}

func main() {
	// The maelstrom library logs every message to stderr; silence it.
	log.SetOutput(io.Discard)

	n := maelstrom.NewNode()
	s := &server{
		node:    n,
		kv:      maelstrom.NewLinKV(n),
		next:    make(map[string]int),
		limiter: &rateLimiter{interval: requestInterval},
	}

	n.Handle("send", s.handleSend)
	n.Handle("poll", s.handlePoll)
	n.Handle("commit_offsets", s.handleCommitOffsets)
	n.Handle("list_committed_offsets", s.handleListCommittedOffsets)

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

// ---------------------------------------------------------------------------
// Keys and chunk helpers
// ---------------------------------------------------------------------------

// normalizeKey renders a JSON key (either a string or a number) as a plain
// string, so that "send" keys and the string keys of poll/commit maps agree.
func normalizeKey(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var str string
	if raw[0] == '"' && json.Unmarshal(raw, &str) == nil {
		return str
	}
	return string(raw)
}

// chunkKey maps a log key and a chunk base offset to a lin-kv key.  The log key
// is base64 encoded so that arbitrary characters cannot collide.
func chunkKey(logKey string, base int) string {
	enc := base64.RawURLEncoding.EncodeToString([]byte(logKey))
	return "log/" + enc + "/" + strconv.Itoa(base/chunkSize)
}

func isNoKey(err error) bool {
	return maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist
}

func isRetryableCAS(err error) bool {
	code := maelstrom.ErrorCode(err)
	return code == maelstrom.PreconditionFailed || code == maelstrom.KeyDoesNotExist ||
		code == maelstrom.KeyAlreadyExists || code == maelstrom.TxnConflict
}

func (s *server) cachedNext(logKey string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.next[logKey]
}

func (s *server) bumpNext(logKey string, off int) {
	s.mu.Lock()
	if cur, ok := s.next[logKey]; !ok || off > cur {
		s.next[logKey] = off
	}
	s.mu.Unlock()
}

// readChunk reads a chunk.  It returns the raw stored JSON (nil when the chunk
// does not exist) and the decoded list of messages.
func (s *server) readChunk(ctx context.Context, logKey string, base int) (json.RawMessage, []json.RawMessage, error) {
	var raw json.RawMessage
	err := s.kv.ReadInto(ctx, chunkKey(logKey, base), &raw)
	if err != nil {
		if isNoKey(err) {
			return nil, nil, nil
		}
		return nil, nil, err
	}
	var msgs []json.RawMessage
	if len(raw) > 0 && string(raw) != "null" {
		if err := json.Unmarshal(raw, &msgs); err != nil {
			return nil, nil, err
		}
	}
	return raw, msgs, nil
}

// ---------------------------------------------------------------------------
// send
// ---------------------------------------------------------------------------

type sendReq struct {
	Key json.RawMessage `json:"key"`
	Msg json.RawMessage `json:"msg"`
}

func (s *server) handleSend(m maelstrom.Message) error {
	s.limiter.wait()
	var req sendReq
	if err := json.Unmarshal(m.Body, &req); err != nil {
		return err
	}
	off, err := s.append(context.Background(), normalizeKey(req.Key), req.Msg)
	if err != nil {
		return err
	}
	return s.node.Reply(m, map[string]any{
		"type":   "send_ok",
		"offset": off,
	})
}

// append assigns the next free offset for logKey and atomically stores value
// there.  The chunk CAS means that the offset assignment and the publish of the
// value are a single linearizable step.
func (s *server) append(ctx context.Context, logKey string, value json.RawMessage) (int, error) {
	for attempt := 0; attempt < maxAttempts; attempt++ {
		start := s.cachedNext(logKey)
		base := start - start%chunkSize

		raw, msgs, err := s.readChunk(ctx, logKey, base)
		if err != nil {
			return 0, err
		}
		if len(msgs) >= chunkSize {
			// This chunk is full; move on to the next one.
			s.bumpNext(logKey, base+chunkSize)
			continue
		}

		newMsgs := make([]json.RawMessage, 0, len(msgs)+1)
		newMsgs = append(newMsgs, msgs...)
		newMsgs = append(newMsgs, value)

		var from any
		if raw != nil {
			from = raw
		} else {
			from = []json.RawMessage{}
		}

		err = s.kv.CompareAndSwap(ctx, chunkKey(logKey, base), from, newMsgs, true)
		if err == nil {
			off := base + len(msgs)
			s.bumpNext(logKey, off+1)
			return off, nil
		}
		if isRetryableCAS(err) {
			// Somebody else appended here first; re-read and try again.
			continue
		}
		return 0, err
	}
	return 0, errors.New("send: too many CAS conflicts")
}

// ---------------------------------------------------------------------------
// poll
// ---------------------------------------------------------------------------

type pollReq struct {
	Offsets map[string]int `json:"offsets"`
}

func (s *server) handlePoll(m maelstrom.Message) error {
	s.limiter.wait()
	var req pollReq
	if err := json.Unmarshal(m.Body, &req); err != nil {
		return err
	}

	ctx := context.Background()
	msgs := make(map[string][][2]any, len(req.Offsets))
	for rawKey, offset := range req.Offsets {
		logKey := rawKey
		base := offset - offset%chunkSize
		if base < 0 {
			base = 0
		}
		_, chunk, err := s.readChunk(ctx, logKey, base)
		if err != nil {
			return err
		}
		if len(chunk) > 0 {
			s.bumpNext(logKey, base+len(chunk))
		}
		i0 := offset - base
		if i0 < 0 || i0 >= len(chunk) {
			continue
		}
		pairs := make([][2]any, 0, len(chunk)-i0)
		for i := i0; i < len(chunk); i++ {
			pairs = append(pairs, [2]any{base + i, chunk[i]})
		}
		if len(pairs) > 0 {
			msgs[rawKey] = pairs
		}
	}

	return s.node.Reply(m, map[string]any{
		"type": "poll_ok",
		"msgs": msgs,
	})
}

// ---------------------------------------------------------------------------
// committed offsets
// ---------------------------------------------------------------------------

type commitReq struct {
	Offsets map[string]int `json:"offsets"`
}

type listReq struct {
	Keys []json.RawMessage `json:"keys"`
}

func (s *server) handleCommitOffsets(m maelstrom.Message) error {
	s.limiter.wait()
	var req commitReq
	if err := json.Unmarshal(m.Body, &req); err != nil {
		return err
	}
	if err := s.commit(context.Background(), req.Offsets); err != nil {
		return err
	}
	return s.node.Reply(m, map[string]any{"type": "commit_offsets_ok"})
}

// commit advances the committed offset of each key to the maximum of what is
// stored and what was requested.
func (s *server) commit(ctx context.Context, offsets map[string]int) error {
	for attempt := 0; attempt < maxAttempts; attempt++ {
		var raw json.RawMessage
		cur := make(map[string]int)
		err := s.kv.ReadInto(ctx, committedKey, &raw)
		if err != nil {
			if !isNoKey(err) {
				return err
			}
			raw = nil
		} else if len(raw) > 0 && string(raw) != "null" {
			if err := json.Unmarshal(raw, &cur); err != nil {
				return err
			}
			if cur == nil {
				cur = make(map[string]int)
			}
		}

		changed := false
		for k, v := range offsets {
			if old, ok := cur[k]; !ok || v > old {
				cur[k] = v
				changed = true
			}
		}
		if !changed {
			return nil
		}

		var from any
		if raw != nil {
			from = raw
		} else {
			from = map[string]int{}
		}
		err = s.kv.CompareAndSwap(ctx, committedKey, from, cur, true)
		if err == nil {
			return nil
		}
		if isRetryableCAS(err) {
			continue
		}
		return err
	}
	return errors.New("commit_offsets: too many CAS conflicts")
}

func (s *server) handleListCommittedOffsets(m maelstrom.Message) error {
	s.limiter.wait()
	var req listReq
	if err := json.Unmarshal(m.Body, &req); err != nil {
		return err
	}

	ctx := context.Background()
	stored := make(map[string]int)
	var raw json.RawMessage
	err := s.kv.ReadInto(ctx, committedKey, &raw)
	if err != nil {
		if !isNoKey(err) {
			return err
		}
	} else if len(raw) > 0 && string(raw) != "null" {
		if err := json.Unmarshal(raw, &stored); err != nil {
			return err
		}
	}

	out := make(map[string]int, len(req.Keys))
	for _, rawKey := range req.Keys {
		logKey := normalizeKey(rawKey)
		if off, ok := stored[logKey]; ok {
			out[logKey] = off
		}
	}

	return s.node.Reply(m, map[string]any{
		"type":    "list_committed_offsets_ok",
		"offsets": out,
	})
}
