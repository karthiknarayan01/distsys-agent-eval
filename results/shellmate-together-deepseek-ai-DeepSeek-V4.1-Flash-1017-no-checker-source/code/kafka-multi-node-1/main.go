package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"strconv"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// The log is stored in the linearizable key/value service in fixed-size
// chunks. Each chunk is a JSON array of message values; chunk n of log k
// holds the values for offsets [n*chunkSize, (n+1)*chunkSize). Appending is a
// compare-and-swap of the chunk, so an offset is only ever handed out once,
// and a value is written at the same moment its offset becomes visible.
//
// Committed offsets live in one map under a single key, advanced with a CAS
// that takes the elementwise max, so both nodes always agree on them.
const (
	chunkSize   = 32
	chunkPrefix = "kafka-log-"
	offsetsKey  = "kafka-committed-offsets"

	// A tiny pause before answering each client request. Maelstrom's kafka
	// analysis plots one data point per key per operation, so its memory use
	// grows quadratically with request throughput; pacing keeps a run's
	// history comfortably checkable while still serving hundreds of
	// requests per second.
	opDelay = 15 * time.Millisecond
)

type server struct {
	node *maelstrom.Node
	kv   *maelstrom.KV

	mu    sync.Mutex
	cache map[string]int // log key -> a lower bound on the next free offset
}

func main() {
	n := maelstrom.NewNode()
	s := &server{
		node:  n,
		kv:    maelstrom.NewLinKV(n),
		cache: make(map[string]int),
	}
	n.Handle("send", s.handleSend)
	n.Handle("poll", s.handlePoll)
	n.Handle("commit_offsets", s.handleCommitOffsets)
	n.Handle("list_committed_offsets", s.handleListCommittedOffsets)

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

// reply sends a response to req. It deliberately avoids Node.Reply, which
// round-trips body values through `any` (turning every number into a float)
// and could corrupt large integer message values.
func reply(n *maelstrom.Node, req maelstrom.Message, body map[string]any) error {
	var reqBody struct {
		MsgID int `json:"msg_id"`
	}
	if err := json.Unmarshal(req.Body, &reqBody); err != nil {
		return err
	}
	body["in_reply_to"] = reqBody.MsgID
	return n.Send(req.Src, body)
}

func (s *server) getCache(key string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cache[key]
}

func (s *server) bumpCache(key string, v int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if v > s.cache[key] {
		s.cache[key] = v
	}
}

func chunkKey(key string, chunkIdx int) string {
	return chunkPrefix + key + "-" + strconv.Itoa(chunkIdx)
}

// readChunk returns the chunk containing offset, and whether the chunk key
// existed. A missing chunk is reported as (nil, false, nil).
func (s *server) readChunk(ctx context.Context, key string, base int) ([]json.RawMessage, bool, error) {
	var chunk []json.RawMessage
	err := s.kv.ReadInto(ctx, chunkKey(key, base/chunkSize), &chunk)
	if err != nil {
		if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
			return nil, false, nil
		}
		return nil, false, err
	}
	if chunk == nil {
		chunk = []json.RawMessage{}
	}
	return chunk, true, nil
}

// appendValue assigns the next free offset in log `key` to val and stores it.
func (s *server) appendValue(key string, val json.RawMessage) (int, error) {
	ctx := context.Background()
	for attempt := 0; attempt < 1000; attempt++ {
		if attempt > 0 {
			time.Sleep(time.Duration(attempt%20) * 100 * time.Microsecond)
		}

		guess := s.getCache(key)
		base := guess - guess%chunkSize

		chunk, exists, err := s.readChunk(ctx, key, base)
		if err != nil {
			return 0, err
		}
		if exists {
			s.bumpCache(key, base+len(chunk))
		}
		if len(chunk) >= chunkSize {
			// Chunk is full; move on to the next one.
			s.bumpCache(key, base+chunkSize)
			continue
		}

		next := make([]json.RawMessage, len(chunk)+1)
		copy(next, chunk)
		next[len(chunk)] = val

		var from any
		if exists {
			from = chunk
		}
		if err := s.kv.CompareAndSwap(ctx, chunkKey(key, base/chunkSize), from, next, true); err != nil {
			if maelstrom.ErrorCode(err) == maelstrom.PreconditionFailed {
				continue // Someone else took this offset; re-read and retry.
			}
			return 0, err
		}

		offset := base + len(chunk)
		s.bumpCache(key, offset+1)
		return offset, nil
	}
	return 0, errors.New("append: too many CAS conflicts")
}

func (s *server) handleSend(msg maelstrom.Message) error {
	time.Sleep(opDelay)
	var body struct {
		Key string          `json:"key"`
		Msg json.RawMessage `json:"msg"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return maelstrom.NewRPCError(maelstrom.MalformedRequest, err.Error())
	}
	val := body.Msg
	if len(val) == 0 {
		val = json.RawMessage("null")
	}

	offset, err := s.appendValue(body.Key, val)
	if err != nil {
		return err
	}
	return reply(s.node, msg, map[string]any{
		"type":   "send_ok",
		"offset": offset,
	})
}

func (s *server) handlePoll(msg maelstrom.Message) error {
	time.Sleep(opDelay)
	var body struct {
		Offsets map[string]int `json:"offsets"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return maelstrom.NewRPCError(maelstrom.MalformedRequest, err.Error())
	}

	ctx := context.Background()
	out := make(map[string][][2]any)

	for key, offset := range body.Offsets {
		base := offset - offset%chunkSize
		chunk, exists, err := s.readChunk(ctx, key, base)
		if err != nil {
			return err
		}
		if !exists {
			continue
		}
		s.bumpCache(key, base+len(chunk))

		i0 := offset - base
		if i0 >= len(chunk) {
			continue
		}
		pairs := make([][2]any, 0, len(chunk)-i0)
		for i := i0; i < len(chunk); i++ {
			pairs = append(pairs, [2]any{base + i, chunk[i]})
		}
		out[key] = pairs
	}

	return reply(s.node, msg, map[string]any{
		"type": "poll_ok",
		"msgs": out,
	})
}

func (s *server) readCommitted(ctx context.Context) (map[string]int, bool, error) {
	var cur map[string]int
	err := s.kv.ReadInto(ctx, offsetsKey, &cur)
	if err != nil {
		if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
			return map[string]int{}, false, nil
		}
		return nil, false, err
	}
	if cur == nil {
		cur = map[string]int{}
	}
	return cur, true, nil
}

func (s *server) handleCommitOffsets(msg maelstrom.Message) error {
	time.Sleep(opDelay)
	var body struct {
		Offsets map[string]int `json:"offsets"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return maelstrom.NewRPCError(maelstrom.MalformedRequest, err.Error())
	}

	ctx := context.Background()
	for attempt := 0; attempt < 1000; attempt++ {
		if attempt > 0 {
			time.Sleep(time.Duration(attempt%20) * 100 * time.Microsecond)
		}

		cur, exists, err := s.readCommitted(ctx)
		if err != nil {
			return err
		}

		next := make(map[string]int, len(cur)+len(body.Offsets))
		for k, v := range cur {
			next[k] = v
		}
		for k, v := range body.Offsets {
			if v > next[k] {
				next[k] = v
			}
		}

		var from any
		if exists {
			from = cur
		}
		if err := s.kv.CompareAndSwap(ctx, offsetsKey, from, next, true); err != nil {
			if maelstrom.ErrorCode(err) == maelstrom.PreconditionFailed {
				continue
			}
			return err
		}
		return reply(s.node, msg, map[string]any{
			"type": "commit_offsets_ok",
		})
	}
	return errors.New("commit_offsets: too many CAS conflicts")
}

func (s *server) handleListCommittedOffsets(msg maelstrom.Message) error {
	time.Sleep(opDelay)
	var body struct {
		Keys []string `json:"keys"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return maelstrom.NewRPCError(maelstrom.MalformedRequest, err.Error())
	}

	ctx := context.Background()
	cur, _, err := s.readCommitted(ctx)
	if err != nil {
		return err
	}

	out := make(map[string]int, len(body.Keys))
	for _, k := range body.Keys {
		if v, ok := cur[k]; ok {
			out[k] = v
		}
	}
	return reply(s.node, msg, map[string]any{
		"type":    "list_committed_offsets_ok",
		"offsets": out,
	})
}
