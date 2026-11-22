package main

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"strconv"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const chunkSize = 32

type server struct {
	node *maelstrom.Node
	kv   *maelstrom.KV

	mu          sync.Mutex
	offsetCache map[string]int
}

func chunkKey(key string, offset int) string {
	return "log-" + key + "-" + strconv.Itoa(offset/chunkSize)
}

func (s *server) getCache(key string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.offsetCache[key]
}

func (s *server) bumpCache(key string, offset int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if offset > s.offsetCache[key] {
		s.offsetCache[key] = offset
	}
}

func toFloat(v any) float64 {
	switch x := v.(type) {
	case float64:
		return x
	case int:
		return float64(x)
	case int64:
		return float64(x)
	case json.Number:
		f, _ := x.Float64()
		return f
	default:
		return 0
	}
}

// append appends msg to the log named key and returns the offset it was given.
// It retries on CAS conflicts until it succeeds.
func (s *server) append(ctx context.Context, key string, msg any) (int, error) {
	for {
		offset := s.getCache(key)
		ci := offset / chunkSize
		ck := chunkKey(key, offset)

		chunk := []any{}
		v, err := s.kv.Read(ctx, ck)
		if err == nil {
			if c, ok := v.([]any); ok {
				chunk = c
			}
		} else if maelstrom.ErrorCode(err) != maelstrom.KeyDoesNotExist {
			return 0, err
		}

		i := len(chunk)
		if i >= chunkSize {
			// Chunk is full; move to the next one.
			s.bumpCache(key, ci*chunkSize+chunkSize)
			continue
		}

		newChunk := make([]any, len(chunk)+1)
		copy(newChunk, chunk)
		newChunk[len(chunk)] = msg

		err = s.kv.CompareAndSwap(ctx, ck, chunk, newChunk, true)
		if err != nil {
			if maelstrom.ErrorCode(err) == maelstrom.PreconditionFailed {
				continue
			}
			return 0, err
		}

		off := ci*chunkSize + i
		s.bumpCache(key, off+1)
		return off, nil
	}
}

func (s *server) poll(ctx context.Context, offsets map[string]int) (map[string][][2]any, error) {
	msgs := map[string][][2]any{}
	for key, offset := range offsets {
		ci := offset / chunkSize
		ck := chunkKey(key, offset)

		v, err := s.kv.Read(ctx, ck)
		if err != nil {
			if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
				continue
			}
			return nil, err
		}
		chunk, _ := v.([]any)

		i0 := offset % chunkSize
		if i0 >= len(chunk) {
			continue
		}

		pairs := make([][2]any, 0, len(chunk)-i0)
		for i := i0; i < len(chunk); i++ {
			pairs = append(pairs, [2]any{ci*chunkSize + i, chunk[i]})
		}
		msgs[key] = pairs
	}
	return msgs, nil
}

func (s *server) commitOffsets(ctx context.Context, offsets map[string]int) error {
	for {
		oldMap := map[string]any{}
		v, err := s.kv.Read(ctx, "offsets")
		if err == nil {
			if m, ok := v.(map[string]any); ok {
				oldMap = m
			}
		} else if maelstrom.ErrorCode(err) != maelstrom.KeyDoesNotExist {
			return err
		}

		newMap := make(map[string]any, len(oldMap)+len(offsets))
		for k, val := range oldMap {
			newMap[k] = val
		}
		for k, val := range offsets {
			if float64(val) > toFloat(newMap[k]) {
				newMap[k] = float64(val)
			}
		}

		err = s.kv.CompareAndSwap(ctx, "offsets", oldMap, newMap, true)
		if err != nil {
			if maelstrom.ErrorCode(err) == maelstrom.PreconditionFailed {
				continue
			}
			return err
		}
		return nil
	}
}

func (s *server) listCommitted(ctx context.Context, keys []string) (map[string]int, error) {
	result := map[string]int{}
	v, err := s.kv.Read(ctx, "offsets")
	if err != nil {
		if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
			return result, nil
		}
		return nil, err
	}
	m, _ := v.(map[string]any)
	for _, k := range keys {
		if val, ok := m[k]; ok {
			result[k] = int(toFloat(val))
		}
	}
	return result, nil
}

type sendBody struct {
	maelstrom.MessageBody
	Key string `json:"key"`
	Msg any    `json:"msg"`
}

type pollBody struct {
	maelstrom.MessageBody
	Offsets map[string]int `json:"offsets"`
}

type commitBody struct {
	maelstrom.MessageBody
	Offsets map[string]int `json:"offsets"`
}

type listBody struct {
	maelstrom.MessageBody
	Keys []string `json:"keys"`
}

func (s *server) handleSend(msg maelstrom.Message) error {
	var body sendBody
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	offset, err := s.append(context.Background(), body.Key, body.Msg)
	if err != nil {
		return err
	}
	return s.node.Reply(msg, map[string]any{
		"type":   "send_ok",
		"offset": offset,
	})
}

func (s *server) handlePoll(msg maelstrom.Message) error {
	var body pollBody
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	msgs, err := s.poll(context.Background(), body.Offsets)
	if err != nil {
		return err
	}
	return s.node.Reply(msg, map[string]any{
		"type": "poll_ok",
		"msgs": msgs,
	})
}

func (s *server) handleCommitOffsets(msg maelstrom.Message) error {
	var body commitBody
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	if err := s.commitOffsets(context.Background(), body.Offsets); err != nil {
		return err
	}
	return s.node.Reply(msg, map[string]any{
		"type": "commit_offsets_ok",
	})
}

func (s *server) handleListCommittedOffsets(msg maelstrom.Message) error {
	var body listBody
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	offsets, err := s.listCommitted(context.Background(), body.Keys)
	if err != nil {
		return err
	}
	return s.node.Reply(msg, map[string]any{
		"type":    "list_committed_offsets_ok",
		"offsets": offsets,
	})
}

func main() {
	// Maelstrom's Go library logs every message to stderr; that is fine for
	// debugging but slows us down under load, so discard it.
	log.SetOutput(io.Discard)

	n := maelstrom.NewNode()
	kv := maelstrom.NewLinKV(n)
	s := &server{
		node:        n,
		kv:          kv,
		offsetCache: map[string]int{},
	}

	n.Handle("send", s.handleSend)
	n.Handle("poll", s.handlePoll)
	n.Handle("commit_offsets", s.handleCommitOffsets)
	n.Handle("list_committed_offsets", s.handleListCommittedOffsets)

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
