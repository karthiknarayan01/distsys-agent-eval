package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const chunkSize = 32

type state struct {
	node  *maelstrom.Node
	kv    *maelstrom.KV
	cacheMu sync.Mutex
	cache   map[string]int
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func chunkKey(logKey string, offset int) string {
	base := offset - (offset % chunkSize)
	chunkIdx := base / chunkSize
	return fmt.Sprintf("log-%s-%d", logKey, chunkIdx)
}

func (s *state) handleSend(msg maelstrom.Message) error {
	var body struct {
		Type  string      `json:"type"`
		MsgID int         `json:"msg_id"`
		Key   string      `json:"key"`
		Msg   interface{} `json:"msg"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	ctx := context.Background()

	for {
		s.cacheMu.Lock()
		offset := s.cache[body.Key]
		s.cacheMu.Unlock()

		base := offset - (offset % chunkSize)
		ck := chunkKey(body.Key, offset)

		chunkVal, err := s.kv.Read(ctx, ck)
		var chunk []interface{}
		if err != nil {
			var rpcErr *maelstrom.RPCError
			if errors.As(err, &rpcErr) && rpcErr.Code == maelstrom.KeyDoesNotExist {
				chunk = []interface{}{}
			} else {
				return err
			}
		} else {
			arr, ok := chunkVal.([]interface{})
			if !ok {
				return fmt.Errorf("unexpected chunk type %T for %s", chunkVal, ck)
			}
			chunk = arr
		}

		if len(chunk) >= chunkSize {
			newOffset := base + chunkSize
			s.cacheMu.Lock()
			s.cache[body.Key] = maxInt(s.cache[body.Key], newOffset)
			s.cacheMu.Unlock()
			continue
		}

		newChunk := make([]interface{}, len(chunk)+1)
		copy(newChunk, chunk)
		newChunk[len(chunk)] = body.Msg

		err = s.kv.CompareAndSwap(ctx, ck, chunk, newChunk, true)
		if err != nil {
			var rpcErr *maelstrom.RPCError
			if errors.As(err, &rpcErr) && (rpcErr.Code == maelstrom.PreconditionFailed || rpcErr.Code == maelstrom.TxnConflict) {
				continue
			}
			return err
		}

		newOffset := base + len(chunk)
		s.cacheMu.Lock()
		s.cache[body.Key] = maxInt(s.cache[body.Key], newOffset+1)
		s.cacheMu.Unlock()

		return s.node.Reply(msg, map[string]interface{}{
			"type":   "send_ok",
			"offset": newOffset,
		})
	}
}

func (s *state) handlePoll(msg maelstrom.Message) error {
	var body struct {
		Type    string            `json:"type"`
		MsgID   int               `json:"msg_id"`
		Offsets map[string]int   `json:"offsets"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	ctx := context.Background()
	msgs := make(map[string][][2]interface{})

	for k, startOffset := range body.Offsets {
		base := startOffset - (startOffset % chunkSize)
		ck := chunkKey(k, startOffset)

		chunkVal, err := s.kv.Read(ctx, ck)
		var chunk []interface{}
		if err != nil {
			var rpcErr *maelstrom.RPCError
			if errors.As(err, &rpcErr) && rpcErr.Code == maelstrom.KeyDoesNotExist {
				chunk = []interface{}{}
			} else {
				return err
			}
		} else {
			arr, ok := chunkVal.([]interface{})
			if !ok {
				return fmt.Errorf("unexpected chunk type %T for %s", chunkVal, ck)
			}
			chunk = arr
		}

		maxOffset := base + len(chunk) - 1
		s.cacheMu.Lock()
		s.cache[k] = maxInt(s.cache[k], maxOffset+1)
		s.cacheMu.Unlock()

		i0 := startOffset % chunkSize
		if i0 < len(chunk) {
			pairs := make([][2]interface{}, 0, len(chunk)-i0)
			for i := i0; i < len(chunk); i++ {
				offset := startOffset + (i - i0)
				pairs = append(pairs, [2]interface{}{offset, chunk[i]})
			}
			msgs[k] = pairs
		}
	}

	return s.node.Reply(msg, map[string]interface{}{
		"type": "poll_ok",
		"msgs": msgs,
	})
}

func (s *state) handleCommitOffsets(msg maelstrom.Message) error {
	var body struct {
		Type    string            `json:"type"`
		MsgID   int               `json:"msg_id"`
		Offsets map[string]int   `json:"offsets"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	ctx := context.Background()
	key := "offsets"

	for {
		val, err := s.kv.Read(ctx, key)
		current := make(map[string]interface{})
		if err != nil {
			var rpcErr *maelstrom.RPCError
			if errors.As(err, &rpcErr) && rpcErr.Code == maelstrom.KeyDoesNotExist {
				current = make(map[string]interface{})
			} else {
				return err
			}
		} else {
			m, ok := val.(map[string]interface{})
			if !ok {
				return fmt.Errorf("offsets value not a map: %T", val)
			}
			current = m
		}

		newOffsets := make(map[string]interface{}, len(current))
		for k, v := range current {
			newOffsets[k] = v
		}

		for k, v := range body.Offsets {
			existingVal, ok := current[k]
			if !ok {
				newOffsets[k] = v
				continue
			}
			existingInt := 0
			switch ev := existingVal.(type) {
			case float64:
				existingInt = int(ev)
			case int:
				existingInt = ev
			default:
				return fmt.Errorf("unexpected offset value type for %s: %T", k, existingVal)
			}
			if v > existingInt {
				newOffsets[k] = v
			} else {
				newOffsets[k] = existingVal
			}
		}

		err = s.kv.CompareAndSwap(ctx, key, current, newOffsets, true)
		if err != nil {
			var rpcErr *maelstrom.RPCError
			if errors.As(err, &rpcErr) && (rpcErr.Code == maelstrom.PreconditionFailed || rpcErr.Code == maelstrom.TxnConflict) {
				continue
			}
			return err
		}

		return s.node.Reply(msg, map[string]interface{}{
			"type": "commit_offsets_ok",
		})
	}
}

func (s *state) handleListCommittedOffsets(msg maelstrom.Message) error {
	var body struct {
		Type string   `json:"type"`
		MsgID int     `json:"msg_id"`
		Keys []string `json:"keys"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	ctx := context.Background()
	key := "offsets"

	val, err := s.kv.Read(ctx, key)
	current := make(map[string]interface{})
	if err != nil {
		var rpcErr *maelstrom.RPCError
		if errors.As(err, &rpcErr) && rpcErr.Code == maelstrom.KeyDoesNotExist {
			current = make(map[string]interface{})
		} else {
			return err
		}
	} else {
		m, ok := val.(map[string]interface{})
		if !ok {
			return fmt.Errorf("offsets value not a map: %T", val)
		}
		current = m
	}

	result := make(map[string]interface{})
	for _, k := range body.Keys {
		if v, ok := current[k]; ok {
			result[k] = v
		}
	}

	return s.node.Reply(msg, map[string]interface{}{
		"type":    "list_committed_offsets_ok",
		"offsets": result,
	})
}

func main() {
	n := maelstrom.NewNode()
	s := &state{
		node:  n,
		kv:    maelstrom.NewLinKV(n),
		cache: make(map[string]int),
	}

	n.Handle("send", s.handleSend)
	n.Handle("poll", s.handlePoll)
	n.Handle("commit_offsets", s.handleCommitOffsets)
	n.Handle("list_committed_offsets", s.handleListCommittedOffsets)

	if err := n.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "run error: %v\n", err)
		os.Exit(1)
	}
}
