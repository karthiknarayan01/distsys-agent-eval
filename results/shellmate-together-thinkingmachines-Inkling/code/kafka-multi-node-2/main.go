package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const chunkSize = 32

type server struct {
	node         *maelstrom.Node
	kv           *maelstrom.KV
	mu           sync.Mutex
	offsetCache  map[string]int
}

func chunkKey(logKey string, offset int) string {
	return fmt.Sprintf("log-%s-%d", logKey, offset/chunkSize)
}

func (s *server) getChunk(ctx context.Context, logKey string, offset int) ([]any, error) {
	key := chunkKey(logKey, offset)
	val, err := s.kv.Read(ctx, key)
	if err != nil {
		if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
			return []any{}, nil
		}
		return nil, err
	}
	arr, ok := val.([]any)
	if !ok {
		return nil, fmt.Errorf("expected array at %s, got %T", key, val)
	}
	return arr, nil
}

func (s *server) tryAppend(ctx context.Context, logKey string, msg any) (int, error) {
	for {
		s.mu.Lock()
		o := s.offsetCache[logKey]
		s.mu.Unlock()

		chunk, err := s.getChunk(ctx, logKey, o)
		if err != nil {
			return 0, err
		}
		base := (o / chunkSize) * chunkSize
		if len(chunk) == chunkSize {
			// Chunk full, jump to next chunk.
			o = base + chunkSize
			s.mu.Lock()
			s.offsetCache[logKey] = o
			s.mu.Unlock()
			continue
		}
		offset := base + len(chunk)
		newChunk := make([]any, len(chunk)+1)
		copy(newChunk, chunk)
		newChunk[len(chunk)] = msg

		key := chunkKey(logKey, o)
		err = s.kv.CompareAndSwap(ctx, key, chunk, newChunk, true)
		if err == nil {
			s.mu.Lock()
			s.offsetCache[logKey] = offset + 1
			s.mu.Unlock()
			return offset, nil
		}
		if maelstrom.ErrorCode(err) == maelstrom.PreconditionFailed {
			continue
		}
		return 0, err
	}
}

func (s *server) poll(ctx context.Context, offsets map[string]int) (map[string][][2]interface{}, error) {
	result := make(map[string][][2]interface{})
	for k, off := range offsets {
		chunk, err := s.getChunk(ctx, k, off)
		if err != nil {
			return nil, err
		}
		base := (off / chunkSize) * chunkSize
		i0 := off - base
		if i0 >= len(chunk) {
			continue
		}
		pairs := make([][2]interface{}, 0, len(chunk)-i0)
		for i := i0; i < len(chunk); i++ {
			pairs = append(pairs, [2]interface{}{base + i, chunk[i]})
		}
		if len(pairs) > 0 {
			result[k] = pairs
		}
	}
	return result, nil
}

func (s *server) listCommitted(ctx context.Context, keys []string) (map[string]int, error) {
	result := make(map[string]int)
	for _, k := range keys {
		val, err := s.kv.ReadInt(ctx, "committed-"+k)
		if err != nil {
			if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
				continue
			}
			return nil, err
		}
		result[k] = val
	}
	return result, nil
}

func (s *server) commitOffsets(ctx context.Context, offsets map[string]int) error {
	for k, off := range offsets {
		for {
			val, err := s.kv.ReadInt(ctx, "committed-"+k)
			if err != nil {
				if maelstrom.ErrorCode(err) != maelstrom.KeyDoesNotExist {
					return err
				}
				// Key does not exist: create it.
				err = s.kv.Write(ctx, "committed-"+k, off)
				if err != nil {
					return err
				}
				break
			}
			if off <= val {
				// No update needed.
				break
			}
			// Try CAS.
			err = s.kv.CompareAndSwap(ctx, "committed-"+k, val, off, false)
			if err == nil {
				break
			}
			if maelstrom.ErrorCode(err) == maelstrom.PreconditionFailed {
				continue
			}
			return err
		}
	}
	return nil
}

func main() {
	n := maelstrom.NewNode()
	s := &server{
		node:        n,
		kv:          maelstrom.NewLinKV(n),
		offsetCache: make(map[string]int),
	}

	n.Handle("send", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		key, _ := body["key"].(string)
		msgVal := body["msg"]

		offset, err := s.tryAppend(context.Background(), key, msgVal)
		if err != nil {
			return err
		}
		return n.Reply(msg, map[string]any{
			"type":   "send_ok",
			"offset": offset,
		})
	})

	n.Handle("poll", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		offsets, ok := body["offsets"].(map[string]interface{})
		if !ok {
			return fmt.Errorf("invalid offsets")
		}
		offMap := make(map[string]int)
		for k, v := range offsets {
			switch val := v.(type) {
			case float64:
				offMap[k] = int(val)
			case int:
				offMap[k] = val
			default:
				return fmt.Errorf("invalid offset for %s: %v", k, v)
			}
		}

		msgs, err := s.poll(context.Background(), offMap)
		if err != nil {
			return err
		}
		return n.Reply(msg, map[string]any{
			"type": "poll_ok",
			"msgs": msgs,
		})
	})

	n.Handle("commit_offsets", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		offsets, ok := body["offsets"].(map[string]interface{})
		if !ok {
			return fmt.Errorf("invalid offsets")
		}
		offMap := make(map[string]int)
		for k, v := range offsets {
			switch val := v.(type) {
			case float64:
				offMap[k] = int(val)
			case int:
				offMap[k] = val
			default:
				return fmt.Errorf("invalid offset for %s: %v", k, v)
			}
		}

		if err := s.commitOffsets(context.Background(), offMap); err != nil {
			return err
		}
		return n.Reply(msg, map[string]any{
			"type": "commit_offsets_ok",
		})
	})

	n.Handle("list_committed_offsets", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		keys, ok := body["keys"].([]interface{})
		if !ok {
			return fmt.Errorf("invalid keys")
		}
		keyList := make([]string, 0, len(keys))
		for _, k := range keys {
			switch val := k.(type) {
			case string:
				keyList = append(keyList, val)
			default:
				return fmt.Errorf("invalid key: %v", k)
			}
		}

		offsets, err := s.listCommitted(context.Background(), keyList)
		if err != nil {
			return err
		}
		return n.Reply(msg, map[string]any{
			"type":     "list_committed_offsets_ok",
			"offsets": offsets,
		})
	})

	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}
}
