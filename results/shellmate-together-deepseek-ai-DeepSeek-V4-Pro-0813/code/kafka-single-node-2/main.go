package main

import (
	"encoding/json"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type entry struct {
	offset int64
	value  json.RawMessage
}

type store struct {
	mu        sync.Mutex
	logs      map[string][]entry
	committed map[string]int64
}

func newStore() *store {
	return &store{
		logs:      make(map[string][]entry),
		committed: make(map[string]int64),
	}
}

func main() {
	n := maelstrom.NewNode()
	s := newStore()

	n.Handle("send", func(msg maelstrom.Message) error {
		time.Sleep(2 * time.Millisecond)
		var body struct {
			Key string          `json:"key"`
			Msg json.RawMessage `json:"msg"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		s.mu.Lock()
		log := s.logs[body.Key]
		offset := int64(len(log))
		s.logs[body.Key] = append(log, entry{offset: offset, value: body.Msg})
		s.mu.Unlock()

		return n.Reply(msg, map[string]any{
			"type":   "send_ok",
			"offset": offset,
		})
	})

	n.Handle("poll", func(msg maelstrom.Message) error {
		time.Sleep(2 * time.Millisecond)
		var body struct {
			Offsets map[string]int64 `json:"offsets"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		msgs := make(map[string][][2]any)
		s.mu.Lock()
		for key, off := range body.Offsets {
			log := s.logs[key]
			var pairs [][2]any
			for _, e := range log {
				if e.offset >= off {
					pairs = append(pairs, [2]any{e.offset, e.value})
				}
			}
			if len(pairs) > 0 {
				msgs[key] = pairs
			}
		}
		s.mu.Unlock()

		return n.Reply(msg, map[string]any{
			"type": "poll_ok",
			"msgs": msgs,
		})
	})

	n.Handle("commit_offsets", func(msg maelstrom.Message) error {
		time.Sleep(2 * time.Millisecond)
		var body struct {
			Offsets map[string]int64 `json:"offsets"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		s.mu.Lock()
		for key, off := range body.Offsets {
			if cur, ok := s.committed[key]; !ok || off > cur {
				s.committed[key] = off
			}
		}
		s.mu.Unlock()

		return n.Reply(msg, map[string]any{
			"type": "commit_offsets_ok",
		})
	})

	n.Handle("list_committed_offsets", func(msg maelstrom.Message) error {
		time.Sleep(2 * time.Millisecond)
		var body struct {
			Keys []string `json:"keys"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		offsets := make(map[string]int64)
		s.mu.Lock()
		for _, key := range body.Keys {
			if off, ok := s.committed[key]; ok {
				offsets[key] = off
			}
		}
		s.mu.Unlock()

		return n.Reply(msg, map[string]any{
			"type":    "list_committed_offsets_ok",
			"offsets": offsets,
		})
	})

	if err := n.Run(); err != nil {
		panic(err)
	}
}
