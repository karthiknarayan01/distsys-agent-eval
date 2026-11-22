package main

import (
	"encoding/json"
	"fmt"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type entry struct {
	offset int
	value interface{}
}

type nodeState struct {
	mu sync.RWMutex
	logs map[string][]entry
	nextOffset map[string]int
	committed map[string]int
}

func main() {
	n := maelstrom.NewNode()
	state := &nodeState{
		logs: make(map[string][]entry),
		nextOffset: make(map[string]int),
		committed: make(map[string]int),
	}

	n.Handle("init", func(msg maelstrom.Message) error {
		return n.Reply(msg, map[string]interface{}{"type": "init_ok"})
	})

	n.Handle("send", func(msg maelstrom.Message) error {
		var body struct {
			maelstrom.MessageBody
			Key string      `json:"key"`
			Msg interface{} `json:"msg"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return fmt.Errorf("unmarshal send: %w", err)
		}

		state.mu.Lock()
		defer state.mu.Unlock()

		logEntries, ok := state.logs[body.Key]
		if !ok {
			logEntries = []entry{}
		}
		off, ok := state.nextOffset[body.Key]
		if !ok || off == 0 {
			off = 1
		}
		logEntries = append(logEntries, entry{offset: off, value: body.Msg})
		state.logs[body.Key] = logEntries
		state.nextOffset[body.Key] = off + 1

		return n.Reply(msg, map[string]interface{}{
			"type": "send_ok",
			"offset": off,
		})
	})

	n.Handle("poll", func(msg maelstrom.Message) error {
		var body struct {
			maelstrom.MessageBody
			Offsets map[string]int `json:"offsets"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return fmt.Errorf("unmarshal poll: %w", err)
		}

		state.mu.RLock()
		defer state.mu.RUnlock()

		msgs := make(map[string]interface{})
		for key, startOff := range body.Offsets {
			logEntries, ok := state.logs[key]
			if !ok {
				continue
			}
			pairs := []interface{}{}
			found := false
			for _, e := range logEntries {
				if e.offset >= startOff {
					found = true
					pairs = append(pairs, []interface{}{e.offset, e.value})
				}
			}
			if found {
				msgs[key] = pairs
			}
		}

		return n.Reply(msg, map[string]interface{}{
			"type": "poll_ok",
			"msgs": msgs,
		})
	})

	n.Handle("commit_offsets", func(msg maelstrom.Message) error {
		var body struct {
			maelstrom.MessageBody
			Offsets map[string]int `json:"offsets"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return fmt.Errorf("unmarshal commit_offsets: %w", err)
		}

		state.mu.Lock()
		defer state.mu.Unlock()
		for k, v := range body.Offsets {
			if existing, ok := state.committed[k]; !ok || v > existing {
				state.committed[k] = v
			}
		}

		return n.Reply(msg, map[string]interface{}{"type": "commit_offsets_ok"})
	})

	n.Handle("list_committed_offsets", func(msg maelstrom.Message) error {
		var body struct {
			maelstrom.MessageBody
			Keys []string `json:"keys"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return fmt.Errorf("unmarshal list_committed_offsets: %w", err)
		}

		state.mu.RLock()
		defer state.mu.RUnlock()

		offsets := make(map[string]interface{})
		for _, k := range body.Keys {
			if off, ok := state.committed[k]; ok {
				offsets[k] = off
			}
		}

		return n.Reply(msg, map[string]interface{}{
			"type": "list_committed_offsets_ok",
			"offsets": offsets,
		})
	})

	if err := n.Run(); err != nil {
		fmt.Printf("ERROR: %v\n", err)
	}
}
