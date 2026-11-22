package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type State struct {
	mu        sync.Mutex
	logs      map[string][]any
	committed map[string]int
}

func main() {
	n := maelstrom.NewNode()
	state := &State{
		logs:      make(map[string][]any),
		committed: make(map[string]int),
	}

	n.Handle("send", func(msg maelstrom.Message) error {
		var body struct {
			maelstrom.MessageBody
			Key string `json:"key"`
			Msg any    `json:"msg"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		state.mu.Lock()
		defer state.mu.Unlock()

		logEntries := state.logs[body.Key]
		offset := len(logEntries)
		logEntries = append(logEntries, body.Msg)
		state.logs[body.Key] = logEntries

		return n.Reply(msg, map[string]any{
			"type":   "send_ok",
			"offset": offset,
		})
	})

	n.Handle("poll", func(msg maelstrom.Message) error {
		var body struct {
			maelstrom.MessageBody
			Offsets map[string]int `json:"offsets"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		state.mu.Lock()
		defer state.mu.Unlock()

		msgs := make(map[string][]any)
		for k, start := range body.Offsets {
			entries := state.logs[k]
			if start < 0 {
			start = 0
		}
			if start >= len(entries) {
			continue
		}
			pairs := make([]any, 0, len(entries)-start)
			for i := start; i < len(entries); i++ {
			pairs = append(pairs, []any{i, entries[i]})
		}
		if len(pairs) > 0 {
			msgs[k] = pairs
		}
		}

		return n.Reply(msg, map[string]any{
			"type":  "poll_ok",
			"msgs": msgs,
		})
	})

	n.Handle("commit_offsets", func(msg maelstrom.Message) error {
		var body struct {
			maelstrom.MessageBody
			Offsets map[string]int `json:"offsets"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		state.mu.Lock()
		defer state.mu.Unlock()

		for k, v := range body.Offsets {
			state.committed[k] = v
		}

		return n.Reply(msg, map[string]any{
			"type": "commit_offsets_ok",
		})
	})

	n.Handle("list_committed_offsets", func(msg maelstrom.Message) error {
		var body struct {
			maelstrom.MessageBody
			Keys []string `json:"keys"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		state.mu.Lock()
		defer state.mu.Unlock()

		offsets := make(map[string]int)
		for _, k := range body.Keys {
			if v, ok := state.committed[k]; ok {
				offsets[k] = v
			}
		}

		return n.Reply(msg, map[string]any{
			"type":     "list_committed_offsets_ok",
			"offsets": offsets,
		})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

