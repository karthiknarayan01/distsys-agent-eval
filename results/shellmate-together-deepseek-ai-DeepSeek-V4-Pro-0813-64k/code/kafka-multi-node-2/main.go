package main

import (
	"context"
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewLinKV(n)

	n.Handle("send", func(msg maelstrom.Message) error {
		var body struct {
			Key string          `json:"key"`
			Msg json.RawMessage `json:"msg"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		ctx := context.Background()
		logKey := "log:" + body.Key

		for {
			// Read the current log (a JSON array stored as a string).
			var old string
			v, err := kv.Read(ctx, logKey)
			if err != nil {
				if maelstrom.ErrorCode(err) != maelstrom.KeyDoesNotExist {
					return err
				}
				old = ""
			} else {
				old, _ = v.(string)
			}

			var arr []json.RawMessage
			if old != "" {
				if err := json.Unmarshal([]byte(old), &arr); err != nil {
					return err
				}
			}

			newArr := append(arr, body.Msg)
			newBytes, err := json.Marshal(newArr)
			if err != nil {
				return err
			}

			err = kv.CompareAndSwap(ctx, logKey, old, string(newBytes), true)
			if err == nil {
				return n.Reply(msg, map[string]any{
					"type":   "send_ok",
					"offset": len(arr),
				})
			}
			if maelstrom.ErrorCode(err) != maelstrom.PreconditionFailed {
				return err
			}
			// Someone else appended concurrently; retry.
		}
	})

	n.Handle("poll", func(msg maelstrom.Message) error {
		var body struct {
			Offsets map[string]int `json:"offsets"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		ctx := context.Background()
		msgs := map[string][]any{}

		for key, start := range body.Offsets {
			logKey := "log:" + key
			v, err := kv.Read(ctx, logKey)
			if err != nil {
				// No log for this key yet; omit it.
				continue
			}
			s, _ := v.(string)
			var arr []json.RawMessage
			if err := json.Unmarshal([]byte(s), &arr); err != nil {
				return err
			}
			if start < 0 {
				start = 0
			}
			if start >= len(arr) {
				continue
			}
			pairs := make([]any, 0, len(arr)-start)
			for i := start; i < len(arr); i++ {
				pairs = append(pairs, []any{i, arr[i]})
			}
			msgs[key] = pairs
		}

		return n.Reply(msg, map[string]any{
			"type": "poll_ok",
			"msgs": msgs,
		})
	})

	n.Handle("commit_offsets", func(msg maelstrom.Message) error {
		var body struct {
			Offsets map[string]int `json:"offsets"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		ctx := context.Background()
		for key, off := range body.Offsets {
			if err := kv.Write(ctx, "commit:"+key, off); err != nil {
				return err
			}
		}
		return n.Reply(msg, map[string]any{"type": "commit_offsets_ok"})
	})

	n.Handle("list_committed_offsets", func(msg maelstrom.Message) error {
		var body struct {
			Keys []string `json:"keys"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		ctx := context.Background()
		offsets := map[string]int{}
		for _, key := range body.Keys {
			v, err := kv.Read(ctx, "commit:"+key)
			if err != nil {
				continue
			}
			if off, ok := v.(int); ok {
				offsets[key] = off
			}
		}
		return n.Reply(msg, map[string]any{
			"type":    "list_committed_offsets_ok",
			"offsets": offsets,
		})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
