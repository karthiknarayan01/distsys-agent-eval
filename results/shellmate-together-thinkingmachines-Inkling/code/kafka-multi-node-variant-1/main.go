package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	node := maelstrom.NewNode()
	kv := maelstrom.NewLinKV(node)

	node.Handle("append", func(msg maelstrom.Message) error {
		var body struct {
			maelstrom.MessageBody
			Journal string `json:"journal"`
			Entry   int    `json:"entry"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		pos, err := appendToJournal(context.Background(), kv, body.Journal, body.Entry)
		if err != nil {
			return err
		}

		return node.Reply(msg, map[string]any{
			"type":     "appended",
			"position": pos,
		})
	})

	node.Handle("fetch", func(msg maelstrom.Message) error {
		var body struct {
			maelstrom.MessageBody
			Positions map[string]int `json:"positions"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		result := make(map[string]any)
		for journal, start := range body.Positions {
			arr, err := readJournal(context.Background(), kv, journal)
			if err != nil {
				return err
			}
			if arr == nil {
				continue
			}
			if start < 0 {
				start = 0
			}
			if start >= len(arr) {
				continue
			}
			maxEntries := 100
			end := start + maxEntries
			if end > len(arr) {
				end = len(arr)
			}
			pairs := make([]any, 0, end-start)
			for i := start; i < end; i++ {
				entry := 0
				switch v := arr[i].(type) {
				case float64:
					entry = int(v)
				case int:
					entry = v
				default:
					entry = 0
				}
				pairs = append(pairs, []any{i, entry})
			}
			if len(pairs) > 0 {
				result[journal] = pairs
			}
		}

		return node.Reply(msg, map[string]any{
			"type":     "fetched",
			"entries": result,
		})
	})

	node.Handle("bookmark", func(msg maelstrom.Message) error {
		var body struct {
			maelstrom.MessageBody
			Positions map[string]int `json:"positions"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		for journal, pos := range body.Positions {
			if err := kv.Write(context.Background(), "bookmark:"+journal, pos); err != nil {
				return err
			}
		}

		return node.Reply(msg, map[string]any{
			"type": "bookmarked",
		})
	})

	node.Handle("bookmarks", func(msg maelstrom.Message) error {
		var body struct {
			maelstrom.MessageBody
			Journals []string `json:"journals"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		result := make(map[string]int)
		for _, journal := range body.Journals {
			v, err := kv.Read(context.Background(), "bookmark:"+journal)
			if err != nil {
				var rpcErr *maelstrom.RPCError
				if errors.As(err, &rpcErr) && rpcErr.Code == maelstrom.KeyDoesNotExist {
					continue
				}
				return err
			}
			switch val := v.(type) {
			case float64:
				result[journal] = int(val)
			case int:
				result[journal] = val
			default:
				result[journal] = 0
			}
		}

		return node.Reply(msg, map[string]any{
			"type":      "bookmarks_are",
			"positions": result,
		})
	})

	if err := node.Run(); err != nil {
		log.Fatalf("node error: %v", err)
	}
}

func readJournal(ctx context.Context, kv *maelstrom.KV, journal string) ([]any, error) {
	v, err := kv.Read(ctx, "journal:"+journal)
	if err != nil {
		var rpcErr *maelstrom.RPCError
		if errors.As(err, &rpcErr) && rpcErr.Code == maelstrom.KeyDoesNotExist {
			return nil, nil
		}
		return nil, err
	}
	arr, ok := v.([]any)
	if !ok {
		return nil, fmt.Errorf("journal %s has invalid format", journal)
	}
	return arr, nil
}

func appendToJournal(ctx context.Context, kv *maelstrom.KV, journal string, entry int) (int, error) {
	for {
		v, err := kv.Read(ctx, "journal:"+journal)
		var arr []any
		if err != nil {
			var rpcErr *maelstrom.RPCError
			if errors.As(err, &rpcErr) && rpcErr.Code == maelstrom.KeyDoesNotExist {
				arr = []any{}
			} else {
				return 0, err
			}
		} else {
			var ok bool
			arr, ok = v.([]any)
			if !ok {
				return 0, fmt.Errorf("journal %s has invalid format", journal)
			}
			copied := make([]any, len(arr))
			copy(copied, arr)
			arr = copied
		}

		newArr := make([]any, len(arr)+1)
		copy(newArr, arr)
		newArr[len(arr)] = entry

		var from any
		create := false
		if len(arr) == 0 && v == nil {
			from = []any{}
			create = true
		} else {
			from = v
		}

		err = kv.CompareAndSwap(ctx, "journal:"+journal, from, newArr, create)
		if err != nil {
			var rpcErr *maelstrom.RPCError
			if errors.As(err, &rpcErr) && rpcErr.Code == maelstrom.PreconditionFailed {
				continue
			}
			return 0, err
		}

		return len(arr), nil
	}
}
