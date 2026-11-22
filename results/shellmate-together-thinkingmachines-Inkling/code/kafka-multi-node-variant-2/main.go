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
	n := maelstrom.NewNode()
	kv := maelstrom.NewLinKV(n)

	n.Handle("append", func(msg maelstrom.Message) error {
		return handleAppend(context.Background(), n, kv, msg)
	})

	n.Handle("fetch", func(msg maelstrom.Message) error {
		return handleFetch(context.Background(), n, kv, msg)
	})

	n.Handle("bookmark", func(msg maelstrom.Message) error {
		return handleBookmark(context.Background(), n, kv, msg)
	})

	n.Handle("bookmarks", func(msg maelstrom.Message) error {
		return handleBookmarks(context.Background(), n, kv, msg)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

func handleAppend(ctx context.Context, n *maelstrom.Node, kv *maelstrom.KV, msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	journal := body["journal"].(string)
	entry := int(body["entry"].(float64))

	pos, err := appendJournal(ctx, kv, journal, entry)
	if err != nil {
		return err
	}

	return n.Reply(msg, map[string]any{
		"type":      "appended",
		"position": pos,
	})
}

func appendJournal(ctx context.Context, kv *maelstrom.KV, journal string, entry int) (int, error) {
	key := "journal:" + journal
	for {
		pairs, err := readPairs(ctx, kv, key)
		if err != nil {
			return 0, err
		}

		maxPos := -1
		for _, p := range pairs {
			if p[0] > maxPos {
				maxPos = p[0]
			}
		}
		newPos := maxPos + 1

		newPairs := append(pairs, []int{newPos, entry})
		newArr := pairsToAny(newPairs)

		var oldArr any
		if len(pairs) == 0 {
			oldArr = nil
		} else {
			oldArr = pairsToAny(pairs)
		}

		err = kv.CompareAndSwap(ctx, key, oldArr, newArr, len(pairs) == 0)
		if err == nil {
			return newPos, nil
		}

		var rpcErr *maelstrom.RPCError
		if errors.As(err, &rpcErr) && rpcErr.Code == maelstrom.PreconditionFailed {
			continue
		}

		return 0, err
	}
}

func pairsToAny(pairs [][]int) []any {
	arr := make([]any, len(pairs))
	for i, p := range pairs {
		arr[i] = []any{float64(p[0]), float64(p[1])}
	}
	return arr
}

func readPairs(ctx context.Context, kv *maelstrom.KV, key string) ([][]int, error) {
	val, err := kv.Read(ctx, key)
	if err != nil {
		var rpcErr *maelstrom.RPCError
		if errors.As(err, &rpcErr) && rpcErr.Code == maelstrom.KeyDoesNotExist {
			return [][]int{}, nil
		}
		return nil, err
	}

	arr, ok := val.([]any)
	if !ok {
		return nil, fmt.Errorf("journal %s is not an array", key)
	}

	pairs := make([][]int, len(arr))
	for i, item := range arr {
		inner, ok := item.([]any)
		if !ok || len(inner) != 2 {
			return nil, fmt.Errorf("bad pair in journal %s at index %d", key, i)
		}
		posF, ok1 := inner[0].(float64)
		entryF, ok2 := inner[1].(float64)
		if !ok1 || !ok2 {
			return nil, fmt.Errorf("bad numbers in journal %s", key)
		}
		pairs[i] = []int{int(posF), int(entryF)}
	}
	return pairs, nil
}

func handleFetch(ctx context.Context, n *maelstrom.Node, kv *maelstrom.KV, msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	positions, ok := body["positions"].(map[string]any)
	if !ok {
		return fmt.Errorf("bad positions")
	}

	entries := make(map[string]any)
	for journalName, posAny := range positions {
		pos := int(posAny.(float64))
		key := "journal:" + journalName
		pairs, err := readPairs(ctx, kv, key)
		if err != nil {
			return err
		}
		filtered := make([]any, 0)
		for _, p := range pairs {
			if p[0] >= pos {
				filtered = append(filtered, []any{float64(p[0]), float64(p[1])})
			}
		}
		if len(filtered) > 0 {
			entries[journalName] = filtered
		}
	}

	return n.Reply(msg, map[string]any{
		"type":     "fetched",
		"entries": entries,
	})
}

func handleBookmark(ctx context.Context, n *maelstrom.Node, kv *maelstrom.KV, msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	positions, ok := body["positions"].(map[string]any)
	if !ok {
		return fmt.Errorf("bad positions")
	}

	for journalName, posAny := range positions {
		pos := int(posAny.(float64))
		key := "bookmark:" + journalName
		if err := kv.Write(ctx, key, pos); err != nil {
			return err
		}
	}

	return n.Reply(msg, map[string]any{
		"type": "bookmarked",
	})
}

func handleBookmarks(ctx context.Context, n *maelstrom.Node, kv *maelstrom.KV, msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	journals, ok := body["journals"].([]any)
	if !ok {
		return fmt.Errorf("bad journals")
	}

	positions := make(map[string]any)
	for _, jAny := range journals {
		journalName := jAny.(string)
		key := "bookmark:" + journalName
		val, err := kv.Read(ctx, key)
		if err != nil {
			var rpcErr *maelstrom.RPCError
			if errors.As(err, &rpcErr) && rpcErr.Code == maelstrom.KeyDoesNotExist {
				continue
			}
			return err
		}
		positions[journalName] = val
	}

	return n.Reply(msg, map[string]any{
		"type":      "bookmarks_are",
		"positions": positions,
	})
}
