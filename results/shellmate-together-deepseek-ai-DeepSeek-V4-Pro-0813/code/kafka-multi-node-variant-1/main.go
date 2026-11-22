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

	// "append" adds an entry to a journal and answers with its position.
	n.Handle("append", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		journal := body["journal"].(string)
		entry := int(body["entry"].(float64))

		for {
			// Read the current log for this journal.
			log, err := kv.Read(context.Background(), journal)
			if err != nil {
				// Journal does not exist yet.
				log = []any{}
			}
			entries := log.([]any)
			position := len(entries)
			newEntries := append(entries, []any{position, entry})

			// Atomically append, retrying if someone else appended first.
			err = kv.CompareAndSwap(context.Background(), journal, log, newEntries, true)
			if err == nil {
				return n.Reply(msg, map[string]any{
					"type":     "appended",
					"position": position,
				})
			}
		}
	})

	// "fetch" reads several journals at once, each from a given position.
	n.Handle("fetch", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		positions := body["positions"].(map[string]any)
		entries := map[string]any{}

		for journal, posVal := range positions {
			pos := int(posVal.(float64))
			log, err := kv.Read(context.Background(), journal)
			if err != nil {
				// No such journal.
				continue
			}
			es := log.([]any)
			if pos < len(es) {
				entries[journal] = es[pos:]
			}
		}

		return n.Reply(msg, map[string]any{
			"type":    "fetched",
			"entries": entries,
		})
	})

	// "bookmark" records the position a reader has finished reading up to.
	n.Handle("bookmark", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		positions := body["positions"].(map[string]any)
		for journal, posVal := range positions {
			pos := int(posVal.(float64))
			if err := kv.Write(context.Background(), "commit:"+journal, pos); err != nil {
				return err
			}
		}

		return n.Reply(msg, map[string]any{
			"type": "bookmarked",
		})
	})

	// "bookmarks" asks for the bookmarked position of each journal named.
	n.Handle("bookmarks", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		journals := body["journals"].([]any)
		positions := map[string]any{}

		for _, jVal := range journals {
			journal := jVal.(string)
			pos, err := kv.ReadInt(context.Background(), "commit:"+journal)
			if err == nil {
				positions[journal] = pos
			}
		}

		return n.Reply(msg, map[string]any{
			"type":      "bookmarks_are",
			"positions": positions,
		})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
