package main

import (
	"context"
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewSeqKV(n)

	n.Handle("init", func(msg maelstrom.Message) error {
		return nil
	})

	n.Handle("count", func(msg maelstrom.Message) error {
		var body struct {
			MsgId    int `json:"msg_id"`
			Visitors int `json:"visitors"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		ctx := context.Background()

		for {
			current, err := kv.ReadInt(ctx, "tally")
			if err != nil {
				if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
					current = 0
				} else {
					return err
				}
			}

			newValue := current + body.Visitors
			err = kv.CompareAndSwap(ctx, "tally", current, newValue, true)
			if err == nil {
				break
			}
			if maelstrom.ErrorCode(err) == maelstrom.PreconditionFailed {
				continue
			}
			return err
		}

		return n.Reply(msg, map[string]interface{}{
			"type": "counted",
		})
	})

	n.Handle("total", func(msg maelstrom.Message) error {
		var body struct {
			MsgId int `json:"msg_id"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		ctx := context.Background()

		// Force sequential service to observe the latest state by performing
		// a write that changes the service state. After this, the client's
		// observed index is the latest, so the following read sees the
		// most recent value.
		_ = kv.Write(ctx, "read_sync", body.MsgId)

		current, err := kv.ReadInt(ctx, "tally")
		if err != nil {
			if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
				current = 0
			} else {
				return err
			}
		}

		return n.Reply(msg, map[string]interface{}{
			"type":              "total_is",
			"visitors_so_far": current,
		})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
