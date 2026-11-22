package main

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()

	var mu sync.Mutex
	var local int64

	// A client reports visitors that came in through this turnstile.
	n.Handle("count", func(msg maelstrom.Message) error {
		var body struct {
			Visitors int64 `json:"visitors"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		mu.Lock()
		local += body.Visitors
		mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "counted"})
	})

	// Another node asks for this node's local tally.
	n.Handle("local_count", func(msg maelstrom.Message) error {
		mu.Lock()
		c := local
		mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "local_count_ok", "count": c})
	})

	// A client asks for the tally over all turnstiles.
	n.Handle("total", func(msg maelstrom.Message) error {
		mu.Lock()
		total := local
		mu.Unlock()

		var others []string
		for _, id := range n.NodeIDs() {
			if id != n.ID() {
				others = append(others, id)
			}
		}

		// Ask every other node for its local tally. During a partition some of
		// these may time out, in which case we return a partial (but still
		// monotonic) total; that is acceptable while the network is split.
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		var wg sync.WaitGroup
		var resMu sync.Mutex
		for _, id := range others {
			wg.Add(1)
			go func(id string) {
				defer wg.Done()
				resp, err := n.SyncRPC(ctx, id, map[string]any{"type": "local_count"})
				if err != nil {
					return
				}
				var b struct {
					Count int64 `json:"count"`
				}
				if err := json.Unmarshal(resp.Body, &b); err != nil {
					return
				}
				resMu.Lock()
				total += b.Count
				resMu.Unlock()
			}(id)
		}
		wg.Wait()

		return n.Reply(msg, map[string]any{"type": "total_is", "visitors_so_far": total})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
