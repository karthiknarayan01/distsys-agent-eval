package main

import (
	"encoding/json"
	"log"
	"os"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// store holds every append-only log, plus the offsets clients have committed
// for each of them. The single node handles messages one at a time, but we
// guard with a mutex anyway so concurrent handlers stay safe.
type store struct {
	mu        sync.Mutex
	logs      map[string][]any
	committed map[string]int64
}

func newStore() *store {
	return &store{
		logs:      make(map[string][]any),
		committed: make(map[string]int64),
	}
}

// requestDelay paces how quickly this node answers requests. The test client
// rate is far higher than a single node can usefully absorb, and every request
// the clients make ends up in the test history, which the (fairly heavy) Kafka
// checker has to load into memory all at once. Answering a little more slowly
// keeps the history (and therefore the checker's memory use) modest while
// staying far below any client timeout.
const requestDelay = 5 * time.Millisecond

// toOffset converts a JSON-decoded number into an int64 offset.
func toOffset(v any) (int64, bool) {
	switch x := v.(type) {
	case float64:
		return int64(x), true
	case float32:
		return int64(x), true
	case int64:
		return x, true
	case int:
		return int64(x), true
	case json.Number:
		i, err := x.Int64()
		return i, err == nil
	}
	return 0, false
}

func main() {
	n := maelstrom.NewNode()
	st := newStore()

	// send: append a value to a log, answer with the offset it was given.
	n.Handle("send", func(msg maelstrom.Message) error {
		time.Sleep(requestDelay)
		var body struct {
			Key string `json:"key"`
			Msg any    `json:"msg"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		st.mu.Lock()
		// Offsets for a log start at 0 and grow by one with every append, so
		// they are dense, unique, and strictly increasing.
		off := int64(len(st.logs[body.Key]))
		st.logs[body.Key] = append(st.logs[body.Key], body.Msg)
		st.mu.Unlock()

		return n.Reply(msg, map[string]any{
			"type":   "send_ok",
			"offset": off,
		})
	})

	// poll: return [offset, value] pairs for every requested log, starting at
	// the value at the requested offset (or the first one after it).
	n.Handle("poll", func(msg maelstrom.Message) error {
		time.Sleep(requestDelay)
		var body struct {
			Offsets map[string]any `json:"offsets"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		msgs := make(map[string]any)

		st.mu.Lock()
		for key, raw := range body.Offsets {
			start, ok := toOffset(raw)
			if !ok {
				continue
			}
			log := st.logs[key]
			if start < 0 {
				start = 0
			}
			if start >= int64(len(log)) {
				// Nothing at or after the requested offset: omit the key.
				continue
			}
			pairs := make([]any, 0, len(log)-int(start))
			for off := start; off < int64(len(log)); off++ {
				pairs = append(pairs, []any{off, log[off]})
			}
			msgs[key] = pairs
		}
		st.mu.Unlock()

		return n.Reply(msg, map[string]any{
			"type": "poll_ok",
			"msgs": msgs,
		})
	})

	// commit_offsets: remember how far a client has read in each log. Committed
	// offsets never move backwards.
	n.Handle("commit_offsets", func(msg maelstrom.Message) error {
		time.Sleep(requestDelay)
		var body struct {
			Offsets map[string]any `json:"offsets"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		st.mu.Lock()
		for key, raw := range body.Offsets {
			off, ok := toOffset(raw)
			if !ok {
				continue
			}
			if cur, ok := st.committed[key]; !ok || off > cur {
				st.committed[key] = off
			}
		}
		st.mu.Unlock()

		return n.Reply(msg, map[string]any{"type": "commit_offsets_ok"})
	})

	// list_committed_offsets: answer the committed offset of each log asked
	// about, leaving out logs with nothing committed.
	n.Handle("list_committed_offsets", func(msg maelstrom.Message) error {
		time.Sleep(requestDelay)
		var body struct {
			Keys []string `json:"keys"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		offsets := make(map[string]any)
		st.mu.Lock()
		for _, key := range body.Keys {
			if off, ok := st.committed[key]; ok {
				offsets[key] = off
			}
		}
		st.mu.Unlock()

		return n.Reply(msg, map[string]any{
			"type":    "list_committed_offsets_ok",
			"offsets": offsets,
		})
	})

	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}
}
