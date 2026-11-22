package main

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// Store is a thread-safe map of integer keys to integer values.
type Store struct {
	mu   sync.Mutex
	data map[int]int
}

func (s *Store) Snapshot() map[int]int {
	s.mu.Lock()
	defer s.mu.Unlock()
	m := make(map[int]int, len(s.data))
	for k, v := range s.data {
		m[k] = v
	}
	return m
}

func main() {
	n := maelstrom.NewNode()
	store := &Store{data: make(map[int]int)}

	// After init, periodically gossip our whole store to the other node. This
	// makes writes eventually reach the other node even if a one-off sync
	// message was dropped while the network was partitioned.
	n.Handle("init", func(msg maelstrom.Message) error {
		go func() {
			for {
				time.Sleep(100 * time.Millisecond)
				snap := store.Snapshot()
				if len(snap) == 0 {
					continue
				}
				writes := make([][2]int, 0, len(snap))
				for k, v := range snap {
					writes = append(writes, [2]int{k, v})
				}
				for _, id := range n.NodeIDs() {
					if id != n.ID() {
						_ = n.Send(id, map[string]any{
							"type":   "sync",
							"writes": writes,
						})
					}
				}
			}
		}()
		return nil
	})

	n.Handle("txn", func(msg maelstrom.Message) error {
		var body struct {
			Txn [][]any `json:"txn"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// Apply the whole transaction atomically so that no other transaction
		// can observe an intermediate value of a key that is written more than
		// once within this transaction.
		store.mu.Lock()
		finalWrites := make(map[int]int)
		for _, op := range body.Txn {
			if len(op) < 3 {
				continue
			}
			f, _ := op[0].(string)
			k := int(op[1].(float64))
			switch f {
			case "r":
				if v, ok := store.data[k]; ok {
					op[2] = v
				} else {
					op[2] = nil
				}
			case "w":
				v := int(op[2].(float64))
				store.data[k] = v
				finalWrites[k] = v
			}
		}
		store.mu.Unlock()

		if err := n.Reply(msg, map[string]any{
			"type": "txn_ok",
			"txn":  body.Txn,
		}); err != nil {
			return err
		}

		// Replicate only the final value of each key written by this
		// transaction, so intermediate values never become visible elsewhere.
		if len(finalWrites) > 0 {
			writes := make([][2]int, 0, len(finalWrites))
			for k, v := range finalWrites {
				writes = append(writes, [2]int{k, v})
			}
			for _, id := range n.NodeIDs() {
				if id != n.ID() {
					if err := n.Send(id, map[string]any{
						"type":   "sync",
						"writes": writes,
					}); err != nil {
						return err
					}
				}
			}
		}
		return nil
	})

	n.Handle("sync", func(msg maelstrom.Message) error {
		var body struct {
			Writes [][2]int `json:"writes"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		store.mu.Lock()
		for _, w := range body.Writes {
			store.data[w[0]] = w[1]
		}
		store.mu.Unlock()
		return nil
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
