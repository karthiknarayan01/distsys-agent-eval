package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"sync"
	"sync/atomic"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const opTimeout = time.Second

func isKeyDoesNotExist(err error) bool {
	var rpcErr *maelstrom.RPCError
	return errors.As(err, &rpcErr) && rpcErr.Code == maelstrom.KeyDoesNotExist
}

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewSeqKV(n)
	var barrierSeq atomic.Int64

	// Each node keeps its own contribution to the shared counter. The total is
	// the sum of every node's contribution, which is read back from seq-kv.
	var mu sync.Mutex
	local := 0
	seen := make(map[int]bool)

	n.Handle("add", func(msg maelstrom.Message) error {
		var body struct {
			MsgID int `json:"msg_id"`
			Delta int `json:"delta"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		mu.Lock()
		if seen[body.MsgID] {
			// Already applied this exact request.
			mu.Unlock()
			return n.Reply(msg, map[string]any{"type": "add_ok"})
		}
		seen[body.MsgID] = true
		local += body.Delta
		value := local

		// Persist our contribution. A plain write is idempotent: if the
		// response is lost and we retry, we write the same value again, so a
		// delta is never applied twice.
		for {
			ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
			err := kv.Write(ctx, "count-"+n.ID(), value)
			cancel()
			if err == nil {
				break
			}
		}
		mu.Unlock()

		return n.Reply(msg, map[string]any{"type": "add_ok"})
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		value := readTotal(n, kv, &barrierSeq)
		return n.Reply(msg, map[string]any{"type": "read_ok", "value": value})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

// readTotal returns the sum of every node's contribution. seq-kv reads may
// observe a slightly stale state, so we first write a fresh, unique value to a
// per-node barrier key. A state-changing write forces the sequential service
// to advance this client to its latest state, after which the counter reads
// are guaranteed to see every acknowledged add.
func readTotal(n *maelstrom.Node, kv *maelstrom.KV, seq *atomic.Int64) int {
	barrierKey := "barrier-" + n.ID()
	for {
		ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
		err := kv.Write(ctx, barrierKey, seq.Add(1))
		cancel()
		if err == nil {
			break
		}
	}

	total := 0
	for _, id := range n.NodeIDs() {
		for {
			ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
			v, err := kv.ReadInt(ctx, "count-"+id)
			cancel()
			if err != nil {
				if isKeyDoesNotExist(err) {
					break
				}
				continue
			}
			total += v
			break
		}
	}
	return total
}
