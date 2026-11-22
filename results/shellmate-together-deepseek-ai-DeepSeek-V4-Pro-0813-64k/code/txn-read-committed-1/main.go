package main

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type node struct {
	n     *maelstrom.Node
	mu    sync.Mutex
	store map[int64]int64
	other string
}

type txnBody struct {
	Type  string  `json:"type"`
	MsgID int     `json:"msg_id"`
	Txn   [][]any `json:"txn"`
}

func main() {
	n := maelstrom.NewNode()
	nd := &node{n: n, store: make(map[int64]int64)}

	n.Handle("init", func(msg maelstrom.Message) error {
		for _, id := range n.NodeIDs() {
			if id != n.ID() {
				nd.other = id
			}
		}
		return nil
	})
	n.Handle("txn", nd.handleTxn)
	n.Handle("replicate", nd.handleReplicate)

	go nd.syncLoop()

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

func (nd *node) handleTxn(msg maelstrom.Message) error {
	var body txnBody
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	pending := make(map[int64]int64)
	result := make([][]any, len(body.Txn))

	for i, op := range body.Txn {
		opType, _ := op[0].(string)
		key := int64(op[1].(float64))
		switch opType {
		case "r":
			if v, ok := pending[key]; ok {
				result[i] = []any{"r", key, v}
			} else {
				nd.mu.Lock()
				v, ok := nd.store[key]
				nd.mu.Unlock()
				if ok {
					result[i] = []any{"r", key, v}
				} else {
					result[i] = []any{"r", key, nil}
				}
			}
		case "w":
			v := int64(op[2].(float64))
			pending[key] = v
			result[i] = []any{"w", key, v}
		}
	}

	// Apply all writes atomically at the end of the transaction so that
	// intermediate values are never observable by other transactions.
	if len(pending) > 0 {
		nd.mu.Lock()
		for k, v := range pending {
			nd.store[k] = v
		}
		nd.mu.Unlock()
	}

	if err := nd.n.Reply(msg, map[string]any{"type": "txn_ok", "txn": result}); err != nil {
		return err
	}

	// Best-effort replication of the committed writes to the other node.
	if len(pending) > 0 && nd.other != "" {
		writes := make([][2]int64, 0, len(pending))
		for k, v := range pending {
			writes = append(writes, [2]int64{k, v})
		}
		if err := nd.n.Send(nd.other, map[string]any{"type": "replicate", "writes": writes}); err != nil {
			return err
		}
	}

	return nil
}

func (nd *node) handleReplicate(msg maelstrom.Message) error {
	var body struct {
		Type   string     `json:"type"`
		Writes [][2]int64 `json:"writes"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	nd.mu.Lock()
	for _, w := range body.Writes {
		nd.store[w[0]] = w[1]
	}
	nd.mu.Unlock()
	return nil
}

// syncLoop periodically pushes the full local store to the other node so that
// writes eventually propagate even when individual messages are lost during a
// network partition.
func (nd *node) syncLoop() {
	for {
		time.Sleep(100 * time.Millisecond)
		if nd.other == "" {
			continue
		}
		nd.mu.Lock()
		writes := make([][2]int64, 0, len(nd.store))
		for k, v := range nd.store {
			writes = append(writes, [2]int64{k, v})
		}
		nd.mu.Unlock()
		if len(writes) == 0 {
			continue
		}
		if err := nd.n.Send(nd.other, map[string]any{"type": "replicate", "writes": writes}); err != nil {
			log.Printf("sync send error: %s", err)
		}
	}
}
