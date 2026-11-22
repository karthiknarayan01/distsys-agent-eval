package main

import (
	"encoding/json"
	"time"
	"fmt"
	"os"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

var (
	nodeID string
	other  string
	store  = make(map[int]int)
	mu     sync.Mutex
)

func main() {
	n := maelstrom.NewNode()

	n.Handle("init", func(msg maelstrom.Message) error {
		var body struct {
			maelstrom.MessageBody
			NodeID  string   `json:"node_id"`
			NodeIDs []string `json:"node_ids"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		nodeID = body.NodeID
		n.Init(body.NodeID, body.NodeIDs)
		for _, id := range body.NodeIDs {
			if id != body.NodeID {
				other = id
			}
		}
		return n.Reply(msg, maelstrom.MessageBody{Type: "init_ok"})
	})

	n.Handle("txn", func(msg maelstrom.Message) error {
		var body struct {
			maelstrom.MessageBody
			Txn [][]interface{} `json:"txn"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		mu.Lock()
		// Simulate transaction on a copy of current store
		txnState := make(map[int]int)
		for k, v := range store {
			txnState[k] = v
		}

		resTxn := make([][]interface{}, len(body.Txn))
		for i, opArr := range body.Txn {
			if len(opArr) != 3 {
				// malformed, but assume correct
			}
			f := opArr[0].(string)
			kFloat, ok := opArr[1].(float64)
			if !ok {
				// try int
				switch v := opArr[1].(type) {
				case int:
					kFloat = float64(v)
				case float64:
					kFloat = v
				default:
					kFloat = float64(opArr[1].(int))
				}
			}
			k := int(kFloat)
			var v interface{}
			if len(opArr) > 2 && opArr[2] != nil {
				switch val := opArr[2].(type) {
				case float64:
					v = int(val)
				case int:
					v = val
				case nil:
					v = nil
				default:
					v = val
				}
			}

			if f == "r" {
				val, exists := txnState[k]
				if exists {
					resTxn[i] = []interface{}{"r", k, val}
				} else {
					resTxn[i] = []interface{}{"r", k, nil}
				}
			} else if f == "w" {
			val := 0
			if intVal, ok := v.(int); ok {
				val = intVal
			} else if floatVal, ok := v.(float64); ok {
				val = int(floatVal)
			} else if v != nil {
				val = int(v.(float64))
			}
				txnState[k] = val
				store[k] = val
				resTxn[i] = []interface{}{"w", k, val}
			}
		}

		mu.Unlock()

		// No sync messages to avoid network congestion

		return n.Reply(msg, map[string]interface{}{
			"type": "txn_ok",
			"txn": resTxn,
		})
	})

	n.Handle("sync", func(msg maelstrom.Message) error {
		var body struct {
			maelstrom.MessageBody
			Store map[int]interface{} `json:"store"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		mu.Lock()
		for k, v := range body.Store {
			if valFloat, ok := v.(float64); ok {
				val := int(valFloat)
				if existing, exists := store[k]; !exists || val > existing {
					store[k] = val
				}
			} else if valInt, ok := v.(int); ok {
				if existing, exists := store[k]; !exists || valInt > existing {
					store[k] = valInt
				}
			}
		}
		mu.Unlock()
		return nil
	})



	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if other == "" {
					continue
				}
				mu.Lock()
				syncStore := make(map[int]int)
				for k, v := range store {
					syncStore[k] = v
				}
				mu.Unlock()
				body := map[string]interface{}{
					"type":  "sync",
					"store": syncStore,
				}
				_ = n.Send(other, body)
			}
		}
	}()

	if err := n.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
		os.Exit(1)
	}
}
