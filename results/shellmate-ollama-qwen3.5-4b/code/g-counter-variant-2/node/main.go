package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	node "github.com/jepsen-io/maelstrom/demo/go"
)

// NodeState tracks local state for this node's turnstile
type NodeState struct {
	id         string
	localCount int64  // count at this specific turnstile
}

var (
	seqKV   *node.KV = nil
	kvKey   string
	ns       *NodeState
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle init message - get our node ID from the maelstrom initialization
	err := handleInit(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to handle init: %v\n", err)
		os.Exit(1)
	}

	seqKV = node.NewSeqKV(node.NewNode(), "seq-kv")
	kvKey = fmt.Sprintf("g-counter-%s", ns.id)
	ns.localCount = 0

	reader := os.Stdin
	writer := os.Stdout

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		buf := make([]byte, 16384)
		n, err := reader.Read(buf)
		if err != nil || n == 0 {
			break
		}

		line := string(buf[:n])
		var req map[string]interface{}
		json.Unmarshal([]byte(line), &req)
		mtype, _ := req["type"].(string)

		switch mtype {
		case "count":
			handleCount(ctx, writer, ns, kvKey, seqKV, line)
		case "total":
			handleTotal(ctx, writer, req)
		default:
			fmt.Fprintf(os.Stderr, "unknown message type: %v\n", mtype)
		}
	}

	seqKV.Close()
}

func handleInit(ctx context.Context) error {
	info, err := node.Handle(ctx, ctx, func(msg node.Message) error {
		switch msg.Type {
		case "init":
			var body map[string]interface{}
			json.Unmarshal(msg.Data, &body)
			ns.id = fmt.Sprintf("%v", body["self"])
		}
		return nil
	})
	if err != nil {
		return err
	}

	// Ensure we have our node ID set from the info message
	for _, b := range info.Bytes() {
		var m map[string]interface{}
		json.Unmarshal(b, &m)
		if val, ok := m["self"]; ok {
			ns.id = fmt.Sprintf("%v", val)
			break
		}
	}

	return nil
}

func handleCount(ctx context.Context, w *os.File, ns *NodeState, kvKey string, seqKV *node.KV, line string) {
	var req map[string]interface{}
	json.Unmarshal([]byte(line), &req)
	visitors := int(req["visitors"].(int))

	ns.localCount += int64(visitors)
	
	// Write to KV store for consistency across nodes (this value is the sum at this turnstile)
	seqKV.Write(ctx, kvKey, ns.localCount)

	// Reply immediately with what we've counted locally at this turnstile
	reply := map[string]interface{}{
		"type":        "counted",
		"in_reply_to": req["msg_id"].(int64),
	}
	json.NewEncoder(w).Encode(reply)
}

func handleTotal(ctx context.Context, w *os.File, req map[string]interface{}) {
	// Read the total from KV store (should have all 4 turnstiles' data accumulated)
	total, err := seqKV.ReadInt(ctx, kvKey)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error reading kv: %v\n", err)
	}

	reply := map[string]interface{}{
		"type":        "total_is",
		"in_reply_to": req["msg_id"].(int64),
		"visitors_so_far": total,
	}
	json.NewEncoder(w).Encode(reply)
}
