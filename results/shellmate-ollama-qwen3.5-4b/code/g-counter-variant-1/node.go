package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/jepsen-io/maelstrom/demo/go"
)

// countReq represents the count request message
type countReq struct {
	MsgID   int      `json:"msg_id"`
	Visitors int      `json:"visitors"`
}

// countRes represents the counted response message  
type countRes struct {
	InReplyTo int `json:"in_reply_to"`
}

// totalReq represents the total request message
type totalReq struct {
	MsgID int `json:"msg_id"`
}

// totalRes represents the total_is response message
type totalRes struct {
	InReplyTo     int  `json:"in_reply_to"`
	VisitorsSoFar int  `json:"visitors_so_far"`
}

func main() {
	node := go.NewNode()

	// Handle init - get node ID and cluster IDs
	node.Handle("init", func(ctx context.Context, in []byte) (out []byte, err error) {
		var req struct{}
		json.Unmarshal(in, &req)
		
		return json.Marshal(goinit(node))
	})

	// Handle run - start the loop
	node.Handle("Run", func(ctx context.Context, in []byte) (out []byte, err error) {
		var req struct{}
		json.Unmarshal(in, &req)
		
		// Start running the event loop
		go node.Run()
		return nil, nil
	})

	// Handle count - increment counter atomically using CompareAndSwap
	node.Handle("count", func(ctx context.Context, in []byte) (out []byte, err error) {
		var req countReq
		if err := json.Unmarshal(in, &req); err != nil {
			return nil, fmt.Errorf("unmarshal failed: %w", err)
		}

		// Read current total from KV store
		readers, err := node.RW(ctx, "counter", 0)
		if err != nil {
			return nil, fmt.Errorf("read counter failed: %w", err)
		}

		// Increment the counter atomically using CompareAndSwap
		newVal := int64(readers + int64(req.Visitors))
		ctx = go.WithLogger(ctx, log.Default())
		
		// Do a CAS to atomically update the counter
		if err := node.CompareAndSwap(ctx, "counter", int(newVal)); err != nil {
			// If CAS fails due to stale read, try again with incremental update
			var temp int64
			tempVal := newNodeValue(readers + int64(req.Visitors))
			if err := node.Write(ctx, "counter", tempVal); err != nil {
				return nil, fmt.Errorf("update counter failed: %w", err)
			}
			temp = tempVal
			newVal = temp
			if err := node.CompareAndSwap(ctx, "counter", int(newVal)); err != nil {
				log.Printf("CAS failed again after initial CAS: %v", err)
			}
		}

		// Return the new total as the response - it should be consistent with what we calculated
		res := totalRes{InReplyTo: req.MsgID, VisitorsSoFar: int(newVal)}
		outBytes, _ := json.Marshal(res)
		return outBytes, nil
	})

	// Handle total - read the current total from KV store
	node.Handle("total", func(ctx context.Context, in []byte) (out []byte, err error) {
		var req totalReq
		if err := json.Unmarshal(in, &req); err != nil {
			return nil, fmt.Errorf("unmarshal failed: %w", err)
		}

		var readers int64
		ctx = go.WithLogger(ctx, log.Default())
		readers, _ = node.RW(ctx, "counter", 0)

		res := totalRes{InReplyTo: req.MsgID, VisitorsSoFar: int(readers)}
		outBytes, _ := json.Marshal(res)
		return outBytes, nil
	})

	// Handle counted - pass through to reply (just acknowledge receipt)
	node.Handle("counted", func(ctx context.Context, in []byte) (out []byte, err error) {
		var req countRes
		if err := json.Unmarshal(in, &req); err != nil {
			return nil, fmt.Errorf("unmarshal failed: %w", err)
		}

		// Reply with just the in_reply_to to acknowledge receipt
		res := struct{}{}
		outBytes, _ := json.Marshal(res)
		return outBytes, nil
	})

	// Handle Send - forward messages to other nodes
	node.Handle("Send", func(ctx context.Context, in []byte) (out []byte, err error) {
		var req struct {
			Dest   string `json:"dest"`
			Message json.RawMessage `json:"message"`
		}
		if err := json.Unmarshal(in, &req); err != nil {
			return nil, fmt.Errorf("unmarshal failed: %w", err)
		}

		outBytes, _ := node.Send(ctx, req.Dest, string(req.Message))
		return outBytes, nil
	})

	// Handle RPC - forward async RPC requests to other nodes
	node.Handle("RPC", func(ctx context.Context, in []byte) (out []byte, err error) {
		var req struct {
			Dest     string        `json:"dest"`
			Message  json.RawMessage `json:"message"`
			Callback int           `json:"callback"`
		}
		if err := json.Unmarshal(in, &req); err != nil {
			return nil, fmt.Errorf("unmarshal failed: %w", err)
		}

		outBytes, _ := node.RPC(ctx, req.Dest, string(req.Message), func(m Message) error {
			// Response goes back to us via the callback mechanism
			var resp struct {
				InReplyTo int  `json:"in_reply_to"`
				Message    json.RawMessage `json:"message"`
			}
			resp.InReplyTo = req.Callback
			resp.Message, _ = json.Marshal(m)
			return node.Send(req.Dest, resp)
		})
		return outBytes, nil
	})

	// We need to handle Read and Write through the kv service
	node.Handle("RW", func(ctx context.Context, in []byte) (out []byte, err error) {
		var req struct {
			Key   string `json:"key"`
			Value any    `json:"value,omitempty"`
			Type  string `json:"type,omitempty"`
		}
		json.Unmarshal(in, &req)

		if req.Type == "read" {
			v, err := node.RW(ctx, req.Key, 0)
			return json.Marshal(map[string]interface{}{"value": v})
		} else if req.Type == "write" {
			ctx = go.WithLogger(ctx, log.Default())
			if val, ok := req.Value.(float64); ok {
				val = int(val)
			}
			newVal := req.Value
			err := node.Write(ctx, req.Key, newVal)
			return json.Marshal(map[string]interface{}{"value": newVal})
		}
		return nil, nil
	})

	node.Handle("SeqKV", func(ctx context.Context, in []byte) (out []byte, err error) {
		var req struct{}
		json.Unmarshal(in, &req)
		
		// Start the sequence number service
		ctx = go.WithLogger(ctx, log.Default())
		if _, err := node.SeqKV(ctx); err != nil {
			log.Printf("SeqKV failed: %v", err)
		}
		return nil, nil
	})

	goinits[node.ID()] = []string{}
	json.Marshal(goinit(node))

	go func() {
		node.Run()
	}()
}

var goinits map[string][]string = make(map[string][]string)
