package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/jepsen-io/maelstrom/demo/go"
)

// LogEntry represents an entry in a log: [offset, value]
type LogEntry struct {
	Offset int
	Value  interface{}
}

// Global state for our Kafka logs
var (
	logs      = make(map[string][]LogEntry)
	committed = make(map[string]int) // key -> committed offset (inclusive)
)

func main() {
	node, err := maelstrom.NewNode(os.Stdin, os.Stdout)
	if err != nil {
		fmt.Fprintf(os.Stderr, "NewNode: %v\n", err)
		os.Exit(1)
	}
	defer node.Close()

	node.Handle("init", func(ctx *go.NodeContext, msg json.RawMessage, inReplyTo int) ([]byte, error) {
		var req struct {
			NodeIDs []string `json:"node_ids"`
		}
		if err := json.Unmarshal(msg, &req); err != nil {
			return nil, fmt.Errorf("init: %v", err)
		}
		resp := map[string]interface{}{
			"type":         "init_ok",
			"in_reply_to": inReplyTo,
		}
		return json.Marshal(resp)
	})

	node.Handle("send", func(ctx *go.NodeContext, msg json.RawMessage, inReplyTo int) ([]byte, error) {
		var req struct {
			Type   string            `json:"type"`
			MsgID  int               `json:"msg_id"`
			Key    string            `json:"key"`
			Msg     interface{}       `json:"msg"`
		}
		if err := json.Unmarshal(msg, &req); err != nil {
			return nil, fmt.Errorf("send: %v", err)
		}

		key := req.Key
		msgVal := req.Msg

		// Initialize log if needed (nil slice means empty but existing)
		if _, exists := logs[key]; !exists {
			logs[key] = nil
		}

		// Get the next offset for this key
		currentLen := len(logs[key])
		nextOffset := currentLen

		// Append to log: [offset, value]
		logs[key] = append(logs[key], LogEntry{Offset: nextOffset, Value: msgVal})
		offset := int64(nextOffset)

		resp := map[string]interface{}{
			"type":         "send_ok",
			"in_reply_to": inReplyTo,
			"offset":       offset,
		}
		return json.Marshal(resp)
	})

	node.Handle("poll", func(ctx *go.NodeContext, msg json.RawMessage, inReplyTo int) ([]byte, error) {
		var req struct {
			Type       string            `json:"type"`
			MsgID      int               `json:"msg_id"`
			Offsets    map[string]int64  `json:"offsets"`
		}
		if err := json.Unmarshal(msg, &req); err != nil {
			return nil, fmt.Errorf("poll: %v", err)
		}

		// Build result map: key -> list of [offset, value] pairs at/after given offset
		resultMsgs := make(map[string][][2]interface{})

		for key, offset := range req.Offsets {
			offsetInt64 := int64(offset)
			keyLogs := logs[key]

			if keyLogs == nil {
				continue // empty log for this key
			}

			// Find entries starting from the requested offset
			var entries [][2]interface{}
			for _, entry := range keyLogs {
				offset64 := int64(entry.Offset)
				if offset64 >= offsetInt64 {
					entries = append(entries, []interface{}{offset64, entry.Value})
				}
			}

			resultMsgs[key] = entries
		}

		resp := map[string]interface{}{
			"type":         "poll_ok",
			"in_reply_to": inReplyTo,
			"msgs":         resultMsgs,
		}
		return json.Marshal(resp)
	})

	node.Handle("commit_offsets", func(ctx *go.NodeContext, msg json.RawMessage, inReplyTo int) ([]byte, error) {
		var req struct {
			Type    string `json:"type"`
			MsgID   int    `json:"msg_id"`
			Offsets map[string]int64 `json:"offsets"`
		}
		if err := json.Unmarshal(msg, &req); err != nil {
			return nil, fmt.Errorf("commit_offsets: %v", err)
		}

		// Update committed offsets for each key
		for key, offset := range req.Offsets {
			committed[key] = int(offset)
		}

		resp := map[string]interface{}{
			"type":         "commit_offsets_ok",
			"in_reply_to": inReplyTo,
		}
		return json.Marshal(resp)
	})

	node.Handle("list_committed_offsets", func(ctx *go.NodeContext, msg json.RawMessage, inReplyTo int) ([]byte, error) {
		var req struct {
			Type       string   `json:"type"`
			MsgID      int      `json:"msg_id"`
			Keys       []string  `json:"keys"`
		}
		if err := json.Unmarshal(msg, &req); err != nil {
			return nil, fmt.Errorf("list_committed_offsets: %v", err)
		}

		resultOffsets := make(map[string]int64)
		for _, key := range req.Keys {
			// Only include keys that have been committed
			if off, ok := committed[key]; ok {
				resultOffsets[key] = int64(off)
			}
		}

		resp := map[string]interface{}{
			"type":                 "list_committed_offsets_ok",
			"in_reply_to": inReplyTo,
			"offsets":              resultOffsets,
		}
		return json.Marshal(resp)
	})

	if err := node.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "node: %v\n", err)
		os.Exit(1)
	}
}
