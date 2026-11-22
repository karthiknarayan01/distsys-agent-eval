package main

import (
	"context"
	"encoding/json"
	"os"
	
	"github.com/jepsen-io/maelstrom/demo/go"
)

// Request and Response types for the send operation
type SendRequest struct {
	Type  string `json:"type"`
	MgID  int    `json:"msg_id"`
	Key   string `json:"key"`
	Value int64  `json:"msg"`
}

type SendOK struct {
	Type         string `json:"type"`
	InReplyTo    int    `json:"in_reply_to"`
	Offset       int64  `json:"offset"`
}

// Request and Response types for the poll operation
type PollRequest struct {
	Type    string            `json:"type"`
	MgID    int                `json:"msg_id"`
	Offsets map[string]int64   `json:"offsets"`
}

type PollOK struct {
	Type      string                 `json:"type"`
	InReplyTo int                    `json:"in_reply_to"`
	Msgs      map[string][][2]int64  `json:"msgs"`
}

// Request and Response types for the commit_offsets operation
type CommitOffsetsRequest struct {
	MgID     int                  `json:"msg_id"`
	Offsets  map[string]int64     `json:"offsets"`
}

type CommitOffsetsOK struct {
	Type       string      `json:"type"`
	InReplyTo  int         `json:"in_reply_to"`
}

// Request and Response types for the list_committed_offsets operation
type ListCommittedOffsetsRequest struct {
	MgID  int    `json:"msg_id"`
	Keys  []string `json:"keys"`
}

type ListCommittedOffsetsOK struct {
	Type            string      `json:"type"`
	InReplyTo       int         `json:"in_reply_to"`
	Offsets         map[string]int64 `json:"offsets"`
}

func main() {
	ctx := context.Background()
	
	// Create lin-kv client
	kv, err := maelstrom.NewLinKV("lin-kv")
	if err != nil {
		os.Exit(1)
	}
	
	node := maelstrom.NewNode(os.Stdout, os.Stdin)
	
	node.Handle("init", func(ctx context.Context, msg maelstrom.Message) (msgID int, next go.NextFunc, _ error) {
		return 0, nil, nil
	})
	
	node.Handle("send", func(_ context.Context, msg maelstrom.Message) (int, go.NextFunc, error) {
		var req SendRequest
		if err := json.Unmarshal([]byte(msg.Body), &req); err != nil {
			os.Exit(1)
		}
		
		logKey := req.Key
		
		// Read next offset counter for this log
		nextOffsetKV := kv.ReadInt(ctx, logKey+".next_offset")
		var nextOffset int64 = 0
		if nextOffsetKV == nil {
			// Initialize if not exists - use a simple incrementing counter
			// Actually, lin-kv.Read will return the current value or nil if doesn't exist
			// We need to create an atomically increasing counter
			var err error
			nextOffset, err = kv.CompareAndSwap(ctx, logKey+".next_offset", 0, nextOffset+1)
			if err != nil {
				os.Exit(1)
			}
		} else {
			nextOffset = *nextOffset + 1
			_ = kv.Write(ctx, logKey+".next_offset", int64(nextOffset))
		}
		
		// Write the value at this offset
		// Key format: "key|offset" -> value
		entryKey := logKey + "|" + string([]byte{nextOffset})
		var entryValue interface{} = req.Value
		kv.Write(ctx, entryKey, entryValue)
		
		resp := SendOK{
			Type:         "send_ok",
			InReplyTo:    req.MgID,
			Offset:       int64(nextOffset),
		}
		
		body, _ := json.Marshal(resp)
		node.Reply(msg.ID(), body)
		
		return 0, nil, nil
	})
	
	node.Handle("poll", func(_ context.Context, msg maelstrom.Message) (int, go.NextFunc, error) {
		var req PollRequest
		if err := json.Unmarshal([]byte(msg.Body), &req); err != nil {
			os.Exit(1)
		}
		
		results := make(map[string][][2]int64)
		
		for logKey, offset := range req.Offsets {
			// Read all values for this log that are at or after the requested offset
			values := kv.ReadIntSlice(ctx, logKey+".values")
			if values == nil {
				results[logKey] = nil // No data in this log yet
				continue
			}
			
			var pairs [][2]int64
			for _, entry := range *values {
				entryOffset, entryVal := int(entry), entry.(int64)
				if entryOffset >= offset {
					pairs = append(pairs, [2]int64{int64(entryOffset), entryVal})
				}
			}
			results[logKey] = pairs
		}
		
		resp := PollOK{
			Type:      "poll_ok",
			InReplyTo: req.MgID,
			Msgs:      results,
		}
		
		body, _ := json.Marshal(resp)
		node.Reply(msg.ID(), body)
		
		return 0, nil, nil
	})
	
	node.Handle("commit_offsets", func(_ context.Context, msg maelstrom.Message) (int, go.NextFunc, error) {
		var req CommitOffsetsRequest
		if err := json.Unmarshal([]byte(msg.Body), &req); err != nil {
			os.Exit(1)
		}
		
		// Record committed offset for each key
		committedOffsets := make(map[string]int64)
		for logKey, offset := range req.Offsets {
			entryKey := logKey + "|c"
			kv.Write(ctx, entryKey, int64(offset))
			committedOffsets[logKey] = offset
		}
		
		resp := CommitOffsetsOK{
			Type:       "commit_offsets_ok",
			InReplyTo:  req.MgID,
		}
		
		body, _ := json.Marshal(resp)
		node.Reply(msg.ID(), body)
		
		return 0, nil, nil
	})
	
	node.Handle("list_committed_offsets", func(_ context.Context, msg maelstrom.Message) (int, go.NextFunc, error) {
		var req ListCommittedOffsetsRequest
		if err := json.Unmarshal([]byte(msg.Body), &req); err != nil {
			os.Exit(1)
		}
		
		offsets := make(map[string]int64)
		for _, key := range req.Keys {
			entryKey := key + "|c"
			result, _ := kv.ReadInt(ctx, entryKey)
			if result != nil {
				offsets[key] = *result
			} else {
				// No committed offset recorded - use highest available as default
				// Actually let's keep it uncommitted (0 or whatever was there)
				entryKey += "|h" // highest
				highest, _ := kv.ReadInt(ctx, entryKey)
				if highest != nil {
					offsets[key] = *highest - 1
				}
			}
		}
		
		resp := ListCommittedOffsetsOK{
			Type:       "list_committed_offsets_ok",
			InReplyTo:  req.MgID,
			Offsets:   offsets,
		}
		
		body, _ := json.Marshal(resp)
		node.Reply(msg.ID(), body)
		
		return 0, nil, nil
	})
	
	if err := node.Run(); err != nil {
		os.Exit(1)
	}
}
