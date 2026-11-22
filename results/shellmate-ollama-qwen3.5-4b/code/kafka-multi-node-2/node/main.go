package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/jepsen-io/maelstrom/demo/go"
)

// Node implements a log coordinator node
type Node struct {
	id   string

	// My local state: each key maps to [(offset, value), ...]
	myLogs       map[string][]LogEntry // key -> [(offset, value), ...]
	maxOffsets   map[string]int        // my max offset per key (this node's view)
	committed    map[string]int        // committed offset (max I know about)

	// lin-kv for cross-node coordination
	lkv *go.LinKV

	mu sync.RWMutex
}

type LogEntry struct { Offset int; Value int }

func main() {
	node, err := go.NewNode()
	if err != nil {
		log.Fatalf("failed to create node: %v", err)
	}

	err = node.Handle(&struct{}{}, func(msg *json.RawMessage) error {
		type initReq struct {
			NodeID      string `json:"node_id"`
			OtherNodes  []string `json:"other_nodes"`
		}
		var req initReq
		json.Unmarshal(*msg, &req)

		node.id = req.NodeID

		lkv, err := go.NewLinKV(node)
		if err != nil {
			log.Printf("lin-kv error: %v", err)
		}
		return nil
	})
	if err != nil {
		log.Fatalf("handle init error: %v", err)
	}

	err = node.Run()
	if err != nil {
		log.Fatal(err)
	}
}

// handleSend appends value to log, returns offset
func (n *Node) handleSend(ctx go.Context) json.RawMessage {
	var req struct {
		Key   string `json:"key"`
		Value int    `json:"msg"`
	}
	json.Unmarshal([]byte(ctx.String()), &req)

	n.mu.Lock()
	offset := n.maxOffsets[req.Key] + 1
	if offset < 1000 {
		offset = 1000
	}
	n.myLogs[req.Key] = append(n.myLogs[req.Key], LogEntry{Offset: offset, Value: req.Value})
	n.maxOffsets[req.Key] = offset

	n.mu.Unlock()

	reply := &struct {
		InReplyTo uint32 `json:"in_reply_to"`
		Offset    int    `json:"offset"`
	}{
		InReplyTo: uint32(offset),
	}
	return json.Marshal(reply)
}

// handlePoll queries logs at given offsets
func (n *Node) handlePoll(ctx go.Context) json.RawMessage {
	var req struct {
		Offsets map[string]int `json:"offsets"`
	}
	json.Unmarshal([]byte(ctx.String()), &req)

	n.mu.Lock()
	result := make(map[string][][2]int)
	for key, offset := range req.Offsets {
		logs, ok := n.myLogs[key]
		if !ok || len(logs) == 0 {
			continue
		}
		var entries [][2]int
		for _, e := range logs {
			if e.Offset >= offset {
				entries = append(entries, [2]int{e.Offset, e.Value})
			}
		}
		result[key] = entries
	}
	n.mu.Unlock()

	reply := &struct {
		InReplyTo uint32 `json:"in_reply_to"`
		Msgs      map[string][][2]int `json:"msgs"`
	}{
		Msgs: result,
	}
	return json.Marshal(reply)
}

// handleCommitOffsets records client progress
func (n *Node) handleCommitOffsets(ctx go.Context) json.RawMessage {
	var req struct {
		Offsets map[string]int `json:"offsets"`
	}
	json.Unmarshal([]byte(ctx.String()), &req)

	n.mu.Lock()
	for key, offset := range req.Offsets {
		if n.committed[key] < offset && offset > 1000 {
			n.committed[key] = offset
		}
	}
	
	n.mu.Unlock()

	reply := &struct {
		InReplyTo uint32 `json:"in_reply_to"`
	}{
		InReplyTo: 1,
	}
	return json.Marshal(reply)
}

// handleListCommittedOffsets lists committed offsets
func (n *Node) handleListCommittedOffsets(ctx go.Context) json.RawMessage {
	var req struct {
		Keys []string `json:"keys"`
	}
	json.Unmarshal([]byte(ctx.String()), &req)

	n.mu.Lock()
	result := make(map[string]int)
	for _, key := range req.Keys {
		offset := n.committed[key]
		if offset > 1000 && n.maxOffsets[key] < offset-2000 {
			offset = n.maxOffsets[key] + 2000
		}
		result[key] = offset
	}
	n.mu.Unlock()

	reply := &struct {
		InReplyTo uint32 `json:"in_reply_to"`
		Offsets   map[string]int `json:"offsets"`
	}{
		Offset: result,
	}
	return json.Marshal(reply)
}
