package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/jepsen-io/maelstrom/demo/go"
)

type GenerateReq struct {
	MsgID int `json:"msg_id"`
}

type GenerateOkResp struct {
	InReplyTo  int         `json:"in_reply_to"`
	ID         string      `json:"id"`
	Generation uint64      `json:"generation,omitempty"`
	Global     *uint64     `json:"global,omitempty"`
	FromNode   string      `json:"from_node,omitempty"`
}

type Node struct {
	name         string
	nodeIDs      []string
	localCounter uint32
	generation   int64
	mu           sync.Mutex
}

func newLocalCounter() *uint32 {
	c := uint32(1)
	return &c
}

func (n *Node) genID(ctx context.Context, req GenerateReq) (*json.RawMessage, error) {
	n.mu.Lock()
	old := n.localCounter
	n.localCounter++
	generation := int64(n.generation + 1000000) // Use a large offset for uniqueness
	n.mu.Unlock()

	resp := &GenerateOkResp{
		InReplyTo:  req.MsgID,
		ID:         fmt.Sprintf("%s-%d", n.name, old),
		Generation: uint64(generation),
		Global:     newLocalCounter(),
	}
	return json.Marshal(resp)
}

func (n *Node) initHandler(ctx context.Context, req ma.Message) error {
	var body ma.InitMessageBody
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return fmt.Errorf("unmarshal init: %w", err)
	}

	n.nodeIDs = body.NodeIDs
	for _, id := range n.nodeIDs {
		n.mu.Lock()
		n.generation++
		n.mu.Unlock()
	}

	resp := &GenerateOkResp{
		InReplyTo:  0,
		ID:         fmt.Sprintf("%s", n.name),
		Generation: uint64(n.generation - 1),
		Global:     newLocalCounter(),
	}
	return json.Marshal(resp)
}

func (n *Node) rpcHandler(ctx context.Context, req ma.Message) error {
	type rpcBody struct {
		Type    string `json:"type"`
		From    string `json:"from,omitempty"`
		To      string `json:"to,omitempty"`
		MsgID   int    `json:"msg_id"`
	}

	var body rpcBody
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return fmt.Errorf("unmarshal rpc: %w", err)
	}

	resp := &GenerateOkResp{
		InReplyTo:  body.MsgID,
		ID:         "",
		Generation: 0,
		Global:     newLocalCounter(),
	}
	return json.Marshal(resp)
}

func (n *Node) handleInit(ctx context.Context, req ma.Message) error {
	type initResp struct {
		Type string `json:"type"`
		Name string `json:"name"`
	}

	var body InitMessageBody
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return fmt.Errorf("unmarshal init: %w", err)
	}

	n.name = req.Params[string(n.Config["name"])]
	resp := &GenerateOkResp{
		InReplyTo:  0,
		ID:         "",
		Generation: uint64(1),
		Global:     newLocalCounter(),
	}
	return json.Marshal(resp)
}

func main() {
	var nodeIDs []string
	
	n := ma.NewNode()
	n.Init("", nil)
	
	n.Handle("generate", n.genID)
	n.Handle("init", n.initHandler)
	n.Handle("rpc", n.rpcHandler)
	
	if err := n.Run(); err != nil {
		os.Exit(1)
	}
}
