package main

import (
	"encoding/json"
	"fmt"
	"io"
	"sync"

	"github.com/jepsen-io/maelstrom/demo/go"
)

// TxnOp represents a single operation in a transaction
type TxnOp struct {
	Op    string // "r" for read, "w" for write
	Key   int
	Value interface{}
}

// TxnMessage is the input transaction message
type TxnMessage struct {
	Mid    int                 `json:"msg_id"`
	Txn    []TxnOp             `json:"txn"`
	Replay []TxnOp             `json:"replay,omitempty"`
}

// TxnResponse is the output transaction response
type TxnResponse struct {
	InReplyTo int            `json:"in_reply_to"`
	Txn       []TxnOp         `json:"txn"`
	Code      int             `json:"code,omitempty"`
	Text      string          `json:"text,omitempty"`
	Type      string          `json:"type"`
}

// LocalState tracks state on this node
type LocalState struct {
	sync.RWMutex
	// key -> value map
	keyToValue map[int]string
	// key -> latest commit epoch
	keyToEpoch map[int]int64
}

func newState() *LocalState {
	return &LocalState{
		keyToValue: make(map[int]string),
		keyToEpoch: make(map[int]int64),
	}
}

type node struct {
	node      *maelstrom.Node
	state     *LocalState
	nodeIDs   []string
	src       string // track who sent us messages during init
	dest      string
	msgID     int
}

func main() {
	n := maelstrom.NewNode()
	nd := &node{
		node: n,
		state: newState(),
	}

	n.Handle("txn", nd.handleTxn)
	n.Handle("init", nd.handleInit)

	if err := n.Run(); err != nil {
		fmt.Fprintln(n.node.Stderr, err)
	}
}

func (nd *node) handleInit(msg maelstrom.Message) error {
	var body maelstrom.InitMessageBody
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return fmt.Errorf("parse init: %v", err)
	}

	// Store the node IDs in the expected order for us to send messages
	nd.nodeIDs = body.NodeIDs

	// Set up our local state initialization from init if needed
	if nd.state.keyToEpoch == nil {
		nd.state.keyToEpoch = make(map[int]int64)
	}

	return nil
}

func (nd *node) handleTxn(msg maelstrom.Message) error {
	var txnReq TxnMessage
	if err := json.Unmarshal(msg.Body, &txnReq); err != nil {
		// Try with simpler structure if replay field exists
		type SimpleTxn struct {
			Mid int      `json:"msg_id"`
			Txn []TxnOp `json:"txn"`
		}
		var simple SimpleTxn
		if err2 := json.Unmarshal(msg.Body, &simple); err2 == nil {
			txnReq.Mid = simple.Mid
			txnReq.Txn = simple.Txn
		} else {
			return fmt.Errorf("parse txn: %v", err)
		}
	}

	// For replay messages, we need to track the epoch and value at commit time
	if len(txnReq.Replay) > 0 && txnReq.Replay[0].Op == "r" {
		txn := make([]TxnOp, len(txnReq.Txn))
		copy(txn, txnReq.Txn)
		
		for i, op := range txnReq.Replay {
			if op.Op == "r" && len(txnReplayMap) > 0 {
				val, exists := txnReplayMap[op.Key]
				op.Value = val
			}
		}
		// We'll handle replay differently - it should be a response to us
		return nd.handleTxnResponse(msg, &txnReq)
	}

	// Process the transaction locally first
	resp := nd.processTxnLocally(&txnReq)

	// Send back our response immediately
	replyBody := TxnResponse{
		Type:      "txn_ok",
		InReplyTo: txnReq.Mid,
		Txn:       resp.Txn,
	}
	if replyBody.Code == 0 && replyBody.Text == "" {
		return nd.node.Reply(msg, replyBody)
	}

	// If there was an error, we still need to send it somehow
	// But for read-committed with availability, we always return txn_ok
	
	// Now asynchronously replicate the writes to other nodes
	for _, op := range resp.Txn {
		if op.Op == "w" {
			nd.replicateWrite(op.Key, op.Value)
		}
	}

	return nil
}
