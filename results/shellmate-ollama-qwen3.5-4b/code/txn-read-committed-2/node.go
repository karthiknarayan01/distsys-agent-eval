package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/jepsen-io/maelstrom/demo/go"
)

// Node represents a single node in the cluster
type Node struct {
	nodeID  string
	nodes   []string
	store   map[string]int // key -> int (0 means unset, -1 for unseen key)
	txns    map[int]*Txn  // transaction ID -> transaction data
}

// Txn represents an in-flight transaction on this node
type Txn struct {
	msgID   int          // the original message ID
	ops     []Op         // operations in this transaction (read/writes)
	results map[int]int  // results indexed by operation index (-1 for null, >=0 for value)
}

// Op represents a single operation in a transaction
type Op struct {
	Key   int
	Type  byte // 'r' for read, 'w' for write
	Value int // for writes, the new value; for reads, -1 = null input, >=0 = result
}

// NewNode creates a new node and starts it
func NewNode() *Node {
	return &Node{
		store: make(map[string]int),
		nodes: []string{"0", "1"}, // two nodes
	}
}

// Handle the input message for this node
func (n *Node) Handle(msg maelstrom.Message) error {
	var body maelstrom.MessageBody

	switch msg.Type() {
	case "init":
		var initBody maelstrom.InitMessageBody
		if err := json.Unmarshal(msg.Body, &initBody); err != nil {
			return fmt.Errorf("unmarshal init: %w", err)
		}
		n.nodeID = initBody.NodeID
		n.nodes = initBody.NodeIDs

		go n.Run()
	case "txn":
		var txnBody maelstrom.TxnMessageBody
		if err := json.Unmarshal(msg.Body, &txnBody); err != nil {
			return fmt.Errorf("unmarshal txn: %w", err)
		}

		msgID := int(txnBody.MsgID)

		// Initialize transaction state
		txns[msgID] = &Txn{
			msgID:   msgID,
			ops:     make([]Op, len(txnBody.Txn)),
			results: make(map[int]int),
		}

		// Copy operations from input
		for i, op := range txnBody.Txn {
			n.txns[msgID].ops[i] = Op{
				Key:  int(op.Key),
				Type: byte(op.Type),
			}
		}

		// Execute the transaction (read committed means we read current state)
		if err := n.executeTxn(msgID); err != nil {
			txns[msgID].err = err
		}

		// Build reply - always return txn_ok as per spec
		replyOps := make(Op, len(txnBody.Txn))
		for i, op := range txnBody.Txn {
			switch op.Type {
			case 'r':
				// Read: get current committed value from store
				if val, ok := n.store[int(op.Key)]; ok && val >= 0 {
					replyOps[i] = Op{Key: int(op.Key), Type: byte('r'), Value: val}
				} else {
					replyOps[i] = Op{Key: int(op.Key), Type: byte('r'), Value: -1} // null
				}
			case 'w':
				// Write: no value change in reply (writes come back as sent)
				replyOps[i] = Op{Key: int(op.Key), Type: byte('w'), Value: 0}
			}
		}

		respBody := &maelstrom.TxnOkMessageBody{
			InReplyTo: msgID,
			Txn:       replyOps,
		}

		return n.Send(n.nodeID, respBody)
	default:
		fmt.Fprintf(os.Stderr, "unknown message type: %s\n", string(msg.Body))
	}
	return nil
}

// executeTxn executes a transaction on this node
func (n *Node) executeTxn(txnMsgID int) error {
	tx := &Txn{msgID: txnMsgID, ops: make(Op, 0)}

	// Read snapshot for reads (as of beginning of txn, before any writes in it)
	state := n.store

	for i, op := range tx.ops {
		switch op.Type {
		case 'r':
			if val, ok := state[int(op.Key)]; ok && val >= 0 {
				tx.results[i] = val
			} else {
				tx.results[i] = -1
			}
		case 'w':
			state[int(op.Key)] = int(op.Value)
		}
	}

	return nil
}

// Run starts the maelstrom node
func (n *Node) Run() error {
	m, err := maelstrom.NewNode().Handle(n.Handle)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create maelstrom node: %w\n", err)
		os.Exit(1)
	}

	return m.Run()
}
