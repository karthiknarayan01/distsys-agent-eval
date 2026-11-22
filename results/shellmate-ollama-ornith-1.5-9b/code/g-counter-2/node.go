package main

import (
	"context"
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// seqKVKey is the key under which the whole cluster agrees on the total sum.
// It lives in the sequentially-consistent seq-kv store.
const seqKVKey = "g-counter-sum"

const defaultTimeout = 5 * time.Second

func main() {
	n := maelstrom.NewNode()

	// Seq-kv client: every node shares this sequentially-consistent key/value
	// store. It lets us broadcast the agreed total sum to the whole cluster and
	// converge to the correct value once the network settles.
	seqKV := maelstrom.NewSeqKV(n)

	// init handler
	n.Handle("init", func(msg maelstrom.Message) error {
		v, err := seqKV.ReadInt(context.Background(), seqKVKey)
		if err != nil {
			log.Printf("init: read seq-kv: %v", err)
		}
		storeLastKnown.Store(v)
		return nil
	})

	// add handler
	n.Handle("add", func(msg maelstrom.Message) error {
		var body addMessageBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// Optimistically advance our local total and advertise the new value to
		// everyone in the cluster, so that reads elsewhere converge faster.
		cur := storeLastKnown.Load()
		next := cur + int64(body.Delta)
		if !storeLastKnown.CompareAndSwap(cur, next) {
			next = storeLastKnown.Load()
		}

		// Tell the seq-kv store the new agreed total. A lost message here is
		// fine (possibly we count it here and nobody via seq-kv).
		ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
		defer cancel()
		if err := seqKV.Write(ctx, seqKVKey, next); err != nil {
			log.Printf("add: write seq-kv: %v", err)
		}

		// Fan out to peers so they can advance their local view too. A lost
		// proposal is fine: this node still owns the increment.
		for _, peer := range n.NodeIDs() {
			if peer == n.ID() {
				continue
			}
			proposal := map[string]any{
				"type":   "proposal",
				"delta":  body.Delta,
				"total":  next,
				"from":   n.ID(),
				"msg_id": nextMsgID(),
			}
			_ = n.Send(peer, proposal)
		}

		return n.Reply(msg, addOKMessageBody{
			MsgID: body.MsgID,
		})
	})

	// add_ok handler: advance local total and broadcast the new agreed value.
	n.Handle("add_ok", func(msg maelstrom.Message) error {
		var body addOKMessageBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		if next := int64(body.MsgID); next > storeLastKnown.Load() {
			if !storeLastKnown.CompareAndSwap(0, next) {
				next = storeLastKnown.Load()
			}
		}

		broadcastTotal(storeLastKnown.Load())

		return nil
	})

	// read handler
	n.Handle("read", func(msg maelstrom.Message) error {
		readOK := map[string]any{
			"type":          "read_ok",
			"in_reply_to":   msg.Body.InReplyTo,
			"msg_id":        msg.Body.MsgID,
			"value":         storeLastKnown.Load(),
			"read_timestamp": time.Now(),
		}
		if err := n.Reply(msg, readOK); err != nil {
			log.Printf("read: reply: %v", err)
		}
		broadcastTotal(storeLastKnown.Load())
		return nil
	})

	// proposal handler: another node proposes a new total. We optimistically
	// accept it if it is larger than our current view.
	n.Handle("proposal", func(msg maelstrom.Message) error {
		var body proposalMessageBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		if body.Total > storeLastKnown.Load() {
			cur := storeLastKnown.Load()
			if !storeLastKnown.CompareAndSwap(cur, body.Total) {
				// A newer proposal beat ours to storage; honour the new value.
				storeLastKnown.Store(body.Total)
			}
		}
		return nil
	})

	// proposal_ok handler: acknowledge a proposal by broadcasting the agreed
	// total again. This helps heal after partitions by re-announcing the value.
	n.Handle("proposal_ok", func(msg maelstrom.Message) error {
		var body proposalMessageBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		if body.Total > storeLastKnown.Load() {
			storeLastKnown.Store(body.Total)
			broadcastTotal(body.Total)
		}
		return nil
	})

	n.Run()
}

// addMessageBody is the request body for an "add" request.
type addMessageBody struct {
	Type   string `json:"type"`
	MsgID  int    `json:"msg_id"`
	Delta  int    `json:"delta"`
}

// addOKMessageBody is the response body for an "add_ok" response.
type addOKMessageBody struct {
	MsgID int `json:"msg_id"`
}

// proposalMessageBody is our custom message used to propagate the agreed total.
type proposalMessageBody struct {
	Type  string `json:"type"`
	Total int    `json:"total"`
	From  string `json:"from"`
	Delta int    `json:"delta"`
	MsgID int    `json:"msg_id"`
}

var storeLastKnown int64
var storeMu sync.Mutex

// broadcastTotal sends the agreed total to every other node so the cluster
// converges quickly on the current value.
func broadcastTotal(total int64) {
	for _, peer := range n.NodeIDs() {
		if peer == n.ID() {
			continue
		}
		proposal := proposalMessageBody{
			Type:  "proposal",
			Total: int(total),
			From:  n.ID(),
			MsgID: nextMsgID(),
		}
		_ = n.Send(peer, proposal)
	}
}

// nextMsgID returns a process-unique message id for our custom messages.
func nextMsgID() int {
	nextMsgIDMu.Lock()
	defer nextMsgIDMu.Unlock()
	nextMsgIDCounter++
	return nextMsgIDCounter
}

var nextMsgIDCounter int
var nextMsgIDMu sync.Mutex
