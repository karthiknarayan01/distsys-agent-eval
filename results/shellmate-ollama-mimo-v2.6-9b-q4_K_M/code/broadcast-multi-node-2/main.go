package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// maxPassMsgs bounds the ring buffer used to de-duplicate propagated
// broadcast values. Values are only ever appended, never removed.
const maxPassMsgs = 200000

// passMsg is the message we send to our peers carrying a broadcast value.
type passMsg struct {
	MsgID   int    `json:"msg_id"`
	Message int    `json:"message"`
	Type    string `json:"type"`
}

// broadcastMsg is the value we keep around so that re-delivered copies
// are cheap.
type broadcastMsg struct {
	MsgID   int    `json:"msg_id"`
	Message int    `json:"message"`
}

// node holds the values seen by this node.
type node struct {
	mu      sync.Mutex
	values  map[int]int
	passes  [maxPassMsgs]broadcastMsg
	passIdx int
	ids     []string
}

func (n *node) snapshot() []int {
	n.mu.Lock()
	defer n.mu.Unlock()

	out := make([]int, 0, len(n.values))
	for _, v := range n.values {
		out = append(out, v)
	}
	return out
}

// store records a broadcast value and, if it is the first time we have seen
// it, propagates it to every other node in the cluster.
func (n *node) store(m maelstrom.Message) error {
	var b struct {
		MsgID   int    `json:"msg_id"`
		Message int    `json:"message"`
	}
	if err := json.Unmarshal(m.Body, &b); err != nil {
		return err
	}

	n.mu.Lock()
	n.values[b.MsgID] = b.Message

	if n.passIdx >= len(n.passes) {
		n.passIdx = 0
	}
	n.passes[n.passIdx] = broadcastMsg{MsgID: b.MsgID, Message: b.Message}
	n.passIdx++

	for _, other := range n.ids {
		if other == n.id {
			continue
		}
		if err := n.Send(other, passMsg{Type: "pass", MsgID: b.MsgID, Message: b.Message}); err != nil {
			log.Printf("node %s: failed to propagate value %d: %v", n.id, b.Message, err)
			return err
		}
	}
	n.mu.Unlock()
	return nil
}

func (n *node) replyRead(m maelstrom.Message) error {
	return n.Reply(m, map[string]any{
		"type":    "read_ok",
		"messages": n.snapshot(),
	})
}

func (n *node) replyTopology(m maelstrom.Message) error {
	return n.Reply(m, map[string]any{
		"type": "topology_ok",
	})
}

func main() {
	var n maelstrom.Node
	n.Handle("broadcast", func(m maelstrom.Message) error {
		var b struct {
			MsgID   int    `json:"msg_id"`
			Message int    `json:"message"`
		}
		if err := json.Unmarshal(m.Body, &b); err != nil {
			return err
		}
		return n.Reply(m, map[string]any{
			"type":    "broadcast_ok",
			"message": b.Message,
		})
	})

	n.Handle("read", func(m maelstrom.Message) error {
		return n.replyRead(m)
	})

	n.Handle("read_ok", func(m maelstrom.Message) error {
		return nil
	})

	n.Handle("topology", func(m maelstrom.Message) error {
		return n.replyTopology(m)
	})

	n.Handle("topology_ok", func(m maelstrom.Message) error {
		return nil
	})

	n.Handle("init", func(m maelstrom.Message) error {
		var b struct {
			maelstrom.MessageBody
			NodeID  string   `json:"node_id"`
			NodeIDs []string `json:"node_ids"`
		}
		if err := json.Unmarshal(m.Body, &b); err != nil {
			return err
		}
		n.id = b.NodeID
		n.ids = b.NodeIDs
		return n.Reply(m, map[string]any{"type": "init_ok"})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}