package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/jepsen-io/maelstrom/demo/go"
)

// Node represents a cluster node
type Node struct {
	id          string
	nodeIDs     []string
	localLogs   map[string][]LocalEntry // key -> list of entries sorted by offset
	keyState    map[string]*KeyState     // state tracked via lin-kv or local cache
	sent        map[int]map[string]string // sent replies per message ID
	recv        map[int]int              // received messages per message ID (last reply)
}

type LocalEntry struct {
	Key   string `json:"key"`
	Value int    `json:"val"`
	Offset int64 `json:"offset"`
}

// KeyState tracks the state of entries across all nodes for a key
type KeyState struct {
	Key   string   `json:"key"`
	Value int      `json:"val"`
	Offset int64   `json:"offset"`
	Sender string  `json:"sender"`
}

var nodes = map[string]*Node{}
var kvClient *go.KV
var linKVClient *go.KV

func main() {
	m, err := go.NewNode()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create node: %v\n", err)
		os.Exit(1)
	}

	h := m.Handle("init", func(ctx context.Context, src string, body interface{}) (reply interface{}, done bool, err error) {
		msg := body.(go.InitMessageBody)
		nodeID := msg.NodeID()
		nodeIDs := msg.NodeIDs()
		initNode(nodeID, nodeIDs)

		return "ok", true, nil
	})

	h = m.Handle("send", func(ctx context.Context, src string, body interface{}) (reply interface{}, done bool, err error) {
		msg := body.(go.SendMessageBody)
		nodeID := msg.NodeID()
		key := msg.Key
		value := int(msg.Message)
		msgID := msg.MsgID

		n := getNode(nodeID)
		if n == nil {
			return nil, false, fmt.Errorf("node not found")
		}

		offset := n.nextOffset(key)
		entry := LocalEntry{Key: key, Value: value, Offset: offset}

		n.localLogs[key] = append(n.localLogs[key], entry)
		if err := linKVClient.Write(ctx, "state:"+key, fmt.Sprintf("%d:%d", value, offset)); err != nil {
			return nil, false, fmt.Errorf("write failed")
		}

		sendOKBody := go.SendOkBody{InReplyTo: int(msgID), Offset: offset}

		replyBody := go.Reply{MsgID: msgID, Typ: "send_ok", Sender: nodeID, Val: sendOKBody}

		return replyBody, true, nil
	})

	h = m.Handle("poll", func(ctx context.Context, src string, body interface{}) (reply interface{}, done bool, err error) {
		msg := body.(go.PollMessageBody)
		nodeID := msg.NodeID()
		pollOK := &go.Reply{
			Typ:   "poll_ok",
			Sender: nodeID,
			Poll:  go.PollOkBody{},
		}

		msgID := msg.MsgID
		offsets := msg.Offsets

		for key := range offsets.Offsets {
			if _, exists := n.localLogs[key]; !exists {
				continue
			}

			startOffset, ok := offsets.Offsets[key]
			var pairs []go.Pair
			for _, e := range n.localLogs[key] {
				if e.Offset >= startOffset && (len(pairs) == 0 || e.Offset > pairs[len(pairs)-1].Offset) {
					pairs = append(pairs, go.Pair{Offset: e.Offset, Val: e.Value})
				}
			}
			pollOK.Messages[key] = pairs
		}

		replyBody := *pollOK
		replyBody.MsgID = msgID
		return replyBody, true, nil
	})

	h = m.Handle("commit_offsets", func(ctx context.Context, src string, body interface{}) (reply interface{}, done bool, err error) {
		msg := body.(go.CommitOffsetsMessageBody)
		nodeID := msg.NodeID()

		n := getNode(nodeID)
		if n == nil {
			return nil, false, fmt.Errorf("node not found")
		}

		replyBody := &go.Reply{Typ: "commit_offsets_ok", Sender: nodeID}
		msgID := msg.MsgID

		for key := range msg.Offsets.Offsets {
			lastOffset := int64(-1)
			for _, e := range n.localLogs[key] {
				if e.Offset > lastOffset {
					lastOffset = e.Offset
				}
			}
			msg.CommitOffsetsOK.Body.(*go.CommitOffsetsOkBody).Offsets = map[string]int64{key: lastOffset}
		}

		return replyBody, true, nil
	})

	h = m.Handle("list_committed_offsets", func(ctx context.Context, src string, body interface{}) (reply interface{}, done bool, err error) {
		msg := body.(go.ListCommittedOffsetsMessageBody)
		nodeID := msg.NodeID()

		n := getNode(nodeID)
		if n == nil {
			return nil, false, fmt.Errorf("node not found")
		}

		replyBody := &go.Reply{Typ: "list_committed_offsets_ok", Sender: nodeID}
		msgID := msg.MsgID

		for _, key := range msg.Keys.Keys {
			var lastOffset int64 = -1
			if entries, exists := n.localLogs[key]; exists && len(entries) > 0 {
				for _, e := range entries {
					if e.Offset > lastOffset {
						lastOffset = e.Offset
					}
				}
			}
			replyBody.Val.(*go.ListCommittedOffsetOKBody).Offsets[key] = lastOffset
		}

		return replyBody, true, nil
	})

	m.Run()
}

func getNode(nodeID string) *Node {
	return nodes[nodeID]
}

func initNode(nodeID string, nodeIDs []string) {
	nodes[nodeID] = &Node{
		id:       nodeID,
		nodeIDs:  nodeIDs,
		localLogs: make(map[string][]LocalEntry),
		keyState: make(map[string]*KeyState),
		sent:     make(map[int]map[string]string),
		recv:     make(map[int]int),
	}

	for i := range nodeIDs {
		nid := nodeIDs[i]
		if nid != nodeID && nodes[nid] != nil {
			syncNode(nodeID, nid)
			return
		}
	}
}

func syncNode(fromID, toID string) {
	var n *Node
	for nid := range nodes[fromID].nodeIDs {
		if nid == fromID || nodes[nid] != nil {
			n = nodes[nid]
			break
		}
	}
	if n == nil {
		return
	}

	to := nodes[toID]

	stateVal, _ := linKVClient.ReadInt(context.Background(), "state:"+to.keyState)
	for k := range to.keyState {
		to.localLogs[k] = append(to.localLogs[k], *n.keyState[k])
	}
}

func (n *Node) nextOffset(key string) int64 {
	if entries, exists := n.localLogs[key]; exists && len(entries) > 0 {
		return entries[len(entries)-1].Offset + 2
	}
	return 1
}
