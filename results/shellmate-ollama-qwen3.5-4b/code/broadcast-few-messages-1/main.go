package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"sync"

	m "github.com/jepsen-io/maelstrom/demo/go"
)

// NodeMessage is our internal message type for inter-node broadcast
type NodeMessage struct {
	Type string `json:"type"`
	Data interface{} `json:"data,omitempty"`
}

var topoMu = sync.RWMutex{}
var nodeTopos map[int][]int = make(map[int][]int)

func main() {
	n := m.NewNode()
	defer func() {
		if r := recover(); r != nil {
			fmt.Fprintln(os.Stderr, "panic:", r)
			os.Exit(1)
		}
		os.Exit(0)
	}()

	nodeValues := make(map[int]int) // key: broadcast msg_id, value: the actual value
	var myValuesMu sync.Mutex

	initDone := false

	handleInit := func(msg m.Message) error {
		body := msg.Body.(m.InitMessageBody)

		if !initDone {
			initDone = true
			if body.OtherIds != nil && len(body.OtherIds) > 0 {
				for _, otherID := range body.OtherIds {
					nodeTopos[body.MyID] = []int{} // Will be filled after topology message
				}
			} else {
				for _, otherID := range body.OtherIDs {
					nodeTopos[otherID] = []int{body.MyID}
				}
			}
		}

		return n.Reply(msg.MsgID, m.InitOkMessageBody{})
	}
	n.Handle("init", handleInit)

	handleBroadcast := func(msg m.Message) error {
		body := msg.Body.(m.BroadcastMessageBody)

		myValuesMu.Lock()
		nodeValues[body.Value] = body.MsgID
		myValuesMu.Unlock()

		topoMu.RLock()
		topology := nodeTopos[n.ID()]
		topoMu.RUnlock()

		if len(topology) == 0 {
			// Default: broadcast to all other nodes in OtherIds list
			var dests []int
			if body.OtherIds != nil && len(body.OtherIds) > 0 {
				dests = make([]int, 0)
				for _, otherID := range body.OtherIds {
					dests = append(dests, int(otherID))
				}
			} else {
				dests = make([]int, 1)
				dests[0] = 1
			}

			var sendCh chan error
			sendCh = make(chan error, len(topology)+100)
			go func() {
				for _, dest := range topology {
					err := n.RPC(dest, NodeMessage{Type: "broadcast", Data: body.Value})
					if err != nil {
						m.Log("RPC failed", "dest=", dest)
					}
					select {
					case sendCh <- err:
					default:
					}
				}
			}()

			for range sendCh {
			}

			return n.Reply(msg.MsgID, m.BroadcastOkMessageBody{})
		} else {
			var sendCh chan error
			sendCh = make(chan error, len(topology))
			go func() {
				for _, dest := range topology {
					err := n.RPC(dest, NodeMessage{Type: "broadcast", Data: body.Value})
					if err != nil {
						m.Log("RPC failed", "dest=", dest)
					}
					select {
					case sendCh <- err:
					default:
					}
				}
			}()

			for range sendCh {
			}

			return n.Reply(msg.MsgID, m.BroadcastOkMessageBody{})
		}
	}
	n.Handle("broadcast", handleBroadcast)

	handleRead := func(msg m.Message) error {
		body := msg.Body.(m.ReadMessageBody)

		result := make([]int, 0, len(nodeValues))
		for k := range nodeValues {
			result = append(result, k)
		}
		sort.Ints(result)

		return n.Reply(msg.MsgID, m.ReadOkMessageBody{Messages: result})
	}
	n.Handle("read", handleRead)

	handleTopology := func(msg m.Message) error {
		body := msg.Body.(m.TopologyMessageBody)

		topoMu.Lock()
		nodeTopos[n.ID()] = body.Neighbors
		if body.OtherIds != nil {
			for _, otherID := range body.OtherIds {
				nodeTopos[otherID] = []int{n.ID()}
			}
		}
		topoMu.Unlock()

		return n.Reply(msg.MsgID, m.TopologyOkMessageBody{})
	}
	n.Handle("topology", handleTopology)

	if err := n.Run(); err != nil {
		m.Log("run error", "err=", err)
		os.Exit(1)
	}
}

func log(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, "%s: ", msg)
	fmt.Println(args...)
}
