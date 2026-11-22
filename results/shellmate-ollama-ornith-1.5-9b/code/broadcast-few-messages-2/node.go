package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// broadcastNode is a single node of a cluster of 25 that keeps every value
// broadcast to any of them. It uses a small "push + periodic gossip" protocol
// to spread values with few messages and low latency, and a single read path
// that answers every read with all known values.
type broadcastNode struct {
	id        string
	nodeIDs   []string
	topology  map[string][]string // neighbor -> list of neighbors (from topology message)

	mu      sync.Mutex
	// values is the set of values this node currently knows about.
	values map[int]int
	// perValue tracks, for each value, the set of neighbors this node believes it
	// has already delivered the value to. This lets us avoid re-delivering a
	// value we already sent, cutting the number of inter-node messages.
	perValue   map[int]map[string]bool
	nextID     int

	// gossip is a periodic ticker that lets a node push out newly-learned
	// values to neighbors it may have missed them on.
	stopGossip chan struct{}
}

// interNodeBody is the body of a message sent between our own nodes.
type interNodeBody struct {
	Type       string   `json:"type"`
	// To tells whether this is an "originate" message (origin -> first hop)
	// or a "forward" message (relay -> next hop).
	To string `json:"to"`
	// From identifies the origin node of the value.
	From string `json:"from,omitempty"`
	// Origin tells the first relay hop to whom to send the value.
	Origin string `json:"origin,omitempty"`
	// Msg is the value being carried.
	Msg int `json:"msg"`
	// SentTo records the neighbors this node has already delivered Msg to.
	SentTo []string `json:"sent_to,omitempty"`
}

func (n *broadcastNode) handleInit(msg maelstrom.Message) error {
	var body maelstrom.InitMessageBody
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return fmt.Errorf("unmarshal init: %w", err)
	}
	n.id = body.NodeID
	n.nodeIDs = body.NodeIDs

	// Ask the topology coordinator which neighbors we should talk to. We reply
	// with topology_ok to be safe.
	n.topology = make(map[string][]string)
	n.stopGossip = make(chan struct{})
	return n.Reply(msg, maelstrom.MessageBody{Type: "topology_ok"})
}

func (n *broadcastNode) handleTopology(msg maelstrom.Message) error {
	var body struct {
		maelstrom.MessageBody
		Topology map[string][]string `json:"topology"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return fmt.Errorf("unmarshal topology: %w", err)
	}
	n.topology = make(map[string][]string, len(body.Topology))
	for from, tos := range body.Topology {
		n.topology[from] = tos
	}
	// Reply to topology_ok.
	return n.Reply(msg, maelstrom.MessageBody{Type: "topology_ok"})
}

func (n *broadcastNode) handleBroadcast(msg maelstrom.Message) error {
	var body struct {
		maelstrom.MessageBody
		Message int `json:"message"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return fmt.Errorf("unmarshal broadcast: %w", err)
	}
	val := body.Message

	n.mu.Lock()
	// Record that we know this value, originating from us.
	if n.values == nil {
		n.values = make(map[int]int)
		n.perValue = make(map[int]map[string]bool)
	}
	if _, ok := n.values[val]; !ok {
		n.values[val] = n.id
		n.perValue[val] = make(map[string]bool)
	}
	// The origin of the value is the node that received the broadcast.
	origin := n.id
	n.mu.Unlock()

	// Send to every neighbor (except ourselves) an "originate" message. The
	// origin node is responsible for forwarding the value down the tree until
	// it reaches a relay node.
	neighbors := n.neighbors()
	for _, nb := range neighbors {
		if nb == n.id {
			continue
		}
		if err := n.sendInter(nb, origin, val, nil); err != nil {
			log.Printf("node %s: send originate %d to %s failed: %v", n.id, val, nb, err)
		}
	}
	return nil
}

func (n *broadcastNode) handleRead(msg maelstrom.Message) error {
	n.mu.Lock()
	vals := make([]int, 0, len(n.values))
	for v := range n.values {
		vals = append(vals, v)
	}
	n.mu.Unlock()
	return n.Reply(msg, maelstrom.MessageBody{
		Type:      "read_ok",
		Messages:  vals,
		InReplyTo: msg.Body.InReplyTo,
	})
}

func (n *broadcastNode) sendInter(dest string, origin int, val int, sentTo []string) error {
	sentToCopy := sentTo
	if sentToCopy == nil {
		sentToCopy = []string{}
	}
	b := interNodeBody{
		Type:     "carry",
		To:       "originate",
		From:     origin,
		Origin:   origin,
		Msg:      val,
		SentTo:   sentToCopy,
	}
	return n.Reply(nil, b)
}
