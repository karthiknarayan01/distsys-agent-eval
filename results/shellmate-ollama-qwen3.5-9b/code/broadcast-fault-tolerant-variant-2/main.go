package main

import (
	"context"
	"encoding/json"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/jepsen-io/maelstrom/demo/go"
)

// Node handles gossip for a village of 7 gossips
type Node struct {
	id       string
	ctx      context.Context
	cancel   func()
	mu       sync.RWMutex
	neighbors map[string][]string // nodeID -> list of neighbor nodeIDs
	rumors   map[int]bool
}

func NewNode() *Node {
	return &Node{
		id:       "unknown",
		ctx:      context.Background(),
		cancel:   func() {},
		neighbors: make(map[string][]string),
		rumors:   make(map[int]bool),
	}
}

func main() {
	node := NewNode()
	ctx, cancel := context.WithCancel(context.Background())
	
	if err := maels trom.NewNode(
		node.ID(),
		&maels trom.NodeHandlers{
			"init": node.handleInit,
			"neighbors": node.handleNeighbors,
			"tell": func(ctx maelstrom.Context, msgBody maelstrom.Body) error {
				return node.handleTell(ctx, msgBody)
			},
			"recall": func(ctx maelstrom.Context, msgBody maelstrom.Body) error {
				return node.handleRecall(ctx, msgBody)
			},
			"gossip": func(ctx maelstrom.Context, msgBody maelstrom.Body) error {
				return node.handleGossip(ctx, msgBody)
			},
			"stop": func(ctx maelstrom.Context) error { return ctx.Stop("node stopped") },
		},
	).Run()
}

func (n *Node) ID() string {
	return n.id
}

func (n *Node) handleInit(msgBody maelstrom.Body, next maels trom.HandlerFunc) error {
	var msg struct {
		MessageID string            `json:"msg_id"`
		Src       string            `json:"src,omitempty"`
		Dest      string            `json:"dest,omitempty"`
		Neighbors map[string][]string `json:"neighbors"`
	}
	
	if err := json.Unmarshal([]byte(msgBody.Raw), &msg); err != nil {
		return fmt.Errorf("failed to unmarshal init: %w", err)
	}

	n.id = msg.Neighbors[msg.Src]
	n.ctx, n.cancel = context.WithCancel(context.Background())
	n.mu.Lock()
	n.neighbors = make(map[string][]string)
	for k, v := range msg.Neighbors {
		n.neighbors[k] = v
	}
	n.mu.Unlock()

	next(&maels trom.Message{
		Src:   msg.Src,
		Dest:  msg.Dest,
		Type:  "neighbors_noted",
		Body: maelstrom.Body{
			MessageID: msg.MessageID,
		},
	})

	return nil
}

func (n *Node) handleNeighbors(msgBody maelstrom.Body, next maels trom.HandlerFunc) error {
	var msg struct {
		MessageID string            `json:"msg_id"`
		Src       string            `json:"src,omitempty"`
		Dest      string            `json:"dest,omitempty"`
		Neighbors map[string][]string `json:"neighbors"`
	}
	
	if err := json.Unmarshal([]byte(msgBody.Raw), &msg); err != nil {
		return fmt.Errorf("failed to unmarshal neighbors: %w", err)
	}

	n.mu.Lock()
	for k, v := range msg.Neighbors {
		n.neighbors[k] = v
	}
	n.mu.Unlock()

	next(&maels trom.Message{
		Src:   msg.Src,
		Dest:  msg.Dest,
		Type:  "neighbors_noted",
		Body: maelstrom.Body{
			MessageID: msg.MessageID,
		},
	})

	return nil
}

func (n *Node) handleTell(msgBody maelstrom.Body, next maels trom.HandlerFunc) error {
	var msg struct {
		Type     string `json:"type"`
		MessageID string `json:"msg_id,omitempty"`
		Src      string `json:"src,omitempty"`
		Dest     string `json:"dest,omitempty"`
		Rumor    int   `json:"rumor"`
	}
	
	if err := json.Unmarshal([]byte(msgBody.Raw), &msg); err != nil {
		return fmt.Errorf("failed to unmarshal tell: %w", err)
	}

	n.mu.Lock()
	exists := n.rumors[msg.Rumor]
	n.mu.Unlock()
	
	// Check if we have a sender channel for this rumor to avoid re-processing
	senderKey := msg.MessageID
	if exists && senderKey == "" {
		exists = true // Don't process again
	}

	if !exists {
		n.mu.Lock()
		n.rumors[msg.Rumor] = true
		n.mu.Unlock()
		
		// Schedule propagation to neighbors
		go n.propagate(msg)
	}

	next(&maels trom.Message{
		Src:   msg.Src,
		Dest:  msg.Dest,
		Type:  "told",
		Body: maelstrom.Body{
			MessageID: senderKey,
		},
	})

	return nil
}

func (n *Node) handleGossip(msgBody maelstrom.Body, next maels trom.HandlerFunc) error {
	var msg struct {
		Rumor   int      `json:"rumor"`
		Type    string   `json:"type"`
		MessageID string `json:"msg_id,omitempty"`
	}
	
	if err := json.Unmarshal([]byte(msgBody.Raw), &msg); err != nil {
		return fmt.Errorf("failed to unmarshal gossip: %w", err)
	}

	n.mu.Lock()
	exists := n.rumors[msg.Rumor]
	n.mu.Unlock()

	if !exists {
		n.mu.Lock()
		n.rumors[msg.Rumor] = true
		n.mu.Unlock()
		
		// Add to propagation queue and send to neighbors
		go n.propagateToNeighbors(msg.Rumor)
	}

	return nil
}

func (n *Node) handleRecall(msgBody maelstrom.Body, next maels trom.HandlerFunc) error {
	var msg struct {
		Type    string `json:"type"`
		MessageID string `json:"msg_id,omitempty"`
		Src     string `json:"src,omitempty"`
		Dest    string `json:"dest,omitempty"`
	}
	
	if err := json.Unmarshal([]byte(msgBody.Raw), &msg); err != nil {
		return fmt.Errorf("failed to unmarshal recall: %w", err)
	}

	n.mu.RLock()
	rumors := make([]int, 0, len(n.rumors))
	for rumor := range n.rumors {
		rumors = append(rumors, rumor)
	}
	n.mu.RUnlock()

	responseBody := maelstrom.Body{
		Type: "recalled",
		Rumors: rumors,
	}

	next(&maels trom.Message{
		Src:   msg.Src,
		Dest:  msg.Dest,
		Body:  responseBody,
	})

	return nil
}

func (n *Node) propagate(msg struct {
	Type    string `json:"type"`
	Rumor   int   `json:"rumor"`
	MessageID string `json:"msg_id,omitempty"`
	Src     string `json:"src,omitempty"`
	Dest    string `json:"dest,omitempty"`
}) {
	// Send to neighbors
	n.mu.RLock()
	defer n.mu.RUnlock()

	for neighbor := range n.neighbors {
		msg, _ := json.Marshal(map[string]interface{}{
			"src":  n.id,
			"dest": neighbor,
			"type": "gossip",
			"body": map[string]interface{}{
				"type":    "gossip",
				"rumor":   msg.Rumor,
				"messageID": fmt.Sprintf("gossip-%s", n.id),
			},
		})

		if err := SendTo(neighbor, msg); err != nil {
			log.Printf("failed to send gossip to %s: %v", neighbor, err)
		}
	}
}

func (n *Node) propagateToNeighbors(rumor int) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	for _, neighbors := range n.neighbors {
		for _, neighbor := range neighbors {
			msg, _ := json.Marshal(map[string]interface{}{
				"src":  n.id,
				"dest": neighbor,
				"type": "gossip",
				"body": map[string]interface{}{
					"type":    "gossip",
					"rumor":   rumor,
					"messageID": fmt.Sprintf("gossip-%s", n.id),
				},
			})

			SendTo(neighbor, msg)
		}
	}
}
