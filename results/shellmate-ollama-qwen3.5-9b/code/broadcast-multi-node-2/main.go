package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Node struct {
	id         string
	valueSet   map[int]struct{}
	nodeIDs    []string
}

func main() {
	node := maelstrom.NewNode()
	defer node.Close()

	n := &Node{
		id:        "",
		valueSet:  make(map[int]struct{}),
		nodeIDs:   make([]string, 0),
	}

	node.Handle("init", n.handleInit)
	node.Handle("broadcast", n.handleBroadcast)
	node.Handle("read", n.handleRead)
	node.Handle("topology", n.handleTopology)

	err := node.Run()
	if err != nil {
		fmt.Fprintf(os.Stderr, "node.Run failed: %v\n", err)
	}
}

func (n *Node) handleInit(typ string, msg maelstrom.Message, reply func(string)) {
	// The init handler receives the node info in the body
	var body struct {
		ID   string   `json:"id"`
		Nodes []string `json:"nodes"`
	}
	
	if err := json.Unmarshal([]byte(msg.Body), &body); err == nil && len(body.Nodes) > 0 {
		n.id = body.Nodes[0]
		n.nodeIDs = make([]string, len(body.Nodes))
		copy(n.nodeIDs, body.Nodes)
	}

	reply("")
}

func (n *Node) handleBroadcast(typ string, msg maelstrom.Message, reply func(string)) {
	var body struct {
		Message int `json:"message"`
	}
	
	if err := json.Unmarshal([]byte(msg.Body), &body); err != nil {
		reply("")
		return
	}

	n.mu.Lock()
	if _, exists := n.valueSet[body.Message]; !exists && body.Message > 0 {
		n.valueSet[body.Message] = struct{}{}
		
		// Send to all other nodes using RPC
		for _, nid := range n.nodeIDs {
			if nid != n.id {
				node.RPC(nid, maelstrom.Message{
					ID:    fmt.Sprintf(`{"type":"gossip","value":%d}`, body.Message),
					Dest:  nid,
					Type:  "gossip",
					Body:  fmt.Sprintf(`{"type":"gossip","src":"%s","dest":"%s","value":%d}`, n.id, nid, body.Message),
				})
			}
		}
	}
	n.mu.Unlock()

	reply("")
}

func (n *Node) handleRead(typ string, msg maelstrom.Message, reply func(string)) {
	var body map[string]interface{}
	json.Unmarshal([]byte(msg.Body), &body)

	inReplyTo := 0
	if v, ok := body["in_reply_to"]; ok {
		if s, ok := v.(float64); ok {
			inReplyTo = int(s)
		} else if str, ok := v.(string); ok {
			if num, err := strconv.Atoi(str); err == nil {
				inReplyTo = num
			}
		}
	}

	n.mu.Lock()
	values := make([]int, 0, len(n.valueSet))
	for k := range n.valueSet {
		values = append(values, k)
	}
	n.mu.Unlock()

	responseBody := fmt.Sprintf(`{"type":"read_ok","in_reply_to":%d,"messages":[%v]}`, inReplyTo, values)
	reply(responseBody)
}

func (n *Node) handleTopology(typ string, msg maelstrom.Message, reply func(string)) {
	var body map[string]interface{}
	json.Unmarshal([]byte(msg.Body), &body)
	
	topology, ok := body["topology"].(map[string]interface{})
	if !ok {
		reply("")
		return
	}

	for k, v := range topology {
		nodesArr, _ := v.([]interface{})
		for i, node := range nodesArr {
			nodeStr := fmt.Sprintf("%v", node)
			n.valueSet[nodeStr] = struct{}{} // Just acknowledge
		}
	}

	reply("")
}

func (n *Node) handleGossip(typ string, msg maelstrom.Message, reply func(string)) {
	var body struct {
		Value int `json:"value"`
		Src   string `json:"src"`
		Dest  string `json:"dest"`
		Type  string `json:"type"`
	}

	if typ != "gossip" {
		return
	}

	json.Unmarshal([]byte(msg.Body), &body)

	n.mu.Lock()
	if _, exists := n.valueSet[body.Value]; !exists && body.Value > 0 {
		n.valueSet[body.Value] = struct{}{}

		// Forward to all other nodes except sender and destination
		for _, nid := range n.nodeIDs {
			if nid != msg.Dest {
				node.RPC(nid, maelstrom.Message{
					ID:    fmt.Sprintf(`{"type":"gossip","value":%d}`, body.Value),
					Dest:  nid,
					Type:  "gossip",
					Body:  fmt.Sprintf(`{"type":"gossip","src":"node","dest":"%s","value":%d}`, nid, body.Value),
				})
			}
		}

		n.mu.Unlock()
		reply("")
	} else {
		n.mu.Unlock()
		reply("")
	}
}
