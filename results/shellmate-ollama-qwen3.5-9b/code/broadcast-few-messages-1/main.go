package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Node struct {
	maelstrom.Node
	self           string
	allNodes       map[string]bool
	nodeNeighbors  map[string]map[string]bool
	values         []int64          // values we've seen (will be accessed with mutex)
	sentEdges      map[string]bool  // key: valueStr:targetID -> sent?
	mu             sync.Mutex
}

func main() {
	n := &Node{
		Node:           maelstrom.NewNode(),
		values:         make([]int64, 0),
		sentEdges:      make(map[string]bool),
		allNodes:       make(map[string]bool),
		nodeNeighbors:  make(map[string]map[string]bool),
	}

	n.Node.Handle(func(msg maelstrom.Message) {
		switch msg.Type {
		case "init":
			log.Printf("initialized, self=%s", n.Self())

		case "topology":
			var topologyMsg struct {
				Topology map[string][]string `json:"topology"`
			}
			json.Unmarshal(msg.Body, &topologyMsg)

			// Store all node IDs and neighbor info
			for nid := range topologyMsg.Topology {
				n.allNodes[nid] = true
			}

			nodeNeighbors := make(map[string]map[string]bool)
			for nodeID, neighborList := range topologyMsg.Topology {
				nodeNeighbors[nodeID] = make(map[string]bool)
				for _, neighbor := range neighborList {
					nodeNeighbors[nodeID][neighbor] = true
				}
			}
			n.nodeNeighbors = nodeNeighbors

			n.Node.Reply("topology_ok", nil, msg)

		case "broadcast":
			var broadcastMsg struct {
				Message   int64  `json:"message"`
				Broadcast bool   `json:"broadcast"`
			}
			json.Unmarshal(msg.Body, &broadcastMsg)

			value := broadcastMsg.Message

			n.mu.Lock()
			
			// Store the value (check for duplicate first to avoid unbounded growth if we get duplicates)
			found := false
			for _, v := range n.values {
				if v == value {
					found = true
					break
				}
			}
			if !found {
				n.values = append(n.values, value)
			}
			
			// Get neighbors and forward (only once per edge for now)
			if nn, ok := n.nodeNeighbors[n.Self()]; ok {
				for targetID := range nn {
					key := string(value) + ":" + targetID
					if !n.sentEdges[key] {
						payload := map[string]interface{}{
							"type":        "forward",
							"value":       value,
							"from_node_id": n.Self(),
							"to_node_id":   targetID,
						}
						body, _ := json.Marshal(payload)
						n.Send(targetID, body)
						n.sentEdges[key] = true
					}
				}
			}
			
			n.mu.Unlock()

			n.Node.Reply("broadcast_ok", nil, msg)

		case "read":
			n.mu.Lock()
			values := make([]int64, 0, len(n.values))
			for _, v := range n.values {
				values = append(values, v)
			}
			n.mu.Unlock()
			
			body, _ := json.Marshal(map[string]interface{}{
				"type":     "read_ok",
				"messages": values,
			})
			n.Node.Reply("read_ok", body, msg)

		default:
			// Unknown message type - reply with nil or ignore
		}
	})

	n.Run()
}
