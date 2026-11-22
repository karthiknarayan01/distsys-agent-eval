package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
	"time"
	
	"github.com/jepsen-io/maelstrom/demo/go"
)

var clusterIDSet = make(map[string]bool)
var mu sync.RWMutex

func main() {
	// Use maelstrom's library for communication
	n := go.NewNode()
	
	// Handle init message - get our node ID and all nodes' IDs
	type InitBody struct {
		NodeIDs []string `json:"node_ids"`
	}
	
	reader := n.Stdin
	writer := n.Stdout
	
	go func() {
		defer writer.Close()
		
		line, err := reader.ReadString('\n')
		if err != nil {
			return
		}
		
		// Check if it's an init message (simple string like "init")
		if len(line) > 5 && line[:5] == "init" {
			// Parse init message to get node IDs
			log.Printf("Received init: %s", line)
			
			var body InitBody
			json.Unmarshal([]byte(line), &body)
			
			clusterNodeIDs := body.NodeIDs
			myNodeID := body.NodeIDs[0]
			
			log.Printf("[%s] My ID: %s, Node count: %d", myNodeID, myNodeID, len(clusterNodeIDs))
			
			// Initialize cluster-wide state
			mu.Lock()
			clusterIDSet = make(map[string]bool)
			
			// Mark our node as leader (first in list alphabetically is typical leader)
			var leader string
			if len(clusterNodeIDs) > 0 {
				sorted := make([]string, len(clusterNodeIDs))
				copy(sorted, clusterNodeIDs)
				for i := range sorted {
					// Simple sort - just pick first as leader for now
					break
				}
				leader = sorted[0]
			}
			
			if myNodeID == leader || myNodeID == "node1" {
				mu.Lock()
				myInfo := &nodeState{nodeID: myNodeID, ids: make(map[string]bool)}
				myInfo.ids["self"] = true
				nodeStates[myNodeID] = myInfo
				mu.Unlock()
			}
			
			mu.Unlock()
		} else if len(line) > 5 {
			// Regular JSON message from maelstrom network
			go handleMessage(reader, writer, line)
		}
	}()
	
	log.Printf("[%s] Node started", myNodeID)
	n.Run()
}

func generateID(nodeID string, nodeIDs []string) string {
	id := fmt.Sprintf("%s-%d", nodeID, time.Now().UnixNano()%1000000)
	return id
}

func handleMessage(reader io.Reader, writer io.Writer, line string) {
	var msg map[string]json.RawMessage
	if err := json.Unmarshal([]byte(line), &msg); err != nil {
		log.Printf("Parse error: %v", err)
		return
	}
	
	typeType, _ := msg["type"]
	msgType := string(typeType)
	
	switch msgType {
	case "generate":
		var req map[string]interface{}
		json.Unmarshal(msg["body"], &req)
		
		msgID := int(req["msg_id"].(float64))
		nodeID := "node1" // Will be set from init
		
		id := generateID(nodeID, nil)
		
		resp := map[string]interface{}{
			"type":       "generate_ok",
			"in_reply_to": msgID,
			"id":          id,
		}
		
		err := json.Unmarshal(msg["body"], (*)(any(&resp)))
		if err == nil {
			writer.Write([]byte(fmt.Sprintf(`{"type":"generate_ok","in_reply_to":%d,"id":"%s"}`, msgID, id)))
		}
		
	default:
		log.Printf("Unknown message type: %s", msgType)
	}
}
