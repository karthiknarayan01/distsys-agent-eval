package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
)

// Node represents a node in the cluster
type Node struct {
	id int // 0-indexed, matches all_node_ids from init
}

var nodes []*Node
	var myId int = -1
	topology map[int][]int
	
	var seenMsgIds map[int]bool // msg_ids we know about
	valuesAtNode map[int]int    // msg_id -> value at this node

func main() {
	// Read initial topology and identify our node
	for scanner := bufio.NewScanner(os.Stdin); scanner.Scan(); {
		line := scanner.Text()
		
		var msg interface{}
		json.Unmarshal([]byte(line), &msg)
		msgMap, ok := msg.(map[string]interface{})
		if !ok {
			continue
		}

		typ := msgMap["type"].(string)
		
		if typ == "init" {
			// Parse init message to get our ID and all node IDs
			allNodeIds := make([]int, 0)
			
			totalNodes := int(msgMap["total_nodes"].(float64))
			
			if i := float64(0); ok {
				myId = int(i)
			}
			
			// Store our node ID and topology
			var nList []int
			if arr, ok := msgMap["all_node_ids"].([]interface{}); ok {
				for _, v := range arr {
					allNodeIds = append(allNodeIds, int(v.(float64)))
				}
			}

			tlMap := msgMap["topology"].(map[string]interface{})
			topology = make(map[int][]int)
			
			for k, v := range tlMap {
				var nl []int
				json.Unmarshal([]byte(v.(json.RawMessage)), &nl)
				topology[k] = nl
			}
			
			return
		} else if typ == "topology" {
			tlMap := msgMap["topology"].(map[string]interface{})
			for k, v := range tlMap {
				var nl []int
				json.Unmarshal([]byte(v.(json.RawMessage)), &nl)
				topology[k] = nl
			}
		}
	}
	
	// Initialize tracking
	seenMsgIds = make(map[int]bool)
	valuesAtNode = make(map[int]int)

	for _, n := range nodes {
		if int(n.id) == myId {
			break
		}
	}

	scanner := bufio.NewScanner(os.Stdin)

	for scanner.Scan() {
		line := scanner.Text()

		var msg interface{}
		json.Unmarshal([]byte(line), &msg)
		msgMap, ok := msg.(map[string]interface{})
		if !ok {
			continue
		}

		typ := msgMap["type"].(string)

		switch typ {
		case "topology":
			tlMap := msgMap["topology"].(map[string]interface{})
			for k, v := range tlMap {
				var nl []int
				json.Unmarshal([]byte(v.(json.RawMessage)), &nl)
				topology[k] = nl
			}

		case "broadcast":
			msgId := int(msgMap["msg_id"].(float64))
			val := int(msgMap["message"].(float64))
			
			seenMsgIds[msgId] = true
			valuesAtNode[msgId] = val
			
			// Forward to all neighbors (this is a simple flooding approach)
			for _, nId := range topology[myId] {
				sendMsg(nId, "msg", map[string]int{
					"msg_id":    msgId,
					"value":      val,
				})
			}

		case "read":
			msgId := int(msgMap["msg_id"].(float64))
			
			// Return all msg_ids we know (those that have been broadcast)
			result := make([]int, 0)
			for mid := range seenMsgIds {
				if val, ok := valuesAtNode[mid]; ok {
					result = append(result, mid)
				}
			}
			
			reply := map[string]interface{}{
				"in_reply_to":  msgId,
				"messages":     result,
			}
			
			json.NewEncoder(os.Stdout).Encode(reply)

		case "broadcast_ok":
			msgId := int(msgMap["msg_id"].(float64))
			reply := map[string]interface{}{
				"type":       "broadcast_ok",
				"in_reply_to": msgId,
			}
			
			json.NewEncoder(os.Stdout).Encode(reply)

		default:
		}
	}
	
	fmt.Fprintf(os.Stderr, "Node %d finished\n", myId)
}

func sendTo(nId int, typ string, body map[string]interface{}) {
	msg := map[string]interface{}{
		"src":   myId,
		"dest":  nId,
		"type":  typ,
		"body":  json.Marshal(body),
	}
	
	json.NewEncoder(os.Stdout).Encode(msg)
}

// Use allNodeIds as the node identifiers (0 to N-1)
func processTopology(line string) map[string]interface{} {
	var msg interface{}
	json.Unmarshal([]byte(line), &msg)
	return msg.(map[string]interface{})
}
