package main

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"

	"github.com/jepsen-io/maelstrom/demo/go"
)

// We need to handle:
// 1. Incoming messages from clients and neighbors via Stdin
// 2. Reply to topology_ok, broadcast_ok, read_ok responses
// 
// Messages flow:
// - init message gives us our ID and full topology map  
// - topology message is a request (with msg_id) we reply with topology_ok
// - broadcast message from client gives us value + msg_id, we store it and forward to neighbors
// - read message asks for all values, we return them
// - Messages from neighbors also have type "broadcast" but come through Send/Reply
//   They have same structure: {"type": "broadcast", "value": N, "msg_id": M}

type broadcastMsg struct {
	Value     int    `json:"value"`
	MsgID     int    `json:"msg_id"`
	SenderID  string `json:"sender_id,omitempty"`
}

// Store maps msg_id -> value for all broadcasts we've received (from anywhere)
var (
	myID        = ""
	topo        map[string][]string
	broadcasts  sync.Map // key: msg_id, value: *broadcastValue
	nextMsgID   int64    // counter for unique IDs per node when forwarding
)

type broadcastValue struct {
	Value int `json:"value"`
}

func main() {
	node := maelstrom.NewNode()

	// Register init handler
	node.Handle("init", func(msg maelstrom.Message) error {
		id, err := node.ID()
		if err != nil {
			return err
		}
		myID = id

		log.Printf("Node %s initialized", myID)

		body := msg.Body.(map[string]interface{})
		
		// Parse node_ids from init
		if ids, ok := body["node_ids"]; ok {
			nodeIDs := make([]string, 0)
			for _, s := range ids.([]interface{}) {
				nodeIDs = append(nodeIDs, s.(string))
			}
			log.Printf("All nodes: %v", nodeIDs)
		}

		// Parse topology map from init
		topoMap, ok := body["topology"].(map[string]any)
		if !ok {
			return fmt.Errorf("no topology in init message")
		}

		topo = make(map[string][]string)
		for fromNode, toList := range topoMap {
			tos := toList.([]interface{})
			var strs []string
			for _, t := range tos {
				strs = append(strs, t.(string))
			}
			topo[fromNode] = strs
			log.Printf("  %s -> neighbors: %v", fromNode, strs)
		}

		log.Printf("Node %s has neighbors: %v", myID, topo[myID])

		return nil
	})

	// Handle topology requests - these are requests asking for who we should talk to
	node.Handle("topology", func(msg maelstrom.Message) error {
		body := msg.Body.(map[string]interface{})
		
		msgID := int(body["msg_id"].(float64))
		
		// The reply is just a simple acknowledgment
		replyBody := map[string]interface{}{
			"type":       "topology_ok",
			"in_reply_to": float64(msgID),
		}

		return node.Reply(msg, replyBody)
	})

	// Handle broadcast requests - from clients or neighbors forwarding us something
	node.Handle("broadcast", func(msg maelstrom.Message) error {
		body := msg.Body.(map[string]interface{})
		
		value := int(body["value"].(float64))
		reqMsgID := int(body["msg_id"].(float64))

		log.Printf("[%s] broadcast: value=%d (id=%d) from %s", myID, value, reqMsgID, msg.Src)

		// Check if this is a reply we're receiving (has in_reply_to set)
		// For forward messages, the body should contain type:"broadcast" with value and msg_id
		
		// Store the broadcast value locally using msg_id as key
		broadcasts.Store(int64(reqMsgID), &broadcastValue{Value: value})

		// If this is a forwarded message (has sender_id field), we might also forward to our neighbors
		senderID, _ := body["sender_id"].(string)
		
		if senderID == "" {
			// This is a direct broadcast request from client, not a neighbor forwarding us something
			// Already stored above; no need to forward further since client sent it directly to us
		} else {
			// This came from a neighbor - we should also forward to our own neighbors (broadcasting)
			for _, neighbor := range topo[myID] {
				log.Printf("[%s] re-forwarding broadcast %d -> %s", myID, reqMsgID, neighbor)
				
				fwdBody := map[string]interface{}{
					"type":       "broadcast", 
					"value":      float64(value),
					"msg_id":     float64(reqMsgID),
					"in_reply_to": 0, // This is a forward, not a reply
				}
				
				if err := node.Send(neighbor, fwdBody); err != nil {
					log.Printf("[%s] error sending to %s: %v", myID, neighbor, err)
				}
			}
		}

		// For broadcast requests from clients (without sender_id), we reply with broadcast_ok
		if senderID == "" {
			bcastOkBody := map[string]interface{}{
				"type":       "broadcast_ok",
				"in_reply_to": float64(reqMsgID),
			}
			
			return node.Reply(msg, bcastOkBody)
		} else {
			// This is from a neighbor forwarding - they'll handle replying to us
			return nil
		}
	})

	// Handle read requests
	node.Handle("read", func(msg maelstrom.Message) error {
		body := msg.Body.(map[string]interface{})
		
		values := []float64{}
		broadcasts.Range(func(key, value any) bool {
			v := value.(*broadcastValue)
			values = append(values, float64(v.Value))
			return true
		})

		replyBody := map[string]interface{}{
			"type":       "read_ok",
			"in_reply_to": int(body["msg_id"].(float64)),
			"messages":   values,
		}

		return node.Reply(msg, replyBody)
	})

	node.Run()
}
