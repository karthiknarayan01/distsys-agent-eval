package main

import (
	"encoding/json"
	"log"
	"sort"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// Internal message type for cluster communication
type InternalMessage struct {
	Value int `json:"value"`
}

var values map[int]struct{} = make(map[int]struct{})

func main() {
	n := maelstrom.NewNode()
	
	// Handle init message - this resets values for each new cluster instance
	n.Handle("init", func(msg maelstrom.Message) error {
		values = make(map[int]struct{})
		log.Printf("Init: myID=%s, nodeCount=%d", n.ID(), len(n.NodeIDs()))

		return nil
	})

	// Handle topology message - we use all-to-all communication anyway
	n.Handle("topology", func(msg maelstrom.Message) error {
		log.Printf("Topology received: %v", msg.Body)
		return n.Reply(msg, "topology_ok")
	})

	// Handle broadcast request from client - receive a value and store it
	n.Handle("broadcast", func(msg maelstrom.Message) error {
		body := string(msg.Body)
		var val int
		
		// Parse the body JSON to get msg_id
		if err := json.Unmarshal([]byte(body), &val); err != nil {
			log.Printf("Error parsing broadcast msg_id: %v", err)
		}
		
		values[val] = struct{}{}

		// Forward to all other nodes in the cluster (all-to-all for reliability)
		if n.NodeIDs() != nil {
			for _, destID := range n.NodeIDs() {
				if destID != msg.Src {
					internalMsg, _ := json.Marshal(InternalMessage{Value: val})
					n.RPC(destID, internalMsg, func(msg maelstrom.Message) error {
						return nil
					})
				}
			}
		}

		return n.Reply(msg, "broadcast_ok")
	})

	// Handle read request from client - return all stored values
	n.Handle("read", func(msg maelstrom.Message) error {
		// Get all values sorted
		vals := make([]int, 0, len(values))
		for v := range values {
			vals = append(vals, v)
		}
		sort.Ints(vals)

		return n.Reply(msg, vals)
	})

	// Handle internal message from another node in the cluster
	n.Handle("internal_message", func(msg maelstrom.Message) error {
		internalMsg := InternalMessage{}
		json.Unmarshal(msg.Body, &internalMsg)

		val := internalMsg.Value
		
		values[val] = struct{}{}

		// Forward to all other nodes (all-to-all for reliability)
		if n.NodeIDs() != nil {
			for _, destID := range n.NodeIDs() {
				if destID != msg.Src {
					internalMsg2, _ := json.Marshal(InternalMessage{Value: val})
					n.RPC(destID, internalMsg2, func(msg maelstrom.Message) error {
						return nil
					})
				}
			}
		}

		return n.Reply(msg, "internal_msg_ack")
	})

	// Start the node in a goroutine so it runs
	go n.Run()

	// Run forever since there's no specific exit condition for broadcast cluster
	select {}
}
