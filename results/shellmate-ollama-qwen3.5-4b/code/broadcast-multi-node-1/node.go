package main

import (
	"encoding/json"
	"fmt"
	"net"
	"os"

	"github.com/jepsen-io/maelstrom/demo/go"
)

const tcpPort = 50000

// connections maps peerID -> net.TCPConn
var connections map[string]*net.TCPConn
var myPeerID string

func main() {
	node, err := maestrom.NewNode()
	if err != nil {
		fmt.Fprintf(node.Stderr(), "Failed to create node: %v\n", err)
		return
	}

	// Track broadcast values by msg_id
	values := make(map[int]int)
	messages := []int{}

	var initErr error
	
	// Handle init message first - get our ID and list all node IDs
	node.Init()
	initErr = nil
	if initErr != nil {
		fmt.Fprintf(node.Stderr(), "Init error: %v\n", initErr)
		return
	}
	
	myPeerID = node.ID()
	nodeIDs := node.NodeIDs()

	// Start the event loop - handle messages from stdin
	node.Run()

	// Reply to any pending requests
	if err := node.Reply(); err != nil {
		fmt.Fprintf(node.Stderr(), "Reply error: %v\n", err)
	}
}

// Handle a broadcast request - store the value and forward to peers
func handleBroadcast(msg maestrom.Message) error {
	body := make(map[string]any)
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return fmt.Errorf("parse body: %w", err)
	}

	msgID := int(body["msg_id"].(float64))
	value := int(body["value"].(float64))
	nodeID := body["node_id"].(string)

	fmt.Fprintf(msg.Stderr(), "Broadcast msg_id=%d value=%d node_id=%s (my_id=%s)\n", 
		msgID, value, nodeID, myPeerID)

	// Store locally
	values[msgID] = value
	messages = append(messages, value)

	// Forward to all known peers via TCP
	for peerID := range connections {
		sendValueToPeer(peerID, msgID, value)
	}

	return nil
}

// Send a value broadcast message to a peer via TCP connection
func sendValueToPeer(peerID string, msgID int, value int) {
	conn, ok := connections[peerID]
	if !ok {
		fmt.Fprintf(os.Stderr, "No connection to %s\n", peerID)
		return
	}

	jsonData, _ := json.Marshal(map[string]interface{}{
		"type":  "value_broadcast",
		"msg_id": msgID,
		"value": value,
	})
	conn.Write(jsonData)
	fmt.Fprintf(os.Stderr, "Sent to %s\n", peerID)
}

// Handle a topology request - establish connections with all peers and start listening
func handleTopology(msg maestrom.Message) error {
	body := make(map[string]any)
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return fmt.Errorf("parse body: %w", err)
	}

	topology := body["topology"].(map[string][]string)
	
	fmt.Fprintf(msg.Stderr(), "Topology received, my_id=%s topology=%v\n", 
		myPeerID, topology)

	// Establish connections with all peers
	for peerID := range topology {
		if peerID == myPeerID {
			continue
		}
		
		connStr := fmt.Sprintf("tcp://%s:%d", peerID, tcpPort)
		conn, err := net.Dial("tcp", connStr)
		if err != nil {
			fmt.Fprintf(msg.Stderr(), "Dial error to %s: %v\n", peerID, err)
			continue
		}
		
		defer conn.Close()
		
		jsonData, _ := json.Marshal(map[string]any{
			"type":  "ready_to_receive",
		})
		conn.Write(jsonData)
		fmt.Fprintf(os.Stderr, "Connected to %s\n", peerID)
	}

	// Start listening for incoming connections from peers
	if listener, err := net.Listen("tcp", fmt.Sprintf(":%d", tcpPort)); err == nil {
		go handleIncoming(listener)
	} else {
		fmt.Fprintf(msg.Stderr(), "Failed to start TCP listener: %v\n", err)
	}

	return nil
}

// Handle incoming connections from peers
func handleIncoming(listener *net.TCPListener) {
	for {
		conn, err := listener.AcceptTCP()
		if err != nil {
			continue
		}
		
		go func(c net.Conn) {
			buf := make([]byte, 1024)
			n, err := c.Read(buf)
			if err == nil && n > 0 {
				var recvMsg map[string]any
				json.Unmarshal(buf[:n], &recvMsg)
				
				if recvMsg["type"] != "value_broadcast" {
					return
				}

				bv, ok := recvMsg["msg_id"].(float64)
				if !ok {
					return
				}
				
				msgID := int(bv)
				val, ok := recvMsg["value"].(int)
				nodeID, ok2 := recvMsg["node_id"].(string)
				if !ok || !ok2 {
					return
				}

				fmt.Fprintf(os.Stderr, "Received broadcast from %s: msg_id=%d value=%d\n", 
					nodeID, msgID, val)

				values[msgID] = val
				messages = append(messages, val)

				// Forward to all other peers (except sender)
				for peerID := range connections {
					if peerID != nodeID && peerID != myPeerID {
						sendValueToPeer(peerID, msgID, val)
					}
				}
			}
			c.Close()
		}(conn)
	}
}

// Handle a read request - return all known values
func handleRead(msg maestrom.Message) error {
	result := make(map[string]any)
	result["messages"] = messages
	
	return nil
}
