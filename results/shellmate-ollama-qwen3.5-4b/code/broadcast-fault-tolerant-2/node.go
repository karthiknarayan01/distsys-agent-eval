package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"sync"
)

// Client request types
type broadcastRequest struct {
	MsgID  int `json:"msg_id"`
	Value  int `json:"value"`
}

type broadcastResponse struct {
	InReplyTo int `json:"in_reply_to"`
}

type readRequest struct {
	MsgID int `json:"msg_id"`
}

type readResponse struct {
	InReplyTo   int    `json:"in_reply_to"`
	Messages    []int  `json:"messages"`
}

type topologyRequest struct {
	MsgID int `json:"msg_id"`
}

type topologyResponse struct {
	InReplyTo int `json:"in_reply_to"`
}

// Node state
type nodeState struct {
	values     map[int]int // value -> msgID
	hasValue   bool        // do we have any values?
	nodeID     string
	otherNodes []string    // all other node IDs (from init)
	wg         sync.WaitGroup
	cancel     chan struct{}
}

func main() {
	fmt.Fprintf(os.Stderr, "Broadcast node starting\n")

	buf := bufio.NewReader(os.Stdin)
	
	for {
		line, err := buf.ReadString('\n')
		if err != nil {
			break
		}
		
		msgType := ""
		json.Unmarshal([]byte(line), &msgType)
		
		switch msgType {
		case "init":
			initMsg := initMessage{}
			json.Unmarshal([]byte(line), &initMsg)
			
			ns.nodeID = initMsg.NodeID
			ns.otherNodes = append(ns.otherNodes, initMsg.OtherNodes...)
			if len(ns.otherNodes) == 0 {
				ns.otherNodes = []string{ns.nodeID}
			}
			
			resp := topologyResponse{InReplyTo: 1}
			fmt.Fprintf(os.Stdout, "%s\n", json.Marshal(resp))
			
			ns.wg.Add(2)
			
			go func() {
				defer ns.wg.Done()
				for _, peerID := range ns.otherNodes {
					if peerID != "" && peerID != ns.nodeID {
						runInternalLoop(peerID)
					}
				}
			}()
			
			break
			
		case "broadcast":
			br := broadcastRequest{}
			json.Unmarshal([]byte(line), &br)
			
			ns.values[br.Value] = br.MsgID
			ns.hasValue = true
			
			resp := broadcastResponse{InReplyTo: br.MsgID}
			fmt.Fprintf(os.Stdout, "%s\n", json.Marshal(resp))
			
			break
			
		case "read":
			rq := readRequest{}
			json.Unmarshal([]byte(line), &rq)
			
			values := ns.getAllValues()
			
			resp := readResponse{InReplyTo: rq.MsgID}
			resp.Messages = values
			
			fmt.Fprintf(os.Stdout, "%s\n", json.Marshal(resp))
			
			break
			
		case "topology":
			tq := topologyRequest{}
			json.Unmarshal([]byte(line), &tq)
			
			resp := topologyResponse{InReplyTo: tq.MsgID}
			fmt.Fprintf(os.Stdout, "%s\n", json.Marshal(resp))
			
			break
			
		default:
			fmt.Fprintf(os.Stderr, "Unknown message type: %s\n", line[:len(line)-1])
		}
	}

	ns.wg.Wait()
	
	fmt.Fprintf(os.Stderr, "Broadcast node finished\n")
}

func initMessage struct {
	NodeID       string `json:"node_id"`
	OtherNodes   []string `json:"other_nodes"`
}

// Get all values this node has stored (sorted by msgID for consistency)
func (ns *nodeState) getAllValues() []int {
	values := make([]int, 0, len(ns.values))
	for _, v := range ns.values {
		values = append(values, v)
	}
	return values
}

// Internal message type - used for node-to-node communication via stdin/stdout
type internalMessage struct {
	Type   string `json:"type"`
	Value  int    `json:"value,omitempty"`
	MsgID  int    `json:"msg_id,omitempty"`
	Source string `json:"source,omitempty"`
}

func (m internalMessage) String() string {
	b, _ := json.Marshal(m)
	return string(b)
}

// Handle broadcast request and propagate to peers
func handleBroadcast(br broadcastRequest) {
	ns.values[br.Value] = br.MsgID
	ns.hasValue = true
	
	// Propagate to all other nodes
	for _, peerID := range ns.otherNodes {
		if peerID != "" && peerID != ns.nodeID {
			propagateToPeer(peerID, br.MsgID)
		}
	}
	
	resp := broadcastResponse{InReplyTo: br.MsgID}
	fmt.Fprintf(os.Stdout, "%s\n", json.Marshal(resp))
}

// Read request - return all stored values
func handleRead(rq readRequest) {
	values := ns.getAllValues()
	
	resp := readResponse{InReplyTo: rq.MsgID}
	resp.Messages = values
	
	fmt.Fprintf(os.Stdout, "%s\n", json.Marshal(resp))
}

// Topology request - just acknowledge (we don't need to parse topology since init gives us all node IDs)
func handleTopology(tq topologyRequest) {
	resp := topologyResponse{InReplyTo: tq.MsgID}
	fmt.Fprintf(os stdout, "%s\n", json.Marshal(resp))
}

// Send internal message to peer via stdin (maelstrom library handles this routing)
func propagateToPeer(peerID string, msgID int) {
	data := map[string]any{
		"type":   "message",
		"body":   map[string]int{"type": "propagate", "msg_id": msgID},
	}
	line := fmt.Sprintf(`{"type":"message","body":%s}`, json.Marshal(data))

	rpc(peerID, line)
}

func rpc(dest string, line string) {
	data, _ := json.Marshal(map[string]string{"dest": dest, "src": ns.nodeID, "msg_id": msgID})
	fmt.Fprintf(os.Stdout, "%s\n", data)
}
