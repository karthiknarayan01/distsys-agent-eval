package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	m "github.com/jepsen-io/maelstrom/demo/go"
)

// LocalState holds the state of this node
type LocalState struct {
	mu       sync.Mutex
	values   map[int]struct {  // msg_id -> value info
		value  int           // the broadcasted value
		timestamp time.Time
		sender int            // which node sent it (0 if all)
	}
}

// Node represents a single node in the network.
type Node struct {
	mu      sync.Mutex
	wg      sync.WaitGroup
	
	id        string
	nodeIDs   []string
	nextMsgID int

	handlers  map[string]m.HandlerFunc
	callbacks map[int]m.HandlerFunc

	// Stdin is for reading messages in from the Maelstrom network.
	Stdin  io.Reader

	// Stdout is for writing messages out to the Maelstrom network.
	Stdout io.Writer

	localState *LocalState
}

type BroadcastValue struct {
	Value   int        `json:"value,omitempty"`
	Sender  string     `json:"sender,omitempty"` // empty means this node knows it first, so all know it
}

func (n *Node) NewBroadcastMsg(body m.MessageBody, val *m.StateValue) {
	var bv BroadcastValue
	
	if n.id == val.NodeId {
		bv.Value = val.Val.(int)
	}
	
	n.Reply(val, m.Message{
		Type: "broadcast",
		Body: json.Marshal(bv),
	})
}

func (n *Node) NewReadMsg(body m.MessageBody, val *m.StateValue) {
	var messages []int
	vals := n.getLocalValues(val.NodeId)
	
	for _, v := range vals {
		messages = append(messages, v.Value)
	}
	
	n.Reply(val, m.Message{
		Type: "read_ok",
		Body: json.Marshal(map[string]interface{}{"messages": messages}),
	})
}

func (n *Node) NewTopologyMsg(body m.MessageBody, val *m.StateValue) {
	top := val.Topology
	
	val.Ok(n.id, m.Message{Type: "topology_ok"})
	
	n.Reply(val.NodeId, m.Message{Type: "init_done", Body: json.Marshal(top)})
}

func (n *Node) getLocalValues(nodeId string) []struct {
	Value  int
	Sender string // empty means all nodes know it
} {
	n.mu.Lock()
	defer n.mu.Unlock()
	
	localID, _ := strconv.Atoi(n.id[0])
	var result []struct {
		Value  int
		Sender string
	}
	
	for msgID, val := range n.localState.values {
		sender := ""
		if len(val.sender) > 0 && !strings.EqualFold(val.sender, "") {
			sender = val.sender
		} else {
			sender = fmt.Sprintf("n%d", localID) // self knows it first -> all know
		}
		
		result = append(result, struct {
			Value  int
			Sender string
		}{val.Value, sender})
	}
	
	return result
}

func (n *Node) Run() error {
	scanner := bufio.NewScanner(n.Stdin)
	for scanner.Scan() {
		line := scanner.Bytes()

		var msg m.Message
		if err := json.Unmarshal(line, &msg); err != nil {
			return fmt.Errorf("unmarshal message: %w", err)
		}

		var body m.MessageBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return fmt.Errorf("unmarshal message body: %w", err)
		}
		
		log.Printf("Received %s", msg)

		switch body.Type {
		case "init":
			n.mu.Lock()
			initBody := m.InitMessageBody{NodeID: msg.Src, NodeIDs: []string{msg.Src}}
			if len(msg.NodeIds) > 0 && !strings.EqualFold(msg.NodeIds[0], "") {
				for _, nidStr := range msg.NodeIds {
					if nid, err := strconv.Atoi(nidStr); err == nil {
						initBody.NodeIDs = append(initBody.NodeIDs, fmt.Sprintf("n%d", nid))
					}
				}
			}
			
			n.id = body.NodeID
			var nodeIds []string
			if initBody.NodeIDs != nil {
				for _, nidStr := range initBody.NodeIDs {
					nodeIds = append(nodeIds, nidStr)
				}
			}
			
			n.nodeIDs = nodeIds
			n.nextMsgID++
			
			val := m.NewStateValue(body.NodeID, body.Val.(int), body.Topology)
			
			if n.localState == nil {
				n.localState = &LocalState{
					values: make(map[int]struct {
						value  int
						sender string
					}),
				}
			}
			
			if len(n.nodeIDs) > 0 {
				for _, nid := range n.nodeIDs {
					nodeID, _ := strconv.Atoi(nid)
					if nodeID != 0 {
						n.localState.values[val.MsgId] = struct {
							value  int
							sender string
						}{val.Val.(int), fmt.Sprintf("n%d", nodeID)}
					}
				}
			}
			
			val.Ok(m.Message{Src: msg.Src, Dest: n.id})
			
			n.handlers["init"] = func(msg m.Message) error {
				return nil
			}
			val.Ok(n.Reply(val, m.Message{Type: "init_done", Body: json.Marshal(n.localState.values)}))

		case "broadcast":
			n.mu.Lock()
			broadcastBody := m.BroadcastMessageBody{NodeID: msg.Src, NodeIds: []string{msg.Src}}
			
			for _, nidStr := range broadcastBody.NodeIds {
				if nid, err := strconv.Atoi(nidStr); err == nil && nid != 0 {
					if sender, ok := n.localState.values[val.MsgId]; !ok || sender.value != msg.Val.(int) {
						sender = fmt.Sprintf("n%d", nid)
						n.localState.values[val.MsgId] = struct {
							value  int
							sender string
						}{msg.Val.(int), sender}
					}
				}
			}
			
			msg.Ok(m.Message{Type: "broadcast_ok"})
			
			for nid := range n.nodeIDs {
				nodeID, _ := strconv.Atoi(nid)
				
				if n.localState.values[val.MsgId].sender == "" && nodeID != 0 && nid != 0 {
					sender := fmt.Sprintf("n%d", nodeID)
					n.localState.values[val.MsgId] = struct {
						value  int
						sender string
					}{msg.Val.(int), sender}
				}
			}
			
			val.Ok(n.Reply(val, m.Message{Type: "broadcast_ok"}))

		case "read":
			n.mu.Lock()
			readBody := m.ReadMessageBody{NodeID: msg.Src, NodeIds: []string{msg.Src}}
			
			for _, nidStr := range readBody.NodeIds {
				if nid, err := strconv.Atoi(nidStr); err == nil && nid != 0 {
					if sender, ok := n.localState.values[val.MsgId]; !ok || sender.value != msg.Val.(int) {
						sender = fmt.Sprintf("n%d", nid)
						n.localState.values[val.MsgId] = struct {
							value  int
							sender string
						}{msg.Val.(int), sender}
					}
				}
			}
			
			val.Ok(n.Reply(val, m.Message{Type: "read_ok"}))

		case "topology":
			n.mu.Lock()
			topologyBody := m.TopologyMessageBody{NodeID: msg.Src, NodeIds: []string{msg.Src}}
			
			if n.localState == nil {
				n.localState = &LocalState{}
			}
			
			for _, nidStr := range topologyBody.NodeIds {
				if nid, err := strconv.Atoi(nidStr); err == nil && nid != 0 {
					if sender, ok := n.localState.values[val.MsgId]; !ok || sender.value != msg.Val.(int) {
						sender = fmt.Sprintf("n%d", nid)
						n.localState.values[val.MsgId] = struct {
							value  int
							sender string
						}{msg.Val.(int), sender}
					}
				}
			}
			
			val.Ok(n.Reply(val, m.Message{Type: "topology_ok"}))

		default:
			break
		}
		
		n.mu.Unlock()
	}

	return nil
}

func main() {
	node := NewNode()
	
	// Register handlers for client messages
	node.Handle("broadcast", func(msg m.Message) error {
		body := msg.Body.(*m.BroadcastBody)
		fmt.Printf("Broadcast: %+v\n", body)
		n.Reply(msg, m.Message{Type: "broadcast_ok"})
		return nil
	})
	
	node.Handle("read", func(msg m.Message) error {
		body := msg.Body.(*m.ReadBody)
		fmt.Printf("Read: %+v\n", body)
		vals := n.getLocalValues(n.id)
		
		var messages []int
		for _, v := range vals {
			messages = append(messages, v.Value)
		}
		
		n.Reply(msg, m.Message{
			Type: "read_ok",
			Body: json.Marshal(map[string]interface{}{"messages": messages}),
		})
		return nil
	})
	
	node.Handle("topology", func(msg m.Message) error {
		body := msg.Body.(*m.TopologyBody)
		fmt.Printf("Topology: %+v\n", body)
		vals := n.getLocalValues(n.id)
		
		var messages []int
		for _, v := range vals {
			messages = append(messages, v.Value)
		}
		
		n.Reply(msg, m.Message{
			Type: "topology_ok",
			Body: json.Marshal(map[string]interface{}{"messages": messages}),
		})
		return nil
	})
	
	node.Handle("broadcast_ok", func(msg m.Message) error {
		body := msg.Body.(*m.BroadcastOkBody)
		fmt.Printf("Broadcast OK: %+v\n", body)
		n.Reply(msg, m.Message{Type: "broadcast_ok"})
		return nil
	})
	
	node.Handle("read_ok", func(msg m.Message) error {
		body := msg.Body.(*m.ReadOkBody)
		fmt.Printf("Read OK: %+v\n", body)
		n.Reply(msg, m.Message{Type: "read_ok"})
		return nil
	})
	
	node.Handle("topology_ok", func(msg m.Message) error {
		body := msg.Body.(*m.TopologyOkBody)
		fmt.Printf("Topology OK: %+v\n", body)
		n.Reply(msg, m.Message{Type: "topology_ok"})
		return nil
	})
	
	node.Run()

	return
}
