package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"sync"
	"time"
)

// NodeID is the unique identifier for this node
type NodeID string

// ValueSet tracks values broadcast to this node
type ValueSet struct {
	mu          sync.Mutex
	values      map[int]int // msg_id -> value
	broadcasts  []int       // msg_ids that have been acknowledged
}

// BroadcastMsg represents a broadcast request
type BroadcastMsg struct {
	MsgID     int      `json:"msg_id"`
	Message   int      `json:"message"`
}

// BroadcastOKMsg represents an acknowledgment
type BroadcastOKMsg struct {
	InReplyTo int `json:"in_reply_to"`
}

// ReadMsg represents a read request
type ReadMsg struct {
	MsgID int `json:"msg_id"`
}

// TopologyMsg represents a topology suggestion
type TopologyMsg struct {
	MsgID      int       `json:"msg_id"`
	Topology   map[string][]string `json:"topology"`
}

// NodeConfig holds node configuration from init message
type NodeConfig struct {
	ID        string  `json:"id"`
	IDs       []string `json:"ids"`
	Names     map[string]string `json:"names"`
	Topo      *map[string][]string `json:"topology,omitempty"`
}

var (
	selfNode    *ValueSet
	selfID       NodeID
	selfConfig   *NodeConfig
	nodeConfigs  map[NodeID]*ValueSet = make(map[NodeID]*ValueSet)
	valueSetsMu  sync.RWMutex
)

// getOrCreateValueSet returns or creates a ValueSet for this node
func getOrCreateValueSet(id NodeID) (*ValueSet, bool) {
	vs, ok := nodeConfigs[id]
	if !ok {
		vs = &ValueSet{values: make(map[int]int), broadcasts: make([]int, 0)}
		nodeConfigs[id] = vs
	}
	return vs, ok
}

// broadcastFlood periodically sends values to other nodes with retries
func broadcastFlood(vs *ValueSet, id NodeID) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			vs.mu.Lock()
			if len(vs.broadcasts) == 0 {
				vs.mu.Unlock()
				continue
			}
			var toBroadcast []int
			ackCount := 0
			remaining := make([]int, 0)
			for _, msgID := range vs.broadcasts {
				if vs.values[msgID] != -1 && !vs.hasAck(msgID) {
					toBroadcast = append(toBroadcast, msgID)
					continue
				} else if vs.values[msgID] == -1 {
					vs.broadcasts = append(vs.broadcasts, msgID)
					continue
				} else {
					ackCount++
				}
				// Still check if we need to broadcast (in case ack came later)
				if !vs.hasAck(msgID) && vs.values[msgID] == -1 {
					toBroadcast = append(toBroadcast, msgID)
					continue
				}
			}
			
			vs.mu.Unlock()

			if len(toBroadcast) > 0 {
				for _, msgID := range toBroadcast {
					time.Sleep(25 * time.Millisecond)
					vs.mu.RLock()
					val, hasVal := vs.values[msgID]
					vs.mu.RUnlock()
					if !hasVal {
						continue
					}
					
					for _, targetID := range selfConfig.Names {
						sendInternal(id, targetID, msgID, val)
					}
				}
			}
		default:
			vs.mu.Lock()
			if len(vs.broadcasts) == 0 {
				vs.mu.Unlock()
				return
			}
			
			var toBroadcast []int
			for _, msgID := range vs.broadcasts {
				val, exists := vs.values[msgID]
				hasAck := vs.hasAck(msgID)
				
				if exists && val != -1 && !hasAck {
					toBroadcast = append(toBroadcast, msgID)
				} else if !exists && msgID < 0 {
					continue
				}
			}
			
			vs.mu.Unlock()

			if len(toBroadcast) == 0 {
				break
			}

			for _, msgID := range toBroadcast {
				time.Sleep(25 * time.Millisecond)
				vs.mu.RLock()
				val, exists := vs.values[msgID]
				vs.mu.RUnlock()
				if !exists || val == -1 {
					continue
				}

				for _, targetID := range selfConfig.Names {
					sendInternal(id, targetID, msgID, val)
				}
			}
		}
	}
}

// sendInternal sends a message to another node via RPC channel
func sendInternal(id, targetID NodeID, msgID int, val int) {
	if id == targetID || len(selfConfig.Names) == 0 {
		return
	}
	sendTarget := fmt.Sprintf("node%s", string(targetID))
	msg := BroadcastMsg{MsgID: msgID, Message: val}
	rpcInternal(id, targetID, msg, &BroadcastOKMsg{})
}

// rpcInternal sends an internal RPC to another node
func rpcInternal(src, dest NodeID, in interface{}, out interface{}) {
	srcMsg := BroadcastOKMsg{}
	msgJSON, _ := json.Marshal(in)
	
	rpcCall := struct{ Src, Dest, Type, Msg []byte }{
		Src: fmt.Sprintf("node%s", string(src)),
		Dest: dest.String(),
		Type: "internal",
		Msg: msgJSON,
	}
	jsonRPC(rpcCall, &srcMsg)
	
	if len(srcMsg.InReplyTo) > 0 {
		handleOK(in.(*BroadcastOKMsg))
	}
}

// jsonRPC calls an RPC channel on another node
func jsonRPC(msg struct{ Src, Dest, Type string, Msg []byte }, out interface{}) {
	rpcCall := map[string]interface{}{"type": "internal", "msg_id": 0, "src": msg.Src, "dest": msg.Dest}
	outJSON, _ := json.Marshal(map[string]interface{}{"type": "rpc"})
	msg["src"] = string(outJSON)
	msg["dest"] = msg.Dest
	
	rpcCall2 := map[string]interface{}{
		"type": "internal",
		"msg_id": 0,
		"src": msg.Src,
		"dest": msg.Dest,
		"type": "rpc_call",
		"body": msg.Msg,
	}

	rpcCall2["msg"] = map[string]interface{}{
		"type":    "internal",
		"src":     msg.Src,
		"dest":    msg.Dest,
		"type":    "rpc_call",
		"body":    msg.Msg,
	}

	rpcCall3 := rpcCall2["msg"].(map[string]interface{})
	
	rpcCall3["type"] = "internal"
	rpcCall3["src"] = string(msg.Src)
	rpcCall3["dest"] = string(msg.Dest)
	
	jsonRPC(rpcCall3, out)
}

// sendBroadcast sends a broadcast message to all nodes
func sendBroadcast(id, targetID NodeID, msgID int, val int) {
	target := fmt.Sprintf("node%s", string(targetID))
	msgJSON, _ := json.Marshal(BroadcastMsg{MsgID: msgID, Message: val})
	
	type Call struct { Src, Dest, Msg []byte }
	call := Call{Src: target, Dest: "", Msg: msgJSON}

	rpcCall1 := map[string]interface{}{"type": "internal", "msg_id": 0, "src": call.Src, "dest": target, "type": "broadcast", "body": json.RawMessage(`{"type":"internal","msg_id":0}`)}
	rpcCall1["msg"] = map[string]interface{}{"type": "internal", "src": call.Src, "dest": target, "type": "broadcast", "body": msgJSON}

	type RPCMsg struct{ Src, Dest string, Body interface{} }
	msg := RPCMsg{Src: fmt.Sprintf("node%s", string(id)), Dest: target, Body: rpcCall1["msg"]}

	// Just send through the internal network
	jsonRPC(msg, nil)
}

// jsonRPCInternal sends an RPC call to another node via stdin/stdout simulation
func jsonRPCInternal(srcID, destID NodeID, in interface{}, out interface{}) {
	srcJSON, _ := json.Marshal(map[string]interface{}{"type": "rpc", "src": fmt.Sprintf("node%s", string(srcID)), "dest": fmt.Sprintf("node%s", string(destID))})

	msgJSON, _ := json.Marshal(in)

	rpcCallJSON := map[string]interface{}{
		"type":      "internal",
		"msg_id":    0,
		"src":       fmt.Sprintf("node%s", string(srcID)),
		"dest":      fmt.Sprintf("node%s", string(destID)),
		"type":      "rpc_call",
		"body":      msgJSON,
	}

	rpcCall2JSON := map[string]interface{}{"type": "internal", "msg_id": 0, "src": rpcCallJSON["src"], "dest": rpcCallJSON["dest"]}
	outJSON, _ := json.Marshal(rpcCall2JSON)
	
	type RPCMsg struct{ Type, Msg []byte }
	rpcCallJSON2 := RPCMsg{Type: "internal", Msg: outJSON}

	jsonRPCInternal(rpcCallJSON, &rpcCallJSON2)
}

func sendBroadcastToNode(id NodeID, targetID NodeID, msgID int, val int) {
	target := fmt.Sprintf("node%s", string(targetID))
	msgJSON, _ := json.Marshal(map[string]interface{}{
		"type": "internal",
		"msg_id": 0,
		"src":   id,
		"dest":  target,
		"type":  "rpc_call",
		"body":  map[string]interface{}{"type": "internal", "msg_id": msgID},
	})

	rpcCallJSON := map[string]interface{}{
		"type":      "internal",
		"msg_id":    0,
		"src":       fmt.Sprintf("node%s", string(id)),
		"dest":      target,
		"type":      "rpc_call",
		"body":      msgJSON,
	}

	rpcCallJSON["type"] = "internal"
	jsonRPCInternal(id, NodeID(target), rpcCallJSON, nil)
}

func sendBroadcastOK(id NodeID, in *BroadcastOKMsg) {
	msg := BroadcastOKMsg{InReplyTo: in.InReplyTo}
	jsonRPCInternal(id, selfID, &msg, nil)
}

func handleBroadcast(in interface{}, vs *ValueSet) error {
	bm := in.(*BroadcastMsg)
	vs.mu.Lock()
	vs.values[bm.MsgID] = bm.Message
	vs.broadcasts = append(vs.broadcasts, bm.MsgID)
	vs.hasAck(bm.MsgID) // Auto-acknowledge initial broadcast
	vs.mu.Unlock()

	return nil
}

func handleBroadcastOK(in interface{}, vs *ValueSet) {
	bok := in.(*BroadcastOKMsg)
	vs.mu.Lock()
	// Already acked initially
	vs.mu.Unlock()
}

func handleRead(in interface{}) error {
	rm := in.(*ReadMsg)
	vs, ok := getOrCreateValueSet(selfID)
	if !ok {
		return fmt.Errorf("node not found: %d", rm.MsgID)
	}

	vs.mu.Lock()
	values := make([]int, 0, len(vs.values))
	for msgID := range vs.values {
		if vs.values[msgID] != -1 {
			values = append(values, vs.values[msgID])
		}
	}
	vs.mu.Unlock()

	sort.Ints(values)
	msgJSON, _ := json.Marshal(map[string]interface{}{
		"type": "read_ok",
		"in_reply_to": rm.MsgID,
		"messages":  values,
	})

	jsonRPCInternal(selfID, selfID, map[string]interface{}{"type": "internal", "msg_id": rm.MsgID}, &map[string]interface{}{})
	return nil
}

func handleTopology(in interface{}) {
	tm := in.(*TopologyMsg)
	if tm.Topology != nil {
		config := tm.Config
		selfConfig = config
		nodeConfigs = make(map[NodeID]*ValueSet)
		for _, idStr := range selfConfig.Ids {
			id := NodeID(idStr)
			vs, _ := getOrCreateValueSet(id)
			nodeConfigs[id] = vs
		}
	}
	fmt.Fprintf(io.Discard, "topology_ack")
}

func runLoop() {
	for line := io.ReadLinesStdin(); true; {
		lineStr, ok := <-line
		if !ok {
			break
		}

		var msg interface{}
		json.Unmarshal([]byte(lineStr), &msg)

		switch valMsg := msg.(type) {
		case BroadcastOKMsg:
			handleBroadcastOK(&valMsg, selfValueSet)
			sendBroadcastOK(selfID, NodeID(string(selfID)), valMsg.InReplyTo, -1)
		case BroadcastMsg:
			vs, ok := getOrCreateValueSet(selfID)
			if !ok {
				continue
			}
			handleBroadcast(&valMsg, vs)
			sendBroadcastToNode(selfID, NodeID(string(selfID)), valMsg.MsgID, valMsg.Message)
		case ReadMsg:
			vs, ok := getOrCreateValueSet(selfID)
			if !ok {
				continue
			}
			handleRead(&valMsg)
		case TopologyMsg:
			handleTopology(&valMsg)
		}
	}

	for line := io.ReadLinesStdin(); true; {
		lineStr, ok := <-line
		if !ok {
			break
		}

		var msg interface{}
		json.Unmarshal([]byte(lineStr), &msg)

		switch valMsg := msg.(type) {
		case BroadcastOKMsg:
			handleBroadcastOK(&valMsg, selfValueSet)
			sendBroadcastOK(selfID, NodeID(string(selfID)), valMsg.InReplyTo, -1)
		case BroadcastMsg:
			vs, ok := getOrCreateValueSet(selfID)
			if !ok {
				continue
			}
			handleBroadcast(&valMsg, vs)
			sendBroadcastToNode(selfID, NodeID(string(selfID)), valMsg.MsgID, valMsg.Message)
		case ReadMsg:
			vs, ok := getOrCreateValueSet(selfID)
			if !ok {
				continue
			}
			handleRead(&valMsg)
		case TopologyMsg:
			handleTopology(&valMsg)
		}
	}

	for line := io.ReadLinesStdin(); true; {
		lineStr, ok := <-line
		if !ok {
			break
		}

		var msg interface{}
		json.Unmarshal([]byte(lineStr), &msg)

		switch valMsg := msg.(type) {
		case BroadcastOKMsg:
			handleBroadcastOK(&valMsg, selfValueSet)
			sendBroadcastOK(selfID, NodeID(string(selfID)), valMsg.InReplyTo, -1)
		case BroadcastMsg:
			vs, ok := getOrCreateValueSet(selfID)
			if !ok {
				continue
			}
			handleBroadcast(&valMsg, vs)
			sendBroadcastToNode(selfID, NodeID(string(selfID)), valMsg.MsgID, valMsg.Message)
		case ReadMsg:
			vs, ok := getOrCreateValueSet(selfID)
			if !ok {
				continue
			}
			handleRead(&valMsg)
		case TopologyMsg:
			handleTopology(&valMsg)
		}
	}

	fmt.Fprintf(io.Discard, "loop_done")
}

func main() {
	for line := io.ReadLinesStdin(); true; {
		lineStr, ok := <-line
		if !ok {
			break
		}

		var cfg NodeConfig
		json.Unmarshal([]byte(lineStr), &cfg)
		selfID = NodeID(cfg.ID)
		selfConfigs = map[string]NodeConfig{"": cfg}
		// Create ValueSet for this node
		vs, _ := getOrCreateValueSet(selfID)
		selfValueSet = vs

		var topology map[string][]string
		json.Unmarshal([]byte(lineStr), &topology)
		if len(topo) > 0 {
			for _, ids := range topo {
				for _, id := range ids {
					fmt.Fprintf(io.Discard, "node_id:" + string(id))
					var vs2 *ValueSet
					vs2, _ = getOrCreateValueSet(NodeID(string(id)))
					nodeConfigs[string(id)] = vs2
				}
			}
		}

		for _, ids := range topo {
			fmt.Fprintf(io.Discard, "topology_done")
			break
		}
	}

	runLoop()
}
