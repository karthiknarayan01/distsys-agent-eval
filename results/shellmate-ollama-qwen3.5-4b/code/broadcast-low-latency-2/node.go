package main

import (
	"encoding/json"
	"log"
	"os"

	"github.com/jepsen-io/maelstrom/demo/go"
)

type Value struct {
	NodeID int64 `json:"node"`
	Value  int64 `json:"value"`
}

// Global topology: each node ID -> list of neighbor IDs (as strings for maelstrom)
var globalTopology = make(map[int][]string)
var topoMu sync.RWMutex

// Per-node state
type NodeState struct {
	mut    sync.Mutex
	values map[int]int64 // msg_id -> value (values this node has locally)
}

func newNodeState() *NodeState {
	return &NodeState{
		values: make(map[int]int64),
	}
}

func main() {
	n := maelstrom.NewNode()
	defer n.Close()

	// Handle init message - register handlers before Run is called
	m, ok := interface{}(maelstrom.GetInitMessage()).(map[string]interface{})
	if !ok {
		log.Fatalf("Failed to parse init message")
	}

	nodeIDStr := m["node_id"].(string)
	idInt, _ := strconv.ParseInt(nodeIDStr, 10, 64)

	// Parse global topology from init message
	for nidStr, neighbors := range m {
		if nidStr == "node_id" {
			continue
		}
		var nIDs []string
		if neighborSlice, ok := neighbors.([]string); ok {
			nIDs = neighborSlice
		} else if neighborSlice, ok := neighbors.([]interface{}); ok {
			for _, s := range neighborSlice {
				nIDs = append(nIDs, s.(string))
			}
		}
		globalTopology[int(nidStr)] = nIDs
	}

	var req MessageRequest
	json.Unmarshal(maelstrom.Incoming(), &req)

	switch req.Type {
	case "topology":
		topoJSON := req.Topology
		for nidStr, neighbors := range topoJSON {
			if _, ok := neighbors["self"]; ok {
				continue
			}
			var nIDs []string
			for nid2Str := range neighbors {
				nIDInt, _ := strconv.ParseInt(nid2Str, 10, 64)
				globalTopology[int(nidInt)] = append(globalTopology[int(nidInt)], nid2Str)
			}
		}
		resp := map[string]interface{}{"in_reply_to": req.MgID}

	case "broadcast":
		value := req.Message
		msgID := req.MsgID
		resp := map[string]interface{}{"in_reply_to": msgID}
		
		mut.Lock()
		localValues[msgID] = value
		
		topologyMu.RLock()
		neighbors := globalTopology[int(idInt)]
		topologyMu.RUnlock()
		
		for _, nIDStr := range neighbors {
			valMap := localValues[nIDStr]
			if valMap == nil || valMap[msgID] == 0 {
				var fwd Value
				fwd.NodeID = int64(idInt)
				fwd.Value = value
				maelstrom.Send(nIDStr, &fwd)
			}
		}

	case "read":
		resp := map[string]interface{}{
			"in_reply_to": msgID,
		}

	case "broadcast_ok":
		inReplyTo := req.InReplyTo
		resp := map[string]interface{}{"in_reply_to": inReplyTo}

	default:
		return
	}

	jsonResp, _ := json.Marshal(resp)
	maelstrom.Send(n.ID(), jsonResp)
}
