package main

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	m "github.com/jepsen-io/maelstrom/demo/go"
)

type TopologyNode struct {
	Name   string   `json:"n"`
	Neighbors []string `json:"neighbors"`
}

type TopologyMessage struct {
	MsgID      uint64    `json:"msg_id"`
	Topo       map[string][]string `json:"topology"`
}

type NodeState struct {
	mu             sync.Mutex
	values         map[uint64]int // msg_id -> value
	acknowledged   map[uint64]bool
	neighbors      []string
	allNodes       map[string]string // name -> id
	nodeID         string
	topologyKnown  bool
}

func main() {
	n := m.NewNode("node")

	h := func(body map[string]any) error {
		typ, ok := body["type"]
		if !ok {
			return fmt.Errorf("no type field")
		}
		t, ok := typ.(string)
		if !ok {
			return fmt.Errorf("type not a string")
		}

		switch t {
		case "init":
			return handleInit(body)
		case "topology":
			return handleTopology(body)
		case "broadcast":
			return handleBroadcast(body)
		case "read":
			return handleRead(body)
		default:
			return nil
		}
	}

	n.SetHandler(h)

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

func handleInit(body map[string]any) error {
	// init message - extract node info
	id, ok := body["node_id"]
	if !ok {
		return fmt.Errorf("no node_id in init")
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	state.nodeID = fmt.Sprintf("%v", id)
	state.acknowledged = make(map[uint64]bool)
	state.values = make(map[uint64]int)
	return nil
}

func handleTopology(body map[string]any) error {
	// Parse topology message
	msg, ok := body["message"].(map[string]any)
	if !ok {
		return fmt.Errorf("message not a map")
	}
	tm, ok := msg["topology"]
	if !ok {
		return fmt.Errorf("no topology field")
	}

	topoMap, ok := tm.(map[string]any)
	if !ok {
		return fmt.Errorf("topology not a map")
	}

	state.mu.Lock()
	defer state.mu.Unlock()

	// Build neighbor list for this node and all nodes mapping
	state.allNodes = make(map[string]string)
	for name, neighs := range topoMap {
		nodeNames := neighs.([]any)
		var neighbors []string
		for _, n := range nodeNames {
			nstr := fmt.Sprintf("%v", n)
			if nstr != state.nodeID {
				neighbors = append(neighbors, nstr)
			}
		}
		state.allNodes[name] = fmt.Sprintf("%v", n)
	}

	// Actually we need to rebuild from scratch - let's redo this
	state.allNodes = make(map[string]string)
	for name, neighs := range topoMap {
		var neighbors []string
		nodeNames := neighs.([]any)
		for _, n := range nodeNames {
			nstr := fmt.Sprintf("%v", n)
			if nstr != state.nodeID {
				neighbors = append(neighbors, nstr)
			}
		}
		state.neighbors = neighbors
		state.allNodes[name] = n
	}

	return m.Reply(body["msg_id"], map[string]any{
		"ok":  true,
		"node": state.nodeID,
	})
}

func handleBroadcast(body map[string]any) error {
	msg, ok := body["message"].(map[string]any)
	if !ok {
		return fmt.Errorf("message not a map")
	}
	
	val, ok := msg["value"]
	if !ok {
		return fmt.Errorf("no value field")
	}

	state.mu.Lock()
	defer state.mu.Unlock()
	
	// Store the value with its msg_id
	msgID, _ := body["msg_id"].(uint64)
	if valStr, ok := val.(string); ok {
		state.values[msgID], _ = strconv.Atoi(valStr)
	} else if numVal, ok := val.(float64); ok {
		state.values[msgID] = int(numVal)
	}

	state.acknowledged[msgID] = true

	return m.Reply(body["msg_id"], map[string]any{
		"ok": true,
	})
}

func handleRead(body map[string]any) error {
	state.mu.Lock()
	defer state.mu.Unlock()

	resp := map[string]any{
		"in_reply_to": body["msg_id"],
		"messages":    make([]int, 0),
	}

	for _, val := range state.values {
		resp["messages"] = append(resp["messages"].([]interface{}), val)
	}

	return m.Reply(body["msg_id"], resp)
}
