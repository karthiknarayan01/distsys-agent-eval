package main

import (
	"encoding/json"
	"log"
	"sync"

	"github.com/jepsen-io/maelstrom/demo/go"
)

type tellRequest struct {
	Mid  int    `json:"msg_id"`
	Rumor int   `json:"rumor"`
	Type string `json:"type"`
}

type neighborsRequest struct {
	Mid       int                   `json:"msg_id"`
	Neighbors map[string][]string   `json:"neighbors"`
	Type      string                `json:"type"`
	InReplyTo int                  `json:"in_reply_to,omitempty"`
}

type tellResponse struct {
	Type           string `json:"type"`
	InReplyTo      int    `json:"in_reply_to"`
	NeighborsNoted bool   `json:"neighbors_noted,omitempty"`
}

type recalled struct {
	Type       string   `json:"type"`
	Mid        int      `json:"msg_id"`
	Rumors     []int    `json:"rumors"`
	Messages   []int    `json:"messages,omitempty"`
	InReplyTo  int      `json:"in_reply_to"`
}

type neighborsNoted struct {
	Type         string `json:"type"`
	InReplyTo    int    `json:"in_reply_to"`
	NeighborsNoted bool  `json:"neighbors_noted,omitempty"`
}

// Node keeps track of known rumors and neighbor list
type nodeState struct {
	mu            sync.RWMutex
	knownRumors   map[int]bool
	neighborList  []string // ordered list of neighbors we should send to
}

func NewNode(id string) *go.Node {
	state := &nodeState{
		knownRumors: make(map[int]bool),
		neighborList: make([]string, 0),
	}

	node := go.NewNode(id)

	// Handle tell messages from clients (broadcast type)
	node.Handle("broadcast", func(ctx *go.Context, msg *go.Message) {
		body := msg.Body.RawBody()
		
		var tellReq tellRequest
		if err := json.Unmarshal(body, &tellReq); err != nil || tellReq.Type != "tell" {
			handleOther(ctx, body, state, log.Fatal("unhandled broadcast message"))
		}

		state.mu.Lock()
		state.knownRumors[tellReq.Rumor] = true
		state.mu.Unlock()

		// Reply to client with told
		toldResp := tellResponse{Type: "told", InReplyTo: tellReq.Mid}
		respBody, _ := json.Marshal(toldResp)
		ctx.Write(string(respBody))

		// Send to our neighbors (using a unique msg_id for internal propagation)
		for _, neighborID := range state.neighborList {
			if msg.Src != neighborID { // don't send back to sender
				propMessage := tellRequest{Type: "tell", Rumor: tellReq.Rumor, Mid: tellReq.Mid + 1}
				body, _ := json.Marshal(propMessage)
				ctx.Send(neighborID, string(body))
			}
		}
	})

	node.Handle("read", func(ctx *go.Context, msg *go.Message) {
		body := msg.Body.RawBody()

		var recallReq struct {
			Type      string `json:"type"`
			Mid       int    `json:"msg_id"`
			Rumors    []int  `json:"rumors,omitempty"`
			Messages  []int  `json:"messages,omitempty"`
			InReplyTo int    `json:"in_reply_to"`
		}

		if err := json.Unmarshal(body, &recallReq); err != nil || recallReq.Type != "recall" {
			handleOther(ctx, body, state, log.Fatal("unhandled read message"))
		}

		state.mu.RLock()
		recalledList := make([]int, 0)
		for rumor := range state.knownRumors {
			recalledList = append(recalledList, rumor)
		}
		state.mu.RUnlock()

		recalledResp := recalled{Type: "recalled", Mid: recallReq.Mid, Rumors: recalledList, InReplyTo: recallReq.Mid}
		body, _ = json.Marshal(recalledResp)
		ctx.Write(string(body))
	})

	node.Handle("neighbors", func(ctx *go.Context, msg *go.Message) {
		body := msg.Body.RawBody()

		var topoReq neighborsRequest
		if err := json.Unmarshal(body, &topoReq); err != nil {
			handleOther(ctx, body, state, log.Fatal("unhandled neighbors message"))
		}

		// Store neighbor list - we should talk to all of them
		state.mu.Lock()
		state.neighborList = make([]string, 0, len(topoReq.Neighbors))
		for nid := range topoReq.Neighbors {
			state.neighborList = append(state.neighborList, nid)
		}
		state.mu.Unlock()

		log.Printf("node %s stored neighbors: %v", msg.Src, state.neighborList)

		notedResp := neighborsNoted{Type: "neighbors_noted", InReplyTo: topoReq.Mid}
		respBody, _ := json.Marshal(notedResp)
		ctx.Write(string(respBody))
	})

	node.Run()
}

func handleOther(ctx *go.Context, body []byte, state *nodeState, fatal func(...interface{})) {
	var msg struct {
		Type      string `json:"type"`
		InReplyTo int    `json:"in_reply_to,omitempty"`
		Mid       int    `json:"msg_id,omitempty"`
	}

	if err := json.Unmarshal(body, &msg); err != nil {
		fatal("failed to unmarshal message")
		return
	}

	switch msg.Type {
	case "neighbors_noted":
		type neighborsNotedResp struct {
			Type         string `json:"type"`
			InReplyTo    int    `json:"in_reply_to"`
			NeighborsNoted bool  `json:"neighbors_noted,omitempty"`
		}
		resp := neighborsNotedResp{Type: "neighbors_noted", InReplyTo: msg.InReplyTo}
		body, _ := json.Marshal(resp)
		ctx.Write(string(body))
	case "tell", "broadcast":
		type tell struct {
			Mid    int  `json:"msg_id"`
			Rumor  int  `json:"rumor"`
			Type   string `json:"type"`
		}
		var t tell
		if json.Unmarshal(body, &t) != nil || t.Type != "tell" {
			fatal("unexpected tell structure")
			return
		}

		state.mu.Lock()
		state.knownRumors[t.Rumor] = true
		state.mu.Unlock()

		type told struct {
			Type           string `json:"type"`
			InReplyTo      int    `json:"in_reply_to"`
			NeighborsNoted bool   `json:"neighbors_noted,omitempty"`
		}
		resp := told{Type: "told", InReplyTo: t.Mid}
		body, _ = json.Marshal(resp)
		ctx.Write(string(body))

		for _, neighborID := range state.neighborList {
			if msg.Src != neighborID {
				propMsg := tell{Type: "tell", Rumor: t.Rumor, Mid: t.Mid + 1}
				body, _ = json.Marshal(propMsg)
				ctx.Send(neighborID, string(body))
			}
		}

	case "recall":
		state.mu.RLock()
		recalledList := make([]int, 0)
		for rumor := range state.knownRumors {
			recalledList = append(recalledList, rumor)
		}
		state.mu.RUnlock()

		type recalledResp struct {
			Type       string   `json:"type"`
			Mid        int      `json:"msg_id"`
			Rumors     []int    `json:"rumors"`
			Messages   []int    `json:"messages,omitempty"`
			InReplyTo  int      `json:"in_reply_to"`
		}
		resp := recalledResp{Type: "recalled", Mid: msg.Mid, Rumors: recalledList, InReplyTo: msg.InReplyTo}
		body, _ = json.Marshal(resp)
		ctx.Write(string(body))

	case "told":
		type toldResp struct {
			Type           string `json:"type"`
			InReplyTo      int    `json:"in_reply_to"`
			NeighborsNoted bool   `json:"neighbors_noted,omitempty"`
		}
		resp := toldResp{Type: "told", InReplyTo: msg.InReplyTo}
		body, _ = json.Marshal(resp)
		ctx.Write(string(body))

	case "recalled":
		// Already handled in read handler
		
	case "neighbors_noted":
		type noted struct {
			Type         string `json:"type"`
			InReplyTo    int    `json:"in_reply_to"`
			NeighborsNoted bool  `json:"neighbors_noted,omitempty"`
		}
		resp := noted{Type: "topology_ok", InReplyTo: msg.InReplyTo}
		body, _ = json.Marshal(resp)
		ctx.Write(string(body))

	case "broadcast_ok":
		type resp struct {
			Type           string `json:"type"`
			InReplyTo      int    `json:"in_reply_to"`
			NeighborsNoted bool   `json:"neighbors_noted,omitempty"`
		}
		resp := resp{Type: "told", InReplyTo: msg.InReplyTo}
		body, _ = json.Marshal(resp)
		ctx.Write(string(body))

	default:
		type resp struct {
			Type           string `json:"type"`
			InReplyTo      int    `json:"in_reply_to"`
			NeighborsNoted bool   `json:"neighbors_noted,omitempty"`
		}
		resp := resp{Type: msg.Type, InReplyTo: msg.InReplyTo}
		body, _ = json.Marshal(resp)
		ctx.Write(string(body))
	}
}
