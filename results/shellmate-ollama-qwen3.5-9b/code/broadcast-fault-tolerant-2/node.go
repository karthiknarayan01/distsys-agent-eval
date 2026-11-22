package main

import (
	"encoding/json"
	"log"

	"github.com/jepsen-io/maelstrom/demo/go"
)

func initHandler(msg go.Message) error {
	var body go.InitMessageBody
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	myID = body.NodeID
	log.Printf("Init: node %s", myID)
	return nil
}

type topologyMsg struct {
	Type      string                     `json:"type"`
	MsgID     int                        `json:"msg_id"`
	Topology   map[string][]string        `json:"topology"`
}

func topologyHandler(msg go.Message) error {
	var body topologyMsg
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		log.Printf("unmarshal topology: %v", err)
		return nil
	}

	// Store topology info for later use
	for dest, peers := range body.Topology {
		for _, peer := range peers {
			if _, ok := knownPeers[dest]; !ok {
				knownPeers[dest] = make(map[string]bool)
			}
			knownPeers[dest][peer] = true
		}
	}

	// Send all our values to every peer in the topology
	for dest, peers := range body.Topology {
		for _, peer := range peers {
			if myID == peer {
				continue
			}
			go sendBroadcastToPeer(peer, msg)
		}
	}

	return nil
}

type broadcastMsg struct {
	Type     string `json:"type"`
	MsgID    int    `json:"msg_id"`
	Message  int    `json:"message"`
}

func broadcastHandler(msg go.Message) error {
	var body broadcastMsg
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		log.Printf("unmarshal broadcast: %v", err)
		return nil
	}

	// Store the value
	values[msg.MsgID()] = body.Message

	// Send to all known peers (including those not yet connected via topology)
	for dest, peers := range knownPeers {
		if _, isKnown := peers[dest]; !isKnown {
			continue
		}
		go sendBroadcastToPeer(dest, msg)
	}

	return nil
}

type readMsg struct {
	Type     string `json:"type"`
	MsgID    int    `json:"msg_id"`
	InReplyTo int    `json:"in_reply_to,omitempty"`
}

func readHandler(msg go.Message) error {
	var body readMsg
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		log.Printf("unmarshal read: %v", err)
		return nil
	}

	log.Printf("read requested with msg_id=%d", body.MsgID)

	// Collect all stored values
	vals := make([]int, 0, len(values))
	for _, v := range values {
		vals = append(vals, v)
	}

	replyBody := map[string]interface{}{
		"messages": vals,
	}

	return msg.Reply(replyBody)
}

func sendBroadcastToPeer(dest string, msg go.Message) {
	valueMsg := struct {
		Type     string `json:"type"`
		MsgID    int    `json:"msg_id"`
		Message  int    `json:"message"`
	}{
		Type:     "broadcast",
		MsgID:    msg.MsgID(),
		Message: values[msg.MsgID()],
	}

	err := msg.Send(dest, valueMsg)
	if err != nil {
		log.Printf("send broadcast to %s: %v", dest, err)
	}
}

var myID string
var knownPeers map[string]map[string]bool = make(map[string]map[string]bool)
var values map[int]int = make(map[int]int)

func main() {
	node := go.NewNode()

	node.Init(initHandler)
	node.Handle("topology", topologyHandler)
	node.Handle("broadcast", broadcastHandler)
	node.Handle("read", readHandler)

	if err := node.Run(); err != nil {
		log.Fatalf("Run: %v", err)
	}
}
