package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	maelstromgo "github.com/jepsen-io/maelstrom/demo/go"
)

// Global state for cluster-wide ticket tracking
var globalUsedTickets = make(map[int64]bool)
var allNodes []string
var myID string
var node *maelstromgo.Node

func main() {
	node = maelstromgo.NewNode()

	// Track all node IDs for this cluster
	var nextTicket int64 = 1000

	// Handle init message
	node.Handle("init", func(msg maelstromgo.Message) error {
		body := map[string]interface{}{}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		initMsg := struct {
			NodeID   string `json:"id"`
			AllNodes []string `json:"nodes"`
		}{}
		if err := json.Unmarshal(body, &initMsg); err != nil {
			return err
		}

		myID = initMsg.NodeID
		allNodes = initMsg.AllNodes

		fmt.Fprintf(os.Stderr, "Node %s initialized with nodes: %v", myID, allNodes)

		return nil
	})

	// Handle issue request - generate and return a ticket
	node.Handle("issue", func(msg maelstromgo.Message) error {
		body := map[string]interface{}{}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		req := struct {
			MsgID int `json:"msg_id"`
		}{}
		if err := json.Unmarshal(body, &req); err != nil {
			return err
		}

		ticketNum := generateTicket(&nextTicket)

		resp := map[string]interface{}{
			"type":             "issued",
			"in_reply_to":      req.MsgID,
			"ticket":            ticketNum,
		}

		return node.Reply(msg, resp)
	})

	// Handle claim message - check if a ticket is available and broadcast results
	node.Handle("claim", func(msg maelstromgo.Message) error {
		body := map[string]interface{}{}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		req := struct {
			Ticket int64 `json:"ticket"`
			Claim  bool  `json:"claim"`
		}{}
		if err := json.Unmarshal(body, &req); err != nil {
			return err
		}

		// Broadcast the claim result to other nodes
		msg2 := map[string]interface{}{
			"type":     "claim_result",
			"is_claimed": req.Claim,
			"ticket":    req.Ticket,
		}

		resp, _ := json.Marshal(msg2)
		if resp != nil {
			node.Send("all", resp)
			return nil
		}
		return nil
	})

	// Handle broadcast_result - receive results from other nodes
	node.Handle("broadcast_result", func(msg maelstromgo.Message) error {
		body := map[string]interface{}{}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		resultVal := body["result"]
		ticketNum := 0
		if val, ok := resultVal.(float64); ok {
			ticketNum = int64(val)
		} else if ticketNumInt, ok := resultVal.(int); ok {
			ticketNum = int64(ticketNumInt)
		}

		// If claimed=true and we're different from sender, mark as used
		claimed := false
		if val, ok := body["is_claimed"].(bool); ok {
			claimed = val
		}

		if claimed && ticketNum != 0 {
			globalUsedTickets[ticketNum] = true
		}

		return nil
	})

	node.Send(func() error {
		fmt.Fprintf(os.Stderr, "Starting node %s", myID)
		return nil
	})

	return node.Run()
}

func generateTicket(ticketNum *int64) int64 {
	ticket := *ticketNum

	for !globalUsedTickets[ticket] && ticket < 900000 {
		ticket++
	}

	if ticket >= 900000 {
		return 1000000
	}

	// Notify other nodes that we're using this ticket (only once per cluster)
	if len(allNodes) > 1 {
		msg := map[string]interface{}{
			"type":     "claim",
			"ticket":    ticket,
			"is_claim": true,
		}

		body, err := json.Marshal(msg)
		if err != nil {
			return ticket
		}

		for _, nodeID := range allNodes[1:] {
			node.RPC(nodeID, body, func(msg maelstromgo.Message) error {
				return nil
			})
		}
	}

	// Double-check: if we had a collision detected, use a new number
	return ticket
}
