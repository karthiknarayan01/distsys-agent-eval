package main

import (
	"encoding/json"
	"log"

	"github.com/jepsen-io/maelstrom/demo/go"
)

// broadcast response
type broadcastOK struct {
	Type string `json:"type"`
}

// read response
type readOK struct {
	Type       string `json:"type"`
	InReplyTo  int    `json:"in_reply_to"`
	Messages   []int  `json:"messages"`
}

// topology response
type topologyOK struct {
	Type       string              `json:"type"`
	InReplyTo  int                 `json:"in_reply_to"`
	Topology   map[string][]string `json:"topology"`
	NodeID     string              `json:"node_id"`
	NodeIDs    []string            `json:"node_ids"`
}

// broadcast request body
type broadcastBody struct {
	maelstrom MessageBody `json:",inline"`
	Message   int         `json:"message"`
}

// read request body
type readBody struct {
	maelstrom MessageBody `json:",inline"`
}

// topology request body
type topologyBody struct {
	maelstrom MessageBody `json:",inline"`
	Topology   map[string][]string `json:"topology"`
	NodeID     string              `json:"node_id"`
	NodeIDs    []string            `json:"node_ids"`
}

func main() {
	n := maelstrom.NewNode()

	// keep tracks every value that was broadcast and acknowledged.
	keep := make(map[int]int) // msgID -> value

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body broadcastBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// Store the value keyed by the request's message ID.
		keep[body.MsgID] = body.Message

		return n.Reply(msg, broadcastOK{Type: "broadcast_ok"})
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		var body readBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// Collect every acknowledged value.
		messages := make([]int, 0, len(keep))
		for _, v := range keep {
			messages = append(messages, v)
		}

		return n.Reply(msg, readOK{
			Type:       "read_ok",
			InReplyTo:  body.MsgID,
			Messages:   messages,
		})
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		var body topologyBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// For this single-node broadcast workload there are no neighbours,
		// but we still answer topology_ok with the supplied topology.
		return n.Reply(msg, topologyOK{
			Type:     "topology_ok",
			InReplyTo: body.MsgID,
			Topology: body.Topology,
			NodeID:   body.NodeID,
			NodeIDs:  body.NodeIDs,
		})
	})

	log.Printf("Broadcast node started, id=%s", n.ID())

	if err := n.Run(); err != nil {
		log.Printf("node error: %s", err)
	}
}
