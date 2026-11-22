package main

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	node := maelstrom.NewNode()

	// Map to store broadcast values per node
	// Key: node ID, Value: map of msg_id -> message value
	broadcastValues := make(map[string]map[int]int64)

	mu := &sync.Mutex{}

	// Handle init message
	node.Handle("init", func(msg maelstrom.Message) error {
		initBody := &maelstrom.InitMessageBody{}
		if err := json.Unmarshal(msg.Body, initBody); err != nil {
			return fmt.Errorf("unmarshal init: %w", err)
		}
		node.Init(initBody.NodeID, initBody.NodeIDs)

		// Initialize storage for this node
		broadcastValues[node.ID()] = make(map[int]int64)

		var replyBody map[string]interface{}
		if b, err := json.Marshal(map[string]any{"node_id": node.ID(), "node_ids": node.NodeIDs()}); err != nil {
			return fmt.Errorf("marshal init_ok: %w", err)
		} else if err := json.Unmarshal(b, &replyBody); err != nil {
			return fmt.Errorf("unmarshal reply body: %w", err)
		}
		replyBody["type"] = "init_ok"
		return node.Reply(msg, replyBody)
	})

	// Handle broadcast message
	node.Handle("broadcast", func(msg maelstrom.Message) error {
		var body map[string]interface{}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return fmt.Errorf("unmarshal broadcast: %w", err)
		}

		msgIDs, _ := msg.GetMsgIDs()
		for _, reqID := range msgIDs {
			if value, ok := body["message"].(float64); ok {
				mu.Lock()
				nodeID := node.ID()
				if broadcastValues[nodeID] == nil {
					broadcastValues[nodeID] = make(map[int]int64)
				}
				broadcastValues[nodeID][reqID] = int64(value)
				mu.Unlock()
			}

			var replyBody map[string]interface{}
			if b, err := json.Marshal(map[string]any{"message": value}); err != nil {
				return fmt.Errorf("marshal broadcast_ok: %w", err)
			} else if err := json.Unmarshal(b, &replyBody); err != nil {
				return fmt.Errorf("unmarshal reply body: %w", err)
			}
			replyBody["type"] = "broadcast_ok"
			replyBody["in_reply_to"] = reqID
			if err := node.Reply(msg, replyBody); err != nil {
				return fmt.Errorf("reply broadcast: %w", err)
			}
		}
		return nil
	})

	// Handle read message
	node.Handle("read", func(msg maelstrom.Message) error {
		var body map[string]interface{}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return fmt.Errorf("unmarshal read: %w", err)
		}

		msgIDs, _ := msg.GetMsgIDs()
		for _, reqID := range msgIDs {
			mu.Lock()
			nodeID := node.ID()
			values := broadcastValues[nodeID]
			var messages []int64
			for _, v := range values {
				messages = append(messages, v)
			}
			mu.Unlock()

			var replyBody map[string]interface{}
			if b, err := json.Marshal(map[string]any{"messages": messages}); err != nil {
				return fmt.Errorf("marshal read_ok: %w", err)
			} else if err := json.Unmarshal(b, &replyBody); err != nil {
				return fmt.Errorf("unmarshal reply body: %w", err)
			}
			replyBody["type"] = "read_ok"
			replyBody["in_reply_to"] = reqID
			if err := node.Reply(msg, replyBody); err != nil {
				return fmt.Errorf("reply read: %w", err)
			}
		}
		return nil
	})

	// Handle topology message
	node.Handle("topology", func(msg maelstrom.Message) error {
		var body map[string]interface{}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return fmt.Errorf("unmarshal topology: %w", err)
		}

		msgIDs, _ := msg.GetMsgIDs()
		for _, reqID := range msgIDs {
			var replyBody map[string]interface{}
			if b, err := json.Marshal(map[string]any{"topology": body["topology"]}); err != nil {
				return fmt.Errorf("marshal topology_ok: %w", err)
			} else if err := json.Unmarshal(b, &replyBody); err != nil {
				return fmt.Errorf("unmarshal reply body: %w", err)
			}
			replyBody["type"] = "topology_ok"
			replyBody["in_reply_to"] = reqID
			if err := node.Reply(msg, replyBody); err != nil {
				return fmt.Errorf("reply topology: %w", err)
			}
		}
		return nil
	})

	node.Run()
}
