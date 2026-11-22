package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func marshal(v interface{}) []byte {
	data, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return data
}

func main() {
	node := maelstrom.NewNode()

	// Handle init message to get our node ID
	node.Handle("init", func(msg maelstrom.Message) error {
		var body map[string]interface{}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return fmt.Errorf("failed to unmarshal: %w", err)
		}

		nodeID := ""
		if nid, ok := body["node_id"].(string); ok {
			nodeID = nid
		} else if ids, ok := body["node_ids"].([]interface{}); ok {
			for _, v := range ids {
				if s, ok := v.(string); ok && strings.HasPrefix(s, nodeID) {
					nodeID = s
					break
				}
			}
		}

		if nodeID == "" {
			return fmt.Errorf("could not determine node_id")
		}

		initResp := map[string]interface{}{
			"type": "init_ok",
		}
		msg.Body = marshal(initResp)
		return nil
	})

	// Handle generate requests
	node.Handle("generate", func(msg maelstrom.Message) error {
		var r struct {
			Type      string `json:"type"`
			MsgID     int64  `json:"msg_id"`
			NodeID    string `json:"node_id"`
			GenNumber int64  `json:"gen_number"`
		}
		if err := json.Unmarshal(msg.Body, &r); err != nil {
			return fmt.Errorf("failed to unmarshal: %w", err)
		}

		nodeID := r.NodeID
		genNum := r.GenNumber

		// Use a simple file-based counter stored in the standard maelstrom store location
		storeDir := os.Getenv("MAELSTROM_NODE_STORE")
		if storeDir == "" {
			storeDir = "/var/lib/maelstrom"
		}
		counterFile := storeDir + "/" + nodeID + "-counter"

		var counter int64 = 0
		if data, err := os.ReadFile(counterFile); err == nil && len(data) > 0 {
			val, _ := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
			counter = val
		}

		counter++

		os.WriteFile(counterFile, []byte(strconv.FormatInt(counter, 10)), 0644)

		id := nodeID + "-" + strconv.FormatInt(counter, 10)

		resp := map[string]interface{}{
			"type":        "generate_ok",
			"in_reply_to": msg.MsgID,
			"id":          id,
		}

		msg.Body = marshal(resp)
		return nil
	})

	// Read init message from the first line of stdin
	initMsg := &maelstrom.InitMessageBody{}
	n := node.Stdin
	buf, _ := io.ReadAll(n)
	fmt.Printf("[DEBUG] raw init: %s\n", string(buf))

	if err := json.Unmarshal(buf, initMsg); err != nil {
		log.Fatalf("failed to read init: %v", err)
	}

	nodeID := initMsg.NodeID
	nodeIDs := initMsg.NodeIDs

	fmt.Printf("[DEBUG] node_id=%s node_ids=%v\n", nodeID, nodeIDs)

	node.Init(nodeID, nodeIDs)
	node.Run()
}
