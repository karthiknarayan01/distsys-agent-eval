package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"

	m "github.com/jepsen-io/maelstrom/demo/go"
)

// LogEntry represents a log entry: {offset, value}
type LogEntry struct {
	Offset int `json:"offset"`
	Value  int `json:"value"`
}

// Log represents an append-only log
type Log struct {
	mu      sync.Mutex
	nextOff int       // next offset to assign (starts at 1)
	entries map[int]int // offset -> value mapping
}

func main() {
	node := m.NewNode()
	logs := make(map[string]*Log)
	commitOffsets := make(map[string]int) // highest committed offset per key

	node.Handle("init", func(msg m.Message) error {
		var body map[string]interface{}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		nid := body["node_id"].(string)
		nodeIDs := body["node_ids"].([]interface{})

		for _, v := range nodeIDs {
			nodeIDs = append(nodeIDs, v.(string))
		}
		m.Init(nid, nodeIDs)
		return nil
	})

	// Handle send request: {"type": "send", "msg_id": 1, "key": "k1", "msg": 123}
	node.Handle("send", func(msg m.Message) error {
		var body map[string]interface{}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		msgID := int(body["msg_id"].(float64))
		key := body["key"].(string)
		val := int(body["msg"].(float64))

		log := logs[key]
		if log == nil {
			log = &Log{nextOff: 1, entries: make(map[int]int)}
			logs[key] = log
		}

		log.mu.Lock()
		offset := log.nextOff
		log.nextOff++
		log.entries[offset] = val
		log.mu.Unlock()

		responseBody := map[string]interface{}{
			"type":       "send_ok",
			"in_reply_to": msgID,
			"offset":      offset,
		}
		node.Reply(msg, responseBody)
		return nil
	})

	// Handle poll request: {"type": "poll", "msg_id": 2, "offsets": {"k1": 1000, "k2": 2000}}
	// Response: msgs is map[key] -> [[offset, value], ...] (each inner element is [offset, value])
	node.Handle("poll", func(msg m.Message) error {
		var body map[string]interface{}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		msgID := int(body["msg_id"].(float64))
		offsetsRaw := body["offsets"].(map[interface{}]interface{})

		result := make(map[string][]struct{ Offset, Value int })
		for key, offRaw := range offsetsRaw {
			keyStr := string(key)
			var off int
			switch v := offRaw.(type) {
			case float64:
				off = int(v)
			case int:
				off = v
			default:
				continue
			}

			log := logs[keyStr]
			if log == nil || len(log.entries) == 0 {
				continue // leave out logs with nothing at or after the offset
			}

			log.mu.Lock()
			var entries []struct{ Offset, Value int }
			
			// Find entries from this offset or first one after it
			start := off
			if start > log.nextOff-1 {
				start = log.nextOff // first entry after the given offset
			}
			
			for o, v := range log.entries {
				if o < start {
					continue
				}
				entries = append(entries, struct{ Offset, Value int }{Offset: o, Value: v})
			}
			log.mu.Unlock()

			result[keyStr] = entries
		}

		responseBody := map[string]interface{}{
			"type":    "poll_ok",
			"in_reply_to": msgID,
			"msgs":    make(map[int]map[int]int),
		}
		
		// msgs should be: {"k1": [[offset, value], ...]} where each inner element is [offset, value]
		// The maelstrom library expects a specific JSON format for this response
		msgsMap := make(map[string][]interface{}) // Using string key to preserve the actual key from input
		for k, v := range result {
			msgsMap[k] = make([]interface{}, len(v))
			for i, e := range v {
				msgsMap[k][i] = []interface{}{float64(e.Offset), float64(e.Value)}
			}
		}
		responseBody["msgs"] = msgsMap
		
		node.Reply(msg, responseBody)
		return nil
	})

	// Handle commit_offsets: {"type": "commit_offsets", "msg_id": 3, "offsets": {"k1": 1000, "k2": 2000}}
	node.Handle("commit_offsets", func(msg m.Message) error {
		var body map[string]interface{}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		msgID := int(body["msg_id"].(float64))
		offsetsRaw := body["offsets"].(map[interface{}]interface{})

		commitOffsets = make(map[string]int) // Update our tracking
		for key, offRaw := range offsetsRaw {
			keyStr := string(key)
			
			var off int
			switch v := offRaw.(type) {
			case float64:
				off = int(v)
			case int:
				off = v
			default:
				continue
			}

			commitOffsets[keyStr] = off
		}

		responseBody := map[string]interface{}{
			"type":       "commit_offsets_ok",
			"in_reply_to": msgID,
		}
		node.Reply(msg, responseBody)
		return nil
	})

	// Handle list_committed_offsets: {"type": "list_committed_offsets", "msg_id": 4, "keys": ["k1", "k2"]}
	node.Handle("list_committed_offsets", func(msg m.Message) error {
		var body map[string]interface{}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		msgID := int(body["msg_id"].(float64))
		keysRaw := body["keys"].([]interface{})
		keys := make([]string, len(keysRaw))
		for i, k := range keysRaw {
			keys[i] = string(k)
		}

		result := make(map[string]int)
		for _, key := range keys {
			if off, ok := commitOffsets[key]; ok {
				result[key] = off
			} else if log, exists := logs[key]; exists && len(log.entries) > 0 {
				// Use the current max offset if not explicitly committed
				log.mu.Lock()
				lastOff := log.nextOff - 1
				if len(log.entries) == 0 {
					lastOff = 0
				}
				log.mu.Unlock()
				result[key] = lastOff
			} else {
				continue // leave out logs with nothing committed
			}
		}

		responseBody := map[string]interface{}{
			"type":       "list_committed_offsets_ok",
			"in_reply_to": msgID,
			"offsets":    result,
		}
		node.Reply(msg, responseBody)
		return nil
	})

	log.Println("Node started")
	if err := node.Run(); err != nil {
		log.Printf("Error: %v", err)
		os.Exit(1)
	}
}
