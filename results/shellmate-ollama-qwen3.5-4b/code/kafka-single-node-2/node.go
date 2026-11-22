package main

import (
	"encoding/json"
	"fmt"
	"os"

	m "github.com/jepsen-io/maelstrom/demo/go"
)

// logEntry holds offset and value
type logEntry struct {
	offset int64
	value  int64
}

func main() {
	node := m.NewNode()
	defer node.Run()

	// Read init message to get our ID and other node IDs
	var initMsg map[string]any
	if err := json.Unmarshal([]byte(m.Init()), &initMsg); err != nil {
		fmt.Fprintf(os.Stderr, "error reading init message: %v\n", err)
		os.Exit(1)
	}
	initJSON, _ := json.Marshal(initMsg)
	node.Reply(initJSON)

	var logs map[string][]logEntry
	logsMutex := sync.Mutex{}

	node.Handle("send", func(msg m.Message) error {
		bodyMap, _ := json.Marshal(msg.Body)
		var body map[string]any
		json.Unmarshal(bodyMap, &body)

		msgID := int64(body["msg_id"].(float64))
		key := body["key"].(string)
		val := int64(body["msg"].(float64))

		logsMutex.Lock()
		var newOffset int64
		if logs == nil {
			logs = make(map[string][]logEntry)
		}
		entryCount := len(logs[key])
		newOffset = int64(entryCount + 1)
		logs[key] = append(logs[key], logEntry{offset: newOffset, value: val})
		logsMutex.Unlock()

		replyMap := map[string]any{"in_reply_to": msgID, "offset": newOffset}
		replyJSON, _ := json.Marshal(replyMap)
		return node.Reply(replyJSON)
	})

	node.Handle("poll", func(msg m.Message) error {
		bodyMap, _ := json.Marshal(msg.Body)
		var body map[string]any
		json.Unmarshal(bodyMap, &body)

		msgID := int64(body["msg_id"].(float64))
		offsetsRaw, _ := body["offsets"].(map[string]any)

		replyMsgs := make(map[string][]any)

		logsMutex.Lock()
		for key := range offsetsRaw {
			valOffset, ok := offsetsRaw[key]
			if !ok {
				continue
			}
			offsetVal := int64(valOffset.(float64))

			entryList, ok := logs[key]
			if !ok || len(entryList) == 0 {
				logsMutex.Unlock()
				return nil
			}

			var pollVals []any
			for _, e := range entryList {
				if e.offset < offsetVal {
					continue
				}
				pollVals = append(pollVals, []any{e.offset, e.value})
			}

			replyMsgs[key] = pollVals
		}
		logsMutex.Unlock()

		jsonReply, _ := json.Marshal(map[string]any{"msgs": replyMsgs})
		return node.Reply(jsonReply)
	})

	node.Handle("commit_offsets", func(msg m.Message) error {
		bodyMap, _ := json.Marshal(msg.Body)
		var body map[string]any
		json.Unmarshal(bodyMap, &body)

		msgID := int64(body["msg_id"].(float64))
		offsetsRaw, _ := body["offsets"].(map[string]any)

		replyMap := make(map[string]int64)

		logsMutex.Lock()
		for key := range offsetsRaw {
			valOffset, ok := offsetsRaw[key]
			if !ok {
				continue
			}
			
			entryList, ok := logs[key]
			if !ok || len(entryList) == 0 {
				replyMap[key] = 0
			} else {
				maxOffset := int64(0)
				for _, e := range entryList {
					if e.offset > maxOffset {
						maxOffset = e.offset
					}
				}
				replyMap[key] = maxOffset
			}
		}
		logsMutex.Unlock()

		jsonReply, _ := json.Marshal(map[string]any{"offsets": replyMap})
		return node.Reply(jsonReply)
	})

	node.Handle("list_committed_offsets", func(msg m.Message) error {
		bodyMap, _ := json.Marshal(msg.Body)
		var body map[string]any
		json.Unmarshal(bodyMap, &body)

		msgID := int64(body["msg_id"].(float64))
		keysRaw, _ := body["keys"].([]string)

		replyMap := make(map[string]int64)

		logsMutex.Lock()
		for _, key := range keysRaw {
			if entryList, ok := logs[key]; ok && len(entryList) > 0 {
				maxOffset := int64(0)
				for _, e := range entryList {
					if e.offset > maxOffset {
						maxOffset = e.offset
					}
				}
				replyMap[key] = maxOffset
			} else {
				delete(replyMap, key)
			}
		}
		logsMutex.Unlock()

		jsonReply, _ := json.Marshal(map[string]any{"offsets": replyMap})
		return node.Reply(jsonReply)
	})

	return nil
}

var logsMutex sync.Mutex
