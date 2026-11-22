package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/jepsen-io/maelstrom/demo/go"
)

// LogState represents the state of one log (key)
type LogState struct {
	log     []string // values in order by offset
	offset  int64     // current offset counter
}

func main() {
	node := maelstrom.NewNode("node")

	// Handle init message - set up our data structures
	node.Handle("init", func(ctx *maelstrom.Context) *maelstrom.Body {
		initBody := ctx.RecvMsg().Body.(*maelstrom.Init)
		fmt.Fprintf(os.Stderr, "Init: node=%s\n", initBody.NodeID)

		// Create a map to hold the state of each log
		logStates := make(map[string]*LogState)

		return ctx.Send(maelstrom.Must(ctx.Node).Reply(initBody.MsgID, nil))
	})

	// Handle send - append a value to a log
	node.Handle("send", func(ctx *maelstrom.Context) *maelstrom.Body {
		sendMsg := ctx.RecvMsg().Body.(*maelstrom.Send)

		key := sendMsg.Key
		msg := fmt.Sprintf("%v", sendMsg.Msg) // Convert any msg type to string for storage

		var logState *LogState
		exists, ok := logStates[key]
		if !ok {
			logStates[key] = &LogState{log: []string{}, offset: 1}
		}
		logState = logStates[key]

		offset := logState.offset
		logState.log = append(logState.log, msg)
		logState.offset++

		respBody := maelstrom.New(maelstrom.SendOK).WithMsgID(sendMsg.MsgID).WithKey(key).WithOffset(offset)
		return ctx.Send(respBody)
	})

	// Handle poll - get values from logs starting at given offsets
	node.Handle("poll", func(ctx *maelstrom.Context) *maelstrom.Body {
		pollMsg := ctx.RecvMsg().Body.(*maelstrom.Poll)

		result := make(map[string][][2]any)

		for key, offset := range pollMsg.Offsets {
			var logState *LogState
			var ok bool
			logState, ok = logStates[key]
			if !ok {
				// Log doesn't exist, skip it
				continue
			}

			// Find the first value at or after the requested offset
			values := make([][2]any, 0)
			for i := range logState.log {
				logOffset := int64(i + 1) // Offsets start at 1 (or whatever we want)
				if logOffset >= offset {
					values = append(values, [2]any{logOffset, logState.log[i]})
				}
			}

			result[key] = values
		}

		respBody := maelstrom.New(maelstrom.PollOK).WithMsgID(pollMsg.MsgID).WithMsgs(result)
		return ctx.Send(respBody)
	})

	// Handle commit_offsets - record committed offset for each log
	node.Handle("commit_offsets", func(ctx *maelstrom.Context) *maelstrom.Body {
		commitMsg := ctx.RecvMsg().Body.(*maelstrom.CommitOffsets)

		for key, offset := range commitMsg.Offsets {
			var logState *LogState
			ok := false
			logState, ok = logStates[key]
			if !ok {
				// Log doesn't exist yet, create it with this committed offset
				logStates[key] = &LogState{log: []string{}, offset: offset}
			} else if int64(len(logState.log)+1) > offset {
				// If our computed next offset would be greater than committed,
				// we need to truncate. But actually, let's just cap the offset
				if logState.offset < offset {
					logState.offset = offset
				} else if logState.offset == offset && len(logState.log) >= int(offset-1) {
					// This is fine, already at or past committed
				}
			}
			// If the offset is beyond what we have, just set it (log will grow later)
			if logState.offset < offset {
				logState.offset = offset
			}
		}

		respBody := maelstrom.New(maelstrom.CommitOffsetsOK).WithMsgID(commitMsg.MsgID)
		return ctx.Send(respBody)
	})

	// Handle list_committed_offsets - return committed offsets for asked keys
	node.Handle("list_committed_offsets", func(ctx *maelstrom.Context) *maelstrom.Body {
		listMsg := ctx.RecvMsg().Body.(*maelstrom.ListCommittedOffsets)

		result := make(map[string]int64)
		for _, key := range listMsg.Keys {
			if logState, ok := logStates[key]; ok && len(logState.log) > 0 {
				// Committed offset is the next offset (len+1), unless we've seen commits
				result[key] = logState.offset
			} else if _, ok := logStates[key]; !ok {
				// No log exists yet, skip it (leave out logs with nothing committed)
			} else {
				// Log exists but has no values yet - committed offset should be 0 or not set
				// Actually, if there are no values, we haven't read anything, so skip
			}
		}

		respBody := maelstrom.New(maelstrom.ListCommittedOffsetsOK).WithMsgID(listMsg.MsgID).WithOffsets(result)
		return ctx.Send(respBody)
	})

	node.Run()
}
