package main

import (
	"encoding/json"
	"fmt"
	"os"
)

// JournalEntry is an entry in a journal at a given position
type JournalEntry struct {
	Position int `json:"position"`
	Value    int `json:"entry"`
}

// NodeState tracks all state for one node
type NodeState struct {
	id       string
	journals map[string][]JournalEntry // journalID -> ordered entries from position 1 onwards
	bookmarks map[string]int            // journalID -> bookmark position
}

func NewNodeState(id string) *NodeState {
	return &NodeState{
		id:       id,
		journals: make(map[string][]JournalEntry),
		bookmarks: make(map[string]int),
	}
}

func main() {
	reader := os.Stdin
	writer := os.Stdout
	
	nodeState := NewNodeState("init")
	
	for {
		line, err := reader.ReadBytes('\n')
		if len(line) == 0 {
			break
		}
		
		raw := json.RawMessage(line[:len(line)-1]) // remove trailing newline if present
		
		var req struct {
			Type string `json:"type"`
		}
		json.Unmarshal(raw, &req)
		
		switch nodeState.id {
		case "init":
			handleInit(nodeState, raw, writer)
		default:
			break
		}

		if req.Type == "append" {
			handleAppend(nodeState, raw, writer)
		} else if req.Type == "fetch" {
			handleFetch(nodeState, raw, writer)
		} else if req.Type == "bookmark" {
			handleBookmark(nodeState, raw, writer)
		} else if req.Type == "bookmarks" {
			handleBookmarks(nodeState, raw, writer)
		} else {
			fmt.Fprint(writer, `{"type":"error","message":"unsupported request type"}`)
		}
	}
}

func handleInit(state *NodeState, _ json.RawMessage, _ fmt.Writer) error {
	var req struct {
		MyID string `json:"my"`
		Nodes []string `json:"nodes"`
	}
	json.Unmarshal(_raw, &req)
	state.id = req.MyID

	response := json.RawMessage(`{"type":"init_ok"}`)
	fmt.Fprint(writer, response)
	return nil
}

func handleAppend(state *NodeState, _ json.RawMessage, writer fmt.Writer) error {
	var req struct {
		Type    string `json:"type"`
		Message ID   `json:"msg_id"`
		Journal string `json:"journal"`
		Entry   int    `json:"entry"`
	}
	json.Unmarshal(raw, &req)

	jour := state.journals[req.Journal]
	if jour == nil {
		jour = make([]JournalEntry, 0)
		state.journals[req.Journal] = jour
	}

	position := len(jour) + 1 // positions start at 1 and increase by 1
	jour = append(jour, JournalEntry{Position: position, Value: req.Entry})
	state.journals[req.Journal] = jour

	response := json.RawMessage(fmt.Sprintf(`{"type":"appended","in_reply_to":%d,"position":%d}`, req.Message, position))
	fmt.Fprint(writer, response)
	return nil
}

func handleFetch(state *NodeState, _ json.RawMessage, writer fmt.Writer) error {
	var req struct {
		Type    string `json:"type"`
		Message ID   `json:"msg_id"`
		Positions map[string]int `json:"positions"`
	}
	json.Unmarshal(raw, &req)

	respMap := make(map[string][]JournalEntry)
	for journalID, pos := range req.Positions {
		jour := state.journals[journalID]
		
		var entryList []JournalEntry
		for i, entry := range jour {
			if entry.Position < pos || (entry.Position == pos && entry.Value != 0) {
				break
			}
			entryList = append(entryList, entry)
		}
		respMap[journalID] = entryList
	}

	jsonResp, _ := json.Marshal(respMap)
	fmt.Fprint(writer, jsonResp)
	return nil
}

func handleBookmark(state *NodeState, _ json.RawMessage, writer fmt.Writer) error {
	var req struct {
		Type    string `json:"type"`
		Message ID   `json:"msg_id"`
		Positions map[string]int `json:"positions"`
	}
	json.Unmarshal(raw, &req)

	for journalID, pos := range req.Positions {
		state.bookmarks[journalID] = pos
	}

	response := json.RawMessage(fmt.Sprintf(`{"type":"bookmarked","in_reply_to":%d}`, req.Message))
	fmt.Fprint(writer, response)
	return nil
}

func handleBookmarks(state *NodeState, _ json.RawMessage, writer fmt.Writer) error {
	var req struct {
		Type    string `json:"type"`
		Message ID   `json:"msg_id"`
		Journals []string `json:"journals"`
	}
	json.Unmarshal(raw, &req)

	respMap := make(map[string]int)
	for _, journalID := range req.Journals {
		pos, ok := state.bookmarks[journalID]
		if !ok {
			continue // skip journals with no bookmark
		}
		respMap[journalID] = pos
	}

	jsonResp, _ := json.Marshal(respMap)
	fmt.Fprint(writer, jsonResp)
	return nil
}
