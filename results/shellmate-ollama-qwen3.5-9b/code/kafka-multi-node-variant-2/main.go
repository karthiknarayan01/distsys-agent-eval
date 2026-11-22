package main

import (
	"encoding/json"
	"fmt"
	"log"
	"node"
)

// Entry represents an entry in a journal
type Entry struct {
	Journal string `json:"journal"`
	Value   int    `json:"value"`
}

// Bookmarks stores bookmark positions per journal
type Bookmarks struct {
	Journals []string            `json:"journals,omitempty"`
	Positions map[string]int      `json:"-"` // Internal: maps journal name to position
}

func init() {
	_ = node.NewLinKV("lin-kv")
}

var kv node.LinKV

func main() {
	node.NewNode(node.Config{
		InitOnce: true,
	}.Handle(func(ctx node.Context) {
		switch ctx.Message().Type {
		case "init":
			return
		case "append":
			AppendHandler(ctx)
		case "fetched":
			// Already handled by Reply
		case "appended":
			// Already handled by Reply
		case "fetch":
			FetchHandler(ctx)
		case "bookmarked":
			// Already handled by Reply
		case "bookmark":
			BookmarkHandler(ctx)
		case "bookmarks_are":
			// Already handled by Reply
		case "bookmarks":
			BookmarksHandler(ctx)
		}
	}))
}

func AppendHandler(ctx node.Context) {
	req := node.AppendRequest{}
	if err := json.Unmarshal([]byte(ctx.Message().Body), &req); err != nil {
		log.Printf("Error unmarshaling append request: %v", err)
		return
	}

	// Get current max position for this journal, or 0 if none exist
	maxPos := getJournalMaxPosition(ctx, req.Journal)

	newPos := maxPos + 1
	entryValue := req.Entry

	// Write to lin-kv
	kv.Write(node.Context{}, fmt.Sprintf("%s:%d", req.Journal, newPos), entryValue)

	ctx.ReplyWith(ctx.Message(), node.NewAppended(msgID(req)))
}

func FetchHandler(ctx node.Context) {
	req := node.FetchRequest{}
	if err := json.Unmarshal([]byte(ctx.Message().Body), &req); err != nil {
		log.Printf("Error unmarshaling fetch request: %v", err)
		return
	}

	result := make(map[string][][][2]int)
	
	for journal, pos := range req.Positions {
		entries := fetchEntries(ctx, journal, pos)
		result[journal] = entries
	}

	ctx.ReplyWith(ctx.Message(), node.NewFetched(msgID(req), result))
}

func BookmarkHandler(ctx node.Context) {
	req := node.BookmarkRequest{}
	if err := json.Unmarshal([]byte(ctx.Message().Body), &req); err != nil {
		log.Printf("Error unmarshaling bookmark request: %v", err)
		return
	}

	result := make(map[string]int)
	
	for journal, pos := range req.Positions {
		entryVal, _ := getEntryValue(ctx, fmt.Sprintf("%s:%d", journal, pos))
		if entryVal != "nil" {
			result[journal] = pos
		} else {
			// If there's no entry at that position, skip it (journal may not have bookmark)
			continue
		}
		
		// Write the bookmark
		kv.Write(node.Context{}, fmt.Sprintf("bookmark:%s", journal), "0")
	}

	ctx.ReplyWith(ctx.Message(), node.NewBookmarked(msgID(req)))
}

func BookmarksHandler(ctx node.Context) {
	req := node.BookmarksRequest{}
	if err := json.Unmarshal([]byte(ctx.Message().Body), &req); err != nil {
		log.Printf("Error unmarshaling bookmarks request: %v", err)
		return
	}

	result := make(map[string]int)
	
	for _, journal := range req.Journals {
		// Read the bookmark for this journal
		entryVal, exists := kv.Read(node.Context{}, fmt.Sprintf("bookmark:%s"), false)
		if exists && entryVal != "nil" {
			// Parse to get position - we store format: "position"
			result[journal] = intFromEntryVal(entryVal)
		}
	}

	ctx.ReplyWith(ctx.Message(), node.NewBookmarksAre(msgID(req), result))
}

func getJournalMaxPosition(ctx node.Context, journal string) int {
	// Try to find the max position by checking for entries
	key := fmt.Sprintf("%s:max", journal)
	entryVal, exists := kv.Read(node.Context{}, key, false)
	if !exists || entryVal == "nil" {
		return -1
	}
	
	pos := intFromEntryVal(entryVal)
	return pos
}

func fetchEntries(ctx node.Context, journal string, startPos int) [][][2]int {
	result := make([][][2]int, 0)
	
	currentPos := startPos
	entryVal, exists := kv.Read(node.Context{}, fmt.Sprintf("%s:%d", journal, currentPos), false)
	for exists && entryVal != "nil" {
		pos := intFromEntryVal(entryVal)
		result = append(result, [2]int{pos, 0}) // We'll need to get actual value too
		currentPos = pos + 1
		
		entryVal, exists = kv.Read(node.Context{}, fmt.Sprintf("%s:%d", journal, currentPos), false)
	}
	
	return result
}

func intFromEntryVal(val string) int {
	if val == "nil" {
		return -1
	}
	return intFromRaw(val)
}

// Helper functions for parsing integer values
func getEntryValue(ctx node.Context, journalPos string) (string, bool) {
	entryVal, exists := kv.Read(node.Context{}, journalPos, false)
	return entryVal, exists
}

func msgID(msg map[string]interface{}) int {
	id, _ := msg["msg_id"].(float64)
	return int(id)
}

// We need to parse integer values stored in lin-kv
func intFromRaw(s string) int {
	val, err := fmt.Sscanf(s, "%d", &newVal)
	if err != nil || newVal == 0 && s != "nil" {
		return -1
	}
	return newVal
}
