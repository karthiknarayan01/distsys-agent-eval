package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"

	"github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	ctx := context.Background()
	node := maelstrom.NewNode("node")
	lkv := maelstrom.NewLinKV(node)

	// In-memory journals
	journals := make(map[string]*Journal)
	var journalsMu sync.Mutex

	// Handler for init message
	node.Handle("init", func(ctx context.Context, msg json.RawMessage) maelstrom.Message {
		return maelstrom.Reply(ctx, nil, "ignored")
	})

	// Handle append requests
	node.Handle("append", func(ctx context.Context, req maelstrom.Request[AppendRequest]) maelstrom.Message {
		r := req.Body
		journalName := r.Journal

		// Get current max position for this journal
		posKey := fmt.Sprintf("journal:%s:pos", journalName)
		maxPos, err := lkv.ReadInt(ctx, posKey)
		if err != nil {
			log.Printf("Error reading position for journal %s: %v", journalName, err)
			maxPos = 0
		}

		newPos := maxPos + 1

		// Update the stored position
		lkv.Write(ctx, posKey, newPos)

		// Store entry in lin-kv and local memory
		entryKey := fmt.Sprintf("journal:%s:%d", journalName, newPos)
		if err := lkv.Write(ctx, entryKey, r.Entry); err != nil {
			log.Printf("Error writing entry: %v", err)
		}

		journalsMu.Lock()
		if journals[journalName] == nil {
			journals[journalName] = &Journal{Entries: []JournalEntry{}}
		}
		journals[journalName].Entries = append(journals[journalName].Entries, JournalEntry{
			Position: newPos,
			Entry:    r.Entry,
		})
		journalsMu.Unlock()

		response := &AppendResponse{
			Type:     "appended",
			MessageID: r.MessageID,
			Position: newPos,
		}
		return maelstrom.Reply(ctx, response)
	})

	// Handle fetch requests
	node.Handle("fetch", func(ctx context.Context, req maelstrom.Request[AppendRequest]) maelstrom.Message {
		r := req.Body

		resultEntries := make(map[string][]JournalEntry)

		for journalName, startPos := range r.Positions {
			if journalName == "" {
				continue
			}
			
			entries := readJournalEntries(ctx, lkv, journals, journalName, startPos)
			if entries != nil {
				resultEntries[journalName] = entries
			}
		}

		response := &AppendResponse{
			Type:     "fetched",
			MessageID: r.MessageID,
			Entries:  resultEntries,
		}
		return maelstrom.Reply(ctx, response)
	})

	// Handle bookmark requests
	node.Handle("bookmark", func(ctx context.Context, req maelstrom.Request[BookmarkRequest]) maelstrom.Message {
		r := req.Body
		
		// Get max position from storage
		posKey := fmt.Sprintf("journal:%s:pos", r.Journal)
		maxPos, err := lkv.ReadInt(ctx, posKey)
		if err != nil || maxPos <= 0 {
			maxPos = 0
		}
		
		// The given position should be stored as the bookmark
		bookmarkPos := r.Position
		
		// Store the bookmark
		bkKey := fmt.Sprintf("bookmark:%s", r.Journal)
		lkv.Write(ctx, bkKey, bookmarkPos)

		response := &AppendResponse{
			Type:       "bookmarked",
			MessageID:  r.MessageID,
		}
		return maelstrom.Reply(ctx, response)
	})

	// Handle bookmarks requests
	node.Handle("bookmarks", func(ctx context.Context, req maelstrom.Request[BookmarksRequest]) maelstrom.Message {
		r := req.Body

		resultPositions := make(map[string]int)

		for _, journalName := range r.Journals {
			bkKey := fmt.Sprintf("bookmark:%s", journalName)
			pos, err := lkv.ReadInt(ctx, bkKey)
			if err != nil || pos <= 0 {
				// Try to get from local memory
				journalsMu.Lock()
				journal := journals[journalName]
				if journal != nil && len(journal.Entries) > 0 {
					pos = journal.Entries[len(journal.Entries)-1].Position
				}
				journalsMu.Unlock()
			}
			resultPositions[journalName] = int(pos)
		}

		response := &AppendResponse{
			Type:       "bookmarks_are",
			MessageID:  r.MessageID,
			Positions:  resultPositions,
		}
		return maelstrom.Reply(ctx, response)
	})

	node.Run(ctx)
}

// Journal represents local journal state
type Journal struct {
	Entries []JournalEntry `json:"entries"`
}

// JournalEntry represents an entry in a journal
type JournalEntry struct {
	Position int `json:"position"`
	Entry    int `json:"entry"`
}

// AppendRequest types
type AppendRequest struct {
	Type       string            `json:"type"`
	MessageID  int               `json:"msg_id"`
	Journal    string            `json:"journal,omitempty"`
	Entry      int               `json:"entry,omitempty"`
	Positions  map[string]int    `json:"positions,omitempty"`
	InReplyTo  int               `json:"in_reply_to,omitempty"`
}

// AppendResponse types
type AppendResponse struct {
	Type     string                    `json:"type"`
	MessageID int                       `json:"msg_id"`
	Position int                        `json:"position,omitempty"`
	Entries  map[string][]JournalEntry `json:"entries,omitempty"`
}

// BookmarkRequest type
type BookmarkRequest struct {
	Type       string            `json:"type"`
	MessageID  int               `json:"msg_id"`
	Journal    string            `json:"journal,omitempty"`
	Position   int               `json:"position,omitempty"`
	InReplyTo  int               `json:"in_reply_to,omitempty"`
}

// BookmarksRequest type
type BookmarksRequest struct {
	Type       string          `json:"type"`
	MessageID  int             `json:"msg_id"`
	Journals   []string        `json:"journals"`
	InReplyTo  int             `json:"in_reply_to,omitempty"`
}

// readJournalEntries reads entries from a journal starting at startPos
func readJournalEntries(ctx context.Context, lkv *maelstrom.LinKVClient, journals map[string]*Journal, journalName string, startPos int) []JournalEntry {
	if journalName == "" {
		return nil
	}

	result := make([]JournalEntry, 0)

	// Get max position for this journal from storage
	posKey := fmt.Sprintf("journal:%s:pos", journalName)
	maxPos, err := lkv.ReadInt(ctx, posKey)
	if err != nil || maxPos <= 0 {
		return nil
	}

	// If startPos is beyond what we have, return nil
	if startPos > maxPos {
		return nil
	}

	// Check in-memory cache first
	journalsMu.Lock()
	journal := journals[journalName]
	journalsMu.Unlock()

	if journal != nil && len(journal.Entries) > 0 {
		// Build a map of positions to entries
		entryMap := make(map[int]JournalEntry)
		for _, e := range journal.Entries {
			entryMap[e.Position] = e
		}

		// Check if startPos is within our range
		minPos := entryMap[entryMap[minKeys(entryMap)].Position].Position
		if startPos <= maxPos && minPos <= startPos {
			// Collect entries from startPos to the last one we have
			for pos, e := range entryMap {
				if pos >= startPos {
					result = append(result, e)
				}
			}
			
			// Sort by position
			sorted := make([]JournalEntry, len(result))
			for i, e := range result {
				sorted[i] = e
			}
			// Simple bubble sort since positions are sparse but ordered
			for i := 0; i < len(sorted)-1; i++ {
				for j := i + 1; j < len(sorted); j++ {
					if sorted[i].Position > sorted[j].Position {
						sorted[i], sorted[j] = sorted[j], sorted[i]
					}
				}
			}
			
			result = sorted
		}
		
		return result
	}

	// Fall back to reading from lin-kv for each position from startPos to maxPos
	for pos := startPos; pos <= maxPos; pos++ {
		key := fmt.Sprintf("journal:%s:%d", journalName, pos)
		entryVal, err := lkv.ReadInt(ctx, key)
		if err != nil {
			break
		}
		result = append(result, JournalEntry{
			Position: pos,
			Entry:    entryVal,
		})
	}

	return result
}

// minKeys finds the minimum position key in a map of entries (helper function)
func minKeys(m map[int]JournalEntry) int {
	minPos := m[0].Position
	for _, e := range m {
		if e.Position < minPos {
			minPos = e.Position
		}
	}
	return minPos
}
