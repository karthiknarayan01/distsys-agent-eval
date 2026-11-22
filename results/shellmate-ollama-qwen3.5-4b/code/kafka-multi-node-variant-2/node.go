package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"sort"
	"strconv"
	"strings"

	m "github.com/jepsen-io/maelstrom/demo/go"
)

// AppendRequest represents the append message body
type AppendRequest struct {
	Journal string `json:"journal"`
	Entry   int    `json:"entry"`
	MsgID   uint64 `json:"msg_id"`
}

// AppendResponse represents the appended message body
type AppendResponse struct {
	InReplyTo uint64 `json:"in_reply_to"`
	Position  int64  `json:"position"`
	MsgID     uint64 `json:"msg_id,omitempty"`
}

// FetchRequest represents the fetch message body
type FetchRequest struct {
	Positions map[string]int64 `json:"positions"`
	MsgID     uint64           `json:"msg_id"`
}

// FetchResponse represents the fetched message body
type FetchResponse struct {
	InReplyTo uint64   `json:"in_reply_to"`
	Entries   map[string][]struct {
		Position int64  `json:"position"`
		Entry    int    `json:"entry"`
	}           `json:"entries"`
	MsgID     uint64 `json:"msg_id,omitempty"`
}

// BookmarkRequest represents the bookmark message body
type BookmarkRequest struct {
	Positions map[string]int `json:"positions"`
	MsgID     uint64        `json:"msg_id"`
}

// BookmarkResponse represents the bookmarked message body
type BookmarkResponse struct {
	InReplyTo uint64 `json:"in_reply_to"`
	MsgID     uint64 `json:"msg_id,omitempty"`
}

// BookmarksRequest represents the bookmarks message body
type BookmarksRequest struct {
	Journals []string `json:"journals"`
	MsgID    uint64   `json:"msg_id"`
}

// BookmarksResponse represents the bookmarks_are message body
type BookmarksResponse struct {
	InReplyTo  uint64  `json:"in_reply_to"`
	Positions  map[string]int `json:"positions"`
	MsgID      uint64 `json:"msg_id,omitempty"`
}

var lastID uint64 = 1

// JournalService manages journals for a cluster node
type JournalService struct {
	nodeID   int
	journals map[string][]int        // journalName -> list of positions (sorted, increasing)
	entryMap map[int64]int           // position -> entry value
}

func NewJournalService(nodeID int) *JournalService {
	return &JournalService{
		journals: make(map[string][]int),
		entryMap: make(map[int64]int),
		nodeID:   nodeID,
	}
}

// GetNextPosition atomically retrieves and returns the next position for a journal
func (js *JournalService) GetNextPosition(ctx context.Context, journal string) (int64, error) {
	key := "journal:" + journal
	val, err := js.jsClient.ReadInt(ctx, key)
	if err != nil {
		return 0, err
	}
	
	// Check if entry at this position already exists locally
	if _, ok := js.entryMap[int64(val)]; !ok {
		// Entry doesn't exist locally, assume it's at another node or not yet created
		// We'll generate a new position starting from val+1
		return val + 1, nil
	} else {
		// Entry exists locally, get it and return next position
		return val + 1, nil
	}
}

// GetEntry retrieves an entry at a given position (reads from lin-kv)
func (js *JournalService) GetEntry(ctx context.Context, journal string, pos int64) (int, error) {
	key := "journal:" + journal + ":entry"
	entryVal, err := js.jsClient.ReadInt(ctx, key)
	return entryVal, err
}

// StoreEntry atomically stores an entry for a position in lin-kv
func (js *JournalService) StoreEntry(ctx context.Context, journal string, pos int64, entry int) error {
	key := "journal:" + journal + ":entry"
	return js.jsClient.Write(ctx, key, entry)
}

// GetNextPositionAtomically atomically retrieves the next position using CAS
func (js *JournalService) GetNextPositionAtomically(ctx context.Context, journal string) (int64, error) {
	key := "journal:" + journal
	from := int64(0) // initial value if key doesn't exist
	
	// Try to read current value
	val, err := js.jsClient.ReadInt(ctx, key)
	if err != nil || val == 0 {
		return 1, nil
	}
	from = val
	
	// Try CAS with createIfNotExists for atomic increment
	to := from + 1
	err = js.jsClient.CompareAndSwap(ctx, key, &from, &to, true)
	if err != nil {
		fmt.Fprintf(m.Node(node).Stderr(), "CAS error: %v\n", err)
		return 0, err
	}
	
	return to, nil
}

// Append handles append requests
func (js *JournalService) Append(ctx context.Context, journal string, entry int) (int64, error) {
	nextPos, err := js.GetNextPositionAtomically(ctx, journal)
	if err != nil {
		return 0, err
	}
	
	// Store the entry atomically in lin-kv
	err = js.StoreEntry(ctx, journal, nextPos, entry)
	if err != nil {
		return 0, err
	}
	
	// Update local journal position list
	js.Lock()
	if js.journals[journal] == nil {
		js.journals[journal] = []int64{nextPos}
	} else {
		js.journals[journal] = append(js.journals[journal], nextPos)
	}
	sort.Ints(js.journals[journal])
	
	pos := int(nextPos)
	js.entryMap[nextPos] = entry
	
	js.Unlock()
	return pos, nil
}

// Fetch handles fetch requests
func (js *JournalService) Fetch(ctx context.Context, positions map[string]int64) map[string][]struct { Position int64; Entry int } {
	respMap := make(map[string][]struct { Position int64; Entry int })
	
	for journalName, startPos := range positions {
		js.Lock()
		
		var filtered []int64
		if js.journals[journalName] != nil {
			// Filter entries at or after startPos
			startIdx := -1
			for i, pos := range js.journals[journalName] {
				if int64(pos) >= startPos {
					startIdx = i
					break
				}
			}
			
			if startIdx == -1 {
				startIdx = len(js.journals[journalName])
			}
			
			filtered = make([]int64, 0)
			for i := startIdx; i < len(js.journals[journalName]); i++ {
				pos := js.journals[journalName][i]
				entryVal := js.entryMap[pos]
				respList[i-startIdx] = struct { Position int64; Entry int }{pos, entryVal}
			}
			
			filtered = append(filtered, js.journals[journalName][startIdx:]...)
		}
		
		js.Unlock()
		
		respMap[journalName] = respList
	}
	
	return respMap, nil
}

// Bookmark handles bookmark requests
func (js *JournalService) Bookmark(ctx context.Context, positions map[string]int) map[string]int {
	respMap := make(map[string]int)
	
	for journalName := range positions {
		js.Lock()
		
		var pos int64 = 1 // start position by default
		if js.journals[journalName] != nil && len(js.journals[journalName]) > 0 {
			pos = int64(js.journals[journalName][len(js.journals[journalName])-1]) + 1
		}
		
		respMap[journalName] = int(pos)
		
		js.Unlock()
	}
	
	return respMap, nil
}

func main() {
	node, err := m.NewNode()
	if err != nil {
		fmt.Fprintf(node.Stderr(), "Failed to create node: %v\n", err)
		return
	}

	js := NewJournalService(node.NodeID)
	kv := m.NewLinKV("lin-kv", node)

	handle := func(body *m.MessageBody) {
		bodyType := string(body.Type)
		
		switch bodyType {
		case "init":
			node.Init(body.Body.(*m.InitMessageBody))
			
		case "send", "poll": // append and fetch requests
			if appendReq, ok := body.Body.(AppendRequest); ok {
				ctx := context.Background()
				
				pos, err := js.Append(ctx, appendReq.Journal, appendReq.Entry)
				if err != nil {
					fmt.Fprintf(node.Stderr(), "Error appending: %v\n", err)
				}
				
				lastID++
				resp := AppendResponse{
					InReplyTo: body.MsgID,
					Position:  pos,
					MsgID:     lastID,
				}
				node.Reply(&m.MessageBody{Type: "appended", Body: resp})

			case "fetch":
				if fetchReq, ok := body.Body.(FetchRequest); ok {
					ctx := context.Background()
					
					respMap := js.Fetch(ctx, fetchReq.Positions)
					
					lastID++
					resp := FetchResponse{
						InReplyTo: body.MsgID,
						Entries:   respMap,
						MsgID:     lastID,
					}
					node.Reply(&m.MessageBody{Type: "fetched", Body: resp})

			case "bookmark":
				if bookmarkReq, ok := body.Body.(BookmarkRequest); ok {
					respMap := js.Bookmark(ctx, bookmarkReq.Positions)
					
					lastID++
					resp := BookmarkResponse{
						InReplyTo: body.MsgID,
						Positions: respMap,
						MsgID:     lastID,
					}
					node.Reply(&m.MessageBody{Type: "bookmarked", Body: resp})

			case "bookmarks":
				if bookmarkReq, ok := body.Body.(BookmarksRequest); ok {
					respMap := js.Bookmark(ctx, bookmarkReq.Positions)
					
					lastID++
					resp := BookmarksResponse{
						InReplyTo: body.MsgID,
						Positions: respMap,
						MsgID:     lastID,
					}
					node.Reply(&m.MessageBody{Type: "bookmarks_are", Body: resp})

				default:
			}
			
		default:
		}
	}

	handle(nil)

	m.Run(node)
}
EOF