package main

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// journalEntry is one immutable entry in a journal.
type journalEntry struct {
	Position int             `json:"position"`
	Value    json.RawMessage `json:"value"`
}

// journalState is the complete state stored for one journal.  Positions are
// dense: Next is one past the last assigned position.  Bookmark is nil until a
// client has committed a position for this journal.
type journalState struct {
	Entries  []journalEntry `json:"entries,omitempty"`
	Next     int            `json:"next"`
	Bookmark *int           `json:"bookmark,omitempty"`
}

type appendRequest struct {
	Type    string          `json:"type"`
	Journal string          `json:"journal"`
	Entry   json.RawMessage `json:"entry"`
}

type appendedResponse struct {
	Type     string `json:"type"`
	Position int    `json:"position"`
}

type fetchedResponse struct {
	Type    string             `json:"type"`
	Entries map[string][][]any `json:"entries"`
}

type bookmarkRequest struct {
	Type      string         `json:"type"`
	Positions map[string]int `json:"positions"`
}

type bookmarkedResponse struct {
	Type string `json:"type"`
}

type bookmarksRequest struct {
	Type     string   `json:"type"`
	Journals []string `json:"journals"`
}

type bookmarksResponse struct {
	Type      string         `json:"type"`
	Positions map[string]int `json:"positions"`
}

type app struct {
	node  *maelstrom.Node
	kv    *maelstrom.KV
	locks sync.Map // journal name -> *sync.Mutex
}

func main() {
	n := maelstrom.NewNode()
	a := &app{node: n, kv: maelstrom.NewLinKV(n)}

	n.Handle("append", a.handleAppend)
	n.Handle("fetch", a.handleFetch)
	n.Handle("bookmark", a.handleBookmark)
	n.Handle("bookmarks", a.handleBookmarks)

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

func (a *app) lockFor(journal string) *sync.Mutex {
	v, _ := a.locks.LoadOrStore(journal, &sync.Mutex{})
	return v.(*sync.Mutex)
}

func stateKey(journal string) string {
	return "journal:" + journal
}

// readState reads a journal state.  The boolean says whether the journal key
// exists.  Since the test has no faults, transient errors are simply retried.
func (a *app) readState(ctx context.Context, journal string) (journalState, bool) {
	var st journalState
	for {
		err := a.kv.ReadInto(ctx, stateKey(journal), &st)
		if err == nil {
			return st, true
		}
		if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
			return journalState{}, false
		}
		log.Printf("read %q: %s", journal, err)
		time.Sleep(time.Millisecond)
		st = journalState{}
	}
}

func retryableCAS(err error) bool {
	switch maelstrom.ErrorCode(err) {
	case maelstrom.KeyDoesNotExist, maelstrom.PreconditionFailed, maelstrom.KeyAlreadyExists, maelstrom.TxnConflict:
		return true
	}
	// With no injected faults this should not normally happen, but retrying a
	// failed KV operation is safer than losing an acknowledged append.
	return err != nil
}

func (a *app) appendEntry(ctx context.Context, journal string, value json.RawMessage) (int, error) {
	mu := a.lockFor(journal)
	mu.Lock()
	defer mu.Unlock()

	for {
		st, exists := a.readState(ctx, journal)
		if !exists {
			pos := 0
			next := journalState{
				Entries: []journalEntry{{Position: pos, Value: value}},
				Next:    1,
			}
			err := a.kv.CompareAndSwap(ctx, stateKey(journal), nil, next, true)
			if err == nil {
				return pos, nil
			}
			if retryableCAS(err) {
				continue
			}
			return 0, err
		}

		old := st
		pos := st.Next
		st.Entries = append(st.Entries, journalEntry{Position: pos, Value: value})
		st.Next = pos + 1

		err := a.kv.CompareAndSwap(ctx, stateKey(journal), old, st, false)
		if err == nil {
			return pos, nil
		}
		if retryableCAS(err) {
			continue
		}
		return 0, err
	}
}

func (a *app) handleAppend(msg maelstrom.Message) error {
	var req appendRequest
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		return err
	}
	pos, err := a.appendEntry(context.Background(), req.Journal, req.Entry)
	if err != nil {
		return err
	}
	return a.node.Reply(msg, appendedResponse{Type: "appended", Position: pos})
}

func (a *app) handleFetch(msg maelstrom.Message) error {
	var req struct {
		Type      string         `json:"type"`
		Positions map[string]int `json:"positions"`
	}
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		return err
	}

	entries := make(map[string][][]any)
	ctx := context.Background()
	for journal, from := range req.Positions {
		st, exists := a.readState(ctx, journal)
		if !exists {
			continue
		}
		var pairs [][]any
		for _, e := range st.Entries {
			if e.Position >= from {
				pairs = append(pairs, []any{e.Position, e.Value})
			}
		}
		if len(pairs) > 0 {
			entries[journal] = pairs
		}
	}

	return a.node.Reply(msg, fetchedResponse{Type: "fetched", Entries: entries})
}

func (a *app) handleBookmark(msg maelstrom.Message) error {
	var req bookmarkRequest
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		return err
	}

	ctx := context.Background()
	for journal, pos := range req.Positions {
		if err := a.setBookmark(ctx, journal, pos); err != nil {
			return err
		}
	}
	return a.node.Reply(msg, bookmarkedResponse{Type: "bookmarked"})
}

func (a *app) setBookmark(ctx context.Context, journal string, pos int) error {
	mu := a.lockFor(journal)
	mu.Lock()
	defer mu.Unlock()

	for {
		st, exists := a.readState(ctx, journal)
		if !exists {
			v := pos
			next := journalState{Bookmark: &v}
			err := a.kv.CompareAndSwap(ctx, stateKey(journal), nil, next, true)
			if err == nil {
				return nil
			}
			if retryableCAS(err) {
				continue
			}
			return err
		}

		if st.Bookmark != nil && *st.Bookmark >= pos {
			return nil
		}
		old := st
		v := pos
		st.Bookmark = &v
		err := a.kv.CompareAndSwap(ctx, stateKey(journal), old, st, false)
		if err == nil {
			return nil
		}
		if retryableCAS(err) {
			continue
		}
		return err
	}
}

func (a *app) handleBookmarks(msg maelstrom.Message) error {
	var req bookmarksRequest
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		return err
	}

	positions := make(map[string]int)
	ctx := context.Background()
	for _, journal := range req.Journals {
		st, exists := a.readState(ctx, journal)
		if exists && st.Bookmark != nil {
			positions[journal] = *st.Bookmark
		}
	}
	return a.node.Reply(msg, bookmarksResponse{Type: "bookmarks_are", Positions: positions})
}
