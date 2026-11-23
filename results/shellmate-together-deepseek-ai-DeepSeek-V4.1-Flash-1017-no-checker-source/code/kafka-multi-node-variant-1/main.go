package main

import (
	"context"
	"encoding/json"
	"log"
	"math/rand"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const (
	logPrefix      = "journal-log\x00"
	bookmarkPrefix = "journal-bookmark\x00"
	maxFetch       = 500
)

type server struct {
	node *maelstrom.Node
	kv   *maelstrom.KV

	mu    sync.Mutex
	locks map[string]*sync.Mutex
}

func newServer(n *maelstrom.Node) *server {
	return &server{
		node:  n,
		kv:    maelstrom.NewLinKV(n),
		locks: make(map[string]*sync.Mutex),
	}
}

// journalLock returns a process-local mutex for a journal.  This does not
// provide cross-node mutual exclusion (lin-kv CAS does that); it just avoids
// wasting CAS attempts on concurrent appends handled by this same node.
func (s *server) journalLock(journal string) *sync.Mutex {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, ok := s.locks[journal]
	if !ok {
		m = &sync.Mutex{}
		s.locks[journal] = m
	}
	return m
}

func logKey(journal string) string      { return logPrefix + journal }
func bookmarkKey(journal string) string { return bookmarkPrefix + journal }

func sleepBackoff(attempt int) {
	// Small jittered backoff to break up CAS contention.
	max := 3 * time.Millisecond
	if attempt < 3 {
		max = time.Duration(attempt+1) * time.Millisecond
	}
	if max > 0 {
		time.Sleep(time.Duration(rand.Int63n(int64(max))))
	}
}

func rpcCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 2*time.Second)
}

// readLog reads the complete entry array for a journal from lin-kv.
// It returns exists=false if the journal has no entries yet.
func (s *server) readLog(journal string) ([]json.RawMessage, bool, error) {
	var lastErr error
	for attempt := 0; attempt < 20; attempt++ {
		ctx, cancel := rpcCtx()
		var entries []json.RawMessage
		err := s.kv.ReadInto(ctx, logKey(journal), &entries)
		cancel()
		if err == nil {
			if entries == nil {
				entries = []json.RawMessage{}
			}
			return entries, true, nil
		}
		if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
			return nil, false, nil
		}
		lastErr = err
		sleepBackoff(attempt)
	}
	return nil, false, lastErr
}

// appendEntry appends a single entry to a journal and returns its position.
func (s *server) appendEntry(journal string, entry json.RawMessage) (int, error) {
	m := s.journalLock(journal)
	m.Lock()
	defer m.Unlock()

	var lastErr error
	for attempt := 0; attempt < 50; attempt++ {
		ctx, cancel := rpcCtx()
		var current []json.RawMessage
		err := s.kv.ReadInto(ctx, logKey(journal), &current)
		cancel()

		exists := true
		if err != nil {
			if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
				exists = false
				current = nil
			} else {
				lastErr = err
				sleepBackoff(attempt)
				continue
			}
		}

		pos := len(current)
		next := make([]json.RawMessage, len(current)+1)
		copy(next, current)
		next[pos] = entry

		var from any
		if exists {
			from = current
		} else {
			from = nil
		}

		ctx, cancel = rpcCtx()
		err = s.kv.CompareAndSwap(ctx, logKey(journal), from, next, true)
		cancel()
		if err == nil {
			return pos, nil
		}
		lastErr = err
		sleepBackoff(attempt)
	}
	return 0, lastErr
}

func (s *server) readBookmark(journal string) (int, bool, error) {
	var lastErr error
	for attempt := 0; attempt < 20; attempt++ {
		ctx, cancel := rpcCtx()
		v, err := s.kv.Read(ctx, bookmarkKey(journal))
		cancel()
		if err == nil {
			switch n := v.(type) {
			case int:
				return n, true, nil
			case float64:
				return int(n), true, nil
			case json.Number:
				i, _ := n.Int64()
				return int(i), true, nil
			}
			return 0, false, nil
		}
		if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
			return 0, false, nil
		}
		lastErr = err
		sleepBackoff(attempt)
	}
	return 0, false, lastErr
}

// commitBookmark advances a journal bookmark, never moving it backwards.
func (s *server) commitBookmark(journal string, position int) error {
	m := s.journalLock(journal)
	m.Lock()
	defer m.Unlock()

	var lastErr error
	for attempt := 0; attempt < 50; attempt++ {
		ctx, cancel := rpcCtx()
		v, err := s.kv.Read(ctx, bookmarkKey(journal))
		cancel()

		exists := true
		cur := 0
		if err != nil {
			if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
				exists = false
			} else {
				lastErr = err
				sleepBackoff(attempt)
				continue
			}
		} else {
			switch n := v.(type) {
			case int:
				cur = n
			case float64:
				cur = int(n)
			case json.Number:
				i, _ := n.Int64()
				cur = int(i)
			}
		}

		if exists && position <= cur {
			return nil
		}

		var from any
		if exists {
			from = cur
		} else {
			from = nil
		}

		ctx, cancel = rpcCtx()
		err = s.kv.CompareAndSwap(ctx, bookmarkKey(journal), from, position, true)
		cancel()
		if err == nil {
			return nil
		}
		lastErr = err
		sleepBackoff(attempt)
	}
	return lastErr
}

type appendBody struct {
	maelstrom.MessageBody
	Journal string          `json:"journal"`
	Entry   json.RawMessage `json:"entry"`
}

type appendedBody struct {
	Type     string `json:"type"`
	Position int    `json:"position"`
}

func (s *server) handleAppend(msg maelstrom.Message) error {
	var body appendBody
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	pos, err := s.appendEntry(body.Journal, body.Entry)
	if err != nil {
		return err
	}
	return s.node.Reply(msg, appendedBody{Type: "appended", Position: pos})
}

type fetchBody struct {
	maelstrom.MessageBody
	Positions map[string]int `json:"positions"`
}

type fetchedBody struct {
	Type    string             `json:"type"`
	Entries map[string][][]any `json:"entries"`
}

func (s *server) handleFetch(msg maelstrom.Message) error {
	var body fetchBody
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	type result struct {
		journal string
		entries [][]any
	}
	results := make(chan result, len(body.Positions))
	var wg sync.WaitGroup
	for journal, start := range body.Positions {
		journal, start := journal, start
		wg.Add(1)
		go func() {
			defer wg.Done()
			logEntries, exists, err := s.readLog(journal)
			if err != nil || !exists {
				results <- result{journal: journal}
				return
			}
			if start < 0 {
				start = 0
			}
			if start >= len(logEntries) {
				results <- result{journal: journal}
				return
			}
			end := len(logEntries)
			if end > start+maxFetch {
				end = start + maxFetch
			}
			out := make([][]any, 0, end-start)
			for i := start; i < end; i++ {
				out = append(out, []any{i, logEntries[i]})
			}
			results <- result{journal: journal, entries: out}
		}()
	}
	wg.Wait()
	close(results)

	entries := make(map[string][][]any)
	for r := range results {
		if len(r.entries) > 0 {
			entries[r.journal] = r.entries
		}
	}
	return s.node.Reply(msg, fetchedBody{Type: "fetched", Entries: entries})
}

type bookmarkBody struct {
	maelstrom.MessageBody
	Positions map[string]int `json:"positions"`
}

type bookmarkedBody struct {
	Type string `json:"type"`
}

func (s *server) handleBookmark(msg maelstrom.Message) error {
	var body bookmarkBody
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	var wg sync.WaitGroup
	errCh := make(chan error, len(body.Positions))
	for journal, pos := range body.Positions {
		journal, pos := journal, pos
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := s.commitBookmark(journal, pos); err != nil {
				errCh <- err
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			return err
		}
	}
	return s.node.Reply(msg, bookmarkedBody{Type: "bookmarked"})
}

type bookmarksBody struct {
	maelstrom.MessageBody
	Journals []string `json:"journals"`
}

type bookmarksAreBody struct {
	Type      string         `json:"type"`
	Positions map[string]int `json:"positions"`
}

func (s *server) handleBookmarks(msg maelstrom.Message) error {
	var body bookmarksBody
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	type result struct {
		journal string
		pos     int
		ok      bool
	}
	results := make(chan result, len(body.Journals))
	var wg sync.WaitGroup
	for _, journal := range body.Journals {
		journal := journal
		wg.Add(1)
		go func() {
			defer wg.Done()
			pos, ok, err := s.readBookmark(journal)
			if err != nil {
				ok = false
			}
			results <- result{journal: journal, pos: pos, ok: ok}
		}()
	}
	wg.Wait()
	close(results)

	positions := make(map[string]int)
	for r := range results {
		if r.ok {
			positions[r.journal] = r.pos
		}
	}
	return s.node.Reply(msg, bookmarksAreBody{Type: "bookmarks_are", Positions: positions})
}

func main() {
	n := maelstrom.NewNode()
	s := newServer(n)

	n.Handle("append", s.handleAppend)
	n.Handle("fetch", s.handleFetch)
	n.Handle("bookmark", s.handleBookmark)
	n.Handle("bookmarks", s.handleBookmarks)

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
