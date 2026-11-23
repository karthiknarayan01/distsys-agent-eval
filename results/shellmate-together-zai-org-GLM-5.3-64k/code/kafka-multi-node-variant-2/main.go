// Journals: an append-only list of integer entries per journal name, plus
// reader bookmarks, replicated over three nodes using Maelstrom's linearizable
// key/value store (lin-kv).
//
// Each journal is stored under the lin-kv key "jl:<journal>" as a JSON array
// of entries: the entry at index i lives at position i (positions start at 0
// and only ever increase). Appends extend the array with a compare-and-swap,
// so every position in a journal is claimed by exactly one entry, and an
// append is only acknowledged once the store has accepted it: from then on
// every node agrees on what lives at that position.
//
// Bookmarks are stored under "jb:<journal>" and only ever move forwards (the
// store keeps the maximum position any reader has reported).
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"sync"

	m "github.com/jepsen-io/maelstrom/demo/go"
)

const (
	jPrefix    = "jl:"
	bPrefix    = "jb:"
	workerCap  = 24
	maxRetries = 500
)

var (
	node *m.Node
	kv   *m.KV

	// journal name -> last known entry list. The true list only ever grows,
	// so a cached list is always a prefix of the real one; a stale cache just
	// makes a CAS fail, after which we re-read.
	jMu    sync.Mutex
	jCache = make(map[string][]any)

	// journal name -> last known bookmark. Bookmarks only ever grow, so a
	// cached value is a lower bound on the real one.
	bMu    sync.Mutex
	bCache = make(map[string]int)
)

func debugf(format string, args ...any) {
	if os.Getenv("NODE_DEBUG") != "" {
		fmt.Fprintf(os.Stderr, format+"\n", args...)
	}
}

// ---------------------------------------------------------------- journals

func jCacheGet(j string) ([]any, bool) {
	jMu.Lock()
	defer jMu.Unlock()
	v, ok := jCache[j]
	return v, ok
}

func jCachePut(j string, v []any) {
	jMu.Lock()
	defer jMu.Unlock()
	jCache[j] = v
}

func jCacheDel(j string) {
	jMu.Lock()
	defer jMu.Unlock()
	delete(jCache, j)
}

// readJournal returns the current entries of a journal, or ok=false if the
// journal has never been written to.
func readJournal(ctx context.Context, journal string) ([]any, bool, error) {
	v, err := kv.Read(ctx, jPrefix+journal)
	if err != nil {
		if m.ErrorCode(err) == m.KeyDoesNotExist {
			return nil, false, nil
		}
		return nil, false, err
	}
	s, ok := v.([]any)
	if !ok {
		return nil, false, fmt.Errorf("journal %q holds a non-list value %v", journal, v)
	}
	return s, true, nil
}

// appendEntry appends entry to the journal and returns its position.
func appendEntry(ctx context.Context, journal string, entry any) (int, error) {
	key := jPrefix + journal
	for attempt := 0; attempt < maxRetries; attempt++ {
		cur, have := jCacheGet(journal)
		if !have {
			s, exists, err := readJournal(ctx, journal)
			if err != nil {
				return 0, err
			}
			if exists {
				cur = s
			} else {
				cur = []any{}
			}
			jCachePut(journal, cur)
		}

		next := make([]any, len(cur)+1)
		copy(next, cur)
		next[len(cur)] = entry

		var err error
		if len(cur) == 0 {
			// The journal probably doesn't exist yet: create it holding just
			// this entry. If it does exist, the CAS fails (nil != real value)
			// and we retry with a fresh read.
			err = kv.CompareAndSwap(ctx, key, nil, next, true)
		} else {
			err = kv.CompareAndSwap(ctx, key, cur, next, false)
		}
		if err == nil {
			jCachePut(journal, next)
			return len(next) - 1, nil
		}

		// Our picture of the journal was out of date: drop it and try again.
		jCacheDel(journal)
		if code := m.ErrorCode(err); code != m.PreconditionFailed && code != m.KeyDoesNotExist {
			return 0, err
		}
	}
	return 0, fmt.Errorf("append to journal %q: gave up after %d attempts", journal, maxRetries)
}

// --------------------------------------------------------------- bookmarks

func bCacheGet(j string) (int, bool) {
	bMu.Lock()
	defer bMu.Unlock()
	v, ok := bCache[j]
	return v, ok
}

func bCachePut(j string, v int) {
	bMu.Lock()
	defer bMu.Unlock()
	bCache[j] = v
}

func bCacheDel(j string) {
	bMu.Lock()
	defer bMu.Unlock()
	delete(bCache, j)
}

// readBookmark returns the current bookmark for a journal, or ok=false.
func readBookmark(ctx context.Context, journal string) (int, bool, error) {
	v, err := kv.ReadInt(ctx, bPrefix+journal)
	if err != nil {
		if m.ErrorCode(err) == m.KeyDoesNotExist {
			return 0, false, nil
		}
		return 0, false, err
	}
	bCachePut(journal, v)
	return v, true, nil
}

// setBookmark records that a reader has read up to position pos. The stored
// bookmark never moves backwards.
func setBookmark(ctx context.Context, journal string, pos int) error {
	key := bPrefix + journal
	for attempt := 0; attempt < maxRetries; attempt++ {
		cur, have := bCacheGet(journal)
		if !have {
			v, exists, err := readBookmark(ctx, journal)
			if err != nil {
				return err
			}
			if exists {
				cur, have = v, true
			}
		}
		if have && pos <= cur {
			return nil // someone has already read at least this far
		}

		var err error
		if have {
			err = kv.CompareAndSwap(ctx, key, cur, pos, false)
		} else {
			err = kv.CompareAndSwap(ctx, key, nil, pos, true)
		}
		if err == nil {
			bCachePut(journal, pos)
			return nil
		}

		bCacheDel(journal)
		if code := m.ErrorCode(err); code != m.PreconditionFailed && code != m.KeyDoesNotExist {
			return err
		}
	}
	return fmt.Errorf("bookmark journal %q: gave up after %d attempts", journal, maxRetries)
}

// ----------------------------------------------------------------- helpers

// forEach runs fn over items with a bounded number of goroutines, returning
// the first error it produced.
func forEach[T any](items []T, fn func(T) error) error {
	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		firstErr error
	)
	sem := make(chan struct{}, workerCap)
	for _, it := range items {
		wg.Add(1)
		go func(it T) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			if err := fn(it); err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				mu.Unlock()
			}
		}(it)
	}
	wg.Wait()
	return firstErr
}

// ---------------------------------------------------------------- handlers

func handleAppend(msg m.Message) error {
	pace()
	var body struct {
		Type    string `json:"type"`
		MsgID   int    `json:"msg_id"`
		Journal string `json:"journal"`
		Entry   any    `json:"entry"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	ctx := context.Background()
	pos, err := appendEntry(ctx, body.Journal, body.Entry)
	if err != nil {
		return err
	}
	debugf("append %s %v -> %d", body.Journal, body.Entry, pos)
	return node.Reply(msg, map[string]any{"type": "appended", "position": pos})
}

func handleFetch(msg m.Message) error {
	pace()
	var body struct {
		Type      string         `json:"type"`
		MsgID     int            `json:"msg_id"`
		Positions map[string]int `json:"positions"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	ctx := context.Background()

	journals := make([]string, 0, len(body.Positions))
	for j := range body.Positions {
		journals = append(journals, j)
	}

	index := make(map[string]int, len(journals))
	for i, j := range journals {
		index[j] = i
	}
	pairs := make([][]any, len(journals))
	if err := forEach(journals, func(j string) error {
		start := body.Positions[j]
		if start < 0 {
			start = 0
		}
		cur, _, err := readJournal(ctx, j)
		if err != nil {
			return err
		}
		jCachePut(j, cur)
		var out []any
		for p := start; p < len(cur); p++ {
			out = append(out, []any{p, cur[p]})
		}
		pairs[index[j]] = out
		return nil
	}); err != nil {
		return err
	}

	entries := make(map[string][]any, len(journals))
	for i, j := range journals {
		if len(pairs[i]) > 0 {
			entries[j] = pairs[i]
		}
	}
	debugf("fetch %v -> %d journals", body.Positions, len(entries))
	return node.Reply(msg, map[string]any{"type": "fetched", "entries": entries})
}

func handleBookmark(msg m.Message) error {
	pace()
	var body struct {
		Type      string         `json:"type"`
		MsgID     int            `json:"msg_id"`
		Positions map[string]int `json:"positions"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	ctx := context.Background()

	type jp struct {
		journal string
		pos     int
	}
	items := make([]jp, 0, len(body.Positions))
	for j, p := range body.Positions {
		items = append(items, jp{j, p})
	}
	if err := forEach(items, func(it jp) error {
		return setBookmark(ctx, it.journal, it.pos)
	}); err != nil {
		return err
	}
	debugf("bookmark %v", body.Positions)
	return node.Reply(msg, map[string]any{"type": "bookmarked"})
}

func handleBookmarks(msg m.Message) error {
	pace()
	var body struct {
		Type     string   `json:"type"`
		MsgID    int      `json:"msg_id"`
		Journals []string `json:"journals"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	ctx := context.Background()

	positions := make(map[string]int, len(body.Journals))
	var mu sync.Mutex
	if err := forEach(body.Journals, func(j string) error {
		v, exists, err := readBookmark(ctx, j)
		if err != nil {
			return err
		}
		if exists {
			mu.Lock()
			positions[j] = v
			mu.Unlock()
		}
		return nil
	}); err != nil {
		return err
	}
	debugf("bookmarks %v -> %v", body.Journals, positions)
	return node.Reply(msg, map[string]any{"type": "bookmarks_are", "positions": positions})
}

func main() {
	// Maelstrom logs every message we write to stderr; that is far too noisy
	// at a thousand requests a second.
	log.SetOutput(io.Discard)

	node = m.NewNode()
	kv = m.NewLinKV(node)

	node.Handle("append", handleAppend)
	node.Handle("fetch", handleFetch)
	node.Handle("bookmark", handleBookmark)
	node.Handle("bookmarks", handleBookmarks)

	if err := node.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "fatal: %v\n", err)
		os.Exit(1)
	}
}
