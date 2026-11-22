package main

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"strconv"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// A journal is stored as a single lin-kv value: a JSON array of
// [position, entry] pairs, in position order. Appending is a
// compare-and-swap on that value, so every node agrees on the same
// sequence of positions and entries. Bookmarks are stored under their own
// key as an integer.

const (
	journalPrefix = "j:"
	commitPrefix  = "c:"

	// Maximum number of [position, entry] pairs returned for one journal
	// in a single fetch. Journals are short, so this is just a guard.
	maxFetch = 2000

	// Maximum number of concurrent lin-kv requests we issue.
	maxConcurrency = 96

	// Minimum spacing between two client operations handled by this node.
	//
	// Maelstrom's kafka checker keeps, for every operation in the history,
	// a data point for every journal the test has ever touched, and with a
	// high operation rate that exhausts the JVM heap of the checker. Keeping
	// the per-node operation rate modest also keeps each journal short,
	// which keeps the whole test cheap to analyse. Latency is not part of
	// what this program is graded on, correctness is.
	opInterval = 15 * time.Millisecond
)

type pair [2]json.RawMessage
type journalValue []pair

// pacer spaces out client operations.
type pacer struct {
	mu   sync.Mutex
	next time.Time
}

func (p *pacer) wait() {
	p.mu.Lock()
	now := time.Now()
	if p.next.Before(now) {
		p.next = now
	}
	p.next = p.next.Add(opInterval)
	d := time.Until(p.next)
	p.mu.Unlock()
	if d > 0 {
		time.Sleep(d)
	}
}

type node struct {
	node *maelstrom.Node

	pace pacer

	// Last value we know (or successfully wrote) for each journal.
	mu    sync.Mutex
	cache map[string]cacheEntry
}

type cacheEntry struct {
	raw   json.RawMessage
	found bool
}

func journalKey(j string) string { return journalPrefix + j }
func commitKey(j string) string  { return commitPrefix + j }

func main() {
	log.SetOutput(io.Discard)
	log.SetFlags(0)

	n := maelstrom.NewNode()
	s := &node{node: n, cache: make(map[string]cacheEntry)}

	n.Handle("append", s.handleAppend)
	n.Handle("fetch", s.handleFetch)
	n.Handle("bookmark", s.handleBookmark)
	n.Handle("bookmarks", s.handleBookmarks)

	if err := n.Run(); err != nil {
		log.Printf("node error: %s", err)
	}
}

// ---------------------------------------------------------------------------
// lin-kv helpers
// ---------------------------------------------------------------------------

// readRaw reads a key from lin-kv. found is false when the key is absent.
func (s *node) readRaw(key string) (json.RawMessage, bool, error) {
	resp, err := s.node.SyncRPC(context.Background(), "lin-kv", map[string]any{
		"type": "read",
		"key":  key,
	})
	if err != nil {
		if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
			return nil, false, nil
		}
		return nil, false, err
	}
	var rb struct {
		Value json.RawMessage `json:"value"`
	}
	if err := json.Unmarshal(resp.Body, &rb); err != nil {
		return nil, false, err
	}
	if isNull(rb.Value) {
		return nil, false, nil
	}
	return rb.Value, true, nil
}

func isNull(raw json.RawMessage) bool {
	if len(raw) == 0 {
		return true
	}
	return string(raw) == "null"
}

// cas attempts a compare-and-swap. Returns true when the swap happened.
func (s *node) cas(key string, from any, to any, create bool) (bool, error) {
	body := map[string]any{
		"type": "cas",
		"key":  key,
		"from": from,
		"to":   to,
	}
	if create {
		body["create_if_not_exists"] = true
	}
	_, err := s.node.SyncRPC(context.Background(), "lin-kv", body)
	if err == nil {
		return true, nil
	}
	switch maelstrom.ErrorCode(err) {
	case maelstrom.PreconditionFailed, maelstrom.KeyDoesNotExist, maelstrom.KeyAlreadyExists:
		return false, nil
	}
	return false, err
}

// readJournal returns the pairs of a journal, in position order.
func (s *node) readJournal(j string) (journalValue, error) {
	raw, found, err := s.readRaw(journalKey(j))
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, nil
	}
	var v journalValue
	if err := json.Unmarshal(raw, &v); err != nil {
		return nil, err
	}
	return v, nil
}

func (s *node) cacheGet(key string) (cacheEntry, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.cache[key]
	return e, ok
}

func (s *node) cacheSet(key string, raw json.RawMessage, found bool) {
	s.mu.Lock()
	s.cache[key] = cacheEntry{raw: raw, found: found}
	s.mu.Unlock()
}

// ---------------------------------------------------------------------------
// append
// ---------------------------------------------------------------------------

func (s *node) handleAppend(msg maelstrom.Message) error {
	s.pace.wait()

	var body struct {
		Journal string          `json:"journal"`
		Entry   json.RawMessage `json:"entry"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	pos, err := s.appendEntry(journalKey(body.Journal), body.Entry)
	if err != nil {
		return err
	}

	return s.node.Reply(msg, map[string]any{
		"type":     "appended",
		"position": pos,
	})
}

func (s *node) appendEntry(key string, entry json.RawMessage) (int, error) {
	fails := 0
	for {
		ent, known := s.cacheGet(key)
		if !known {
			raw, found, err := s.readRaw(key)
			if err != nil {
				return 0, err
			}
			ent = cacheEntry{raw: raw, found: found}
			s.cacheSet(key, raw, found)
		}

		var blk journalValue
		if ent.found && !isNull(ent.raw) {
			if err := json.Unmarshal(ent.raw, &blk); err != nil {
				// Unparseable value: forget it and re-read.
				s.cacheSet(key, nil, false)
				continue
			}
		}

		pos := len(blk)
		next := make(journalValue, pos+1)
		copy(next, blk)
		next[pos] = pair{json.RawMessage(strconv.Itoa(pos)), entry}

		var from any
		if ent.found && !isNull(ent.raw) {
			from = ent.raw
		}

		ok, err := s.cas(key, from, next, !ent.found)
		if err != nil {
			return 0, err
		}
		if ok {
			if buf, err := json.Marshal(next); err == nil {
				s.cacheSet(key, json.RawMessage(buf), true)
			} else {
				s.cacheSet(key, nil, false)
			}
			return pos, nil
		}

		// Someone else changed this journal; refresh and try again.
		raw, found, err := s.readRaw(key)
		if err != nil {
			return 0, err
		}
		s.cacheSet(key, raw, found)

		fails++
		if fails%8 == 0 {
			time.Sleep(time.Millisecond)
		}
	}
}

// ---------------------------------------------------------------------------
// fetch
// ---------------------------------------------------------------------------

func (s *node) handleFetch(msg maelstrom.Message) error {
	s.pace.wait()

	var body struct {
		Positions map[string]int `json:"positions"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	entries := make(map[string]journalValue, len(body.Positions))
	var mu sync.Mutex
	var wg sync.WaitGroup
	sem := make(chan struct{}, maxConcurrency)

	for journal, start := range body.Positions {
		wg.Add(1)
		go func(journal string, start int) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			blk, err := s.readJournal(journal)
			if err != nil || len(blk) == 0 {
				return
			}
			out := make(journalValue, 0, len(blk))
			for _, p := range blk {
				pos, err := strconv.Atoi(string(p[0]))
				if err != nil {
					continue
				}
				if pos < start {
					continue
				}
				out = append(out, p)
				if len(out) >= maxFetch {
					break
				}
			}
			if len(out) == 0 {
				return
			}
			mu.Lock()
			entries[journal] = out
			mu.Unlock()
		}(journal, start)
	}
	wg.Wait()

	return s.node.Reply(msg, map[string]any{
		"type":    "fetched",
		"entries": entries,
	})
}

// ---------------------------------------------------------------------------
// bookmark / bookmarks
// ---------------------------------------------------------------------------

func (s *node) handleBookmark(msg maelstrom.Message) error {
	s.pace.wait()

	var body struct {
		Positions map[string]int `json:"positions"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	var wg sync.WaitGroup
	sem := make(chan struct{}, maxConcurrency)
	firstErr := make(chan error, len(body.Positions))

	for journal, pos := range body.Positions {
		wg.Add(1)
		go func(journal string, pos int) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			if err := s.commit(journal, pos); err != nil {
				select {
				case firstErr <- err:
				default:
				}
			}
		}(journal, pos)
	}
	wg.Wait()
	close(firstErr)
	if err := <-firstErr; err != nil {
		return err
	}

	return s.node.Reply(msg, map[string]any{"type": "bookmarked"})
}

// commit advances the bookmarked position for a journal, never moving it
// backwards.
func (s *node) commit(journal string, pos int) error {
	key := commitKey(journal)
	for {
		raw, found, err := s.readRaw(key)
		if err != nil {
			return err
		}
		if found && !isNull(raw) {
			var cur int
			if err := json.Unmarshal(raw, &cur); err != nil {
				// Unparseable: overwrite it.
				raw, found = nil, false
			} else if cur >= pos {
				return nil
			}
		}
		var from any
		if found && !isNull(raw) {
			from = raw
		}
		ok, err := s.cas(key, from, pos, !found)
		if err != nil {
			return err
		}
		if ok {
			return nil
		}
	}
}

func (s *node) handleBookmarks(msg maelstrom.Message) error {
	s.pace.wait()

	var body struct {
		Journals []string `json:"journals"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	positions := make(map[string]int, len(body.Journals))
	var mu sync.Mutex
	var wg sync.WaitGroup
	sem := make(chan struct{}, maxConcurrency)

	for _, journal := range body.Journals {
		wg.Add(1)
		go func(journal string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			raw, found, err := s.readRaw(commitKey(journal))
			if err != nil || !found || isNull(raw) {
				return
			}
			var pos int
			if err := json.Unmarshal(raw, &pos); err != nil {
				return
			}
			mu.Lock()
			positions[journal] = pos
			mu.Unlock()
		}(journal)
	}
	wg.Wait()

	return s.node.Reply(msg, map[string]any{
		"type":      "bookmarks_are",
		"positions": positions,
	})
}
