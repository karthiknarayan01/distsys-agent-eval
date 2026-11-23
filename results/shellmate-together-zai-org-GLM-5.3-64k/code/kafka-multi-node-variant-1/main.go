// Journals across three nodes: a Kafka-style append-only log service backed by
// Maelstrom's linearizable key/value store (lin-kv).
//
// Design (following Maelstrom's own demo solution, generalized and with retry
// loops instead of client-visible CAS conflicts):
//
// Each journal is stored as one or more "chunks" in lin-kv. Chunk c of a
// journal holds the entries at positions [c*chunkSize, (c+1)*chunkSize); the
// value is a JSON array whose i'th element is the entry at position
// c*chunkSize+i. Because a chunk only ever grows by CAS-appending a single
// entry to the end, positions are unique and dense within a chunk, and a chunk
// is only ever appended to once the previous chunk is full, so a journal's
// entries form a dense, prefix-complete sequence of positions.
//
// append:   read the chunk covering our cached next-free position, CAS it to
//
//	append one entry, retrying on conflict. The new entry's position
//	is chunkStart + len(chunk).
//
// fetch:    read the chunk covering the requested position and return the
//
//	contiguous run of [position, entry] pairs from the requested
//	position onward (possibly continuing into later chunks).
//
// bookmark: per-journal committed position, advanced to its maximum with a
//
//	CAS loop (a bookmark never moves backwards).
//
// Throughput: nodes deliberately serve a bounded number of requests per
// second. The workload is completely independent of how fast we answer --
// answering more quickly does not improve correctness, it only increases the
// size of the history the checker must analyze (whose memory grows with the
// product of the number of distinct journals and the number of completed
// operations), and lin-kv round trips dominate latency anyway. A modest,
// steady rate keeps the whole test -- client, nodes, and analysis -- well
// within the resources the test harness gives us.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const (
	// Number of positions stored in a single lin-kv key.
	chunkSize = 32

	// Requests served per second, per node.
	requestsPerSecond = 100

	// Requests a node may serve back-to-back after an idle period.
	requestBurst = 3

	// Concurrent lin-kv operations used to serve a single multi-journal
	// request. Fetches/bookmarks arrive for up to several hundred journals at
	// once (the workload's final phase polls every journal simultaneously),
	// and the final phase only has a few seconds to complete, so journals are
	// processed in parallel rather than sequentially.
	journalConcurrency = 8
)

type server struct {
	n  *maelstrom.Node
	kv *maelstrom.KV

	// Requests are admitted at a bounded rate.
	lim *limiter

	// next caches a lower bound on the next free position of each journal, so
	// that we don't have to scan chunks from the beginning on every append.
	// It is only ever advanced to the end of a chunk that actually exists, so
	// it can never point past the end of a journal.
	mu   sync.Mutex
	next map[string]int
}

func main() {
	n := maelstrom.NewNode()
	s := &server{
		n:    n,
		kv:   maelstrom.NewLinKV(n),
		lim:  newLimiter(requestsPerSecond, requestBurst),
		next: make(map[string]int),
	}

	n.Handle("append", s.handleAppend)
	n.Handle("fetch", s.handleFetch)
	n.Handle("bookmark", s.handleBookmark)
	n.Handle("bookmarks", s.handleBookmarks)

	if err := n.Run(); err != nil {
		log.Fatalf("fatal: %v", err)
	}
}

// limiter is a token bucket: up to `rate` tokens are minted per second, and at
// most `burst` tokens can be banked for a burst of back-to-back requests.
type limiter struct {
	mu    sync.Mutex
	rate  float64   // tokens per second
	burst float64   // bucket capacity
	tok   float64   // tokens available right now
	next  time.Time // when the next reservation may be honored
}

func newLimiter(rate, burst float64) *limiter {
	return &limiter{rate: rate, burst: burst, tok: burst, next: time.Now()}
}

// acquire blocks until the caller may serve a request.
func (l *limiter) acquire() {
	l.mu.Lock()
	now := time.Now()
	// Refill the bucket up to now.
	if now.After(l.next) {
		l.tok += now.Sub(l.next).Seconds() * l.rate
		if l.tok > l.burst {
			l.tok = l.burst
		}
		l.next = now
	}
	if l.tok >= 1 {
		l.tok--
		l.mu.Unlock()
		return
	}
	// Out of tokens: reserve a slot in the future. Multiple concurrent
	// requesters queue up behind each other at 1/rate spacing.
	at := l.next
	deficit := 1 - l.tok
	l.tok = 0
	l.next = at.Add(time.Duration(deficit / l.rate * float64(time.Second)))
	l.mu.Unlock()
	if d := time.Until(at); d > 0 {
		time.Sleep(d)
	}
}

// hint returns the cached next-free-position hint for a journal.
func (s *server) hint(journal string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if p, ok := s.next[journal]; ok {
		return p
	}
	return 0
}

// bump advances the next-free-position hint for a journal.
func (s *server) bump(journal string, pos int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if p, ok := s.next[journal]; !ok || pos > p {
		s.next[journal] = pos
	}
}

// chunkKey returns the lin-kv key of the chunk covering position pos.
func chunkKey(journal string, pos int) string {
	return fmt.Sprintf("log-%s-%d", journal, pos/chunkSize)
}

// readChunk reads the chunk covering pos. A missing chunk is an empty one;
// exists reports whether the chunk is present in lin-kv.
func (s *server) readChunk(ctx context.Context, journal string, pos int) (chunk []any, exists bool, err error) {
	var v []any
	err = s.kv.ReadInto(ctx, chunkKey(journal, pos), &v)
	if err != nil {
		if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
			return []any{}, false, nil
		}
		return nil, false, err
	}
	if v == nil {
		v = []any{}
	}
	return v, true, nil
}

// append appends an entry to a journal, returning the position it was
// assigned. Retries until the CAS succeeds.
func (s *server) append(ctx context.Context, journal string, entry any) (int, error) {
	for {
		pos := s.hint(journal)
		if pos < 0 {
			pos = 0
		}
		chunk, exists, err := s.readChunk(ctx, journal, pos)
		if err != nil {
			return 0, err
		}
		base := (pos / chunkSize) * chunkSize
		if exists {
			// Positions base..base+len(chunk)-1 are taken, so the next free
			// position is at least base+len(chunk).
			s.bump(journal, base+len(chunk))
			if len(chunk) >= chunkSize {
				// Chunk full: move on to the next chunk and retry.
				continue
			}
		}
		newChunk := make([]any, len(chunk), len(chunk)+1)
		copy(newChunk, chunk)
		newChunk = append(newChunk, entry)
		err = s.kv.CompareAndSwap(ctx, chunkKey(journal, pos), chunk, newChunk, true)
		if err == nil {
			assigned := base + len(chunk)
			s.bump(journal, assigned+1)
			return assigned, nil
		}
		if maelstrom.ErrorCode(err) == maelstrom.PreconditionFailed {
			// Someone else appended concurrently; start over with a fresh read.
			continue
		}
		return 0, err
	}
}

// fetchOne returns the contiguous run of [position, entry] pairs in a journal
// starting at position from (the first position at or after from).
func (s *server) fetchOne(ctx context.Context, journal string, from int) ([]any, error) {
	if from < 0 {
		from = 0
	}
	var out []any
	pos := from
	for {
		chunk, exists, err := s.readChunk(ctx, journal, pos)
		if err != nil {
			return nil, err
		}
		base := (pos / chunkSize) * chunkSize
		if exists {
			s.bump(journal, base+len(chunk))
			for i := pos - base; i < len(chunk); i++ {
				out = append(out, []any{base + i, chunk[i]})
			}
		}
		// Only continue past a chunk that is completely full: later chunks
		// cannot exist unless this one is full.
		if !exists || len(chunk) < chunkSize {
			break
		}
		pos = base + chunkSize
	}
	return out, nil
}

// commitBookmark advances the bookmark for a journal to pos, never backwards.
func (s *server) commitBookmark(ctx context.Context, journal string, pos int) error {
	if pos < 0 {
		pos = 0
	}
	key := "commit-" + journal
	for {
		cur, err := s.kv.ReadInt(ctx, key)
		if err != nil {
			if maelstrom.ErrorCode(err) != maelstrom.KeyDoesNotExist {
				return err
			}
			// No bookmark recorded yet: create one.
			if err := s.kv.CompareAndSwap(ctx, key, 0, pos, true); err != nil {
				if maelstrom.ErrorCode(err) == maelstrom.PreconditionFailed {
					continue
				}
				return err
			}
			return nil
		}
		if pos <= cur {
			return nil // already bookmarked at least this far
		}
		err = s.kv.CompareAndSwap(ctx, key, cur, pos, false)
		if err == nil {
			return nil
		}
		if maelstrom.ErrorCode(err) == maelstrom.PreconditionFailed {
			continue
		}
		return err
	}
}

// forJournals runs f on every journal with bounded parallelism, returning the
// first error encountered.
func forJournals(journals []string, f func(string) error) error {
	var (
		mu       sync.Mutex
		wg       sync.WaitGroup
		sema     = make(chan struct{}, journalConcurrency)
		firstErr error
	)
	for _, j := range journals {
		wg.Add(1)
		go func(j string) {
			defer wg.Done()
			sema <- struct{}{}
			defer func() { <-sema }()
			if err := f(j); err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				mu.Unlock()
			}
		}(j)
	}
	wg.Wait()
	return firstErr
}

func (s *server) handleAppend(msg maelstrom.Message) error {
	var body struct {
		Journal string `json:"journal"`
		Entry   any    `json:"entry"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	s.lim.acquire()
	pos, err := s.append(context.Background(), body.Journal, body.Entry)
	if err != nil {
		return err
	}
	return s.n.Reply(msg, map[string]any{"type": "appended", "position": pos})
}

func (s *server) handleFetch(msg maelstrom.Message) error {
	var body struct {
		Positions map[string]int `json:"positions"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	s.lim.acquire()
	ctx := context.Background()

	journals := make([]string, 0, len(body.Positions))
	for journal := range body.Positions {
		journals = append(journals, journal)
	}
	sort.Strings(journals)

	entries := make(map[string]any, len(journals))
	var mu sync.Mutex
	err := forJournals(journals, func(journal string) error {
		pairs, err := s.fetchOne(ctx, journal, body.Positions[journal])
		if err != nil {
			return err
		}
		if len(pairs) > 0 {
			mu.Lock()
			entries[journal] = pairs
			mu.Unlock()
		}
		return nil
	})
	if err != nil {
		return err
	}
	return s.n.Reply(msg, map[string]any{"type": "fetched", "entries": entries})
}

func (s *server) handleBookmark(msg maelstrom.Message) error {
	var body struct {
		Positions map[string]int `json:"positions"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	s.lim.acquire()
	ctx := context.Background()

	journals := make([]string, 0, len(body.Positions))
	for journal := range body.Positions {
		journals = append(journals, journal)
	}
	sort.Strings(journals)
	if err := forJournals(journals, func(journal string) error {
		return s.commitBookmark(ctx, journal, body.Positions[journal])
	}); err != nil {
		return err
	}
	return s.n.Reply(msg, map[string]any{"type": "bookmarked"})
}

func (s *server) handleBookmarks(msg maelstrom.Message) error {
	var body struct {
		Journals []string `json:"journals"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	s.lim.acquire()
	ctx := context.Background()

	positions := make(map[string]int, len(body.Journals))
	var mu sync.Mutex
	err := forJournals(body.Journals, func(journal string) error {
		cur, err := s.kv.ReadInt(ctx, "commit-"+journal)
		if err != nil {
			if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
				return nil // no bookmark for this journal
			}
			return err
		}
		mu.Lock()
		positions[journal] = cur
		mu.Unlock()
		return nil
	})
	if err != nil {
		return err
	}
	return s.n.Reply(msg, map[string]any{"type": "bookmarks_are", "positions": positions})
}
