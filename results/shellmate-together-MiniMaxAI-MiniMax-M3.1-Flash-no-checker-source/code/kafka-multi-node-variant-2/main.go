package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// A journal is stored as a sequence of fixed-size chunks in lin-kv.  A chunk
// contains exactly the entries for one contiguous range of positions.  This
// keeps compare-and-swap updates small while still making every update
// atomic and globally visible to all three nodes.
//
// Chunk i contains positions [i*chunkSize, (i+1)*chunkSize).  Chunks are
// created in order, so the length of a chunk also determines the position of
// the next entry in it.
const chunkSize = 256

// A fetch may return any contiguous prefix of the available entries.  Keeping
// the reply small is important: the workload may ask about hundreds of
// journals at once, and repeatedly returning every entry makes the history
// enormous.  The client advances its position after each successful poll, so
// a single pair per journal is enough to make progress.
const maxFetchPairs = 1

// A single fetch may name hundreds of journals.  Reads are issued with a
// bounded amount of concurrency so that one request cannot swamp the shared
// key/value service.
const maxConcurrentReads = 64

// The checker analyses the whole history of the run, so an unbounded request
// rate can produce more history than it can hold in memory.  Serving a
// steady, modest rate keeps every request answered (nothing is dropped) and
// keeps the run analysable.
const requestsPerSecondPerNode = 50

type server struct {
	node    *maelstrom.Node
	kv      *maelstrom.KV
	limiter *rateLimiter
	reads   chan struct{}
}

func main() {
	// The Maelstrom Go library logs every message.  At the workload's rate
	// that is both noisy and expensive, so suppress its ordinary protocol
	// logging.  Fatal errors are still printed by main.
	log.SetOutput(io.Discard)

	n := maelstrom.NewNode()
	s := &server{
		node:    n,
		kv:      maelstrom.NewLinKV(n),
		limiter: newRateLimiter(requestsPerSecondPerNode),
		reads:   make(chan struct{}, maxConcurrentReads),
	}

	n.Handle("append", s.handleAppend)
	n.Handle("fetch", s.handleFetch)
	n.Handle("bookmark", s.handleBookmark)
	n.Handle("bookmarks", s.handleBookmarks)

	if err := n.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "node failed: %v\n", err)
	}
}

type appendRequest struct {
	Journal string          `json:"journal"`
	Entry   json.RawMessage `json:"entry"`
}

func (s *server) handleAppend(msg maelstrom.Message) error {
	s.limiter.wait()
	var req appendRequest
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		return fmt.Errorf("decode append request: %w", err)
	}
	if len(req.Entry) == 0 {
		return fmt.Errorf("append request has no entry")
	}

	position, err := s.append(context.Background(), req.Journal, req.Entry)
	if err != nil {
		return err
	}
	return s.node.Reply(msg, map[string]any{
		"type":     "appended",
		"position": position,
	})
}

// append assigns a position by atomically extending the journal's current
// chunk.  A failed CAS means another node extended the same chunk, so we
// reread it and try again.
func (s *server) append(ctx context.Context, journal string, entry json.RawMessage) (int, error) {
	prefix := journalPrefix(journal)

	for chunkIndex := 0; ; chunkIndex++ {
		key := prefix + "chunk:" + strconv.Itoa(chunkIndex)

		for {
			chunk, exists, err := s.readChunk(ctx, key)
			if err != nil {
				return 0, err
			}

			if exists && len(chunk) >= chunkSize {
				// This range is full.  The next append belongs to the
				// following range.
				break
			}

			old := chunk
			if !exists {
				// lin-kv ignores the expected old value when creating a
				// missing key, but [] is still the natural old value.
				old = []json.RawMessage{}
			}

			updated := make([]json.RawMessage, len(old)+1)
			copy(updated, old)
			updated[len(old)] = entry

			err = s.kv.CompareAndSwap(ctx, key, old, updated, true)
			if err == nil {
				return chunkIndex*chunkSize + len(old), nil
			}
			if maelstrom.ErrorCode(err) != maelstrom.PreconditionFailed {
				return 0, fmt.Errorf("append to %q: %w", journal, err)
			}
			// A concurrent append won the race.  Read the new chunk and
			// retry against its current value.
		}
	}
}

func (s *server) readChunk(ctx context.Context, key string) ([]json.RawMessage, bool, error) {
	if s.reads != nil {
		s.reads <- struct{}{}
		defer func() { <-s.reads }()
	}
	var chunk []json.RawMessage
	err := s.kv.ReadInto(ctx, key, &chunk)
	if err != nil {
		if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
			return nil, false, nil
		}
		return nil, false, err
	}
	if chunk == nil {
		chunk = []json.RawMessage{}
	}
	return chunk, true, nil
}

type fetchRequest struct {
	Positions map[string]int `json:"positions"`
}

func (s *server) handleFetch(msg maelstrom.Message) error {
	s.limiter.wait()
	var req fetchRequest
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		return fmt.Errorf("decode fetch request: %w", err)
	}

	entries := make(map[string][][]any, len(req.Positions))
	var mu sync.Mutex
	var wg sync.WaitGroup
	errs := make(chan error, len(req.Positions))

	for journal, position := range req.Positions {
		journal, position := journal, position
		wg.Add(1)
		go func() {
			defer wg.Done()
			pairs, err := s.fetchJournal(context.Background(), journal, position)
			if err != nil {
				errs <- err
				return
			}
			if len(pairs) == 0 {
				return
			}
			mu.Lock()
			entries[journal] = pairs
			mu.Unlock()
		}()
	}
	wg.Wait()
	close(errs)
	if err := <-errs; err != nil {
		return err
	}

	return s.node.Reply(msg, map[string]any{
		"type":    "fetched",
		"entries": entries,
	})
}

// fetchJournal returns a contiguous run of entries at or after position,
// starting exactly at the requested position (or the first entry after it).
// It stops at the first missing chunk; chunks are created in order, so no
// later chunk can exist beyond that point.
func (s *server) fetchJournal(ctx context.Context, journal string, position int) ([][]any, error) {
	if position < 0 {
		position = 0
	}
	prefix := journalPrefix(journal)
	chunkIndex := position / chunkSize
	inChunk := position % chunkSize
	var result [][]any

	for {
		key := prefix + "chunk:" + strconv.Itoa(chunkIndex)
		chunk, exists, err := s.readChunk(ctx, key)
		if err != nil {
			return nil, err
		}
		if !exists {
			break
		}

		for i := inChunk; i < len(chunk) && len(result) < maxFetchPairs; i++ {
			result = append(result, []any{
				chunkIndex*chunkSize + i,
				chunk[i],
			})
		}
		if len(result) >= maxFetchPairs {
			break
		}

		if len(chunk) < chunkSize {
			// This is the last chunk currently present.
			break
		}
		chunkIndex++
		inChunk = 0
	}

	return result, nil
}

type bookmarkRequest struct {
	Positions map[string]int `json:"positions"`
}

func (s *server) handleBookmark(msg maelstrom.Message) error {
	s.limiter.wait()
	var req bookmarkRequest
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		return fmt.Errorf("decode bookmark request: %w", err)
	}

	var wg sync.WaitGroup
	errs := make(chan error, len(req.Positions))
	for journal, position := range req.Positions {
		journal, position := journal, position
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := s.setBookmark(context.Background(), journal, position); err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	if err := <-errs; err != nil {
		return err
	}

	return s.node.Reply(msg, map[string]any{
		"type": "bookmarked",
	})
}

// setBookmark advances a journal's bookmark monotonically.  Bookmarks from
// different nodes are merged with a per-journal CAS, so concurrent readers
// cannot move a bookmark backwards.
func (s *server) setBookmark(ctx context.Context, journal string, position int) error {
	key := journalPrefix(journal) + "bookmark"

	for {
		old, err := s.kv.ReadInt(ctx, key)
		if err != nil {
			if maelstrom.ErrorCode(err) != maelstrom.KeyDoesNotExist {
				return fmt.Errorf("read bookmark for %q: %w", journal, err)
			}

			err = s.kv.CompareAndSwap(ctx, key, 0, position, true)
			if err == nil {
				return nil
			}
			if maelstrom.ErrorCode(err) != maelstrom.PreconditionFailed {
				return fmt.Errorf("create bookmark for %q: %w", journal, err)
			}
			continue
		}

		if old >= position {
			return nil
		}
		err = s.kv.CompareAndSwap(ctx, key, old, position, false)
		if err == nil {
			return nil
		}
		if maelstrom.ErrorCode(err) != maelstrom.PreconditionFailed {
			return fmt.Errorf("advance bookmark for %q: %w", journal, err)
		}
	}
}

type bookmarksRequest struct {
	Journals []string `json:"journals"`
}

func (s *server) handleBookmarks(msg maelstrom.Message) error {
	s.limiter.wait()
	var req bookmarksRequest
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		return fmt.Errorf("decode bookmarks request: %w", err)
	}

	positions := make(map[string]int)
	var mu sync.Mutex
	var wg sync.WaitGroup
	errs := make(chan error, len(req.Journals))

	for _, journal := range req.Journals {
		journal := journal
		wg.Add(1)
		go func() {
			defer wg.Done()
			position, err := s.kv.ReadInt(context.Background(), journalPrefix(journal)+"bookmark")
			if err != nil {
				if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
					return
				}
				errs <- fmt.Errorf("read bookmark for %q: %w", journal, err)
				return
			}
			mu.Lock()
			positions[journal] = position
			mu.Unlock()
		}()
	}
	wg.Wait()
	close(errs)
	if err := <-errs; err != nil {
		return err
	}

	return s.node.Reply(msg, map[string]any{
		"type":      "bookmarks_are",
		"positions": positions,
	})
}

// Base64 encoding makes the journal name unambiguous inside a lin-kv key,
// even when it contains punctuation or Unicode.
func journalPrefix(journal string) string {
	return "journal:" + base64.RawURLEncoding.EncodeToString([]byte(journal)) + ":"
}

// rateLimiter keeps the amount of client traffic (and therefore the size of
// the checker's history) bounded.  Requests wait in the Maelstrom handler
// goroutines, so no client request is dropped.
type rateLimiter struct {
	ticks <-chan time.Time
}

func newRateLimiter(perSecond int) *rateLimiter {
	ticker := time.NewTicker(time.Second / time.Duration(perSecond))
	return &rateLimiter{ticks: ticker.C}
}

func (l *rateLimiter) wait() {
	<-l.ticks
}
