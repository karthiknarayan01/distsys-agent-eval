package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
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

const (
	// Chunks keep individual CAS values small while allowing appends to
	// remain atomic.  A chunk is immutable once it becomes full.
	chunkSize = 64

	bookmarkKey = "maelstrom-journal-bookmarks-v1"
	requestTTL  = 4500 * time.Millisecond
)

type positionEntry [2]int64

type appendRequest struct {
	Type    string `json:"type"`
	Journal string `json:"journal"`
	Entry   int64  `json:"entry"`
}

type appendedResponse struct {
	Type     string `json:"type"`
	Position int64  `json:"position"`
}

type fetchRequest struct {
	Type      string           `json:"type"`
	Positions map[string]int64 `json:"positions"`
}

type fetchedResponse struct {
	Type    string                     `json:"type"`
	Entries map[string][]positionEntry `json:"entries"`
}

type bookmarkRequest struct {
	Type      string           `json:"type"`
	Positions map[string]int64 `json:"positions"`
}

type bookmarkedResponse struct {
	Type string `json:"type"`
}

type bookmarksRequest struct {
	Type     string   `json:"type"`
	Journals []string `json:"journals"`
}

type bookmarksResponse struct {
	Type      string           `json:"type"`
	Positions map[string]int64 `json:"positions"`
}

type server struct {
	node *maelstrom.Node
	kv   *maelstrom.KV

	// tails remembers the chunk in which the next append is likely to occur.
	// It is only a hint: every append still reads and CASes the chunk itself,
	// so a stale hint cannot compromise correctness.
	tailMu sync.Mutex
	tails  map[string]int64
}

func main() {
	// The Maelstrom library logs every message at info level.  That is very
	// noisy for this workload and has no effect on protocol behavior.
	if os.Getenv("NODE_DEBUG") == "" {
		log.SetOutput(io.Discard)
	}

	node := maelstrom.NewNode()
	s := &server{
		node:  node,
		kv:    maelstrom.NewLinKV(node),
		tails: make(map[string]int64),
	}

	node.Handle("append", s.handleAppend)
	node.Handle("fetch", s.handleFetch)
	node.Handle("bookmark", s.handleBookmark)
	node.Handle("bookmarks", s.handleBookmarks)

	if err := node.Run(); err != nil {
		log.Printf("node exited: %v", err)
		os.Exit(1)
	}
}

func requestContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), requestTTL)
}

func (s *server) handleAppend(msg maelstrom.Message) error {
	var req appendRequest
	if err := decodeBody(msg, &req); err != nil {
		return err
	}

	ctx, cancel := requestContext()
	defer cancel()

	position, err := s.appendEntry(ctx, req.Journal, req.Entry)
	if err != nil {
		return err
	}
	return s.node.Reply(msg, appendedResponse{
		Type:     "appended",
		Position: position,
	})
}

func (s *server) handleFetch(msg maelstrom.Message) error {
	var req fetchRequest
	if err := decodeBody(msg, &req); err != nil {
		return err
	}

	ctx, cancel := requestContext()
	defer cancel()

	entries, err := s.fetchEntries(ctx, req.Positions)
	if err != nil {
		return err
	}
	return s.node.Reply(msg, fetchedResponse{
		Type:    "fetched",
		Entries: entries,
	})
}

func (s *server) handleBookmark(msg maelstrom.Message) error {
	var req bookmarkRequest
	if err := decodeBody(msg, &req); err != nil {
		return err
	}

	ctx, cancel := requestContext()
	defer cancel()
	if err := s.updateBookmarks(ctx, req.Positions); err != nil {
		return err
	}
	return s.node.Reply(msg, bookmarkedResponse{Type: "bookmarked"})
}

func (s *server) handleBookmarks(msg maelstrom.Message) error {
	var req bookmarksRequest
	if err := decodeBody(msg, &req); err != nil {
		return err
	}

	ctx, cancel := requestContext()
	defer cancel()
	all, err := s.readBookmarks(ctx)
	if err != nil {
		return err
	}

	positions := make(map[string]int64)
	for _, journal := range req.Journals {
		if position, ok := all[journal]; ok {
			positions[journal] = position
		}
	}
	return s.node.Reply(msg, bookmarksResponse{
		Type:      "bookmarks_are",
		Positions: positions,
	})
}

// appendEntry appends entry to journal and returns its unique position.  The
// CAS on a chunk is the linearization point for the append.
func (s *server) appendEntry(ctx context.Context, journal string, entry int64) (int64, error) {
	base := s.tailHint(journal)

	for {
		chunk, exists, err := s.readChunk(ctx, journal, base)
		if err != nil {
			return 0, err
		}

		if !exists {
			old := []int64{}
			next := []int64{entry}
			err := s.kv.CompareAndSwap(ctx, chunkKey(journal, base), old, next, true)
			if err == nil {
				s.setTailHint(journal, base)
				return base, nil
			}
			if maelstrom.ErrorCode(err) == maelstrom.PreconditionFailed {
				// Another appender created this chunk (or the CAS raced
				// with one).  Read it again and retry.
				continue
			}
			return 0, err
		}

		if len(chunk) >= chunkSize {
			base += chunkSize
			s.setTailHint(journal, base)
			continue
		}

		next := make([]int64, len(chunk)+1)
		copy(next, chunk)
		next[len(chunk)] = entry
		err = s.kv.CompareAndSwap(ctx, chunkKey(journal, base), chunk, next, false)
		if err == nil {
			position := base + int64(len(chunk))
			if len(next) == chunkSize {
				s.setTailHint(journal, base+chunkSize)
			} else {
				s.setTailHint(journal, base)
			}
			return position, nil
		}
		if maelstrom.ErrorCode(err) == maelstrom.PreconditionFailed {
			// A concurrent append won.  Re-read this chunk so that the
			// position we return is based on its current length.
			continue
		}
		return 0, err
	}
}

// fetchEntries returns, for each journal, all available entries at or after
// the requested position.  Journals are fetched concurrently, while chunks
// within one journal are read in order.
func (s *server) fetchEntries(ctx context.Context, requested map[string]int64) (map[string][]positionEntry, error) {
	result := make(map[string][]positionEntry, len(requested))
	if len(requested) == 0 {
		return result, nil
	}

	type fetchResult struct {
		journal string
		entries []positionEntry
		err     error
	}
	results := make(chan fetchResult, len(requested))
	var wg sync.WaitGroup

	for journal, position := range requested {
		wg.Add(1)
		go func(journal string, position int64) {
			defer wg.Done()
			entries, err := s.fetchJournal(ctx, journal, position)
			results <- fetchResult{journal: journal, entries: entries, err: err}
		}(journal, position)
	}
	wg.Wait()
	close(results)

	for item := range results {
		if item.err != nil {
			return nil, item.err
		}
		if len(item.entries) != 0 {
			result[item.journal] = item.entries
		}
	}
	return result, nil
}

func (s *server) fetchJournal(ctx context.Context, journal string, position int64) ([]positionEntry, error) {
	if position < 0 {
		position = 0
	}

	base := (position / chunkSize) * chunkSize
	chunk, exists, err := s.readChunk(ctx, journal, base)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, nil
	}

	index := int(position - base)
	if index >= len(chunk) {
		// The requested position is beyond the end of a non-full tail
		// chunk.  Since chunks are created densely and in order, there
		// cannot be a later chunk.
		return nil, nil
	}

	entries := make([]positionEntry, 0, len(chunk)-index)
	for i := index; i < len(chunk); i++ {
		entries = append(entries, positionEntry{base + int64(i), chunk[i]})
	}

	// A full chunk may be followed by more chunks.  A partial chunk is the
	// current tail, so there is nothing after it.
	base += chunkSize
	for len(chunk) == chunkSize {
		chunk, exists, err = s.readChunk(ctx, journal, base)
		if err != nil {
			return nil, err
		}
		if !exists {
			break
		}
		for i, entry := range chunk {
			entries = append(entries, positionEntry{base + int64(i), entry})
		}
		base += chunkSize
	}
	return entries, nil
}

func (s *server) readChunk(ctx context.Context, journal string, base int64) ([]int64, bool, error) {
	var chunk []int64
	err := s.kv.ReadInto(ctx, chunkKey(journal, base), &chunk)
	if err != nil {
		if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
			return nil, false, nil
		}
		return nil, false, err
	}
	if chunk == nil {
		chunk = []int64{}
	}
	return chunk, true, nil
}

// updateBookmarks advances each requested bookmark monotonically.  A CAS
// loop prevents concurrent requests on different nodes from losing an
// update.
func (s *server) updateBookmarks(ctx context.Context, requested map[string]int64) error {
	if len(requested) == 0 {
		return nil
	}

	for {
		current, err := s.readBookmarks(ctx)
		if err != nil {
			return err
		}

		next := make(map[string]int64, len(current)+len(requested))
		for journal, position := range current {
			next[journal] = position
		}
		for journal, position := range requested {
			if old, ok := next[journal]; !ok || position > old {
				next[journal] = position
			}
		}

		err = s.kv.CompareAndSwap(ctx, bookmarkKey, current, next, true)
		if err == nil {
			return nil
		}
		if maelstrom.ErrorCode(err) != maelstrom.PreconditionFailed {
			return err
		}
	}
}

func (s *server) readBookmarks(ctx context.Context) (map[string]int64, error) {
	bookmarks := make(map[string]int64)
	err := s.kv.ReadInto(ctx, bookmarkKey, &bookmarks)
	if err != nil {
		if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
			return make(map[string]int64), nil
		}
		return nil, err
	}
	if bookmarks == nil {
		bookmarks = make(map[string]int64)
	}
	return bookmarks, nil
}

func (s *server) tailHint(journal string) int64 {
	s.tailMu.Lock()
	defer s.tailMu.Unlock()
	return s.tails[journal]
}

func (s *server) setTailHint(journal string, base int64) {
	s.tailMu.Lock()
	defer s.tailMu.Unlock()
	s.tails[journal] = base
}

func chunkKey(journal string, base int64) string {
	sum := sha256.Sum256([]byte(journal))
	return "maelstrom-journal-" + hex.EncodeToString(sum[:]) + "-" + strconv.FormatInt(base, 10)
}

func decodeBody(msg maelstrom.Message, target any) error {
	if err := json.Unmarshal(msg.Body, target); err != nil {
		return fmt.Errorf("decode request body: %w", err)
	}
	return nil
}
