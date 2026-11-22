// A Maelstrom node that implements shared journals.
//
// Every node keeps its journal state in Maelstrom's linearizable "lin-kv"
// service, so all three nodes observe exactly the same log: the same entry at
// the same position, no matter which node a client talks to.
//
// A journal is stored as a sequence of fixed-size chunks.  A chunk holds the
// entries of one contiguous range of positions and is only ever extended, so
// allocating a position and storing its entry is a single atomic
// compare-and-swap: no position can be skipped, duplicated, or lost.
package main

import (
	"context"
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
	// chunkSize is the number of entries in one chunk of a journal.
	chunkSize = 32

	// kvService is Maelstrom's linearizable key/value service.
	kvService = "lin-kv"

	// readAttempts bounds retries of a read against the KV service.
	readAttempts = 5
)

// The four client request bodies, after maelstrom-rename has translated them.
type appendRequest struct {
	Journal string `json:"journal"`
	Entry   int    `json:"entry"`
}

type fetchRequest struct {
	Positions map[string]int `json:"positions"`
}

type bookmarkRequest struct {
	Positions map[string]int `json:"positions"`
}

type bookmarksRequest struct {
	Journals []string `json:"journals"`
}

// server implements the journal operations on top of lin-kv.
type server struct {
	node *maelstrom.Node

	// Updates to one journal are serialized within a process, which keeps
	// compare-and-swap conflicts to (at most) one per other node.
	locksMu sync.Mutex
	locks   map[string]*sync.Mutex
}

func newServer(node *maelstrom.Node) *server {
	return &server{
		node:  node,
		locks: make(map[string]*sync.Mutex),
	}
}

func (s *server) lockFor(name string) *sync.Mutex {
	s.locksMu.Lock()
	defer s.locksMu.Unlock()
	lock := s.locks[name]
	if lock == nil {
		lock = new(sync.Mutex)
		s.locks[name] = lock
	}
	return lock
}

// chunkKey is the KV key holding one chunk of a journal.
func chunkKey(journal string, chunk int) string {
	return "journals:" + journal + ":chunks:" + strconv.Itoa(chunk)
}

// bookmarkKey is the KV key holding a journal's bookmark.
func bookmarkKey(journal string) string {
	return "journal-bookmarks:" + journal
}

// readChunk returns the entries stored in one chunk of a journal.  A chunk
// that does not exist yet reads as an empty chunk.
func (s *server) readChunk(journal string, chunk int) ([]int, error) {
	key := chunkKey(journal, chunk)
	var lastErr error
	for attempt := 0; attempt < readAttempts; attempt++ {
		msg, err := s.node.SyncRPC(context.Background(), kvService, map[string]any{
			"type": "read",
			"key":  key,
		})
		if err == nil {
			var body struct {
				Value []int `json:"value"`
			}
			if err := json.Unmarshal(msg.Body, &body); err != nil {
				return nil, fmt.Errorf("decode chunk %q: %w", journal, err)
			}
			return body.Value, nil
		}
		if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
			return nil, nil
		}
		lastErr = err
		time.Sleep(time.Duration(attempt+1) * time.Millisecond)
	}
	return nil, lastErr
}

// extendChunk atomically appends an entry to a chunk.  It reports whether the
// compare-and-swap succeeded; a false result means another writer changed the
// chunk first and the caller must re-read it.
func (s *server) extendChunk(journal string, chunk int, old []int, entry int) (bool, error) {
	next := make([]int, len(old)+1)
	copy(next, old)
	next[len(old)] = entry

	_, err := s.node.SyncRPC(context.Background(), kvService, map[string]any{
		"type":                 "cas",
		"key":                  chunkKey(journal, chunk),
		"from":                 old,
		"to":                   next,
		"create_if_not_exists": true,
	})
	if err == nil {
		return true, nil
	}
	if maelstrom.ErrorCode(err) == maelstrom.PreconditionFailed {
		return false, nil
	}
	return false, err
}

// append adds an entry to a journal and returns its position.
func (s *server) append(journal string, entry int) (int, error) {
	lock := s.lockFor("journal:" + journal)
	lock.Lock()
	defer lock.Unlock()

	for {
		for chunk := 0; ; chunk++ {
			entries, err := s.readChunk(journal, chunk)
			if err != nil {
				return 0, err
			}
			if len(entries) == chunkSize {
				// This chunk is full; the next position lives in the next one.
				continue
			}

			ok, err := s.extendChunk(journal, chunk, entries, entry)
			if err != nil {
				return 0, err
			}
			if ok {
				return chunk*chunkSize + len(entries), nil
			}
			// Somebody else changed this chunk.  Re-read it and try again;
			// the entries we read are never lost or overwritten.
			break
		}
	}
}

// fetch returns the entries of a journal at or after the requested position.
// The returned pairs are contiguous and in increasing position order.
func (s *server) fetch(journal string, position int) ([][2]int, error) {
	if position < 0 {
		position = 0
	}
	chunk := position / chunkSize
	base := chunk * chunkSize

	entries, err := s.readChunk(journal, chunk)
	if err != nil {
		return nil, err
	}
	index := position - base
	if index < 0 {
		index = 0
	}
	if index >= len(entries) {
		return nil, nil
	}

	result := make([][2]int, 0, len(entries)-index)
	for i := index; i < len(entries); i++ {
		result = append(result, [2]int{base + i, entries[i]})
	}
	return result, nil
}

// advanceBookmark moves a journal's bookmark forward.  Bookmarks never move
// backwards, so concurrent readers cannot undo one another's progress.
func (s *server) advanceBookmark(journal string, position int) error {
	lock := s.lockFor("bookmark:" + journal)
	lock.Lock()
	defer lock.Unlock()

	for {
		msg, err := s.node.SyncRPC(context.Background(), kvService, map[string]any{
			"type": "read",
			"key":  bookmarkKey(journal),
		})
		if err != nil {
			if maelstrom.ErrorCode(err) != maelstrom.KeyDoesNotExist {
				return err
			}
			// The service ignores "from" when creating a missing key.
			_, err = s.node.SyncRPC(context.Background(), kvService, map[string]any{
				"type":                 "cas",
				"key":                  bookmarkKey(journal),
				"from":                 0,
				"to":                   position,
				"create_if_not_exists": true,
			})
			if err == nil {
				return nil
			}
			if maelstrom.ErrorCode(err) != maelstrom.PreconditionFailed {
				return err
			}
			// Another node created the bookmark first; re-read it.
			continue
		}

		var body struct {
			Value int `json:"value"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		if body.Value >= position {
			return nil
		}

		_, err = s.node.SyncRPC(context.Background(), kvService, map[string]any{
			"type": "cas",
			"key":  bookmarkKey(journal),
			"from": body.Value,
			"to":   position,
		})
		if err == nil {
			return nil
		}
		if maelstrom.ErrorCode(err) != maelstrom.PreconditionFailed {
			return err
		}
		// Another node moved the bookmark; re-read it.
	}
}

// readBookmark returns a journal's bookmark, if it has one.
func (s *server) readBookmark(journal string) (int, bool, error) {
	msg, err := s.node.SyncRPC(context.Background(), kvService, map[string]any{
		"type": "read",
		"key":  bookmarkKey(journal),
	})
	if err != nil {
		if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
			return 0, false, nil
		}
		return 0, false, err
	}
	var body struct {
		Value int `json:"value"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return 0, false, err
	}
	return body.Value, true, nil
}

func (s *server) handleAppend(msg maelstrom.Message) error {
	var body appendRequest
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	position, err := s.append(body.Journal, body.Entry)
	if err != nil {
		return err
	}
	return s.node.Reply(msg, map[string]any{
		"type":     "appended",
		"position": position,
	})
}

func (s *server) handleFetch(msg maelstrom.Message) error {
	var body fetchRequest
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	type result struct {
		journal string
		entries [][2]int
		err     error
	}
	results := make(chan result, len(body.Positions))
	for journal, position := range body.Positions {
		go func(journal string, position int) {
			entries, err := s.fetch(journal, position)
			results <- result{journal: journal, entries: entries, err: err}
		}(journal, position)
	}

	entries := make(map[string][][2]int, len(body.Positions))
	for range body.Positions {
		r := <-results
		if r.err != nil {
			return r.err
		}
		if len(r.entries) > 0 {
			entries[r.journal] = r.entries
		}
	}
	return s.node.Reply(msg, map[string]any{
		"type":    "fetched",
		"entries": entries,
	})
}

func (s *server) handleBookmark(msg maelstrom.Message) error {
	var body bookmarkRequest
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	type update struct {
		err error
	}
	updates := make(chan update, len(body.Positions))
	for journal, position := range body.Positions {
		go func(journal string, position int) {
			updates <- update{err: s.advanceBookmark(journal, position)}
		}(journal, position)
	}
	for range body.Positions {
		u := <-updates
		if u.err != nil {
			return u.err
		}
	}
	return s.node.Reply(msg, map[string]any{"type": "bookmarked"})
}

func (s *server) handleBookmarks(msg maelstrom.Message) error {
	var body bookmarksRequest
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	type result struct {
		journal  string
		position int
		present  bool
		err      error
	}
	results := make(chan result, len(body.Journals))
	for _, journal := range body.Journals {
		go func(journal string) {
			position, present, err := s.readBookmark(journal)
			results <- result{journal: journal, position: position, present: present, err: err}
		}(journal)
	}

	positions := make(map[string]int, len(body.Journals))
	for range body.Journals {
		r := <-results
		if r.err != nil {
			return r.err
		}
		if r.present {
			positions[r.journal] = r.position
		}
	}
	return s.node.Reply(msg, map[string]any{
		"type":      "bookmarks_are",
		"positions": positions,
	})
}

func main() {
	// The library logs every message it sends and receives.  At the
	// benchmark's request rate that is expensive and noisy, so discard those
	// logs; real failures are still reported on stderr.
	log.SetOutput(io.Discard)

	n := maelstrom.NewNode()
	s := newServer(n)
	n.Handle("append", s.handleAppend)
	n.Handle("fetch", s.handleFetch)
	n.Handle("bookmark", s.handleBookmark)
	n.Handle("bookmarks", s.handleBookmarks)

	if err := n.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "node failed: %v\n", err)
		os.Exit(1)
	}
}
