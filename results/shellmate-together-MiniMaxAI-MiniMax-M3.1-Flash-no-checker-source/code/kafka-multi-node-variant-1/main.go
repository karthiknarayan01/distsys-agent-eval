package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sort"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// A journal is stored in the shared linearizable key/value store.  The stored
// value is a JSON string (rather than a JSON object) so compare-and-swap has
// an unambiguous, byte-for-byte old value.
type journalState struct {
	Next    int64      `json:"next"`
	Entries [][2]int64 `json:"entries"`
}

type server struct {
	node *maelstrom.Node
	kv   *maelstrom.KV
}

func journalKey(journal string) string {
	return "journal/" + base64.RawURLEncoding.EncodeToString([]byte(journal))
}

func bookmarkKey(journal string) string {
	return "bookmark/" + base64.RawURLEncoding.EncodeToString([]byte(journal))
}

func (s *server) readJournal(ctx context.Context, journal string) (journalState, string, bool, error) {
	var encoded string
	err := s.kv.ReadInto(ctx, journalKey(journal), &encoded)
	if err != nil {
		if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
			return journalState{}, "", false, nil
		}
		return journalState{}, "", false, err
	}
	var state journalState
	if err := json.Unmarshal([]byte(encoded), &state); err != nil {
		return journalState{}, "", false, fmt.Errorf("decode journal %q: %w", journal, err)
	}
	return state, encoded, true, nil
}

// append assigns a fresh, strictly increasing position and appends the entry
// in one atomic CAS.  Retrying after a failed CAS obtains a new linearization
// point and therefore cannot duplicate a position.
func (s *server) append(ctx context.Context, journal string, entry int64) (int64, error) {
	key := journalKey(journal)
	for {
		state, encoded, exists, err := s.readJournal(ctx, journal)
		if err != nil {
			return 0, err
		}

		position := state.Next
		candidate := journalState{
			Next:    state.Next + 1,
			Entries: make([][2]int64, len(state.Entries), len(state.Entries)+1),
		}
		copy(candidate.Entries, state.Entries)
		candidate.Entries = append(candidate.Entries, [2]int64{position, entry})
		newEncodedBytes, err := json.Marshal(candidate)
		if err != nil {
			return 0, err
		}
		newEncoded := string(newEncodedBytes)

		var casErr error
		if exists {
			casErr = s.kv.CompareAndSwap(ctx, key, encoded, newEncoded, false)
		} else {
			casErr = s.kv.CompareAndSwap(ctx, key, nil, newEncoded, true)
		}
		if casErr == nil {
			return position, nil
		}
		if maelstrom.ErrorCode(casErr) != maelstrom.PreconditionFailed {
			return 0, casErr
		}
		// Another append won the race.  Read its result and try again.
	}
}

func (s *server) fetch(ctx context.Context, positions map[string]int64) (map[string][][2]int64, error) {
	type result struct {
		journal string
		entries [][2]int64
		err     error
	}

	journals := make([]string, 0, len(positions))
	for journal := range positions {
		journals = append(journals, journal)
	}
	sort.Strings(journals)

	results := make(chan result, len(journals))
	var wg sync.WaitGroup
	for _, journal := range journals {
		journal := journal
		wg.Add(1)
		go func() {
			defer wg.Done()
			state, _, exists, err := s.readJournal(ctx, journal)
			if err != nil {
				results <- result{journal: journal, err: err}
				return
			}
			if !exists {
				results <- result{journal: journal}
				return
			}
			from := positions[journal]
			// Positions are dense and sorted, so binary search for the first
			// entry at or after the requested position.
			first := sort.Search(len(state.Entries), func(i int) bool {
				return state.Entries[i][0] >= from
			})
			// A fetch is allowed to return only part of a journal.  Returning
			// one pair at a time keeps responses small while still never
			// skipping an entry at or after the requested position.
			entries := make([][2]int64, 0, 1)
			if first < len(state.Entries) {
				entries = append(entries, state.Entries[first])
			}
			results <- result{journal: journal, entries: entries}
		}()
	}
	wg.Wait()
	close(results)

	answer := make(map[string][][2]int64)
	for r := range results {
		if r.err != nil {
			return nil, r.err
		}
		if len(r.entries) != 0 {
			answer[r.journal] = r.entries
		}
	}
	return answer, nil
}

func (s *server) setBookmark(ctx context.Context, journal string, position int64) error {
	key := bookmarkKey(journal)
	for {
		var old int64
		err := s.kv.ReadInto(ctx, key, &old)
		exists := true
		if err != nil {
			if maelstrom.ErrorCode(err) != maelstrom.KeyDoesNotExist {
				return err
			}
			exists = false
		}
		var casErr error
		if exists {
			casErr = s.kv.CompareAndSwap(ctx, key, old, position, false)
		} else {
			casErr = s.kv.CompareAndSwap(ctx, key, nil, position, true)
		}
		if casErr == nil {
			return nil
		}
		if maelstrom.ErrorCode(casErr) != maelstrom.PreconditionFailed {
			return casErr
		}
	}
}

func (s *server) setBookmarks(ctx context.Context, positions map[string]int64) error {
	journals := make([]string, 0, len(positions))
	for journal := range positions {
		journals = append(journals, journal)
	}
	sort.Strings(journals)

	// Different journals are independent, so update them concurrently.
	var wg sync.WaitGroup
	errs := make(chan error, len(journals))
	for _, journal := range journals {
		journal := journal
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := s.setBookmark(ctx, journal, positions[journal]); err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		return err
	}
	return nil
}

func (s *server) getBookmarks(ctx context.Context, journals []string) (map[string]int64, error) {
	type result struct {
		journal  string
		position int64
		found    bool
		err      error
	}
	results := make(chan result, len(journals))
	var wg sync.WaitGroup
	for _, journal := range journals {
		journal := journal
		wg.Add(1)
		go func() {
			defer wg.Done()
			var position int64
			err := s.kv.ReadInto(ctx, bookmarkKey(journal), &position)
			if err != nil {
				if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
					results <- result{journal: journal}
					return
				}
				results <- result{journal: journal, err: err}
				return
			}
			results <- result{journal: journal, position: position, found: true}
		}()
	}
	wg.Wait()
	close(results)

	answer := make(map[string]int64)
	for r := range results {
		if r.err != nil {
			return nil, r.err
		}
		if r.found {
			answer[r.journal] = r.position
		}
	}
	return answer, nil
}

func decodeBody(msg maelstrom.Message, dst any) error {
	if err := json.Unmarshal(msg.Body, dst); err != nil {
		return fmt.Errorf("decode request body: %w", err)
	}
	return nil
}

func main() {
	log.SetFlags(0)
	n := maelstrom.NewNode()
	s := &server{node: n, kv: maelstrom.NewLinKV(n)}
	ctx := context.Background()

	n.Handle("append", func(msg maelstrom.Message) error {
		var body struct {
			Type    string `json:"type"`
			MsgID   int    `json:"msg_id"`
			Journal string `json:"journal"`
			Entry   int64  `json:"entry"`
		}
		if err := decodeBody(msg, &body); err != nil {
			return err
		}
		position, err := s.append(ctx, body.Journal, body.Entry)
		if err != nil {
			return err
		}
		return n.Reply(msg, map[string]any{
			"type":     "appended",
			"position": position,
		})
	})

	n.Handle("fetch", func(msg maelstrom.Message) error {
		var body struct {
			Type      string           `json:"type"`
			MsgID     int              `json:"msg_id"`
			Positions map[string]int64 `json:"positions"`
		}
		if err := decodeBody(msg, &body); err != nil {
			return err
		}
		entries, err := s.fetch(ctx, body.Positions)
		if err != nil {
			return err
		}
		return n.Reply(msg, map[string]any{
			"type":    "fetched",
			"entries": entries,
		})
	})

	n.Handle("bookmark", func(msg maelstrom.Message) error {
		var body struct {
			Type      string           `json:"type"`
			MsgID     int              `json:"msg_id"`
			Positions map[string]int64 `json:"positions"`
		}
		if err := decodeBody(msg, &body); err != nil {
			return err
		}
		if err := s.setBookmarks(ctx, body.Positions); err != nil {
			return err
		}
		return n.Reply(msg, map[string]any{"type": "bookmarked"})
	})

	n.Handle("bookmarks", func(msg maelstrom.Message) error {
		var body struct {
			Type     string   `json:"type"`
			MsgID    int      `json:"msg_id"`
			Journals []string `json:"journals"`
		}
		if err := decodeBody(msg, &body); err != nil {
			return err
		}
		positions, err := s.getBookmarks(ctx, body.Journals)
		if err != nil {
			return err
		}
		return n.Reply(msg, map[string]any{
			"type":      "bookmarks_are",
			"positions": positions,
		})
	})

	if err := n.Run(); err != nil {
		log.Printf("node error: %s", err)
		os.Exit(1)
	}
}
