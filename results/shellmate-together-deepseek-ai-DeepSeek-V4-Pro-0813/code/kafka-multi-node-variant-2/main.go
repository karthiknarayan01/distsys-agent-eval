package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// chunkSize is how many entries we store in a single lin-kv key. The kafka
// workload never writes more than 32 messages to a single key, so a single
// chunk is almost always enough, but the code handles multiple chunks too.
const chunkSize = 32

type server struct {
	n  *maelstrom.Node
	kv *maelstrom.KV

	mu          sync.Mutex
	offsetCache map[string]int
}

func newServer(n *maelstrom.Node) *server {
	return &server{
		n:           n,
		kv:          maelstrom.NewLinKV(n),
		offsetCache: make(map[string]int),
	}
}

// chunkKey returns the lin-kv key holding the chunk that contains the given
// offset for a journal.
func chunkKey(journal string, offset int) string {
	return fmt.Sprintf("log-%s-%d", journal, offset/chunkSize)
}

func (s *server) getCache(journal string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.offsetCache[journal]
}

func (s *server) bumpCache(journal string, offset int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if offset > s.offsetCache[journal] {
		s.offsetCache[journal] = offset
	}
}

// getChunk reads the chunk containing the given offset. A missing chunk (or a
// read error) is treated as an empty chunk; a subsequent CAS will fail if the
// chunk actually has data, which is safe.
func (s *server) getChunk(ctx context.Context, journal string, offset int) ([]any, error) {
	v, err := s.kv.Read(ctx, chunkKey(journal, offset))
	if err != nil {
		return []any{}, nil
	}
	chunk, ok := v.([]any)
	if !ok {
		return []any{}, nil
	}
	base := offset - offset%chunkSize
	s.bumpCache(journal, base+len(chunk))
	return chunk, nil
}

// tryAppend appends an entry to a journal and returns its position.
func (s *server) tryAppend(ctx context.Context, journal string, entry any) (int, error) {
	for {
		offset := s.getCache(journal)
		chunk, err := s.getChunk(ctx, journal, offset)
		if err != nil {
			return 0, err
		}
		i := len(chunk)
		if i >= chunkSize {
			// Chunk is full; move to the next one.
			base := offset - offset%chunkSize
			s.bumpCache(journal, base+chunkSize)
			continue
		}

		newChunk := make([]any, 0, len(chunk)+1)
		newChunk = append(newChunk, chunk...)
		newChunk = append(newChunk, entry)

		err = s.kv.CompareAndSwap(ctx, chunkKey(journal, offset), chunk, newChunk, true)
		if err != nil {
			if maelstrom.ErrorCode(err) == maelstrom.PreconditionFailed {
				// Someone else appended concurrently; retry.
				continue
			}
			return 0, err
		}

		base := offset - offset%chunkSize
		pos := base + i
		s.bumpCache(journal, pos+1)
		return pos, nil
	}
}

// getOffsets reads the committed-offsets map from lin-kv.
func (s *server) getOffsets(ctx context.Context) (map[string]int, error) {
	v, err := s.kv.Read(ctx, "offsets")
	if err != nil {
		if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
			return map[string]int{}, nil
		}
		return nil, err
	}
	m, ok := v.(map[string]any)
	if !ok {
		return map[string]int{}, nil
	}
	result := make(map[string]int, len(m))
	for k, val := range m {
		switch n := val.(type) {
		case float64:
			result[k] = int(n)
		case int:
			result[k] = n
		}
	}
	return result, nil
}

func (s *server) handleAppend(msg maelstrom.Message) error {
	var body struct {
		Journal string `json:"journal"`
		Entry   any    `json:"entry"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	pos, err := s.tryAppend(context.Background(), body.Journal, body.Entry)
	if err != nil {
		return err
	}
	return s.n.Reply(msg, map[string]any{
		"type":     "appended",
		"position": pos,
	})
}

func (s *server) handleFetch(msg maelstrom.Message) error {
	var body struct {
		Positions map[string]int `json:"positions"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	ctx := context.Background()
	entries := map[string][]any{}
	for journal, offset := range body.Positions {
		chunk, err := s.getChunk(ctx, journal, offset)
		if err != nil {
			chunk = []any{}
		}
		i0 := offset % chunkSize
		if i0 >= len(chunk) {
			continue
		}
		pairs := make([]any, 0, len(chunk)-i0)
		for i := i0; i < len(chunk); i++ {
			pairs = append(pairs, []any{offset + (i - i0), chunk[i]})
		}
		if len(pairs) > 0 {
			entries[journal] = pairs
		}
	}
	return s.n.Reply(msg, map[string]any{
		"type":    "fetched",
		"entries": entries,
	})
}

func (s *server) handleBookmark(msg maelstrom.Message) error {
	var body struct {
		Positions map[string]int `json:"positions"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	ctx := context.Background()
	for {
		offsets, err := s.getOffsets(ctx)
		if err != nil {
			return err
		}
		merged := make(map[string]int, len(offsets)+len(body.Positions))
		for k, v := range offsets {
			merged[k] = v
		}
		for k, v := range body.Positions {
			if v > merged[k] {
				merged[k] = v
			}
		}
		err = s.kv.CompareAndSwap(ctx, "offsets", offsets, merged, true)
		if err != nil {
			if maelstrom.ErrorCode(err) == maelstrom.PreconditionFailed {
				continue
			}
			return err
		}
		break
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

	offsets, err := s.getOffsets(context.Background())
	if err != nil {
		return err
	}
	result := map[string]int{}
	for _, j := range body.Journals {
		if v, ok := offsets[j]; ok {
			result[j] = v
		}
	}
	return s.n.Reply(msg, map[string]any{
		"type":      "bookmarks_are",
		"positions": result,
	})
}

func main() {
	n := maelstrom.NewNode()
	s := newServer(n)

	n.Handle("append", s.handleAppend)
	n.Handle("fetch", s.handleFetch)
	n.Handle("bookmark", s.handleBookmark)
	n.Handle("bookmarks", s.handleBookmarks)
	// Ignore stray error messages (they are normally replies, handled by the
	// RPC machinery, but a no-op handler keeps the node from crashing).
	n.Handle("error", func(msg maelstrom.Message) error { return nil })

	if err := n.Run(); err != nil {
		panic(err)
	}
}
