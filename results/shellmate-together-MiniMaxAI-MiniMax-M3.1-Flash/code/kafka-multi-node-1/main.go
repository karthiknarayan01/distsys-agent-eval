package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"strconv"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const (
	// A chunk holds the values whose offsets are
	// chunkIndex*chunkSize ... chunkIndex*chunkSize+len(chunk)-1.
	chunkSize = 32

	// Upper bound on the number of [offset value] pairs returned for a single
	// poll request.  The workload's final polls ask for hundreds of logs at
	// once, so we spread a fixed budget over the requested logs.  Returning
	// fewer pairs than a log holds is explicitly allowed.
	pairBudget = 4096

	// How many lin-kv requests we keep in flight at once.  Requests for
	// different logs are independent, so doing them concurrently keeps the
	// latency of a multi-log poll low.
	maxWorkers = 32

	// A request should normally complete much faster than this.  The timeout
	// is below Maelstrom's five second client timeout, so a transient problem
	// cannot turn into an indefinitely pending request.
	requestTimeout = 4500 * time.Millisecond
)

// A chunk contains the values whose offsets are
// chunkIndex*chunkSize ... chunkIndex*chunkSize+len(chunk)-1.
type chunk []json.RawMessage

type app struct {
	node *maelstrom.Node
	kv   *maelstrom.KV

	lockMu sync.Mutex
	locks  map[string]*sync.Mutex
}

func newApp(node *maelstrom.Node, kv *maelstrom.KV) *app {
	return &app{
		node:  node,
		kv:    kv,
		locks: make(map[string]*sync.Mutex),
	}
}

// lockFor serializes appends for a log on this node.  Different logs remain
// concurrent, while the lin-kv compare-and-set still arbitrates appends made
// by the other node.
func (a *app) lockFor(key string) *sync.Mutex {
	a.lockMu.Lock()
	defer a.lockMu.Unlock()
	m := a.locks[key]
	if m == nil {
		m = new(sync.Mutex)
		a.locks[key] = m
	}
	return m
}

// parallelDo runs fn for every index in [0, n) using a bounded pool of
// workers.  It returns the first error observed, if any.
func parallelDo(n int, fn func(i int) error) error {
	if n == 0 {
		return nil
	}
	workers := maxWorkers
	if workers > n {
		workers = n
	}

	var (
		mu       sync.Mutex
		firstErr error
		wg       sync.WaitGroup
	)
	jobs := make(chan int)
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func() {
			defer wg.Done()
			for i := range jobs {
				err := fn(i)
				if err != nil {
					mu.Lock()
					if firstErr == nil {
						firstErr = err
					}
					mu.Unlock()
				}
			}
		}()
	}
	for i := 0; i < n; i++ {
		jobs <- i
	}
	close(jobs)
	wg.Wait()
	return firstErr
}

// Encode user-provided log keys before using them as lin-kv keys.  This keeps
// arbitrary strings (including punctuation and Unicode) unambiguous.
func encodedKey(key string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(key))
}

func chunkKey(logKey string, index int) string {
	return "kafka/chunk/" + encodedKey(logKey) + "/" + strconv.Itoa(index)
}

func commitKey(logKey string) string {
	return "kafka/commit/" + encodedKey(logKey)
}

func isMissing(err error) bool {
	return maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist
}

func isConflict(err error) bool {
	return maelstrom.ErrorCode(err) == maelstrom.PreconditionFailed
}

// readChunk returns the chunk and whether it exists.  Missing chunks are
// normal: they denote either the end of a log or a not-yet-created chunk.
func (a *app) readChunk(ctx context.Context, key string, index int) (chunk, bool, error) {
	var c chunk
	err := a.kv.ReadInto(ctx, chunkKey(key, index), &c)
	if err != nil {
		if isMissing(err) {
			return nil, false, nil
		}
		return nil, false, err
	}
	return c, true, nil
}

// append appends msg to logKey and returns its stable offset.  A chunk is an
// append-only array.  Its compare-and-set both allocates the offset and
// stores the value atomically, so concurrent sends on the two nodes cannot
// receive the same offset.
func (a *app) append(ctx context.Context, logKey string, msg json.RawMessage) (int, error) {
	mu := a.lockFor(logKey)
	mu.Lock()
	defer mu.Unlock()

	index := 0
	for {
		old, exists, err := a.readChunk(ctx, logKey, index)
		if err != nil {
			return 0, err
		}
		if !exists {
			old = chunk{}
		}

		if len(old) >= chunkSize {
			index++
			continue
		}

		updated := make(chunk, len(old), len(old)+1)
		copy(updated, old)
		updated = append(updated, msg)

		err = a.kv.CompareAndSwap(ctx, chunkKey(logKey, index), old, updated, true)
		if err == nil {
			return index*chunkSize + len(old), nil
		}
		if !isConflict(err) {
			return 0, err
		}

		// Another node appended after our read.  Read the new chunk and try
		// again; the successful CAS determines the offset.
		runtime.Gosched()
	}
}

type sendRequest struct {
	Type string          `json:"type"`
	Key  string          `json:"key"`
	Msg  json.RawMessage `json:"msg"`
}

func (a *app) handleSend(msg maelstrom.Message) error {
	var req sendRequest
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		return maelstrom.NewRPCError(maelstrom.MalformedRequest, err.Error())
	}
	if len(req.Msg) == 0 {
		return maelstrom.NewRPCError(maelstrom.MalformedRequest, "send requires msg")
	}

	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()
	offset, err := a.append(ctx, req.Key, req.Msg)
	if err != nil {
		return err
	}
	return a.node.Reply(msg, map[string]any{
		"type":   "send_ok",
		"offset": offset,
	})
}

type pollRequest struct {
	Type    string         `json:"type"`
	Offsets map[string]int `json:"offsets"`
}

// pollLog returns up to limit values at or after requested.  Logs are
// contiguous, but handling a partially-filled final chunk also makes the
// representation safe if a future change ever permits sparse offsets.
func (a *app) pollLog(ctx context.Context, logKey string, requested, limit int) ([][2]any, error) {
	if requested < 0 {
		requested = 0
	}
	index := requested / chunkSize
	within := requested % chunkSize

	for {
		values, exists, err := a.readChunk(ctx, logKey, index)
		if err != nil {
			return nil, err
		}
		if !exists {
			return nil, nil
		}

		if within < len(values) {
			result := make([][2]any, 0, len(values)-within)
			for i := within; i < len(values) && len(result) < limit; i++ {
				offset := index*chunkSize + i
				result = append(result, [2]any{offset, json.RawMessage(values[i])})
			}
			return result, nil
		}

		// The requested position is beyond this chunk.  A full chunk may be
		// followed by another one; a partial chunk is the end of the log.
		if len(values) < chunkSize {
			return nil, nil
		}
		index++
		within = 0
	}
}

func (a *app) handlePoll(msg maelstrom.Message) error {
	var req pollRequest
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		return maelstrom.NewRPCError(maelstrom.MalformedRequest, err.Error())
	}

	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()

	keys := make([]string, 0, len(req.Offsets))
	offsets := make([]int, 0, len(req.Offsets))
	for key, offset := range req.Offsets {
		keys = append(keys, key)
		offsets = append(offsets, offset)
	}

	// Spread the pair budget over the requested logs, but always allow at
	// least one pair per log so that clients make progress.
	limit := pairBudget
	if len(keys) > 0 {
		limit = pairBudget / len(keys)
		if limit < 1 {
			limit = 1
		}
	}
	if limit > chunkSize {
		limit = chunkSize
	}

	results := make([][][2]any, len(keys))
	if err := parallelDo(len(keys), func(i int) error {
		values, err := a.pollLog(ctx, keys[i], offsets[i], limit)
		if err != nil {
			return err
		}
		results[i] = values
		return nil
	}); err != nil {
		return err
	}

	answers := make(map[string][][2]any, len(keys))
	for i, values := range results {
		if len(values) != 0 {
			answers[keys[i]] = values
		}
	}

	return a.node.Reply(msg, map[string]any{
		"type": "poll_ok",
		"msgs": answers,
	})
}

type commitRequest struct {
	Type    string         `json:"type"`
	Offsets map[string]int `json:"offsets"`
}

// commitOffset advances the committed offset monotonically.  Each log has its
// own lin-kv key, avoiding contention between unrelated logs.
func (a *app) commitOffset(ctx context.Context, logKey string, target int) error {
	if target < 0 {
		// Nothing can have been read at a negative offset; ignore it.
		return nil
	}
	for {
		current, err := a.kv.ReadInt(ctx, commitKey(logKey))
		switch {
		case err == nil:
			if current >= target {
				return nil
			}
		case isMissing(err):
			// Nothing has been committed for this log yet.
		default:
			return err
		}

		// A missing commit is represented by nil in the CAS.  Passing
		// create_if_not_exists makes the first commit possible.
		var from any
		if err == nil {
			from = current
		}
		err = a.kv.CompareAndSwap(ctx, commitKey(logKey), from, target, true)
		if err == nil {
			return nil
		}
		if !isConflict(err) {
			return err
		}
		runtime.Gosched()
	}
}

func (a *app) handleCommit(msg maelstrom.Message) error {
	var req commitRequest
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		return maelstrom.NewRPCError(maelstrom.MalformedRequest, err.Error())
	}

	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()

	keys := make([]string, 0, len(req.Offsets))
	offsets := make([]int, 0, len(req.Offsets))
	for key, offset := range req.Offsets {
		keys = append(keys, key)
		offsets = append(offsets, offset)
	}

	if err := parallelDo(len(keys), func(i int) error {
		return a.commitOffset(ctx, keys[i], offsets[i])
	}); err != nil {
		return err
	}
	return a.node.Reply(msg, map[string]any{"type": "commit_offsets_ok"})
}

type listRequest struct {
	Type string   `json:"type"`
	Keys []string `json:"keys"`
}

func (a *app) handleList(msg maelstrom.Message) error {
	var req listRequest
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		return maelstrom.NewRPCError(maelstrom.MalformedRequest, err.Error())
	}

	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()

	offsets := make([]int, len(req.Keys))
	found := make([]bool, len(req.Keys))
	if err := parallelDo(len(req.Keys), func(i int) error {
		offset, err := a.kv.ReadInt(ctx, commitKey(req.Keys[i]))
		if err == nil {
			offsets[i] = offset
			found[i] = true
			return nil
		}
		if isMissing(err) {
			return nil
		}
		return err
	}); err != nil {
		return err
	}

	answer := make(map[string]int)
	for i, key := range req.Keys {
		if found[i] {
			answer[key] = offsets[i]
		}
	}
	return a.node.Reply(msg, map[string]any{
		"type":    "list_committed_offsets_ok",
		"offsets": answer,
	})
}

func main() {
	// The demo library logs every message.  At the workload's rate that is a
	// large amount of I/O and obscures useful diagnostics, so keep stderr
	// quiet during normal operation.
	log.SetOutput(io.Discard)

	node := maelstrom.NewNode()
	kv := maelstrom.NewLinKV(node)
	a := newApp(node, kv)

	node.Handle("send", a.handleSend)
	node.Handle("poll", a.handlePoll)
	node.Handle("commit_offsets", a.handleCommit)
	node.Handle("list_committed_offsets", a.handleList)

	if err := node.Run(); err != nil && !errors.Is(err, os.ErrClosed) {
		fmt.Fprintf(os.Stderr, "node: %v\n", err)
		os.Exit(1)
	}
}
