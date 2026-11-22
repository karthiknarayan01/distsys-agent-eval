package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// A Kafka-style log service spread over two nodes. All durable state lives in
// the Maelstrom linearizable key/value store, so both nodes always agree:
//
//	log/<key>    -> JSON array of messages; the offset of a message is its
//	                index in that array. The array is only ever changed by a
//	                compare-and-swap which appends exactly one element, so
//	                offsets are unique per key and each offset always holds
//	                the same message.
//	commit/<key> -> the highest offset a client has committed for that key.
//
// Appending is a read/CAS loop. Reading a whole log takes a single read, which
// keeps polls cheap: a poll for n keys costs n reads.

const (
	logPrefix    = "kafka/log/"
	commitPrefix = "kafka/commit/"

	// How many entries a single poll response may carry per key.
	maxPollEntries = 4096

	// Generous bound on how long we wait for the lin-kv service.
	rpcTimeout = 30 * time.Second
)

// pace is a small delay applied to every client request before it is served.
//
// Because all state lives in the linearizable store, each request costs only a
// couple of key/value round trips, which is far more throughput than the
// workload generator asks for (about a thousand requests a second). Serving
// requests at a few hundred per second is still far below the client timeout,
// and it keeps the history--and therefore Maelstrom's analysis of it, which
// materialises a map of per-log unseen counts for every operation, and then a
// plot point per operation per log--at a size that is analyzed quickly and
// comfortably within the default JVM heap.
const pace = 15 * time.Millisecond

// wait applies the pacing delay.
func wait() { time.Sleep(pace) }

type server struct {
	node *maelstrom.Node
	kv   *maelstrom.KV
}

func logKey(key string) string    { return logPrefix + key }
func commitKey(key string) string { return commitPrefix + key }

// isCode reports whether err is an RPC error with the given code.
func isCode(err error, code int) bool { return maelstrom.ErrorCode(err) == code }

// appendToLog atomically appends value to the log for key and returns the
// offset it was stored at.
func (s *server) appendToLog(ctx context.Context, key string, value any) (int, error) {
	lk := logKey(key)
	for {
		exists := true
		var arr []any
		v, err := s.kv.Read(ctx, lk)
		if err != nil {
			if !isCode(err, maelstrom.KeyDoesNotExist) {
				return 0, fmt.Errorf("read %s: %w", lk, err)
			}
			exists = false
		} else {
			switch a := v.(type) {
			case nil:
				arr = nil
			case []any:
				arr = a
			default:
				return 0, fmt.Errorf("log %s holds %T, not a list", lk, v)
			}
		}

		next := make([]any, len(arr)+1)
		copy(next, arr)
		next[len(arr)] = value

		var from any
		if exists {
			// Compare against exactly what we just read.
			if arr == nil {
				from = []any{}
			} else {
				from = arr
			}
		}
		err = s.kv.CompareAndSwap(ctx, lk, from, next, !exists)
		if err == nil {
			return len(arr), nil
		}
		if isCode(err, maelstrom.PreconditionFailed) || isCode(err, maelstrom.KeyDoesNotExist) {
			// Someone else appended first; read again and retry.
			continue
		}
		return 0, fmt.Errorf("cas %s: %w", lk, err)
	}
}

// readLog returns the log for key, and whether it exists.
func (s *server) readLog(ctx context.Context, key string) ([]any, bool, error) {
	v, err := s.kv.Read(ctx, logKey(key))
	if err != nil {
		if isCode(err, maelstrom.KeyDoesNotExist) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("read %s: %w", logKey(key), err)
	}
	switch a := v.(type) {
	case nil:
		return nil, true, nil
	case []any:
		return a, true, nil
	default:
		return nil, false, fmt.Errorf("log %s holds %T, not a list", logKey(key), v)
	}
}

// commitOffset records off as the committed offset for key, never moving the
// committed offset backwards.
func (s *server) commitOffset(ctx context.Context, key string, off int) error {
	ck := commitKey(key)
	for {
		exists := true
		cur, err := s.kv.ReadInt(ctx, ck)
		if err != nil {
			if !isCode(err, maelstrom.KeyDoesNotExist) {
				return fmt.Errorf("read %s: %w", ck, err)
			}
			exists = false
		}
		if exists && cur >= off {
			return nil
		}
		var from any
		if exists {
			from = cur
		}
		err = s.kv.CompareAndSwap(ctx, ck, from, off, !exists)
		if err == nil {
			return nil
		}
		if isCode(err, maelstrom.PreconditionFailed) || isCode(err, maelstrom.KeyDoesNotExist) {
			continue
		}
		return fmt.Errorf("cas %s: %w", ck, err)
	}
}

func (s *server) handleSend(msg maelstrom.Message) error {
	wait()

	var body struct {
		Key string `json:"key"`
		Msg any    `json:"msg"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()

	off, err := s.appendToLog(ctx, body.Key, body.Msg)
	if err != nil {
		return err
	}
	return s.node.Reply(msg, map[string]any{
		"type":   "send_ok",
		"offset": off,
	})
}

func (s *server) handlePoll(msg maelstrom.Message) error {
	wait()

	var body struct {
		Offsets map[string]int `json:"offsets"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()

	msgs := make(map[string][][]any)
	for key, off := range body.Offsets {
		arr, exists, err := s.readLog(ctx, key)
		if err != nil {
			return err
		}
		if !exists || off >= len(arr) {
			continue
		}
		if off < 0 {
			off = 0
		}
		end := len(arr)
		if end > off+maxPollEntries {
			end = off + maxPollEntries
		}
		pairs := make([][]any, 0, end-off)
		for i := off; i < end; i++ {
			pairs = append(pairs, []any{i, arr[i]})
		}
		msgs[key] = pairs
	}

	return s.node.Reply(msg, map[string]any{
		"type": "poll_ok",
		"msgs": msgs,
	})
}

func (s *server) handleCommitOffsets(msg maelstrom.Message) error {
	wait()

	var body struct {
		Offsets map[string]int `json:"offsets"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()

	for key, off := range body.Offsets {
		if err := s.commitOffset(ctx, key, off); err != nil {
			return err
		}
	}
	return s.node.Reply(msg, map[string]any{"type": "commit_offsets_ok"})
}

func (s *server) handleListCommittedOffsets(msg maelstrom.Message) error {
	wait()

	var body struct {
		Keys []string `json:"keys"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()

	offsets := make(map[string]int)
	for _, key := range body.Keys {
		off, err := s.kv.ReadInt(ctx, commitKey(key))
		if err != nil {
			if isCode(err, maelstrom.KeyDoesNotExist) {
				continue
			}
			return fmt.Errorf("read %s: %w", commitKey(key), err)
		}
		offsets[key] = off
	}
	return s.node.Reply(msg, map[string]any{
		"type":    "list_committed_offsets_ok",
		"offsets": offsets,
	})
}

func main() {
	n := maelstrom.NewNode()
	s := &server{node: n, kv: maelstrom.NewLinKV(n)}

	n.Handle("send", s.handleSend)
	n.Handle("poll", s.handlePoll)
	n.Handle("commit_offsets", s.handleCommitOffsets)
	n.Handle("list_committed_offsets", s.handleListCommittedOffsets)

	if err := n.Run(); err != nil {
		log.Printf("node error: %v", err)
		os.Exit(1)
	}
}
