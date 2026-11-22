package main

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"os"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const (
	// How long we wait for a peer to answer a direct query.
	peerTimeout = 250 * time.Millisecond

	// Overall budget for one snapshot (which may retry).
	gatherBudget = 600 * time.Millisecond

	// Timeout for a single write to the sequenced KV store.
	flushTimeout = 2 * time.Second

	// Prefix for the per-node counter keys in seq-kv.
	keyPrefix = "counter/"
)

// server holds all of a node's state. Each node is authoritative for exactly
// one value: the number of visitors which have come through its own turnstile.
// That value is kept in memory (source of truth) and mirrored into seq-kv so
// other nodes can pick it up even when they can't talk to this node directly.
type server struct {
	n     *maelstrom.Node
	kv    *maelstrom.KV
	dirty chan struct{}

	mu      sync.Mutex
	local   int64            // visitors counted at this node
	flushed int64            // highest value we know made it into seq-kv
	known   map[string]int64 // highest best-known value for each other node
}

func main() {
	// The maelstrom library logs every message; that's far too chatty.
	log.SetOutput(io.Discard)

	node := maelstrom.NewNode()
	s := &server{
		n:     node,
		kv:    maelstrom.NewSeqKV(node),
		dirty: make(chan struct{}, 1),
		known: make(map[string]int64),
	}

	node.Handle("count", s.handleCount)
	node.Handle("total", s.handleTotal)
	node.Handle("peer_read", s.handlePeerRead)

	go s.flusher()

	if err := node.Run(); err != nil {
		log.SetOutput(os.Stderr)
		log.Printf("node exited: %v", err)
		os.Exit(1)
	}
}

// handleCount records visitors at this turnstile. The visitor count is
// accepted (and acknowledged) as soon as it is in local memory: the mirrored
// write into seq-kv happens in the background and is retried until it sticks,
// so an acknowledged count is never lost.
func (s *server) handleCount(msg maelstrom.Message) error {
	var body struct {
		Visitors int `json:"visitors"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	if body.Visitors < 0 {
		body.Visitors = 0
	}

	s.mu.Lock()
	s.local += int64(body.Visitors)
	s.mu.Unlock()

	// Nudge the background flusher (coalescing: the channel holds one token).
	select {
	case s.dirty <- struct{}{}:
	default:
	}

	return s.n.Reply(msg, map[string]any{"type": "counted"})
}

// handleTotal answers with the best known tally over all four turnstiles.
func (s *server) handleTotal(msg maelstrom.Message) error {
	total := s.snapshot()
	return s.n.Reply(msg, map[string]any{
		"type":            "total_is",
		"visitors_so_far": total,
	})
}

// handlePeerRead is a node-to-node query: "what's your own tally?"
func (s *server) handlePeerRead(msg maelstrom.Message) error {
	s.mu.Lock()
	v := s.local
	s.mu.Unlock()
	return s.n.Reply(msg, map[string]any{
		"type":  "peer_read_ok",
		"total": v,
	})
}

// flusher mirrors this node's tally into seq-kv. Only one goroutine ever
// writes this node's key, so the values written are non-decreasing.
func (s *server) flusher() {
	key := keyPrefix + s.n.ID()
	for range s.dirty {
		for {
			s.mu.Lock()
			v := s.local
			if v <= s.flushed {
				s.mu.Unlock()
				break
			}
			s.mu.Unlock()

			ctx, cancel := context.WithTimeout(context.Background(), flushTimeout)
			err := s.kv.Write(ctx, key, v)
			cancel()

			if err == nil {
				s.mu.Lock()
				if v > s.flushed {
					s.flushed = v
				}
				s.mu.Unlock()
				// Loop again in case more visitors arrived meanwhile.
				continue
			}

			// The store is unreachable right now; keep trying. The count has
			// already been acknowledged, so we must not give up on it.
			time.Sleep(50 * time.Millisecond)
		}
	}
}

// snapshot sums every node's tally. For each other node we take the larger of
// what seq-kv says and what a direct query says. Both values are always a
// lower bound on that node's true tally, so taking the max never double counts
// and is exact whenever the direct query succeeds.
func (s *server) snapshot() int64 {
	me := s.n.ID()
	ids := s.n.NodeIDs()

	type result struct {
		kv   int64
		peer int64
		ok   bool
	}

	deadline := time.Now().Add(gatherBudget)
	for {
		results := make(map[string]result, len(ids))
		var mu sync.Mutex
		var wg sync.WaitGroup

		for _, id := range ids {
			if id == me {
				continue
			}
			wg.Add(1)
			go func(id string) {
				defer wg.Done()
				ctx, cancel := context.WithTimeout(context.Background(), peerTimeout)
				defer cancel()

				var r result
				if v, err := s.kv.ReadInt(ctx, keyPrefix+id); err == nil {
					r.kv = int64(v)
				}
				if v, err := s.peerTotal(ctx, id); err == nil {
					r.peer = v
					r.ok = true
				}

				mu.Lock()
				results[id] = r
				mu.Unlock()
			}(id)
		}
		wg.Wait()

		allOK := true
		s.mu.Lock()
		for id, r := range results {
			v := r.kv
			if r.peer > v {
				v = r.peer
			}
			if v > s.known[id] {
				s.known[id] = v
			}
			if !r.ok {
				allOK = false
			}
		}
		total := s.local
		for _, v := range s.known {
			total += v
		}
		s.mu.Unlock()

		if allOK || time.Now().After(deadline) {
			return total
		}
	}
}

// peerTotal asks another node for its own tally.
func (s *server) peerTotal(ctx context.Context, id string) (int64, error) {
	respCh := make(chan int64, 1)
	err := s.n.RPC(id, map[string]any{"type": "peer_read"}, func(msg maelstrom.Message) error {
		var body struct {
			Total int64 `json:"total"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return nil
		}
		select {
		case respCh <- body.Total:
		default:
		}
		return nil
	})
	if err != nil {
		return 0, err
	}

	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	case v := <-respCh:
		return v, nil
	}
}
