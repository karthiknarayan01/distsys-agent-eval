package main

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const (
	// How often we push our full state to our peers in the background.
	gossipInterval = 100 * time.Millisecond
	// How often we re-write our own component to the shared key/value store.
	kvFlushInterval = 100 * time.Millisecond

	// Timeouts for the various network operations.
	kvWriteTimeout = 500 * time.Millisecond
	kvReadTimeout  = 250 * time.Millisecond
	peerTimeout    = 300 * time.Millisecond
)

type stateMsg struct {
	Type string         `json:"type"`
	Comp map[string]int `json:"comp"`
}

type server struct {
	n  *maelstrom.Node
	kv *maelstrom.KV

	mu   sync.Mutex
	comp map[string]int // per-node component counters; each entry only grows

	seen map[string]bool // dedup of count requests: "<src>:<msg_id>"

	kvMu sync.Mutex // serialises writes of our own key
}

func main() {
	n := maelstrom.NewNode()
	s := &server{
		n:    n,
		kv:   maelstrom.NewSeqKV(n),
		comp: make(map[string]int),
		seen: make(map[string]bool),
	}

	n.Handle("init", s.handleInit)
	n.Handle("count", s.handleCount)
	n.Handle("total", s.handleTotal)
	n.Handle("gc_gossip", s.handleGossip)
	n.Handle("gc_state", s.handleStateRequest)

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

// handleInit only kicks off the background tasks; the library itself replies
// with init_ok.
func (s *server) handleInit(msg maelstrom.Message) error {
	go s.gossipLoop()
	go s.kvFlushLoop()
	return nil
}

func (s *server) handleCount(msg maelstrom.Message) error {
	var body struct {
		MsgID    int `json:"msg_id"`
		Visitors int `json:"visitors"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	delta := body.Visitors
	if delta <= 0 {
		// A zero increment is harmless; negative increments are not part of the
		// contract but treat them as no-ops.
		if delta < 0 {
			delta = 0
		}
	}

	id := s.n.ID()

	// Deduplicate retransmissions of the very same request.
	key := msg.Src + ":" + itoa(body.MsgID)
	s.mu.Lock()
	if body.MsgID != 0 && s.seen[key] {
		s.mu.Unlock()
		return s.n.Reply(msg, map[string]any{"type": "counted"})
	}
	if body.MsgID != 0 {
		s.seen[key] = true
	}
	s.comp[id] += delta
	st := s.snapshotLocked()
	s.mu.Unlock()

	// Push our new state out immediately...
	s.broadcastState(st)

	// ...and make sure it is visible in the shared store before acknowledging,
	// so that even a reader which cannot reach us sees this increment.
	s.flushOurComponent()

	return s.n.Reply(msg, map[string]any{"type": "counted"})
}

func (s *server) handleTotal(msg maelstrom.Message) error {
	comp := s.snapshot()

	// Ask every peer for its current state, plus read every component back from
	// the shared store.  Both sources are monotonic and we keep the maximum per
	// node, so mixing them can never double-count anything.
	var wg sync.WaitGroup
	var mu sync.Mutex

	ctx, cancel := context.WithTimeout(context.Background(), peerTimeout)
	defer cancel()

	for _, peer := range s.n.NodeIDs() {
		if peer == s.n.ID() {
			continue
		}
		wg.Add(1)
		go func(p string) {
			defer wg.Done()
			res, err := s.n.SyncRPC(ctx, p, map[string]any{"type": "gc_state"})
			if err != nil {
				return
			}
			var body stateMsg
			if err := json.Unmarshal(res.Body, &body); err != nil {
				return
			}
			mu.Lock()
			mergeInto(comp, body.Comp)
			mu.Unlock()
		}(peer)
	}

	for _, peer := range s.n.NodeIDs() {
		wg.Add(1)
		go func(p string) {
			defer wg.Done()
			kctx, kcancel := context.WithTimeout(context.Background(), kvReadTimeout)
			defer kcancel()
			v, err := s.kv.ReadInt(kctx, compKey(p))
			if err != nil {
				return
			}
			mu.Lock()
			if v > comp[p] {
				comp[p] = v
			}
			mu.Unlock()
		}(peer)
	}

	wg.Wait()

	total := 0
	for _, v := range comp {
		total += v
	}

	return s.n.Reply(msg, map[string]any{
		"type":            "total_is",
		"visitors_so_far": total,
	})
}

func (s *server) handleGossip(msg maelstrom.Message) error {
	var body stateMsg
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	s.mu.Lock()
	mergeInto(s.comp, body.Comp)
	s.mu.Unlock()
	return nil
}

func (s *server) handleStateRequest(msg maelstrom.Message) error {
	st := s.snapshot()
	return s.n.Reply(msg, stateMsg{Type: "gc_state_ok", Comp: st})
}

// ---------------------------------------------------------------------------
// background work

func (s *server) gossipLoop() {
	t := time.NewTicker(gossipInterval)
	defer t.Stop()
	for range t.C {
		s.broadcastState(s.snapshot())
	}
}

func (s *server) kvFlushLoop() {
	t := time.NewTicker(kvFlushInterval)
	defer t.Stop()
	for range t.C {
		s.flushOurComponent()
	}
}

func (s *server) broadcastState(st map[string]int) {
	if len(st) == 0 {
		return
	}
	body := stateMsg{Type: "gc_gossip", Comp: st}
	for _, peer := range s.n.NodeIDs() {
		if peer == s.n.ID() {
			continue
		}
		// Fire and forget: state is re-sent periodically, so a lost gossip
		// message costs nothing.
		go func(p string) {
			_ = s.n.Send(p, body)
		}(peer)
	}
}

// flushOurComponent publishes our own (monotonically increasing) component to
// the shared store.  Holding kvMu while reading the current value means writes
// land in non-decreasing order.
func (s *server) flushOurComponent() {
	id := s.n.ID()
	if id == "" {
		return
	}

	s.kvMu.Lock()
	defer s.kvMu.Unlock()

	s.mu.Lock()
	v := s.comp[id]
	s.mu.Unlock()
	if v == 0 {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), kvWriteTimeout)
	defer cancel()
	_ = s.kv.Write(ctx, compKey(id), v)
}

// ---------------------------------------------------------------------------
// helpers

func compKey(node string) string { return "gc/" + node }

func (s *server) snapshot() map[string]int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.snapshotLocked()
}

func (s *server) snapshotLocked() map[string]int {
	out := make(map[string]int, len(s.comp))
	for k, v := range s.comp {
		out[k] = v
	}
	return out
}

func mergeInto(dst, src map[string]int) {
	for k, v := range src {
		if v > dst[k] {
			dst[k] = v
		}
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var buf [20]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		pos--
		buf[pos] = '-'
	}
	return string(buf[pos:])
}
