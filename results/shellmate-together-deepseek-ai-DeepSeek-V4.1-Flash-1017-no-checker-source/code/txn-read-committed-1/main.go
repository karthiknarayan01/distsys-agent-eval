package main

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// A versioned register value.
//
// Writes are stamped with a Lamport timestamp (plus the node index as a
// deterministic tie-breaker) so that replicas can merge concurrently written
// values with a last-writer-wins rule: a delayed, retried or duplicated
// replication message never clobbers a newer value.
type wval struct {
	val  int64
	ts   uint64
	node int
}

// newer reports whether a strictly supersedes b.
func newer(a, b wval) bool {
	if a.ts != b.ts {
		return a.ts > b.ts
	}
	return a.node > b.node
}

// entry is the wire representation of one replicated key/value pair.
type entry struct {
	K  int64  `json:"k"`
	V  int64  `json:"v"`
	Ts uint64 `json:"ts"`
	N  int    `json:"n"`
}

// flight is a batch of writes which has been shipped to the peer but not yet
// acknowledged. Unacknowledged batches are merged back into the pending pool
// and resent, which is what lets writes made during a partition reach the other
// node once the network heals.
type flight struct {
	writes []entry
	sentAt time.Time
}

// mop is a single micro-operation of a transaction.
type mop struct {
	f string
	k int64
	v int64
}

func (m *mop) UnmarshalJSON(data []byte) error {
	var arr []json.RawMessage
	if err := json.Unmarshal(data, &arr); err != nil {
		return err
	}
	if len(arr) != 3 {
		return fmt.Errorf("micro-op must have 3 elements, got %d", len(arr))
	}
	if err := json.Unmarshal(arr[0], &m.f); err != nil {
		return err
	}
	if err := json.Unmarshal(arr[1], &m.k); err != nil {
		return err
	}
	if string(arr[2]) != "null" {
		if err := json.Unmarshal(arr[2], &m.v); err != nil {
			return err
		}
	}
	return nil
}

const (
	// Maximum number of key/value pairs in a single replication message. This
	// keeps every message far below Maelstrom's 64KiB line limit.
	maxBatch = 100
	// How often the replication loop runs.
	tickInterval = 25 * time.Millisecond
	// How long to wait for an acknowledgement before retrying a batch.
	ackTimeout = 250 * time.Millisecond
)

type server struct {
	n *maelstrom.Node

	mu       sync.Mutex
	state    map[int64]wval
	clock    uint64
	me       int
	peer     string
	pending  map[int64]wval // committed locally, not yet acked by the peer
	inflight map[uint64]*flight
	seq      uint64
	started  bool
}

func main() {
	s := &server{
		n:        maelstrom.NewNode(),
		state:    make(map[int64]wval),
		pending:  make(map[int64]wval),
		inflight: make(map[uint64]*flight),
	}

	s.n.Handle("init", s.handleInit)
	s.n.Handle("txn", s.handleTxn)
	s.n.Handle("replicate", s.handleReplicate)
	s.n.Handle("replicate_ok", s.handleReplicateOK)

	if err := s.n.Run(); err != nil {
		log.Fatal(err)
	}
}

func (s *server) handleInit(msg maelstrom.Message) error {
	ids := s.n.NodeIDs()
	s.mu.Lock()
	s.me = 0
	s.peer = ""
	for i, id := range ids {
		if id == s.n.ID() {
			s.me = i
		} else if s.peer == "" {
			s.peer = id
		}
	}
	started := s.started
	s.started = true
	s.mu.Unlock()

	if !started {
		go s.replicateLoop()
	}
	return nil
}

// handleTxn executes a transaction.
//
// All the micro-operations of a transaction are applied to the local store
// while holding the store lock, so that no other transaction on this node can
// observe a half-applied transaction, and no other transaction can read a write
// which this one subsequently overwrites. Only the final value this transaction
// gives each key is ever published to the other node - intermediate values such
// as the 6 in a "w 6, w 7" pair never leave this node - and publication happens
// only after the transaction has been answered. That is what keeps the store
// read-committed: nobody can read an intermediate or uncommitted write.
//
// Nothing here waits on the other node, so every transaction is answered.
func (s *server) handleTxn(msg maelstrom.Message) error {
	var body struct {
		Txn []mop `json:"txn"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	out := make([][]any, 0, len(body.Txn))
	written := make(map[int64]wval)

	s.mu.Lock()
	s.clock++
	ts := s.clock
	for _, op := range body.Txn {
		switch op.f {
		case "r":
			if cur, ok := s.state[op.k]; ok {
				out = append(out, []any{"r", op.k, cur.val})
			} else {
				out = append(out, []any{"r", op.k, nil})
			}
		case "w":
			// Applied unconditionally: this transaction always observes its own
			// writes. A concurrent remote version may be merged over the top
			// later, which read-committed permits.
			v := wval{val: op.v, ts: ts, node: s.me}
			s.state[op.k] = v
			written[op.k] = v
			out = append(out, []any{"w", op.k, op.v})
		default:
			// Unknown micro-op: echo it back untouched.
			out = append(out, []any{op.f, op.k, nil})
		}
	}
	s.mu.Unlock()

	err := s.n.Reply(msg, map[string]any{
		"type": "txn_ok",
		"txn":  out,
	})

	// Commit: now (and only now) the transaction's final writes become eligible
	// for replication to the other node.
	if len(written) > 0 {
		s.mu.Lock()
		for k, v := range written {
			if cur, ok := s.pending[k]; !ok || newer(v, cur) {
				s.pending[k] = v
			}
		}
		s.mu.Unlock()
	}

	return err
}

// handleReplicate merges a batch of writes from the other node and acknowledges
// it. Merging is last-writer-wins on (lamport timestamp, node index) and
// idempotent, so duplicate or reordered deliveries are harmless.
func (s *server) handleReplicate(msg maelstrom.Message) error {
	var body struct {
		Seq    uint64  `json:"seq"`
		Writes []entry `json:"writes"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	s.mu.Lock()
	for _, e := range body.Writes {
		in := wval{val: e.V, ts: e.Ts, node: e.N}
		if cur, ok := s.state[e.K]; !ok || newer(in, cur) {
			s.state[e.K] = in
		}
		if e.Ts > s.clock {
			s.clock = e.Ts
		}
	}
	s.mu.Unlock()

	return s.n.Send(msg.Src, map[string]any{
		"type": "replicate_ok",
		"seq":  body.Seq,
	})
}

func (s *server) handleReplicateOK(msg maelstrom.Message) error {
	var body struct {
		Seq uint64 `json:"seq"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	s.mu.Lock()
	delete(s.inflight, body.Seq)
	s.mu.Unlock()
	return nil
}

// replicateLoop ships committed local writes to the other node. Each tick it
// retires unacknowledged batches back into the pending pool and sends one fresh
// batch. Batches are small, so no message approaches the line-length limit;
// unacknowledged work is retried, so nothing is lost when a partition drops
// messages, and it all lands once the network heals.
func (s *server) replicateLoop() {
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()

	for range ticker.C {
		now := time.Now()

		s.mu.Lock()
		peer := s.peer
		if peer == "" {
			s.mu.Unlock()
			continue
		}

		// Retire unacknowledged batches: put their writes back into the
		// pending pool (keeping the newest version per key) so they are resent.
		for seq, f := range s.inflight {
			if now.Sub(f.sentAt) < ackTimeout {
				continue
			}
			for _, e := range f.writes {
				in := wval{val: e.V, ts: e.Ts, node: e.N}
				if cur, ok := s.pending[e.K]; !ok || newer(in, cur) {
					s.pending[e.K] = in
				}
			}
			delete(s.inflight, seq)
		}

		// Ship one batch of pending writes.
		var payload map[string]any
		if len(s.pending) > 0 {
			batch := make([]entry, 0, maxBatch)
			for k, v := range s.pending {
				batch = append(batch, entry{K: k, V: v.val, Ts: v.ts, N: v.node})
				delete(s.pending, k)
				if len(batch) >= maxBatch {
					break
				}
			}
			seq := s.seq
			s.seq++
			s.inflight[seq] = &flight{writes: batch, sentAt: now}
			payload = map[string]any{
				"type":   "replicate",
				"seq":    seq,
				"writes": batch,
			}
		}
		s.mu.Unlock()

		if payload != nil {
			if err := s.n.Send(peer, payload); err != nil {
				log.Printf("replicate send: %v", err)
			}
		}
	}
}
