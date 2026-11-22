// A Maelstrom node implementing read-committed transactions over integer
// registers with total availability.
//
// Each node keeps a full copy of the store and serves every transaction from
// its local state, executing all micro-operations of a transaction
// atomically (under one lock) so that no other transaction ever observes an
// intermediate write. Committed transactions are replicated to the other
// nodes by a retransmit-until-acked gossip loop, which tolerates partitions
// (messages sent across a split are silently dropped). A transaction's final
// value for each key is the only thing ever applied at a replica, so a
// transaction's intermediate values are never visible anywhere, and since
// transactions always answer txn_ok, no aborted write is ever visible either.
package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// writeOp is a single write (key -> value) belonging to a committed txn.
// Keys and values are kept as raw JSON so that integers are echoed back to
// clients with exactly the same literal representation they arrived with.
type writeOp struct {
	K json.RawMessage `json:"k"`
	V json.RawMessage `json:"v"`
}

// txnRec is a committed transaction, identified by a per-node sequence
// number. Only the final value written to each key is recorded, so replaying
// the record at a replica exposes exactly one version per key.
type txnRec struct {
	Seq    int64     `json:"seq"`
	Writes []writeOp `json:"w"`
}

// mop is one parsed micro-operation: [f, k, v]. All fields are kept as raw
// JSON text so replies echo them byte-for-byte.
type mop struct {
	f string // "r" or "w"
	k string // raw JSON text of the key
	v string // raw JSON text of the value; "" means null
}

type server struct {
	n *maelstrom.Node

	mu sync.Mutex
	// Local replica of the store: raw JSON key text -> raw JSON value text.
	store map[string]string
	// Commit log of transactions that originated here, in order.
	log []txnRec
	// Per peer: how many of the leading log records that peer has acked.
	acked map[string]int64
	// Per source node: how many of its leading records we've applied, plus
	// any out-of-order records we've applied beyond that point.
	applied map[string]int64
	extra   map[string]map[int64]bool

	sending int32 // guards against concurrent replicate sweeps
}

func newServer(n *maelstrom.Node) *server {
	return &server{
		n:       n,
		store:   make(map[string]string),
		acked:   make(map[string]int64),
		applied: make(map[string]int64),
		extra:   make(map[string]map[int64]bool),
	}
}

// peers returns all node IDs other than ours.
func (s *server) peers() []string {
	ids := s.n.NodeIDs()
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		if id != s.n.ID() {
			out = append(out, id)
		}
	}
	return out
}

// parseTxn decodes a client transaction into mops.
func parseTxn(raw json.RawMessage) ([]mop, bool) {
	var fields [][]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return nil, false
	}
	mops := make([]mop, 0, len(fields))
	for _, f := range fields {
		if len(f) != 3 {
			return nil, false
		}
		var op string
		if err := json.Unmarshal(f[0], &op); err != nil {
			return nil, false
		}
		k := strings.TrimSpace(string(f[1]))
		v := strings.TrimSpace(string(f[2]))
		if v == "null" {
			v = ""
		}
		mops = append(mops, mop{f: op, k: k, v: v})
	}
	return mops, true
}

// encodeMop renders a micro-operation with its value filled in.
func encodeMop(m mop, val string) json.RawMessage {
	if val == "" {
		val = "null"
	}
	return json.RawMessage(`["` + m.f + `",` + m.k + `,` + val + `]`)
}

// handleTxn executes a client transaction against the local state and
// replies immediately; replication to peers happens afterwards.
func (s *server) handleTxn(msg maelstrom.Message) error {
	var req struct {
		MsgID int             `json:"msg_id"`
		Txn   json.RawMessage `json:"txn"`
	}
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		return err
	}

	mops, ok := parseTxn(req.Txn)
	if !ok {
		// Should never happen: Maelstrom validates the txn schema. If it
		// somehow does, echo the transaction back rather than failing (we
		// must stay totally available).
		return s.n.Send(msg.Src, map[string]any{
			"type":        "txn_ok",
			"in_reply_to": req.MsgID,
			"txn":         req.Txn,
		})
	}

	// Execute the transaction atomically, in order: each micro-operation sees
	// the effects of the ones before it. Nothing outside this critical
	// section can observe the store while we hold the lock, so other
	// transactions only ever see the final value of every key we write.
	finals := make(map[string]string, len(mops))
	out := make([]json.RawMessage, len(mops))

	s.mu.Lock()
	for i, m := range mops {
		if m.f == "w" {
			s.store[m.k] = m.v
			finals[m.k] = m.v
			out[i] = encodeMop(m, m.v) // writes are echoed as sent
		} else {
			var val string
			if v, ok := s.store[m.k]; ok {
				val = v
			}
			out[i] = encodeMop(m, val)
		}
	}
	if len(finals) > 0 {
		writes := make([]writeOp, 0, len(finals))
		for k, v := range finals {
			writes = append(writes, writeOp{K: json.RawMessage(k), V: json.RawMessage(v)})
		}
		s.log = append(s.log, txnRec{Seq: int64(len(s.log)), Writes: writes})
	}
	s.mu.Unlock()

	if err := s.n.Send(msg.Src, struct {
		Type      string            `json:"type"`
		InReplyTo int               `json:"in_reply_to,omitempty"`
		Txn       []json.RawMessage `json:"txn"`
	}{Type: "txn_ok", InReplyTo: req.MsgID, Txn: out}); err != nil {
		return err
	}

	// Best-effort immediate replication; the ticker covers the rest.
	s.replicateStep(64)
	return nil
}

// handleReplicate applies transactions from a peer and acks the highest
// contiguous sequence number we've applied, so the sender can stop
// retransmitting them. Records are applied atomically, and each record only
// carries the final value written to each key.
func (s *server) handleReplicate(msg maelstrom.Message) error {
	var req struct {
		Txns []txnRec `json:"txns"`
	}
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		return err
	}

	s.mu.Lock()
	prefix := s.applied[msg.Src]
	extra := s.extra[msg.Src]
	if extra == nil {
		extra = make(map[int64]bool)
	}
	for _, rec := range req.Txns {
		if rec.Seq < prefix || extra[rec.Seq] {
			continue // already applied
		}
		for _, w := range rec.Writes {
			s.store[string(w.K)] = string(w.V)
		}
		extra[rec.Seq] = true
	}
	for extra[prefix] {
		delete(extra, prefix)
		prefix++
	}
	s.applied[msg.Src] = prefix
	if len(extra) == 0 {
		delete(s.extra, msg.Src)
	} else {
		s.extra[msg.Src] = extra
	}
	s.mu.Unlock()

	return s.n.Send(msg.Src, struct {
		Type string `json:"type"`
		Upto int64  `json:"upto"`
	}{Type: "replicate_ack", Upto: prefix})
}

// handleReplicateAck advances the per-peer ack watermark.
func (s *server) handleReplicateAck(msg maelstrom.Message) error {
	var req struct {
		Upto int64 `json:"upto"`
	}
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		return err
	}
	s.mu.Lock()
	if req.Upto > s.acked[msg.Src] {
		s.acked[msg.Src] = req.Upto
	}
	s.mu.Unlock()
	return nil
}

// replicateStep pushes up to max unacknowledged transactions to each peer.
// Messages lost across a network split are simply retransmitted on a later
// step, and replicas deduplicate by sequence number, so replication is
// at-least-once and eventually complete.
func (s *server) replicateStep(max int64) {
	if max <= 0 {
		return
	}
	// Only one sweep at a time; a sweep that finds nothing to do is cheap,
	// and the ticker will pick up anything we skip.
	if !atomic.CompareAndSwapInt32(&s.sending, 0, 1) {
		return
	}
	defer atomic.StoreInt32(&s.sending, 0)

	for _, peer := range s.peers() {
		s.mu.Lock()
		start := s.acked[peer]
		total := int64(len(s.log))
		if start >= total {
			s.mu.Unlock()
			continue
		}
		end := total
		if end-start > max {
			end = start + max
		}
		recs := make([]txnRec, end-start)
		copy(recs, s.log[start:end])
		s.mu.Unlock()

		if err := s.n.Send(peer, struct {
			Type string   `json:"type"`
			Txns []txnRec `json:"txns"`
		}{Type: "replicate", Txns: recs}); err != nil {
			// Nothing we can do about a broken pipe here; try again later.
			return
		}
	}
}

// startReplicator retransmits unacknowledged transactions forever.
func (s *server) startReplicator(interval time.Duration) {
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for range t.C {
			s.replicateStep(1024)
		}
	}()
}

func main() {
	n := maelstrom.NewNode()
	s := newServer(n)

	n.Handle("txn", s.handleTxn)
	n.Handle("replicate", s.handleReplicate)
	n.Handle("replicate_ack", s.handleReplicateAck)
	// Maelstrom sends a topology message to some workloads; answer it so we
	// never die on an unhandled message type.
	n.Handle("topology", func(msg maelstrom.Message) error {
		return s.n.Reply(msg, map[string]any{"type": "topology_ok"})
	})

	s.startReplicator(50 * time.Millisecond)

	if err := n.Run(); err != nil {
		fmt.Println("fatal:", err)
	}
}
