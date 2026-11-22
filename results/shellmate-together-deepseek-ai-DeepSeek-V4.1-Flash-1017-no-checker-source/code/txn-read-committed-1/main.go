package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// A timestamp is a Lamport counter plus the originating node, which makes it
// globally unique.  [(counter, node)] is ordered lexicographically.
type Timestamp struct {
	Counter int64  `json:"counter"`
	Node    string `json:"node"`
}

func (a Timestamp) Less(b Timestamp) bool {
	if a.Counter != b.Counter {
		return a.Counter < b.Counter
	}
	return a.Node < b.Node
}

type Register struct {
	Value any       `json:"value"`
	TS    Timestamp `json:"ts"`
}

// TxnPlus is a completed transaction together with its timestamp.  The
// remote node only needs the writes, but keeping the completed txn makes the
// replication protocol easy to inspect and mirrors the standard HAT design.
type TxnPlus struct {
	TS  Timestamp `json:"ts"`
	Txn [][]any   `json:"txn"`
}

type Store struct {
	mu      sync.Mutex
	nodeID  string
	lamport int64
	kv      map[string]Register
}

func NewStore() *Store {
	return &Store{kv: make(map[string]Register)}
}

func (s *Store) SetNodeID(id string) {
	s.mu.Lock()
	s.nodeID = id
	s.mu.Unlock()
}

func keyID(v any) string {
	switch x := v.(type) {
	case json.Number:
		return "n:" + x.String()
	case string:
		return "s:" + x
	case float64:
		return "n:" + strconv.FormatFloat(x, 'g', -1, 64)
	case float32:
		return "n:" + strconv.FormatFloat(float64(x), 'g', -1, 32)
	case int:
		return "n:" + strconv.Itoa(x)
	case int64:
		return "n:" + strconv.FormatInt(x, 10)
	case nil:
		return "nil"
	default:
		b, _ := json.Marshal(v)
		return "j:" + string(b)
	}
}

// Transact executes a transaction atomically against the local state.  Writes
// are buffered until the end so no other transaction can observe an
// intermediate write.  Reads before a write in the same transaction see the
// committed local state; reads after a write see the transaction's own value.
func (s *Store) Transact(ops [][]any) (result [][]any, tp TxnPlus) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.lamport++
	ts := Timestamp{Counter: s.lamport, Node: s.nodeID}

	result = make([][]any, len(ops))
	overlay := make(map[string]any)
	writes := make(map[string]any)

	for i, raw := range ops {
		if len(raw) < 3 {
			result[i] = raw
			continue
		}
		f, _ := raw[0].(string)
		k := keyID(raw[1])
		switch f {
		case "r":
			v, ok := overlay[k]
			if !ok {
				if reg, exists := s.kv[k]; exists {
					v = reg.Value
				}
			}
			result[i] = []any{"r", raw[1], v}
		case "w":
			val := raw[2]
			overlay[k] = val
			writes[k] = val
			result[i] = []any{"w", raw[1], val}
		default:
			result[i] = raw
		}
	}

	for k, v := range writes {
		cur, exists := s.kv[k]
		if !exists || cur.TS.Less(ts) {
			s.kv[k] = Register{Value: v, TS: ts}
		}
	}

	tp = TxnPlus{TS: ts, Txn: result}
	return result, tp
}

// ApplyRemote applies the final writes of a replicated transaction.  The
// whole transaction is applied under one lock, so local reads can never see a
// partially applied transaction.
func (s *Store) ApplyRemote(tp TxnPlus) {
	final := make(map[string]any)
	for _, raw := range tp.Txn {
		if len(raw) < 3 {
			continue
		}
		if f, _ := raw[0].(string); f == "w" {
			final[keyID(raw[1])] = raw[2]
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.lamport <= tp.TS.Counter {
		s.lamport = tp.TS.Counter + 1
	}
	for k, v := range final {
		cur, exists := s.kv[k]
		if !exists || cur.TS.Less(tp.TS) {
			s.kv[k] = Register{Value: v, TS: tp.TS}
		}
	}
}

type pendingTxn struct {
	tp       TxnPlus
	lastSent time.Time
}

type Replicator struct {
	mu      sync.Mutex
	pending map[Timestamp]*pendingTxn
	node    *maelstrom.Node
	started bool
}

func NewReplicator(n *maelstrom.Node) *Replicator {
	return &Replicator{
		pending: make(map[Timestamp]*pendingTxn),
		node:    n,
	}
}

func (r *Replicator) Add(tp TxnPlus) {
	r.mu.Lock()
	r.pending[tp.TS] = &pendingTxn{tp: tp}
	r.mu.Unlock()
}

func (r *Replicator) Ack(tss []Timestamp) {
	r.mu.Lock()
	for _, ts := range tss {
		delete(r.pending, ts)
	}
	r.mu.Unlock()
}

func (r *Replicator) Start() {
	r.mu.Lock()
	if r.started {
		r.mu.Unlock()
		return
	}
	r.started = true
	r.mu.Unlock()
	go r.loop()
}

const (
	tickInterval     = 20 * time.Millisecond
	retryAfter       = 120 * time.Millisecond
	maxBatch         = 500
	maxBatchesPerRun = 8
)

func (r *Replicator) loop() {
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()
	for range ticker.C {
		r.flush()
	}
}

func (r *Replicator) flush() {
	self := r.node.ID()
	if self == "" {
		return
	}
	var peers []string
	for _, id := range r.node.NodeIDs() {
		if id != self {
			peers = append(peers, id)
		}
	}
	if len(peers) == 0 {
		return
	}

	now := time.Now()
	for _, dest := range peers {
		for batchNo := 0; batchNo < maxBatchesPerRun; batchNo++ {
			batch := make([]TxnPlus, 0, maxBatch)
			r.mu.Lock()
			for _, p := range r.pending {
				if now.Sub(p.lastSent) >= retryAfter {
					batch = append(batch, p.tp)
					p.lastSent = now
					if len(batch) >= maxBatch {
						break
					}
				}
			}
			r.mu.Unlock()

			if len(batch) == 0 {
				break
			}
			// If the message is lost, it remains pending and is retried
			// after retryAfter.
			_ = r.node.Send(dest, map[string]any{
				"type": "replicate",
				"txns": batch,
			})
		}
	}
}

type txnRequest struct {
	Type  string  `json:"type"`
	MsgID int     `json:"msg_id"`
	Txn   [][]any `json:"txn"`
}

type txnResponse struct {
	Type string  `json:"type"`
	Txn  [][]any `json:"txn"`
}

type replicateRequest struct {
	Type string    `json:"type"`
	Txns []TxnPlus `json:"txns"`
}

type replicateAck struct {
	Type string      `json:"type"`
	TSS  []Timestamp `json:"tss"`
}

type ackBody struct {
	Type string      `json:"type"`
	TSS  []Timestamp `json:"tss"`
}

func decodeUseNumber(data []byte, out any) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	return dec.Decode(out)
}

func main() {
	n := maelstrom.NewNode()
	store := NewStore()
	replicator := NewReplicator(n)

	n.Handle("init", func(msg maelstrom.Message) error {
		store.SetNodeID(n.ID())
		replicator.Start()
		return nil
	})

	n.Handle("txn", func(msg maelstrom.Message) error {
		var req txnRequest
		if err := decodeUseNumber(msg.Body, &req); err != nil {
			return fmt.Errorf("decode txn: %w", err)
		}
		result, tp := store.Transact(req.Txn)
		replicator.Add(tp)
		return n.Reply(msg, txnResponse{Type: "txn_ok", Txn: result})
	})

	n.Handle("replicate", func(msg maelstrom.Message) error {
		var req replicateRequest
		if err := decodeUseNumber(msg.Body, &req); err != nil {
			return fmt.Errorf("decode replicate: %w", err)
		}
		tss := make([]Timestamp, 0, len(req.Txns))
		for _, tp := range req.Txns {
			store.ApplyRemote(tp)
			tss = append(tss, tp.TS)
		}
		if msg.Src != "" && msg.Src != n.ID() {
			_ = n.Send(msg.Src, replicateAck{Type: "replicate_ack", TSS: tss})
		}
		return nil
	})

	n.Handle("replicate_ack", func(msg maelstrom.Message) error {
		var body ackBody
		if err := decodeUseNumber(msg.Body, &body); err != nil {
			return fmt.Errorf("decode replicate_ack: %w", err)
		}
		replicator.Ack(body.TSS)
		return nil
	})

	if err := n.Run(); err != nil {
		panic(err)
	}
}
