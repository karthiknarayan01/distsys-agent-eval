package main

import (
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"strconv"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// Mop is one micro-operation in a transaction.  Mops are encoded as JSON
// arrays, so this type supplies the array encoding itself.
type Mop struct {
	Op    string
	Key   int64
	Value *int64
}

func (m *Mop) UnmarshalJSON(data []byte) error {
	var a []json.RawMessage
	if err := json.Unmarshal(data, &a); err != nil {
		return err
	}
	if len(a) != 3 {
		return fmt.Errorf("micro-operation must have three elements, got %d", len(a))
	}
	if err := json.Unmarshal(a[0], &m.Op); err != nil {
		return fmt.Errorf("decode operation: %w", err)
	}
	if err := json.Unmarshal(a[1], &m.Key); err != nil {
		return fmt.Errorf("decode key: %w", err)
	}
	if string(a[2]) != "null" {
		var v int64
		if err := json.Unmarshal(a[2], &v); err != nil {
			return fmt.Errorf("decode value: %w", err)
		}
		m.Value = &v
	}
	return nil
}

func (m Mop) MarshalJSON() ([]byte, error) {
	b := make([]byte, 0, 32)
	b = append(b, '[')
	b = strconv.AppendQuote(b, m.Op)
	b = append(b, ',')
	b = strconv.AppendInt(b, m.Key, 10)
	b = append(b, ',')
	if m.Value == nil {
		b = append(b, "null"...)
	} else {
		b = strconv.AppendInt(b, *m.Value, 10)
	}
	b = append(b, ']')
	return b, nil
}

// TxID is a globally unique, Lamport-style transaction timestamp.  Counter
// orders causally related transactions; Node breaks ties between concurrent
// transactions.
type TxID struct {
	Counter int64  `json:"counter"`
	Node    string `json:"node"`
}

func (id TxID) less(other TxID) bool {
	if id.Counter != other.Counter {
		return id.Counter < other.Counter
	}
	return id.Node < other.Node
}

type Write struct {
	Key   int64 `json:"key"`
	Value int64 `json:"value"`
}

// Record is a committed transaction.  Only the final write to each key is
// retained: intermediate writes must never become externally visible.
type Record struct {
	ID     TxID    `json:"id"`
	Origin string  `json:"origin"`
	Writes []Write `json:"writes"`
}

type Version struct {
	ID    TxID
	Value int64
}

type txnRequest struct {
	Type  string `json:"type"`
	MsgID int    `json:"msg_id"`
	Txn   []Mop  `json:"txn"`
}

type txnResponse struct {
	Type string `json:"type"`
	Txn  []Mop  `json:"txn"`
}

type replicateRequest struct {
	Type string   `json:"type"`
	Txns []Record `json:"txns"`
}

type replicateAck struct {
	Type string `json:"type"`
	IDs  []TxID `json:"ids"`
}

type store struct {
	mu sync.Mutex

	node  *maelstrom.Node
	self  string
	peers []string

	// clock is also the counter used for the next locally-created timestamp.
	clock int64

	// state contains the latest version of each key.
	state map[int64]Version

	// records and pending are retained for eventual, idempotent replication.
	records  map[TxID]Record
	pending  map[string]map[TxID]Record
	lastSent map[string]map[TxID]time.Time
}

func newStore(n *maelstrom.Node) *store {
	return &store{
		node:     n,
		state:    make(map[int64]Version),
		records:  make(map[TxID]Record),
		pending:  make(map[string]map[TxID]Record),
		lastSent: make(map[string]map[TxID]time.Time),
	}
}

func (s *store) start() {
	go s.replicateLoop()
}

func (s *store) handleTxn(msg maelstrom.Message) error {
	var req txnRequest
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		return fmt.Errorf("decode txn request: %w", err)
	}

	s.mu.Lock()

	// Every local transaction gets a fresh Lamport timestamp.  It is larger
	// than every timestamp this node has observed.
	s.clock++
	id := TxID{Counter: s.clock, Node: s.self}

	// Execute all operations as one local transaction.  A read after a write
	// in this transaction observes that write; writes are otherwise read from
	// the committed state.
	result := make([]Mop, len(req.Txn))
	copy(result, req.Txn)
	own := make(map[int64]int64)
	for i := range result {
		m := &result[i]
		switch m.Op {
		case "r":
			if v, ok := own[m.Key]; ok {
				v := v
				m.Value = &v
			} else if v, ok := s.state[m.Key]; ok {
				v := v.Value
				m.Value = &v
			} else {
				m.Value = nil
			}
		case "w":
			if m.Value == nil {
				s.mu.Unlock()
				return fmt.Errorf("write to key %d has a null value", m.Key)
			}
			own[m.Key] = *m.Value
		default:
			s.mu.Unlock()
			return fmt.Errorf("unknown micro-operation %q", m.Op)
		}
	}

	// The final write to each key is the transaction's externally visible
	// effect.  Intermediate writes are deliberately discarded.
	writes := make([]Write, 0, len(own))
	for k, v := range own {
		writes = append(writes, Write{Key: k, Value: v})
	}
	sort.Slice(writes, func(i, j int) bool { return writes[i].Key < writes[j].Key })

	// A local timestamp is newer than every version currently known here.
	for _, w := range writes {
		s.state[w.Key] = Version{ID: id, Value: w.Value}
	}

	rec := Record{ID: id, Origin: s.self, Writes: writes}
	s.records[id] = rec
	for _, peer := range s.peers {
		if s.pending[peer] == nil {
			s.pending[peer] = make(map[TxID]Record)
		}
		s.pending[peer][id] = rec
	}
	s.mu.Unlock()

	return s.node.Reply(msg, txnResponse{Type: "txn_ok", Txn: result})
}

func (s *store) applyRecord(rec Record) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Receiving a transaction advances our Lamport clock, so transactions we
	// create later are ordered after it.
	if rec.ID.Counter >= s.clock {
		s.clock = rec.ID.Counter + 1
	}
	if _, known := s.records[rec.ID]; known {
		return
	}
	s.records[rec.ID] = rec

	// Apply all final writes while holding the same lock used by client
	// transactions.  Thus another client can never see a partially applied
	// replicated transaction.
	for _, w := range rec.Writes {
		cur, ok := s.state[w.Key]
		if !ok || rec.ID.less(cur.ID) {
			s.state[w.Key] = Version{ID: rec.ID, Value: w.Value}
		}
	}
}

func (s *store) handleReplicate(msg maelstrom.Message) error {
	var req replicateRequest
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		return fmt.Errorf("decode replicate request: %w", err)
	}

	ids := make([]TxID, 0, len(req.Txns))
	for _, rec := range req.Txns {
		// Only accept records from the other node in this two-node cluster.
		if rec.Origin == s.self {
			continue
		}
		s.applyRecord(rec)
		ids = append(ids, rec.ID)
	}
	return s.node.Send(msg.Src, replicateAck{Type: "replicate_ack", IDs: ids})
}

func (s *store) handleAck(msg maelstrom.Message) error {
	var ack replicateAck
	if err := json.Unmarshal(msg.Body, &ack); err != nil {
		return fmt.Errorf("decode replicate ack: %w", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	for _, id := range ack.IDs {
		for _, peer := range s.peers {
			delete(s.pending[peer], id)
			delete(s.lastSent[peer], id)
		}
	}
	return nil
}

func (s *store) replicateLoop() {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for range ticker.C {
		now := time.Now()
		type item struct {
			peer string
			rec  Record
		}
		batches := make(map[string][]Record)

		s.mu.Lock()
		for peer, pending := range s.pending {
			sent := s.lastSent[peer]
			for id, rec := range pending {
				// Retransmit periodically.  Messages sent during a partition
				// disappear without an error, so an acknowledgement alone
				// cannot be used as proof of delivery.
				if last, ok := sent[id]; ok && now.Sub(last) < 100*time.Millisecond {
					continue
				}
				if len(batches[peer]) >= 128 {
					continue
				}
				batches[peer] = append(batches[peer], rec)
				if sent == nil {
					sent = make(map[TxID]time.Time)
					s.lastSent[peer] = sent
				}
				sent[id] = now
			}
		}
		s.mu.Unlock()

		for peer, recs := range batches {
			if err := s.node.Send(peer, replicateRequest{Type: "replicate", Txns: recs}); err != nil {
				log.Printf("replication send to %s failed: %v", peer, err)
			}
		}
	}
}

func main() {
	n := maelstrom.NewNode()
	s := newStore(n)

	n.Handle("txn", s.handleTxn)
	n.Handle("replicate", s.handleReplicate)
	n.Handle("replicate_ack", s.handleAck)
	n.Handle("init", func(msg maelstrom.Message) error {
		s.mu.Lock()
		s.self = n.ID()
		for _, id := range n.NodeIDs() {
			if id != s.self {
				s.peers = append(s.peers, id)
			}
		}
		s.mu.Unlock()
		s.start()
		return nil
	})

	if err := n.Run(); err != nil {
		log.Fatalf("node failed: %v", err)
	}
}
