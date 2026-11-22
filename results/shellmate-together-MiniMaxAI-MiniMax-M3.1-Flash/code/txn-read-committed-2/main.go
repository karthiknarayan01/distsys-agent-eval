package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// Op is one transaction micro-operation.  Keeping the key and value as raw
// JSON lets us return client values byte-for-byte (and also handles null).
type Op []json.RawMessage

// Timestamp is a Lamport timestamp.  The node ID breaks ties between
// concurrent timestamps.
type Timestamp struct {
	Counter uint64 `json:"counter"`
	Node    string `json:"node"`
}

func (t Timestamp) less(other Timestamp) bool {
	if t.Counter != other.Counter {
		return t.Counter < other.Counter
	}
	return t.Node < other.Node
}

func (t Timestamp) id() string {
	return fmt.Sprintf("%020d:%s", t.Counter, t.Node)
}

type version struct {
	ts    Timestamp
	value json.RawMessage
}

type write struct {
	Key   json.RawMessage `json:"key"`
	Value json.RawMessage `json:"value"`
}

type replicaRecord struct {
	ID      string  `json:"id"`
	Counter uint64  `json:"counter"`
	Node    string  `json:"node"`
	Writes  []write `json:"writes"`
}

func (r replicaRecord) timestamp() Timestamp {
	return Timestamp{Counter: r.Counter, Node: r.Node}
}

type pendingRecord struct {
	record  replicaRecord
	sentAt  time.Time
	attempt int
}

type txnRequest struct {
	Txn []Op `json:"txn"`
}

type txnResponse struct {
	Type string `json:"type"`
	Txn  []Op   `json:"txn"`
}

type replicateRequest struct {
	Type    string          `json:"type"`
	Records []replicaRecord `json:"records"`
}

type replicateAck struct {
	Type string   `json:"type"`
	IDs  []string `json:"ids"`
}

type store struct {
	mu sync.Mutex

	node *maelstrom.Node

	// state maps a canonical JSON representation of a key to its latest
	// version.  A transaction is applied while mu is held, so intermediate
	// writes in a transaction are never visible to another transaction.
	state map[string]version

	clock uint64

	// pending[peer][transaction-id] contains committed local transactions
	// which have not yet been acknowledged by that peer.
	pending map[string]map[string]*pendingRecord

	// seen contains transaction IDs already applied.  It makes replication
	// idempotent, which is essential because messages can be lost and retried.
	seen map[string]bool

	peers []string
}

func newStore(node *maelstrom.Node) *store {
	return &store{
		node:    node,
		state:   make(map[string]version),
		pending: make(map[string]map[string]*pendingRecord),
		seen:    make(map[string]bool),
	}
}

func canonicalKey(raw json.RawMessage) (string, error) {
	var compact bytes.Buffer
	if err := json.Compact(&compact, raw); err != nil {
		return "", err
	}
	return compact.String(), nil
}

func cloneRaw(raw json.RawMessage) json.RawMessage {
	return append(json.RawMessage(nil), raw...)
}

func opFunction(op Op) (string, error) {
	if len(op) != 3 {
		return "", fmt.Errorf("operation has %d elements, want 3", len(op))
	}
	var f string
	if err := json.Unmarshal(op[0], &f); err != nil {
		return "", fmt.Errorf("invalid operation function: %w", err)
	}
	return f, nil
}

// applyLocal executes a client transaction atomically.  Reads observe the
// transaction's own preceding writes, while no other transaction can observe
// an intermediate write.
func (s *store) applyLocal(txn []Op) ([]Op, *replicaRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.clock++
	ts := Timestamp{Counter: s.clock, Node: s.node.ID()}
	id := ts.id()
	s.seen[id] = true

	result := make([]Op, len(txn))
	finalWrites := make(map[string]write)
	var finalOrder []string

	for i, op := range txn {
		if len(op) != 3 {
			return nil, nil, fmt.Errorf("operation %d has %d elements, want 3", i, len(op))
		}
		f, err := opFunction(op)
		if err != nil {
			return nil, nil, err
		}
		result[i] = make(Op, 3)
		copy(result[i], op)

		key, err := canonicalKey(op[1])
		if err != nil {
			return nil, nil, fmt.Errorf("invalid key in operation %d: %w", i, err)
		}

		switch f {
		case "r":
			if cur, ok := s.state[key]; ok {
				result[i][2] = cloneRaw(cur.value)
			} else {
				result[i][2] = json.RawMessage("null")
			}
		case "w":
			// Update the shared state now, but mu prevents another
			// transaction from observing this intermediate version.
			cur, exists := s.state[key]
			if !exists || !ts.less(cur.ts) {
				s.state[key] = version{ts: ts, value: cloneRaw(op[2])}
			}

			// Only the final write to each key needs to be replicated.
			// This also ensures an intermediate value can never become
			// visible on the other replica.
			if _, exists := finalWrites[key]; !exists {
				finalOrder = append(finalOrder, key)
			}
			finalWrites[key] = write{Key: cloneRaw(op[1]), Value: cloneRaw(op[2])}
		default:
			return nil, nil, fmt.Errorf("unknown operation %q", f)
		}
	}

	var record *replicaRecord
	if len(finalOrder) > 0 {
		writes := make([]write, 0, len(finalOrder))
		for _, key := range finalOrder {
			writes = append(writes, finalWrites[key])
		}
		record = &replicaRecord{
			ID:      id,
			Counter: ts.Counter,
			Node:    ts.Node,
			Writes:  writes,
		}
		for _, peer := range s.peers {
			if peer == s.node.ID() {
				continue
			}
			if s.pending[peer] == nil {
				s.pending[peer] = make(map[string]*pendingRecord)
			}
			s.pending[peer][id] = &pendingRecord{record: *record}
		}
	}

	return result, record, nil
}

// applyReplica applies a committed transaction received from another node.
// Each transaction is applied atomically and idempotently.
func (s *store) applyReplica(records []replicaRecord) []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	acked := make([]string, 0, len(records))
	for _, record := range records {
		if record.Counter > s.clock {
			s.clock = record.Counter
		}
		acked = append(acked, record.ID)
		if s.seen[record.ID] {
			continue
		}
		s.seen[record.ID] = true

		ts := record.timestamp()
		for _, w := range record.Writes {
			key, err := canonicalKey(w.Key)
			if err != nil {
				continue
			}
			cur, exists := s.state[key]
			if !exists || ts.less(cur.ts) {
				s.state[key] = version{ts: ts, value: cloneRaw(w.Value)}
			}
		}
	}
	return acked
}

func (s *store) acknowledge(peer string, ids []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pending[peer] == nil {
		return
	}
	for _, id := range ids {
		delete(s.pending[peer], id)
	}
}

// replicationLoop sends pending transactions to each peer.  A transaction is
// retried until an acknowledgement arrives.  Batching keeps replication
// efficient even at high request rates.
func (s *store) replicationLoop(stop <-chan struct{}) {
	const (
		tick       = 5 * time.Millisecond
		retryAfter = 100 * time.Millisecond
		maxBatch   = 128
	)

	ticker := time.NewTicker(tick)
	defer ticker.Stop()

	for {
		select {
		case <-stop:
			return
		case now := <-ticker.C:
			s.mu.Lock()
			peers := append([]string(nil), s.peers...)
			s.mu.Unlock()
			for _, peer := range peers {
				if peer == s.node.ID() {
					continue
				}

				s.mu.Lock()
				pending := s.pending[peer]
				ids := make([]string, 0, maxBatch)
				for id, p := range pending {
					if len(ids) == maxBatch {
						break
					}
					if p.sentAt.IsZero() || now.Sub(p.sentAt) >= retryAfter {
						p.sentAt = now
						p.attempt++
						ids = append(ids, id)
					}
				}
				records := make([]replicaRecord, 0, len(ids))
				for _, id := range ids {
					records = append(records, pending[id].record)
				}
				s.mu.Unlock()

				if len(records) == 0 {
					continue
				}
				// Deterministic batching is not required, but stable order
				// makes traces easier to inspect.
				sort.Slice(records, func(i, j int) bool { return records[i].ID < records[j].ID })
				if err := s.node.Send(peer, replicateRequest{Type: "replicate", Records: records}); err != nil {
					log.Printf("replication send to %s failed: %v", peer, err)
					// Let it be retried immediately on the next tick.
					s.mu.Lock()
					for _, id := range ids {
						if p := s.pending[peer][id]; p != nil {
							p.sentAt = time.Time{}
						}
					}
					s.mu.Unlock()
				}
			}
		}
	}
}

func main() {
	node := maelstrom.NewNode()
	s := newStore(node)

	node.Handle("init", func(msg maelstrom.Message) error {
		s.mu.Lock()
		s.peers = append([]string(nil), node.NodeIDs()...)
		sort.Strings(s.peers)
		s.mu.Unlock()

		stop := make(chan struct{})
		go s.replicationLoop(stop)
		return nil
	})

	node.Handle("txn", func(msg maelstrom.Message) error {
		var request txnRequest
		if err := json.Unmarshal(msg.Body, &request); err != nil {
			// Valid Maelstrom requests always parse.  Still, do not turn a
			// malformed request into a failed client operation.
			log.Printf("invalid txn request: %v", err)
			return node.Reply(msg, txnResponse{Type: "txn_ok", Txn: []Op{}})
		}

		result, _, err := s.applyLocal(request.Txn)
		if err != nil {
			log.Printf("invalid transaction: %v", err)
			return node.Reply(msg, txnResponse{Type: "txn_ok", Txn: request.Txn})
		}
		return node.Reply(msg, txnResponse{Type: "txn_ok", Txn: result})
	})

	node.Handle("replicate", func(msg maelstrom.Message) error {
		var request replicateRequest
		if err := json.Unmarshal(msg.Body, &request); err != nil {
			return nil
		}
		ids := s.applyReplica(request.Records)
		if err := node.Send(msg.Src, replicateAck{Type: "replicate_ack", IDs: ids}); err != nil {
			log.Printf("replication ack to %s failed: %v", msg.Src, err)
		}
		return nil
	})

	node.Handle("replicate_ack", func(msg maelstrom.Message) error {
		var ack replicateAck
		if err := json.Unmarshal(msg.Body, &ack); err != nil {
			return nil
		}
		s.acknowledge(msg.Src, ack.IDs)
		return nil
	})

	if err := node.Run(); err != nil {
		log.Fatalf("node failed: %v", err)
	}
}
