package main

import (
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// An operation is represented on the wire as ["r", key, value] or
// ["w", key, value].  Value is nil for a read of a key which has never been
// written.
type operation struct {
	Kind  string
	Key   int
	Value *int
}

func (o *operation) UnmarshalJSON(data []byte) error {
	var tuple []json.RawMessage
	if err := json.Unmarshal(data, &tuple); err != nil {
		return err
	}
	if len(tuple) != 3 {
		return fmt.Errorf("transaction operation must have three elements")
	}
	if err := json.Unmarshal(tuple[0], &o.Kind); err != nil {
		return err
	}
	if err := json.Unmarshal(tuple[1], &o.Key); err != nil {
		return err
	}
	if o.Kind == "r" {
		// The value in a request is always null.  It is filled in when the
		// transaction is executed.
		o.Value = nil
		return nil
	}
	if o.Kind != "w" {
		return fmt.Errorf("unknown transaction operation %q", o.Kind)
	}
	var value int
	if err := json.Unmarshal(tuple[2], &value); err != nil {
		return err
	}
	o.Value = &value
	return nil
}

func (o operation) MarshalJSON() ([]byte, error) {
	var value any
	if o.Value != nil {
		value = *o.Value
	}
	return json.Marshal([]any{o.Kind, o.Key, value})
}

type txnRequest struct {
	Type string      `json:"type"`
	Txn  []operation `json:"txn"`
}

type txnOKResponse struct {
	Type string      `json:"type"`
	Txn  []operation `json:"txn"`
}

// timestamp is a Lamport timestamp.  The node ID makes timestamps globally
// unique and gives concurrent timestamps a deterministic total order.
type timestamp struct {
	Counter int64  `json:"counter"`
	Node    string `json:"node"`
}

func timestampLess(a, b timestamp) bool {
	if a.Counter != b.Counter {
		return a.Counter < b.Counter
	}
	return a.Node < b.Node
}

type writeValue struct {
	Key   int `json:"key"`
	Value int `json:"value"`
}

// replicatedTxn contains only the final value written for each key.  In
// particular, an intermediate value in a transaction is never put into the
// replicated state.
type replicatedTxn struct {
	ID     string       `json:"id"`
	TS     timestamp    `json:"ts"`
	Writes []writeValue `json:"writes"`
}

type version struct {
	TS    timestamp
	Value int
}

type pendingTxn struct {
	Txn      replicatedTxn
	Acked    map[string]bool
	LastSent map[string]time.Time
}

type replicateBody struct {
	Type string          `json:"type"`
	Txns []replicatedTxn `json:"txns"`
}

type replicateAckBody struct {
	Type string   `json:"type"`
	IDs  []string `json:"ids"`
}

type server struct {
	node *maelstrom.Node

	mu      sync.Mutex
	clock   int64
	state   map[int]version
	pending map[string]*pendingTxn
	peers   []string
	started bool
}

func newServer(node *maelstrom.Node) *server {
	return &server{
		node:    node,
		state:   make(map[int]version),
		pending: make(map[string]*pendingTxn),
	}
}

// execute executes a client transaction atomically with respect to other
// transactions.  Reads use a private overlay, so an intermediate write in
// this transaction is visible to this transaction but not to another one.
func (s *server) execute(ops []operation) ([]operation, replicatedTxn, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.clock++
	ts := timestamp{Counter: s.clock, Node: s.node.ID()}
	overlay := make(map[int]int)
	result := make([]operation, len(ops))

	for i, op := range ops {
		result[i] = op
		switch op.Kind {
		case "r":
			if value, ok := overlay[op.Key]; ok {
				v := value
				result[i].Value = &v
			} else if current, ok := s.state[op.Key]; ok {
				v := current.Value
				result[i].Value = &v
			} else {
				result[i].Value = nil
			}
		case "w":
			if op.Value == nil {
				return nil, replicatedTxn{}, false
			}
			overlay[op.Key] = *op.Value
		default:
			return nil, replicatedTxn{}, false
		}
	}

	// Sort writes to make replication messages deterministic and compact.
	keys := make([]int, 0, len(overlay))
	for key := range overlay {
		keys = append(keys, key)
	}
	sort.Ints(keys)
	writes := make([]writeValue, 0, len(keys))
	for _, key := range keys {
		writes = append(writes, writeValue{Key: key, Value: overlay[key]})
	}

	// A locally-created timestamp is newer than every version known locally.
	// Still use the same merge rule as for replicated transactions.
	for _, write := range writes {
		current, exists := s.state[write.Key]
		if !exists || !timestampLess(current.TS, ts) {
			s.state[write.Key] = version{TS: ts, Value: write.Value}
		}
	}

	id := fmt.Sprintf("%s:%d", s.node.ID(), ts.Counter)
	txn := replicatedTxn{ID: id, TS: ts, Writes: writes}
	if len(writes) != 0 {
		s.pending[id] = &pendingTxn{
			Txn:      txn,
			Acked:    make(map[string]bool),
			LastSent: make(map[string]time.Time),
		}
	}
	return result, txn, true
}

// applyReplicated merges a transaction received from the other node.  All of
// the transaction's final writes are installed while holding the same lock,
// so readers can never observe a partially applied transaction.
func (s *server) applyReplicated(txns []replicatedTxn) []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	ids := make([]string, 0, len(txns))
	for _, txn := range txns {
		ids = append(ids, txn.ID)
		if txn.TS.Counter >= s.clock {
			s.clock = txn.TS.Counter + 1
		}
		for _, write := range txn.Writes {
			current, exists := s.state[write.Key]
			if !exists || !timestampLess(current.TS, txn.TS) {
				s.state[write.Key] = version{TS: txn.TS, Value: write.Value}
			}
		}
	}
	return ids
}

func (s *server) acknowledge(ids []string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, id := range ids {
		pending, ok := s.pending[id]
		if !ok {
			continue
		}
		for _, peer := range s.peers {
			pending.Acked[peer] = true
		}
		complete := true
		for _, peer := range s.peers {
			if !pending.Acked[peer] {
				complete = false
				break
			}
		}
		if complete {
			delete(s.pending, id)
		}
	}
}

// replicationRound sends transactions which have not yet been acknowledged.
// It never waits for the network: an acknowledgement merely stops needless
// retransmission.
func (s *server) replicationRound() {
	const (
		maxBatch       = 50
		resendInterval = 200 * time.Millisecond
	)

	s.mu.Lock()
	now := time.Now()
	batches := make(map[string][]replicatedTxn)
	for _, pending := range s.pending {
		for _, peer := range s.peers {
			if pending.Acked[peer] {
				continue
			}
			last := pending.LastSent[peer]
			if !last.IsZero() && now.Sub(last) < resendInterval {
				continue
			}
			batches[peer] = append(batches[peer], pending.Txn)
			pending.LastSent[peer] = now
		}
	}
	s.mu.Unlock()

	for peer, txns := range batches {
		// Keep every message comfortably below bufio.Scanner's 64 KiB limit.
		for start := 0; start < len(txns); start += maxBatch {
			end := start + maxBatch
			if end > len(txns) {
				end = len(txns)
			}
			if err := s.node.Send(peer, replicateBody{Type: "replicate", Txns: txns[start:end]}); err != nil {
				log.Printf("failed to send replication batch: %v", err)
			}
		}
	}
}

func (s *server) replicationLoop() {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for range ticker.C {
		s.replicationRound()
	}
}

func main() {
	node := maelstrom.NewNode()
	server := newServer(node)

	node.Handle("init", func(msg maelstrom.Message) error {
		server.mu.Lock()
		if !server.started {
			server.started = true
			for _, id := range node.NodeIDs() {
				if id != node.ID() {
					server.peers = append(server.peers, id)
				}
			}
			go server.replicationLoop()
		}
		server.mu.Unlock()
		return nil
	})

	node.Handle("txn", func(msg maelstrom.Message) error {
		var request txnRequest
		if err := json.Unmarshal(msg.Body, &request); err != nil {
			return fmt.Errorf("decode txn request: %w", err)
		}
		result, _, ok := server.execute(request.Txn)
		if !ok {
			return fmt.Errorf("invalid transaction")
		}
		return node.Reply(msg, txnOKResponse{Type: "txn_ok", Txn: result})
	})

	node.Handle("replicate", func(msg maelstrom.Message) error {
		var request replicateBody
		if err := json.Unmarshal(msg.Body, &request); err != nil {
			return fmt.Errorf("decode replication request: %w", err)
		}
		ids := server.applyReplicated(request.Txns)
		return node.Send(msg.Src, replicateAckBody{Type: "replicate_ack", IDs: ids})
	})

	node.Handle("replicate_ack", func(msg maelstrom.Message) error {
		var request replicateAckBody
		if err := json.Unmarshal(msg.Body, &request); err != nil {
			return fmt.Errorf("decode replication acknowledgement: %w", err)
		}
		server.acknowledge(request.IDs)
		return nil
	})

	if err := node.Run(); err != nil {
		log.Fatalf("node failed: %v", err)
	}
}
