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

// Op is one micro-operation in a transaction.  The workload represents an
// operation as ["r", key, value] or ["w", key, value], rather than as an
// object, so Op has custom JSON marshalling methods.
type Op struct {
	F     string
	Key   json.RawMessage
	Value json.RawMessage
}

func (o *Op) UnmarshalJSON(data []byte) error {
	var a []json.RawMessage
	if err := json.Unmarshal(data, &a); err != nil {
		return err
	}
	if len(a) != 3 {
		return fmt.Errorf("transaction operation has %d elements, want 3", len(a))
	}
	if err := json.Unmarshal(a[0], &o.F); err != nil {
		return fmt.Errorf("invalid operation function: %w", err)
	}
	o.Key = append(json.RawMessage(nil), a[1]...)
	o.Value = append(json.RawMessage(nil), a[2]...)
	return nil
}

func (o Op) MarshalJSON() ([]byte, error) {
	f, err := json.Marshal(o.F)
	if err != nil {
		return nil, err
	}
	return json.Marshal([]json.RawMessage{json.RawMessage(f), o.Key, o.Value})
}

// Version totally orders committed transactions.  Counter is a Lamport
// counter; node breaks ties between concurrent transactions.
type Version struct {
	Counter uint64 `json:"counter"`
	Node    string `json:"node"`
}

func (v Version) less(other Version) bool {
	if v.Counter != other.Counter {
		return v.Counter < other.Counter
	}
	return v.Node < other.Node
}

// Write is the final write to one key in a transaction.  A transaction which
// writes a key more than once is represented by its last write: intermediate
// values must never be visible to another transaction.
type Write struct {
	Key   string          `json:"key"`
	Value json.RawMessage `json:"value"`
}

// Record is a committed transaction which is replicated to the other node.
type Record struct {
	Version Version `json:"version"`
	Seq     uint64  `json:"seq"`
	Writes  []Write `json:"writes"`
}

type Cell struct {
	Version Version
	Value   json.RawMessage
}

// peerState contains the anti-entropy state for one peer.
type peerState struct {
	// Acked is the greatest contiguous sequence number which the peer says
	// it has received from us.
	Acked uint64
	// Received and Contiguous track records received from the peer.
	Received   map[uint64]bool
	Contiguous uint64

	// Waiting is true while an acknowledgement for the current batch is
	// outstanding.  We resend that batch after a timeout rather than
	// flooding the network with every unacknowledged record.
	Waiting  bool
	LastSent time.Time
}

type server struct {
	mu   sync.Mutex
	node *maelstrom.Node

	nodeID string
	peers  []string

	// state maps a key's canonical JSON representation to its latest value.
	state map[string]Cell

	// Lamport is advanced for both local events and received transactions.
	lamport uint64

	// Records originated by this node, keyed by a gap-free local sequence
	// number.  Records are removed after the peer acknowledges them.
	originLog map[uint64]Record
	originSeq uint64

	// One peer state per remote node.
	peer map[string]*peerState
}

func newServer(node *maelstrom.Node) *server {
	return &server{
		node:      node,
		state:     make(map[string]Cell),
		originLog: make(map[uint64]Record),
		peer:      make(map[string]*peerState),
	}
}

func (s *server) peerStateLocked(id string) *peerState {
	p := s.peer[id]
	if p == nil {
		p = &peerState{Received: make(map[uint64]bool)}
		s.peer[id] = p
	}
	return p
}

// applyRecord atomically incorporates a committed transaction.  Each key has
// an independent last-writer-wins version, but a transaction is always
// applied while holding the state lock, so no reader can observe a partially
// applied transaction.
func (s *server) applyRecord(r Record) {
	if r.Version.Counter > s.lamport {
		s.lamport = r.Version.Counter
	}
	for _, w := range r.Writes {
		old, ok := s.state[w.Key]
		if ok && !r.Version.less(old.Version) {
			continue
		}
		s.state[w.Key] = Cell{
			Version: r.Version,
			Value:   append(json.RawMessage(nil), w.Value...),
		}
	}
}

type txnBody struct {
	Type string `json:"type"`
	Txn  []Op   `json:"txn"`
}

// handleTxn executes a transaction locally.  It never waits for the other
// node: reads use the local committed state, and writes are made visible
// together before the client is answered.
func (s *server) handleTxn(req maelstrom.Message) error {
	var body txnBody
	if err := json.Unmarshal(req.Body, &body); err != nil {
		// The workload always sends valid transactions.  Still, do not fail
		// the process or turn a malformed request into an unavailable node.
		log.Printf("malformed txn request: %v", err)
		return nil
	}

	ops := make([]Op, len(body.Txn))
	for i := range body.Txn {
		ops[i] = Op{
			F:     body.Txn[i].F,
			Key:   append(json.RawMessage(nil), body.Txn[i].Key...),
			Value: append(json.RawMessage(nil), body.Txn[i].Value...),
		}
	}

	s.mu.Lock()

	// pending contains this transaction's own writes.  They are visible to
	// later reads in the transaction, but no other transaction can see an
	// intermediate value.
	pending := make(map[string]json.RawMessage)
	finalWrites := make(map[string]Write)
	for i := range ops {
		op := &ops[i]
		key := string(op.Key)
		switch op.F {
		case "r":
			if value, ok := pending[key]; ok {
				op.Value = append(json.RawMessage(nil), value...)
				continue
			}
			if cell, ok := s.state[key]; ok {
				op.Value = append(json.RawMessage(nil), cell.Value...)
			} else {
				op.Value = json.RawMessage("null")
			}
		case "w":
			value := append(json.RawMessage(nil), op.Value...)
			pending[key] = value
			finalWrites[key] = Write{
				Key:   key,
				Value: append(json.RawMessage(nil), value...),
			}
		default:
			// Unknown functions are outside the workload.  Leaving the
			// operation unchanged is safer than returning an error.
			log.Printf("unknown transaction function %q", op.F)
		}
	}

	// Only transactions with writes need to be replicated.  Publish their
	// final values atomically and assign a globally comparable version.
	var record Record
	if len(finalWrites) > 0 {
		s.lamport++
		s.originSeq++
		version := Version{Counter: s.lamport, Node: s.nodeID}

		keys := make([]string, 0, len(finalWrites))
		for key := range finalWrites {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		writes := make([]Write, 0, len(keys))
		for _, key := range keys {
			writes = append(writes, finalWrites[key])
		}

		record = Record{Version: version, Seq: s.originSeq, Writes: writes}
		s.applyRecord(record)
		s.originLog[s.originSeq] = record
	}
	s.mu.Unlock()

	return s.node.Reply(req, txnBody{Type: "txn_ok", Txn: ops})
}

type replicateBody struct {
	Type    string   `json:"type"`
	Records []Record `json:"records"`
}

type replicateAckBody struct {
	Type string `json:"type"`
	Ack  uint64 `json:"ack"`
}

// handleReplicate receives committed transactions.  Applying a batch under
// one lock makes each transaction atomic to local readers.
func (s *server) handleReplicate(req maelstrom.Message) error {
	var body replicateBody
	if err := json.Unmarshal(req.Body, &body); err != nil {
		log.Printf("malformed replicate message: %v", err)
		return nil
	}

	s.mu.Lock()
	p := s.peerStateLocked(req.Src)
	for _, record := range body.Records {
		if record.Seq == 0 {
			continue
		}
		if !p.Received[record.Seq] {
			p.Received[record.Seq] = true
			s.applyRecord(record)
		}
	}
	for p.Received[p.Contiguous+1] {
		p.Contiguous++
	}
	ack := p.Contiguous
	s.mu.Unlock()

	// Acknowledgements are repeated until the sender sees them.  A lost ack
	// therefore cannot leave a transaction permanently unreplicated.
	return s.node.Send(req.Src, replicateAckBody{Type: "replicate_ack", Ack: ack})
}

// handleReplicateAck records that the peer has a contiguous prefix of our
// transaction log.
func (s *server) handleReplicateAck(req maelstrom.Message) error {
	var body replicateAckBody
	if err := json.Unmarshal(req.Body, &body); err != nil {
		log.Printf("malformed replicate ack: %v", err)
		return nil
	}

	s.mu.Lock()
	p := s.peerStateLocked(req.Src)
	if body.Ack > p.Acked {
		oldAck := p.Acked
		p.Acked = body.Ack
		p.Waiting = false
		for seq := oldAck + 1; seq <= body.Ack; seq++ {
			delete(s.originLog, seq)
		}
	}
	s.mu.Unlock()
	return nil
}

// sync sends unacknowledged records to each peer.  It is safe to resend:
// records are idempotent, and acknowledgements cover only a contiguous
// prefix.  Thus messages lost in a partition are retried after healing.
func (s *server) sync() {
	const maxBatch = 512
	const retryAfter = 250 * time.Millisecond

	type batch struct {
		peer string
		body replicateBody
	}
	var batches []batch
	now := time.Now()

	s.mu.Lock()
	for _, peer := range s.peers {
		p := s.peerStateLocked(peer)
		if p.Waiting && now.Sub(p.LastSent) < retryAfter {
			continue
		}

		// Only one batch is in flight at a time.  If it is lost, the same
		// batch is retried; after an acknowledgement, the next batch is
		// sent.  This avoids quadratic traffic during a long partition.
		records := make([]Record, 0, maxBatch)
		for seq := p.Acked + 1; len(records) < maxBatch; seq++ {
			record, ok := s.originLog[seq]
			if !ok {
				break
			}
			records = append(records, record)
		}
		if len(records) == 0 {
			continue
		}
		p.Waiting = true
		p.LastSent = now
		batches = append(batches, batch{
			peer: peer,
			body: replicateBody{Type: "replicate", Records: records},
		})
	}
	s.mu.Unlock()

	for _, b := range batches {
		if err := s.node.Send(b.peer, b.body); err != nil {
			log.Printf("replication send to %s failed: %v", b.peer, err)
		}
	}
}

func (s *server) handleInit(req maelstrom.Message) error {
	var body struct {
		NodeID  string   `json:"node_id"`
		NodeIDs []string `json:"node_ids"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil
	}

	s.mu.Lock()
	s.nodeID = body.NodeID
	s.peers = nil
	for _, id := range body.NodeIDs {
		if id != body.NodeID {
			s.peers = append(s.peers, id)
		}
	}
	s.mu.Unlock()
	return nil
}

func main() {
	log.SetFlags(0)
	node := maelstrom.NewNode()
	server := newServer(node)

	node.Handle("txn", server.handleTxn)
	node.Handle("replicate", server.handleReplicate)
	node.Handle("replicate_ack", server.handleReplicateAck)
	node.Handle("init", server.handleInit)

	go func() {
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		for range ticker.C {
			server.sync()
		}
	}()

	if err := node.Run(); err != nil {
		log.Printf("node stopped: %v", err)
	}
}
