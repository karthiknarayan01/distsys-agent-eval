package main

import (
	"encoding/json"
	"log"
	"strconv"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// ts is a Lamport timestamp made globally unique with the node id.
// Timestamps are compared lexicographically: lamport first, then node id.
type ts struct {
	L int64  `json:"l"`
	N string `json:"n"`
}

// less reports whether a is older than b.
func (a ts) less(b ts) bool {
	if a.L != b.L {
		return a.L < b.L
	}
	return a.N < b.N
}

func (a ts) key() string {
	return strconv.FormatInt(a.L, 10) + "|" + a.N
}

// cell is one version of a key: its value and the timestamp that wrote it.
type cell struct {
	Val int64
	TS  ts
}

// txnPlus is a transaction together with the timestamp it committed at.
type txnPlus struct {
	TS  ts    `json:"ts"`
	Txn []any `json:"txn"`
}

// pendingTxn is a transaction that has not yet been acknowledged by every peer.
type pendingTxn struct {
	tp   txnPlus
	need map[string]bool
}

type server struct {
	node *maelstrom.Node

	mu    sync.Mutex
	clock int64
	kv    map[int64]cell
	// pending transactions, keyed by timestamp, still to be replicated.
	pending map[string]*pendingTxn
	peers   []string
}

func main() {
	s := &server{
		node:    maelstrom.NewNode(),
		kv:      make(map[int64]cell),
		pending: make(map[string]*pendingTxn),
	}

	s.node.Handle("init", s.handleInit)
	s.node.Handle("txn", s.handleTxn)
	s.node.Handle("replicate", s.handleReplicate)
	s.node.Handle("replicate_ack", s.handleReplicateAck)

	if err := s.node.Run(); err != nil {
		log.Fatal(err)
	}
}

// handleTxn executes a client transaction locally: atomically, against the
// local state, and then replies. Replication to peers happens asynchronously
// so that we never block on the network.
func (s *server) handleTxn(msg maelstrom.Message) error {
	var body struct {
		Txn []json.RawMessage `json:"txn"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	// Decode each micro-op as [f, k, v].
	ops := make([][3]any, len(body.Txn))
	for i, raw := range body.Txn {
		var arr []any
		if err := json.Unmarshal(raw, &arr); err != nil {
			return err
		}
		if len(arr) != 3 {
			return maelstrom.NewRPCError(maelstrom.MalformedRequest, "malformed operation")
		}
		ops[i] = [3]any{arr[0], arr[1], arr[2]}
	}

	s.mu.Lock()

	// Assign this transaction a timestamp which is greater than anything we
	// have ever seen locally.
	s.clock++
	tsn := ts{L: s.clock, N: s.node.ID()}

	// Run the transaction against the local store. Writes are buffered and
	// only become visible to other transactions once the whole transaction is
	// applied, so no other transaction can ever observe an intermediate value.
	type bufferedWrite struct {
		key int64
		val int64
	}
	var writes []bufferedWrite
	writeIdx := make(map[int64]int)     // key -> index into writes
	order := make([]int64, 0, len(ops)) // commit order of distinct keys

	out := make([]any, len(ops))
	for i, op := range ops {
		fname, _ := op[0].(string)
		key, ok := toInt(op[1])
		if !ok {
			s.mu.Unlock()
			return maelstrom.NewRPCError(maelstrom.MalformedRequest, "bad key")
		}
		switch fname {
		case "r":
			if j, ok := writeIdx[key]; ok {
				// Read your own writes.
				out[i] = []any{"r", key, writes[j].val}
			} else if c, ok := s.kv[key]; ok {
				out[i] = []any{"r", key, c.Val}
			} else {
				out[i] = []any{"r", key, nil}
			}
		case "w":
			val, ok := toInt(op[2])
			if !ok {
				s.mu.Unlock()
				return maelstrom.NewRPCError(maelstrom.MalformedRequest, "bad value")
			}
			if j, ok := writeIdx[key]; ok {
				// Overwrite within this transaction: keep only the final value
				// so intermediate values are never exposed.
				writes[j].val = val
			} else {
				writeIdx[key] = len(writes)
				writes = append(writes, bufferedWrite{key: key, val: val})
				order = append(order, key)
			}
			out[i] = []any{"w", key, val}
		default:
			s.mu.Unlock()
			return maelstrom.NewRPCError(maelstrom.MalformedRequest, "bad op "+fname)
		}
	}

	// Commit atomically.
	for _, key := range order {
		w := writes[writeIdx[key]]
		s.kv[key] = cell{Val: w.val, TS: tsn}
	}

	// Queue the transaction for asynchronous replication to every peer.
	if len(order) > 0 && len(s.peers) > 0 {
		txn := make([]any, len(out))
		copy(txn, out)
		need := make(map[string]bool, len(s.peers))
		for _, p := range s.peers {
			need[p] = true
		}
		s.pending[tsn.key()] = &pendingTxn{tp: txnPlus{TS: tsn, Txn: txn}, need: need}
	}

	s.mu.Unlock()

	return s.node.Reply(msg, map[string]any{"type": "txn_ok", "txn": out})
}

// handleReplicate applies transactions replicated from a peer. Each
// transaction is applied atomically at its own timestamp; applying the same
// transaction twice is harmless because timestamps are totally ordered.
func (s *server) handleReplicate(msg maelstrom.Message) error {
	var body struct {
		Txns []txnPlus `json:"txns"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	s.mu.Lock()
	for _, tp := range body.Txns {
		if tp.TS.N == "" {
			continue
		}
		if tp.TS.L > s.clock {
			s.clock = tp.TS.L
		}
		for _, rawOp := range tp.Txn {
			op, ok := rawOp.([]any)
			if !ok || len(op) != 3 {
				continue
			}
			fname, _ := op[0].(string)
			if fname != "w" {
				continue
			}
			key, ok := toInt(op[1])
			if !ok {
				continue
			}
			val, ok := toInt(op[2])
			if !ok {
				continue
			}
			if cur, ok := s.kv[key]; ok && tp.TS.less(cur.TS) {
				continue // our version is newer
			}
			s.kv[key] = cell{Val: val, TS: tp.TS}
		}
	}
	s.mu.Unlock()

	// Acknowledge so the peer can stop retransmitting.
	tss := make([]ts, len(body.Txns))
	for i, tp := range body.Txns {
		tss[i] = tp.TS
	}
	return s.node.Send(msg.Src, map[string]any{"type": "replicate_ack", "tss": tss})
}

func (s *server) handleReplicateAck(msg maelstrom.Message) error {
	var body struct {
		Tss []ts `json:"tss"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	src := msg.Src

	s.mu.Lock()
	for _, t := range body.Tss {
		k := t.key()
		if p, ok := s.pending[k]; ok {
			delete(p.need, src)
			if len(p.need) == 0 {
				delete(s.pending, k)
			}
		}
	}
	s.mu.Unlock()
	return nil
}

// replicateLoop retransmits pending transactions to each peer until they are
// acknowledged. Messages lost to a network partition are simply retried, so
// writes eventually reach the other node once the network allows.
func (s *server) replicateLoop() {
	const maxBatch = 200
	for {
		time.Sleep(3 * time.Millisecond)

		s.mu.Lock()
		peers := s.peers
		s.mu.Unlock()
		if len(peers) == 0 {
			continue
		}

		for _, peer := range peers {
			batch := make([]txnPlus, 0, maxBatch)
			s.mu.Lock()
			for _, p := range s.pending {
				if p.need[peer] {
					batch = append(batch, p.tp)
					if len(batch) >= maxBatch {
						break
					}
				}
			}
			s.mu.Unlock()

			if len(batch) == 0 {
				continue
			}
			if err := s.node.Send(peer, map[string]any{"type": "replicate", "txns": batch}); err != nil {
				log.Printf("replicate send: %s", err)
			}
		}
	}
}

// handleInit records the ids of the other nodes and starts the replicator.
func (s *server) handleInit(msg maelstrom.Message) error {
	ids := s.node.NodeIDs()
	peers := make([]string, 0, len(ids))
	for _, id := range ids {
		if id != s.node.ID() {
			peers = append(peers, id)
		}
	}
	s.mu.Lock()
	s.peers = peers
	s.mu.Unlock()

	go s.replicateLoop()
	return nil
}

// toInt converts a JSON number into an int64.
func toInt(v any) (int64, bool) {
	switch x := v.(type) {
	case float64:
		return int64(x), true
	case int64:
		return x, true
	case int:
		return int64(x), true
	case json.Number:
		i, err := x.Int64()
		if err != nil {
			return 0, false
		}
		return i, true
	}
	return 0, false
}
