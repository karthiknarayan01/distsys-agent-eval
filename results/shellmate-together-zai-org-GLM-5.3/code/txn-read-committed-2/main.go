// Read-committed, totally-available transactions over a two-node cluster.
//
// Each node keeps its own copy of the store and answers every transaction from
// that local copy, so a client is never blocked on (or failed by) the other
// node. Transactions are applied atomically to the local copy: the reads of a
// transaction see the values already in the store plus that transaction's own
// writes, and only the *final* value written to a key during a transaction is
// ever published. That gives read-committed isolation:
//
//   - nobody can observe an intermediate value that a transaction overwrote,
//     because only final values are ever stored;
//   - nobody can observe a value from an aborted transaction, because values
//     only enter the store once a transaction has been committed and answered
//     with txn_ok.
//
// Committed writes are replicated to the other node by gossip: deltas are
// shipped every few milliseconds, and a full snapshot is re-shipped
// periodically so that messages lost during a network partition are
// eventually repaired. Replicas merge writes last-writer-wins using a total
// order on (sequence number, node id), which is commutative and idempotent,
// so both nodes converge on the same set of committed values no matter what
// order the gossip messages arrive in. Staleness is fine: read committed only
// constrains *which* values may be observed, not how fresh they are.
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const (
	// How often pending deltas are shipped to peers.
	deltaInterval = 25 * time.Millisecond
	// How often the full store is re-shipped, to repair messages lost while
	// the network was split.
	syncInterval = 250 * time.Millisecond
	// Maximum number of keys per gossip message, keeping each message well
	// under the size of a line.
	syncChunk = 256
)

// versioned is a single write of one key: the value written, stamped with a
// totally ordered version.
type versioned struct {
	V json.RawMessage `json:"v"`
	S int64           `json:"s"`
	N string          `json:"n"`
}

// newer reports whether b should replace a.
func newer(a, b versioned) bool {
	if a.S != b.S {
		return b.S > a.S
	}
	return b.N > a.N
}

// replica is the node's copy of the store.
type replica struct {
	n *maelstrom.Node

	mu     sync.Mutex
	data   map[string]versioned // canonical key -> newest committed write
	seq    int64               // local version counter, bumped per transaction
	outbox map[string]versioned
}

// op is one decoded micro-operation of a transaction.
type op struct {
	f    string
	keyR json.RawMessage // key as the client wrote it
	keyC string          // canonical key identity
	valR json.RawMessage // value as the client wrote it (writes)
}

// canonKey maps a JSON-encoded key to a canonical string identity. Integer
// keys are normalized to decimal so that the two replicas agree on their
// identity; anything else is used as-is.
func canonKey(raw []byte) string {
	var i int64
	if err := json.Unmarshal(raw, &i); err == nil {
		return strconv.FormatInt(i, 10)
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return s
	}
	return string(raw)
}

// parseTxn decodes a txn request body.
func parseTxn(body []byte) ([]op, error) {
	var b struct {
		Txn [][]json.RawMessage `json:"txn"`
	}
	if err := json.Unmarshal(body, &b); err != nil {
		return nil, fmt.Errorf("parse txn body: %w", err)
	}
	ops := make([]op, 0, len(b.Txn))
	for _, m := range b.Txn {
		var o op
		if len(m) > 0 {
			if err := json.Unmarshal(m[0], &o.f); err != nil {
				return nil, fmt.Errorf("parse micro-op function: %w", err)
			}
		}
		if len(m) > 1 {
			o.keyR = m[1]
			o.keyC = canonKey(m[1])
		}
		if len(m) > 2 {
			o.valR = m[2]
		}
		ops = append(ops, o)
	}
	return ops, nil
}

// exec runs a transaction against the local store, atomically, and returns the
// transaction with read values filled in. Reads observe the store plus this
// transaction's own earlier writes; writes are staged and only published to
// the store once the whole transaction has been processed, at a single
// version.
func (r *replica) exec(ops []op) [][]any {
	r.mu.Lock()
	defer r.mu.Unlock()

	pending := make(map[string]json.RawMessage, len(ops))
	out := make([][]any, 0, len(ops))
	for _, o := range ops {
		switch o.f {
		case "r":
			var v json.RawMessage
			if p, ok := pending[o.keyC]; ok {
				v = p
			} else if e, ok := r.data[o.keyC]; ok {
				v = e.V
			}
			out = append(out, []any{"r", o.keyR, v})
		case "w":
			pending[o.keyC] = o.valR
			out = append(out, []any{"w", o.keyR, o.valR})
		default:
			// Not part of this workload; echo it back untouched.
			out = append(out, []any{o.f, o.keyR, o.valR})
		}
	}

	if len(pending) > 0 {
		r.seq++
		v := versioned{S: r.seq, N: r.n.ID()}
		for k, val := range pending {
			v.V = val
			r.data[k] = v
			if r.outbox == nil {
				r.outbox = make(map[string]versioned)
			}
			r.outbox[k] = v
		}
	}
	return out
}

// merge folds gossipped writes into the store, last-writer-wins.
func (r *replica) merge(writes map[string]versioned) {
	if len(writes) == 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for k, w := range writes {
		if cur, ok := r.data[k]; !ok || newer(cur, w) {
			r.data[k] = w
		}
	}
}

// peers returns the ids of the other nodes in the cluster.
func (r *replica) peers() []string {
	var ps []string
	for _, id := range r.n.NodeIDs() {
		if id != r.n.ID() {
			ps = append(ps, id)
		}
	}
	return ps
}

func (r *replica) gossip(body map[string]any) {
	for _, p := range r.peers() {
		if err := r.n.Send(p, body); err != nil {
			// Best effort: messages dropped while the network is split are
			// repaired by the next full sync.
			log.Printf("gossip to %s: %v", p, err)
		}
	}
}

// gossipOnce ships pending deltas, and every syncEvery-th round the entire
// store as well.
func (r *replica) gossipOnce(full bool) {
	r.mu.Lock()
	var delta map[string]versioned
	if len(r.outbox) > 0 {
		delta = r.outbox
		r.outbox = nil
	}
	var snap map[string]versioned
	if full {
		snap = make(map[string]versioned, len(r.data))
		for k, v := range r.data {
			snap[k] = v
		}
	}
	r.mu.Unlock()

	if len(delta) == 0 && len(snap) == 0 {
		return
	}

	if len(delta) > 0 {
		r.gossip(map[string]any{"type": "apply", "writes": delta})
	}

	if len(snap) > 0 {
		// Split the snapshot so no single message grows without bound.
		keys := make([]string, 0, len(snap))
		for k := range snap {
			keys = append(keys, k)
		}
		for start := 0; start < len(keys); start += syncChunk {
			end := start + syncChunk
			if end > len(keys) {
				end = len(keys)
			}
			chunk := make(map[string]versioned, end-start)
			for _, k := range keys[start:end] {
				chunk[k] = snap[k]
			}
			r.gossip(map[string]any{"type": "apply", "writes": chunk})
		}
	}
}

// gossipLoop replicates local commits to the rest of the cluster until the
// process exits.
func (r *replica) gossipLoop() {
	syncEvery := int(syncInterval / deltaInterval)
	if syncEvery < 1 {
		syncEvery = 1
	}
	t := time.NewTicker(deltaInterval)
	defer t.Stop()
	for i := 0; ; i++ {
		<-t.C
		r.gossipOnce(i%syncEvery == 0)
	}
}

func main() {
	log.SetFlags(log.Ltime | log.Lmicroseconds)

	n := maelstrom.NewNode()
	r := &replica{n: n, data: make(map[string]versioned)}

	handlers := map[string]func(msg maelstrom.Message) error{
		"txn": func(msg maelstrom.Message) error {
			ops, err := parseTxn(msg.Body)
			if err != nil {
				return err
			}
			return n.Reply(msg, map[string]any{"type": "txn_ok", "txn": r.exec(ops)})
		},
		"apply": func(msg maelstrom.Message) error {
			var b struct {
				Writes map[string]versioned `json:"writes"`
			}
			if err := json.Unmarshal(msg.Body, &b); err != nil {
				return fmt.Errorf("parse apply body: %w", err)
			}
			r.merge(b.Writes)
			return nil
		},
		// Ignored: the daemon may report that one of our messages could not be
		// delivered.
		"error": func(msg maelstrom.Message) error { return nil },
		// Not used by this workload, but acknowledged for good measure.
		"topology": func(msg maelstrom.Message) error {
			return n.Reply(msg, map[string]any{"type": "topology_ok"})
		},
	}

	sc := bufio.NewScanner(n.Stdin)
	sc.Buffer(make([]byte, 64*1024), 64*1024*1024)
	for sc.Scan() {
		var msg maelstrom.Message
		if err := json.Unmarshal(sc.Bytes(), &msg); err != nil {
			log.Printf("dropping malformed message: %v", err)
			continue
		}
		var head struct {
			Type    string   `json:"type"`
			NodeID  string   `json:"node_id"`
			NodeIDs []string `json:"node_ids"`
		}
		if err := json.Unmarshal(msg.Body, &head); err != nil {
			log.Printf("dropping malformed body: %v", err)
			continue
		}

		if head.Type == "init" {
			n.Init(head.NodeID, head.NodeIDs)
			go r.gossipLoop()
			if err := n.Reply(msg, map[string]any{"type": "init_ok"}); err != nil {
				log.Printf("reply to init: %v", err)
			}
			continue
		}

		h := handlers[head.Type]
		if h == nil {
			// Unknown message type: ignore it rather than dying.
			log.Printf("ignoring %s message from %s", head.Type, msg.Src)
			continue
		}
		func() {
			defer func() {
				if p := recover(); p != nil {
					log.Printf("panic handling %s: %v", head.Type, p)
				}
			}()
			if err := h(msg); err != nil {
				log.Printf("error handling %s: %v", head.Type, err)
			}
		}()
	}
	if err := sc.Err(); err != nil {
		log.Printf("reading stdin: %v", err)
		os.Exit(1)
	}
}
