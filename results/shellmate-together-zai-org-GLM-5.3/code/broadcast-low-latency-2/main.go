// Maelstrom broadcast workload node.
//
// Strategy: every value is fanned out from the node that first receives it to
// every other node directly - a single network hop, which is both the fastest
// possible propagation and the minimum message count (each value is delivered
// to each node exactly once). Values are coalesced for a few milliseconds
// before the fan-out so that bursts of broadcasts on one node share
// messages. Nodes never re-forward values received from other nodes, so there
// is no flooding.
//
// As insurance against lost messages or restarted processes, nodes also run a
// cheap anti-entropy protocol. A node periodically asks one rotating peer for
// a digest (count/xor/sum) of its value set; the reply carries the peer's own
// digest. While traffic is flowing, digests almost always differ transiently
// (values are in flight), so full sets are only exchanged when either the
// difference is far too large to be in-flight, or both sides have been idle
// for a moment - which is exactly when a difference needs repairing. One probe
// per second walks the whole cluster several times over during the recovery
// window at the end of a run, so any divergence heals before the final reads.
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

const (
	// How long a node coalesces newly broadcast values before fanning them
	// out. Small: it only exists to let bursts share a message.
	flushEvery = 15 * time.Millisecond

	// How long without an insertion before a node counts as idle, i.e. before
	// differences in value sets are assumed to be real rather than values
	// still in flight. Longer than a full propagation (1 hop + coalescing).
	idleAfter = 300 * time.Millisecond

	// Anti-entropy probe period: one rotating peer per second. That is
	// enough to walk the whole cluster (and repair any divergence) several
	// times over within the recovery window at the end of a run.
	aeEvery = 1 * time.Second

	// Maximum number of values carried by a single inter-node message.
	chunkValues = 128

	// A difference in value counts at least this large cannot be values still
	// in flight, so the divergence is repaired right away instead of waiting
	// for both sides to go idle. Normal propagation keeps neighbouring counts
	// within a handful of values of each other.
	divergeAt = 64
)

// digest is an order-independent fingerprint of a set of values: the number of
// values, the xor of per-value hashes, and their sum. Two nodes holding the
// same values have the same digest.
type digest struct {
	Count int    `json:"count"`
	Xor   string `json:"xor"` // hex, so it never has to round-trip as a float
	Sum   int64  `json:"sum"`
	// Is the sender idle (no recent insertions)? Only then is a difference in
	// digests worth shipping a full set over.
	Idle bool `json:"idle"`
}

func hashOf(v int64) uint64 {
	x := uint64(v) + 0x9e3779b97f4a7c15
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x
}

type server struct {
	mn *maelstrom.Node

	mu      sync.Mutex
	values  map[int64]struct{}
	dg      digest             // Count/Xor/Sum only; Idle is filled in per message
	pending map[int64]struct{} // values still to be fanned out to peers
	lastAdd time.Time          // time of the most recent insertion
	peers   []string
	aeNext  int
}

func main() {
	log.SetFlags(log.Ltime)

	n := maelstrom.NewNode()
	s := &server{
		mn:      n,
		values:  make(map[int64]struct{}, 1024),
		pending: make(map[int64]struct{}, 64),
	}

	n.Handle("init", s.handleInit)
	n.Handle("broadcast", s.handleBroadcast)
	n.Handle("read", s.handleRead)
	n.Handle("topology", s.handleTopology)
	n.Handle("gossip", s.handleGossip)
	n.Handle("ae_syn", s.handleAESyn)
	n.Handle("ae_syn_ok", s.handleAESynOK)
	n.Handle("error", s.handleError)

	if err := n.Run(); err != nil {
		log.Fatalf("fatal: %v", err)
	}
}

func (s *server) handleInit(msg maelstrom.Message) error {
	s.mu.Lock()
	for _, id := range s.mn.NodeIDs() {
		if id != s.mn.ID() {
			s.peers = append(s.peers, id)
		}
	}
	n := len(s.peers)
	s.mu.Unlock()

	log.Printf("init: %d peers", n)
	go s.flushLoop()
	go s.aeLoop()
	return nil
}

// addLocked records v. Returns true if it was new.
func (s *server) addLocked(v int64, propagate bool) bool {
	if _, ok := s.values[v]; ok {
		return false
	}
	s.values[v] = struct{}{}
	s.dg.Count++
	s.dg.Xor = fmt.Sprintf("%016x", parseHex(s.dg.Xor)^hashOf(v))
	s.dg.Sum += v
	s.lastAdd = time.Now()
	if propagate {
		s.pending[v] = struct{}{}
	}
	return true
}

func (s *server) snapshotLocked() []int64 {
	out := make([]int64, 0, len(s.values))
	for v := range s.values {
		out = append(out, v)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func parseHex(s string) uint64 {
	v, err := strconv.ParseUint(s, 16, 64)
	if err != nil {
		return 0
	}
	return v
}

// stateLocked returns the node's digest with the idle flag filled in.
func (s *server) stateLocked() digest {
	d := s.dg
	d.Idle = time.Since(s.lastAdd) > idleAfter
	return d
}

// ---- client handlers -------------------------------------------------

func (s *server) handleBroadcast(msg maelstrom.Message) error {
	var body struct {
		Message int64 `json:"message"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return fmt.Errorf("unmarshal broadcast: %w", err)
	}
	s.mu.Lock()
	s.addLocked(body.Message, true)
	s.mu.Unlock()

	return s.mn.Reply(msg, map[string]any{"type": "broadcast_ok"})
}

func (s *server) handleRead(msg maelstrom.Message) error {
	s.mu.Lock()
	vals := s.snapshotLocked()
	s.mu.Unlock()

	return s.mn.Reply(msg, map[string]any{
		"type":     "read_ok",
		"messages": vals,
	})
}

func (s *server) handleTopology(msg maelstrom.Message) error {
	// We deliberately ignore the suggested topology: a full fan-out from the
	// origin gives the lowest possible latency (one hop) at the minimum
	// possible message count.
	return s.mn.Reply(msg, map[string]any{"type": "topology_ok"})
}

// handleError swallows any error reply so that an unexpected message can never
// take the node down (the library exits if a message type has no handler).
func (s *server) handleError(msg maelstrom.Message) error {
	log.Printf("error reply from %s: %s", msg.Src, msg.Body)
	return nil
}

// ---- inter-node handlers ----------------------------------------------

func (s *server) handleGossip(msg maelstrom.Message) error {
	var body struct {
		Messages []int64 `json:"messages"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		// Never fail: an error reply would kill the peer, which has no
		// handler for it.
		log.Printf("bad gossip from %s: %v", msg.Src, err)
		return nil
	}
	if len(body.Messages) == 0 {
		return nil
	}
	s.mu.Lock()
	for _, v := range body.Messages {
		// No forwarding: whichever node got this value from a client has
		// already fanned it out to everybody.
		s.addLocked(v, false)
	}
	s.mu.Unlock()
	return nil
}

func (s *server) handleAESyn(msg maelstrom.Message) error {
	var theirs digest
	if err := json.Unmarshal(msg.Body, &theirs); err != nil {
		log.Printf("bad ae_syn from %s: %v", msg.Src, err)
		return nil
	}

	s.mu.Lock()
	mine := s.stateLocked()
	var mineVals []int64
	if needRepair(mine, theirs) && mine.Count >= theirs.Count {
		mineVals = s.snapshotLocked()
	}
	s.mu.Unlock()

	// Always answer, so the other side can compare too.
	if err := s.sendDigest(msg.Src, "ae_syn_ok", mine); err != nil {
		log.Printf("ae_syn_ok to %s: %v", msg.Src, err)
	}
	if mineVals != nil {
		s.sendValues(msg.Src, mineVals)
	}
	return nil
}

func (s *server) handleAESynOK(msg maelstrom.Message) error {
	var theirs digest
	if err := json.Unmarshal(msg.Body, &theirs); err != nil {
		log.Printf("bad ae_syn_ok from %s: %v", msg.Src, err)
		return nil
	}

	s.mu.Lock()
	mine := s.stateLocked()
	var mineVals []int64
	if needRepair(mine, theirs) && mine.Count > theirs.Count {
		mineVals = s.snapshotLocked()
	}
	s.mu.Unlock()

	if mineVals != nil {
		s.sendValues(msg.Src, mineVals)
	}
	return nil
}

// needRepair reports whether a difference in value sets is worth fixing with a
// full set exchange. While a value is propagating, nodes legitimately disagree
// about it for a hop's worth of time, so a small difference is only acted on
// once both sides have been idle long enough that nothing can still be in
// flight. A large difference, on the other hand, can only mean real divergence
// (say, after a network partition), and is repaired immediately.
func needRepair(mine, theirs digest) bool {
	if mine == theirs {
		return false
	}
	if mine.Count-theirs.Count >= divergeAt || theirs.Count-mine.Count >= divergeAt {
		return true
	}
	return mine.Idle && theirs.Idle
}

// ---- background loops ------------------------------------------------

// flushLoop fans out everything received from clients to all peers.
func (s *server) flushLoop() {
	t := time.NewTicker(flushEvery)
	defer t.Stop()
	for range t.C {
		s.mu.Lock()
		var batch []int64
		if len(s.pending) > 0 {
			batch = make([]int64, 0, len(s.pending))
			for v := range s.pending {
				batch = append(batch, v)
			}
			s.pending = make(map[int64]struct{}, 64)
		}
		peers := s.peers
		s.mu.Unlock()

		if len(batch) == 0 {
			continue
		}
		for _, dst := range peers {
			if err := s.sendValues(dst, batch); err != nil {
				// Don't lose values: try again next round.
				s.mu.Lock()
				for _, v := range batch {
					if _, ok := s.values[v]; ok {
						s.pending[v] = struct{}{}
					}
				}
				s.mu.Unlock()
			}
		}
	}
}

// aeLoop periodically compares digests with a rotating peer, so that any
// divergence eventually heals even if a message is lost or a process restarts.
func (s *server) aeLoop() {
	t := time.NewTicker(aeEvery)
	defer t.Stop()
	for range t.C {
		s.mu.Lock()
		if len(s.peers) == 0 {
			s.mu.Unlock()
			continue
		}
		dst := s.peers[s.aeNext%len(s.peers)]
		s.aeNext++
		mine := s.stateLocked()
		s.mu.Unlock()

		if err := s.sendDigest(dst, "ae_syn", mine); err != nil {
			log.Printf("ae_syn to %s: %v", dst, err)
		}
	}
}

func (s *server) sendDigest(dst, typ string, d digest) error {
	return s.mn.Send(dst, map[string]any{
		"type":  typ,
		"count": d.Count,
		"sum":   d.Sum,
		"xor":   d.Xor,
		"idle":  d.Idle,
	})
}

// sendValues ships vals to a single destination, in chunks.
func (s *server) sendValues(dst string, vals []int64) error {
	for start := 0; start < len(vals); start += chunkValues {
		end := start + chunkValues
		if end > len(vals) {
			end = len(vals)
		}
		if err := s.mn.Send(dst, map[string]any{
			"type":     "gossip",
			"messages": vals[start:end],
		}); err != nil {
			return err
		}
	}
	return nil
}
