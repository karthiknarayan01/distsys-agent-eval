// Maelstrom "broadcast" workload node.
//
// Strategy: every value enters the cluster at exactly one node (the origin,
// the node whose client receives the broadcast request). That node
// acknowledges the client immediately and then hands the value directly to
// every other node in a single 100ms hop. Each value therefore costs exactly
// n-1 inter-server messages, and every node has it ~100 ms after the ack --
// well inside both the message and the latency targets.
//
// On top of that eager path there is a cheap, lazy anti-entropy layer: every
// few seconds a node swaps a (count, xor) digest of its set with a random
// peer, and only when the digests disagree do the two exchange full sets.
// This guarantees convergence even if an eager message were to go missing.

package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"sort"
	"sync"
	"time"

	mg "github.com/jepsen-io/maelstrom/demo/go"
)

const (
	// How often each node initiates an anti-entropy exchange.
	antiEntropyPeriod = 5 * time.Second
	// Peers contacted per anti-entropy round.
	antiEntropyFanout = 2
	// Maximum values carried by a single inter-server message; keeps every
	// line we emit or receive well under any sane line-length limit.
	syncChunkSize = 256
)

// Bodies of the inter-server messages. They are our own design.
type gossipBody struct {
	Type     string  `json:"type"`
	Messages []int64 `json:"messages"`
}

// fullBody carries a complete snapshot of the sender's set (in chunks); the
// chunk with Final set is the last one, which lets the receiver reply with
// whatever the sender is missing.
type fullBody struct {
	Type     string  `json:"type"`
	Messages []int64 `json:"messages"`
	Final    bool    `json:"final,omitempty"`
}

// stateBody is a cheap digest of the sender's whole set.
type stateBody struct {
	Type  string `json:"type"`
	Count int    `json:"count"`
	Xor   int64  `json:"xor"`
}

type server struct {
	n *mg.Node

	mu     sync.Mutex
	peers  []string
	values map[int64]struct{}
}

func newServer(n *mg.Node) *server {
	return &server{
		n:      n,
		values: make(map[int64]struct{}),
	}
}

// ---------------------------------------------------------------- clients --

// handleInit runs once, right after the library has learned our own ID and the
// full membership list.
func (s *server) handleInit(msg mg.Message) error {
	me := s.n.ID()
	ids := s.n.NodeIDs()

	s.mu.Lock()
	s.peers = make([]string, 0, len(ids))
	for _, id := range ids {
		if id != me {
			s.peers = append(s.peers, id)
		}
	}
	s.mu.Unlock()

	go s.antiEntropyLoop()
	return nil
}

// handleBroadcast stores a value and acknowledges the client before doing
// anything else: the client's latency clock starts at the ack.
func (s *server) handleBroadcast(msg mg.Message) error {
	var body struct {
		Message int64 `json:"message"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return fmt.Errorf("unmarshal broadcast body: %w", err)
	}

	// Ack first, then spread.
	if err := s.n.Reply(msg, map[string]string{"type": "broadcast_ok"}); err != nil {
		return fmt.Errorf("reply broadcast_ok: %w", err)
	}

	if !s.addValue(body.Message) {
		return nil // already known; somebody has already spread it
	}
	s.flood(body.Message)
	return nil
}

// handleRead returns every value this node knows about, each exactly once.
func (s *server) handleRead(msg mg.Message) error {
	s.mu.Lock()
	vals := make([]int64, 0, len(s.values))
	for v := range s.values {
		vals = append(vals, v)
	}
	s.mu.Unlock()

	sort.Slice(vals, func(i, j int) bool { return vals[i] < vals[j] })
	return s.n.Reply(msg, gossipBody{Type: "read_ok", Messages: vals})
}

// handleTopology acknowledges the suggested topology. We deliberately ignore
// its contents: talking to every node directly is both faster and, for this
// workload, cheaper than routing along a suggested sparse graph.
func (s *server) handleTopology(msg mg.Message) error {
	return s.n.Reply(msg, map[string]string{"type": "topology_ok"})
}

// ----------------------------------------------------------------- gossip --

// flood hands a brand new value to every other node in one hop.
func (s *server) flood(v int64) {
	s.mu.Lock()
	peers := append([]string(nil), s.peers...)
	s.mu.Unlock()

	if len(peers) == 0 {
		return
	}
	body := gossipBody{Type: "gossip", Messages: []int64{v}}
	for _, p := range peers {
		if err := s.n.Send(p, body); err != nil {
			logErr(fmt.Errorf("gossip to %s: %w", p, err))
		}
	}
}

// handleGossip merges values received from a peer. No re-forwarding: each
// value has exactly one origin, and it is the origin's job to spread it.
func (s *server) handleGossip(msg mg.Message) error {
	var body gossipBody
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return fmt.Errorf("unmarshal gossip body: %w", err)
	}
	s.addValues(body.Messages)
	return nil
}

// handleState compares digests; on disagreement we ship our full set to the
// peer, and the peer will tell us about anything we lack.
func (s *server) handleState(msg mg.Message) error {
	var body stateBody
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return fmt.Errorf("unmarshal state body: %w", err)
	}

	s.mu.Lock()
	count := len(s.values)
	xor := xorOf(s.values)
	s.mu.Unlock()

	if count == body.Count && xor == body.Xor {
		return nil // nothing to do
	}
	s.sendFull(msg.Src)
	return nil
}

// handleFull merges a snapshot of the peer's set and, on the last chunk,
// replies with whatever the peer is missing.
func (s *server) handleFull(msg mg.Message) error {
	var body fullBody
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return fmt.Errorf("unmarshal full body: %w", err)
	}
	s.addValues(body.Messages)

	if !body.Final {
		return nil
	}

	theirs := make(map[int64]struct{}, len(body.Messages))
	for _, v := range body.Messages {
		theirs[v] = struct{}{}
	}

	s.mu.Lock()
	missing := make([]int64, 0)
	for v := range s.values {
		if _, ok := theirs[v]; !ok {
			missing = append(missing, v)
		}
	}
	s.mu.Unlock()

	if len(missing) == 0 {
		return nil
	}
	sort.Slice(missing, func(i, j int) bool { return missing[i] < missing[j] })
	for i := 0; i < len(missing); i += syncChunkSize {
		end := i + syncChunkSize
		if end > len(missing) {
			end = len(missing)
		}
		if err := s.n.Send(msg.Src, gossipBody{Type: "gossip", Messages: missing[i:end]}); err != nil {
			logErr(fmt.Errorf("gossip to %s: %w", msg.Src, err))
		}
	}
	return nil
}

// ------------------------------------------------------------ anti-entropy --

func (s *server) antiEntropyLoop() {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	ticker := time.NewTicker(antiEntropyPeriod)
	defer ticker.Stop()

	for range ticker.C {
		s.mu.Lock()
		peers := append([]string(nil), s.peers...)
		body := stateBody{Type: "state", Count: len(s.values), Xor: xorOf(s.values)}
		s.mu.Unlock()

		if len(peers) == 0 {
			continue
		}
		for i := 0; i < antiEntropyFanout && i < len(peers); i++ {
			p := peers[rng.Intn(len(peers))]
			if err := s.n.Send(p, body); err != nil {
				logErr(fmt.Errorf("state to %s: %w", p, err))
			}
		}
	}
}

// sendFull ships our entire set, in chunks, telling the peer when we're done.
func (s *server) sendFull(dest string) {
	s.mu.Lock()
	vals := make([]int64, 0, len(s.values))
	for v := range s.values {
		vals = append(vals, v)
	}
	s.mu.Unlock()

	sort.Slice(vals, func(i, j int) bool { return vals[i] < vals[j] })
	if len(vals) == 0 {
		// Still send an (empty) final chunk so the peer learns it owes us
		// nothing... or rather, so it can send us what we're missing.
		if err := s.n.Send(dest, fullBody{Type: "full", Messages: []int64{}, Final: true}); err != nil {
			logErr(fmt.Errorf("full to %s: %w", dest, err))
		}
		return
	}
	for i := 0; i < len(vals); i += syncChunkSize {
		end := i + syncChunkSize
		if end > len(vals) {
			end = len(vals)
		}
		if err := s.n.Send(dest, fullBody{
			Type:     "full",
			Messages: vals[i:end],
			Final:    end == len(vals),
		}); err != nil {
			logErr(fmt.Errorf("full to %s: %w", dest, err))
		}
	}
}

// ------------------------------------------------------------------ state --

func (s *server) addValue(v int64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.values[v]; ok {
		return false
	}
	s.values[v] = struct{}{}
	return true
}

func (s *server) addValues(vs []int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, v := range vs {
		s.values[v] = struct{}{}
	}
}

func xorOf(m map[int64]struct{}) int64 {
	var x int64
	for v := range m {
		x ^= v
	}
	return x
}

func logErr(err error) {
	fmt.Fprintf(os.Stderr, "%v\n", err)
}

func main() {
	// The library logs every message it reads and writes; that is a lot of
	// noise at 100 msg/s per node, so send the standard logger to the void.
	// Our own diagnostics go straight to stderr.
	log.SetOutput(io.Discard)

	n := mg.NewNode()
	s := newServer(n)

	n.Handle("init", s.handleInit)
	n.Handle("broadcast", s.handleBroadcast)
	n.Handle("read", s.handleRead)
	n.Handle("topology", s.handleTopology)
	n.Handle("gossip", s.handleGossip)
	n.Handle("state", s.handleState)
	n.Handle("full", s.handleFull)

	if err := n.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "fatal: %v\n", err)
		os.Exit(1)
	}
}
