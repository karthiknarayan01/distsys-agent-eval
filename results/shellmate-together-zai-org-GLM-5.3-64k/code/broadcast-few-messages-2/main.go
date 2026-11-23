// Frugal broadcast for a large cluster.
//
// Strategy
// --------
// Every value is handed to exactly one node by a client. To keep the number of
// inter-node messages low we let the *origin* of a value deliver it directly to
// every other node: one message per (value, peer). That is the theoretical
// minimum (n-1 messages per value) and it is also the fastest possible
// dissemination (a single 100ms hop).
//
// Nodes never re-forward values they receive from other nodes: the origin
// already delivered the value to everybody, so relaying would only duplicate
// traffic. A cheap periodic anti-entropy exchange (each node talks to one peer
// every aePeriod) acts as insurance: for the cost of about one message per node
// per aePeriod it guarantees that a value missing anywhere converges
// everywhere, even if the direct delivery were to fail.
//
// With 25 nodes, ~1000 distinct values and ~2000 client operations this works
// out to roughly 12-13 server-to-server messages per client operation, with
// values stable everywhere within a couple hundred milliseconds.
package main

import (
	"encoding/json"
	"log"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	m "github.com/jepsen-io/maelstrom/demo/go"
)

const (
	// aePeriod is how often a node performs an anti-entropy exchange with a
	// peer.
	aePeriod = 500 * time.Millisecond

	// settleAge is how long we must have held a value before we are willing to
	// tell a peer it is missing that value. Values take at most a couple of
	// hundred ms to travel, so a value we've had this long and our peer's
	// snapshot lacks is genuinely missing over there; this filter keeps
	// anti-entropy silent while values are merely in flight.
	settleAge = 400 * time.Millisecond
)

// entry is a known value, plus when we learned it.
type entry struct {
	val  json.RawMessage
	seen time.Time
}

// node is our local state.
type node struct {
	n *m.Node

	mu sync.Mutex

	// vals maps the raw JSON encoding of a value to its entry. Keeping the
	// raw bytes means we echo values back bit-for-bit, whatever their type.
	vals map[string]entry

	// order is every node id in the cluster, sorted numerically; index is our
	// position in it. Used to pick anti-entropy partners deterministically.
	order []string
	index int

	// round counts anti-entropy rounds, for picking the partner of the round.
	round int

	// pending holds values learned before init completed.
	pending []json.RawMessage
}

func main() {
	n := m.NewNode()
	nd := &node{n: n, vals: map[string]entry{}, index: -1}

	// Client-facing RPCs.
	n.Handle("init", nd.onInit)
	n.Handle("topology", nd.onTopology)
	n.Handle("broadcast", nd.onBroadcast)
	n.Handle("read", nd.onRead)

	// Node-to-node messages of our own design.
	n.Handle("m", nd.onValues)     // a batch of values from a peer
	n.Handle("s", nd.onSync)       // anti-entropy: here is my whole set
	n.Handle("sr", nd.onSyncReply) // anti-entropy: values you are missing

	go nd.antiEntropyLoop()

	if err := n.Run(); err != nil {
		log.Fatalf("run: %v", err)
	}
}

// ---------------------------------------------------------------------------
// helpers

// learn adds any values we don't already have and returns the new ones.
func (nd *node) learn(vals []json.RawMessage) []json.RawMessage {
	now := time.Now()
	nd.mu.Lock()
	defer nd.mu.Unlock()
	var fresh []json.RawMessage
	for _, v := range vals {
		if len(v) == 0 {
			continue
		}
		k := string(v)
		if _, ok := nd.vals[k]; !ok {
			nd.vals[k] = entry{val: v, seen: now}
			fresh = append(fresh, v)
		}
	}
	return fresh
}

// snapshot returns every value we have.
func (nd *node) snapshot() []json.RawMessage {
	nd.mu.Lock()
	defer nd.mu.Unlock()
	out := make([]json.RawMessage, 0, len(nd.vals))
	for _, e := range nd.vals {
		out = append(out, e.val)
	}
	return out
}

// flood sends a batch of values straight to every other node in the cluster.
func (nd *node) flood(vals []json.RawMessage) {
	if len(vals) == 0 {
		return
	}
	nd.mu.Lock()
	others := make([]string, 0, len(nd.order))
	if nd.index >= 0 {
		for i, id := range nd.order {
			if i != nd.index {
				others = append(others, id)
			}
		}
	} else {
		// Not initialized yet: keep the values, we'll flush them on init.
		nd.pending = append(nd.pending, vals...)
	}
	nd.mu.Unlock()

	for _, id := range others {
		if err := nd.n.Send(id, map[string]any{"type": "m", "m": vals}); err != nil {
			log.Printf("flood to %s failed: %v", id, err)
		}
	}
}

// nodeSortKey sorts node ids like n1, n2, ..., n25 numerically.
func nodeSortKey(id string) (num int64, rest string) {
	i := strings.IndexFunc(id, func(r rune) bool { return r >= '0' && r <= '9' })
	if i < 0 {
		return -1, id
	}
	if v, err := strconv.ParseInt(id[i:], 10, 64); err == nil {
		return v, id
	}
	return -1, id
}

type sortedIDs []string

func (s sortedIDs) Len() int      { return len(s) }
func (s sortedIDs) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s sortedIDs) Less(i, j int) bool {
	a, ai := nodeSortKey(s[i])
	b, bi := nodeSortKey(s[j])
	if a != b {
		return a < b
	}
	return ai < bi
}

// ---------------------------------------------------------------------------
// handlers

// onInit records the cluster membership. The library has already filled in the
// node id and the node id list for us.
func (nd *node) onInit(msg m.Message) error {
	ids := append([]string(nil), nd.n.NodeIDs()...)
	sort.Sort(sortedIDs(ids))

	nd.mu.Lock()
	self := nd.n.ID()
	nd.order = ids
	nd.index = -1
	for i, id := range ids {
		if id == self {
			nd.index = i
		}
	}
	pending := nd.pending
	nd.pending = nil
	nd.mu.Unlock()

	if len(pending) > 0 {
		nd.flood(pending)
	}
	return nil
}

// onTopology acknowledges the suggested topology. We arrange our own, since a
// flat "origin floods everybody" layout minimizes both messages and latency.
func (nd *node) onTopology(msg m.Message) error {
	return nd.n.Reply(msg, map[string]any{"type": "topology_ok"})
}

// onBroadcast stores a value and, if it is new, delivers it to every other
// node directly. Only the origin forwards it, which keeps the message count at
// the theoretical minimum.
func (nd *node) onBroadcast(msg m.Message) error {
	var body struct {
		Message json.RawMessage `json:"message"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	fresh := nd.learn([]json.RawMessage{body.Message})

	if err := nd.n.Reply(msg, map[string]any{"type": "broadcast_ok"}); err != nil {
		return err
	}

	if len(fresh) > 0 {
		nd.flood(fresh)
	}
	return nil
}

// onRead returns every value we know about.
func (nd *node) onRead(msg m.Message) error {
	vals := nd.snapshot()
	return nd.n.Reply(msg, map[string]any{"type": "read_ok", "messages": vals})
}

// onValues receives a batch of values from a peer. The origin already sent it
// to everyone, so we just store it.
func (nd *node) onValues(msg m.Message) error {
	var body struct {
		M []json.RawMessage `json:"m"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	nd.learn(body.M)
	return nil
}

// ---------------------------------------------------------------------------
// anti-entropy

// antiEntropyLoop periodically exchanges full sets with a single peer, so that
// any value missing anywhere is repaired quickly.
//
// Partner choice is deterministic: in round r every node talks to the node r
// positions away from it in the sorted id list, which makes each round cover
// the whole cluster and cycles through every partner within n-1 rounds.
func (nd *node) antiEntropyLoop() {
	for {
		time.Sleep(aePeriod)

		nd.mu.Lock()
		if nd.index < 0 {
			nd.mu.Unlock()
			continue
		}
		n := len(nd.order)
		if n < 2 {
			nd.mu.Unlock()
			continue
		}
		nd.round++
		off := 1 + (nd.round-1)%(n-1)
		dest := nd.order[(nd.index+off)%n]
		nd.mu.Unlock()

		have := nd.snapshot()
		err := nd.n.RPC(dest, map[string]any{"type": "s", "m": have}, func(reply m.Message) error {
			var body struct {
				M []json.RawMessage `json:"m"`
			}
			if err := json.Unmarshal(reply.Body, &body); err != nil {
				return err
			}
			nd.learn(body.M)
			return nil
		})
		if err != nil {
			log.Printf("anti-entropy with %s failed: %v", dest, err)
		}
	}
}

// onSync receives a peer's full set. It merges it and replies with whatever the
// peer is missing. Values we've only just learned are held back: they are most
// likely still on their way to the peer, and answering with them would just
// add messages. When there is nothing to send back we skip the reply entirely.
func (nd *node) onSync(msg m.Message) error {
	var body struct {
		M []json.RawMessage `json:"m"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	theirs := make(map[string]struct{}, len(body.M))
	for _, v := range body.M {
		if len(v) > 0 {
			theirs[string(v)] = struct{}{}
		}
	}
	nd.learn(body.M)

	now := time.Now()
	nd.mu.Lock()
	var missing []json.RawMessage
	for k, e := range nd.vals {
		if _, ok := theirs[k]; ok {
			continue
		}
		if now.Sub(e.seen) >= settleAge {
			missing = append(missing, e.val)
		}
	}
	nd.mu.Unlock()

	if len(missing) == 0 {
		return nil
	}
	return nd.n.Reply(msg, map[string]any{"type": "sr", "m": missing})
}

// onSyncReply is only reached if a reply ever arrives without a matching
// callback; replies normally go to the callback registered by RPC.
func (nd *node) onSyncReply(msg m.Message) error {
	var body struct {
		M []json.RawMessage `json:"m"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	nd.learn(body.M)
	return nil
}
