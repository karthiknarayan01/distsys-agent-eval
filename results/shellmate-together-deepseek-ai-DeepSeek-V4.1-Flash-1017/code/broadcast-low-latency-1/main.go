package main

import (
	"encoding/json"
	"io"
	"log"
	"sort"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// A broadcast node for a small cluster.
//
// Every value is pushed directly from the node that first hears about it (from
// a client) to *all* other nodes.  With 25 nodes that costs 24 inter-node
// messages per broadcast - the information-theoretic minimum for one-hop
// dissemination - and gives the lowest possible latency: a single 100ms hop.
//
// A light-weight anti-entropy pass (one peer messaged per tick, round robin)
// acts as a safety net so that values can never be permanently lost.

type server struct {
	node *maelstrom.Node

	mu    sync.RWMutex
	vals  map[int64]struct{}
	slice []int64 // all values, insertion order (deduplicated)
	peers []string

	syncMu   sync.Mutex
	nextPeer int
}

func main() {
	// The demo library logs every message it sends and receives to stderr; at a
	// few thousand messages a second that is pure overhead.
	log.SetOutput(io.Discard)

	s := &server{vals: make(map[int64]struct{})}
	n := maelstrom.NewNode()
	s.node = n

	n.Handle("init", s.handleInit)
	n.Handle("topology", s.handleTopology)
	n.Handle("broadcast", s.handleBroadcast)
	n.Handle("read", s.handleRead)
	n.Handle("bcast", s.handleBcast)
	n.Handle("sync", s.handleSync)
	n.Handle("pull", s.handlePull)
	n.Handle("repair", s.handleRepair)

	if err := n.Run(); err != nil {
		log.Printf("node exited: %s", err)
	}
}

func (s *server) handleInit(msg maelstrom.Message) error {
	var body maelstrom.InitMessageBody
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	peers := make([]string, 0, len(body.NodeIDs))
	for _, id := range body.NodeIDs {
		if id != body.NodeID {
			peers = append(peers, id)
		}
	}
	s.mu.Lock()
	s.peers = peers
	s.mu.Unlock()
	go s.syncLoop()
	return nil
}

func (s *server) handleTopology(msg maelstrom.Message) error {
	return s.node.Reply(msg, map[string]any{"type": "topology_ok"})
}

// handleBroadcast handles a client request: store the value, tell everybody
// else about it, then acknowledge.
func (s *server) handleBroadcast(msg maelstrom.Message) error {
	var body struct {
		Message int64 `json:"message"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	s.add(body.Message)

	s.mu.RLock()
	peers := s.peers
	s.mu.RUnlock()

	out := map[string]any{"type": "bcast", "value": body.Message}
	for _, p := range peers {
		if err := s.node.Send(p, out); err != nil {
			return err
		}
	}

	return s.node.Reply(msg, map[string]any{"type": "broadcast_ok"})
}

// handleBcast receives a value pushed by another node.
func (s *server) handleBcast(msg maelstrom.Message) error {
	var body struct {
		Value int64 `json:"value"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	s.add(body.Value)
	return nil
}

func (s *server) handleRead(msg maelstrom.Message) error {
	s.mu.RLock()
	vals := make([]int64, len(s.slice))
	copy(vals, s.slice)
	s.mu.RUnlock()

	return s.node.Reply(msg, map[string]any{
		"type":     "read_ok",
		"messages": vals,
	})
}

// add inserts v, reporting whether it was new.
func (s *server) add(v int64) bool {
	s.mu.Lock()
	if _, ok := s.vals[v]; ok {
		s.mu.Unlock()
		return false
	}
	s.vals[v] = struct{}{}
	s.slice = append(s.slice, v)
	s.mu.Unlock()
	return true
}

// ---------------------------------------------------------------------------
// Anti-entropy

func (s *server) syncLoop() {
	t := time.NewTicker(500 * time.Millisecond)
	defer t.Stop()
	for range t.C {
		s.mu.RLock()
		peers := s.peers
		s.mu.RUnlock()
		if len(peers) == 0 {
			continue
		}
		s.syncMu.Lock()
		p := peers[s.nextPeer%len(peers)]
		s.nextPeer++
		s.syncMu.Unlock()

		s.node.Send(p, map[string]any{
			"type":   "sync",
			"ranges": s.ranges(),
		})
	}
}

// ranges returns the set as a sorted list of inclusive [lo, hi] intervals.
func (s *server) ranges() [][2]int64 {
	s.mu.RLock()
	vals := make([]int64, len(s.slice))
	copy(vals, s.slice)
	s.mu.RUnlock()
	return s.rangesOf(vals)
}

// rangeSet expands intervals into a set, bailing out on absurdly large spans.
func rangeSet(rs [][2]int64) map[int64]struct{} {
	m := make(map[int64]struct{})
	for _, r := range rs {
		if r[1] < r[0] {
			continue
		}
		if r[1]-r[0] > 1<<22 {
			continue
		}
		for v := r[0]; ; v++ {
			m[v] = struct{}{}
			if v == r[1] {
				break
			}
		}
	}
	return m
}

func (s *server) handleSync(msg maelstrom.Message) error {
	var body struct {
		Ranges [][2]int64 `json:"ranges"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	theirs := body.Ranges
	theirSet := rangeSet(theirs)

	s.mu.RLock()
	mine := make([]int64, len(s.slice))
	copy(mine, s.slice)
	s.mu.RUnlock()

	// Anything I know which they do not: push it.
	missing := make([]int64, 0)
	for _, v := range mine {
		if _, ok := theirSet[v]; !ok {
			missing = append(missing, v)
		}
	}
	if len(missing) > 0 {
		s.sendValues(msg.Src, missing)
	}

	// Anything they know which I do not: ask for just that difference.
	if !covers(s.rangesOf(mine), theirs) {
		return s.node.Send(msg.Src, map[string]any{
			"type":   "pull",
			"ranges": s.rangesOf(mine),
		})
	}
	return nil
}

// rangesOf compresses a sorted copy of vals into inclusive intervals.
func (s *server) rangesOf(vals []int64) [][2]int64 {
	sorted := make([]int64, len(vals))
	copy(sorted, vals)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	out := make([][2]int64, 0, 16)
	for i, v := range sorted {
		if i == 0 || v != sorted[i-1]+1 {
			out = append(out, [2]int64{v, v})
		} else {
			out[len(out)-1][1] = v
		}
	}
	return out
}

// covers reports whether the sorted, disjoint interval list mine covers every
// value in the sorted, disjoint interval list theirs.
func covers(mine, theirs [][2]int64) bool {
	j := 0
	for _, t := range theirs {
		cur := t[0]
		for j < len(mine) && mine[j][1] < cur {
			j++
		}
		for {
			if j >= len(mine) || mine[j][0] > cur {
				return false
			}
			if mine[j][1] >= t[1] {
				break
			}
			cur = mine[j][1] + 1
			j++
		}
	}
	return true
}

func (s *server) handlePull(msg maelstrom.Message) error {
	var body struct {
		Ranges [][2]int64 `json:"ranges"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	theirSet := rangeSet(body.Ranges)

	s.mu.RLock()
	mine := make([]int64, len(s.slice))
	copy(mine, s.slice)
	s.mu.RUnlock()

	extra := make([]int64, 0)
	for _, v := range mine {
		if _, ok := theirSet[v]; !ok {
			extra = append(extra, v)
		}
	}
	if len(extra) > 0 {
		s.sendValues(msg.Src, extra)
	}
	return nil
}

func (s *server) handleRepair(msg maelstrom.Message) error {
	var body struct {
		Values []int64 `json:"values"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	for _, v := range body.Values {
		s.add(v)
	}
	return nil
}

func (s *server) sendValues(dest string, vals []int64) {
	const chunk = 2000
	for len(vals) > chunk {
		s.node.Send(dest, map[string]any{"type": "repair", "values": vals[:chunk]})
		vals = vals[chunk:]
	}
	if len(vals) > 0 {
		s.node.Send(dest, map[string]any{"type": "repair", "values": vals})
	}
}
