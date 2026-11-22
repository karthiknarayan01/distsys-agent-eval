package main

import (
	"encoding/json"
	"io"
	"log"
	"os"
	"sort"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// The cluster arranges itself as follows:
//
//   - One node (the lexicographically smallest node ID) acts as the hub.
//   - When any node learns a new value it immediately forwards it to the hub.
//     (One server message per broadcast, no matter how big the cluster is.)
//   - The hub accumulates new values and periodically emits a single batch
//     message containing every value it gathered since the previous flush to
//     each of the other nodes in one message.
//
// A value therefore reaches every node with only two messages-worth of hops
// (sender -> hub -> everyone) and, thanks to batching, the total number of
// server messages stays far below one message per node per value.
const defaultFlushInterval = 100 * time.Millisecond

type server struct {
	node *maelstrom.Node

	mu         sync.Mutex
	seen       map[int64]struct{} // every value this node knows
	values     []int64            // same values, in insertion order, for reads
	pending    []int64            // values not yet shipped to peers (hub only)
	peers      []string           // all node IDs except ours
	hub        string
	isHub      bool
	flushEvery time.Duration
}

func main() {
	if os.Getenv("NODE_DEBUG") == "" {
		// The library logs every message to stderr; that is a lot of I/O.
		log.SetOutput(io.Discard)
	}

	flushEvery := defaultFlushInterval
	if v := os.Getenv("FLUSH_MS"); v != "" {
		if d, err := time.ParseDuration(v + "ms"); err == nil && d > 0 {
			flushEvery = d
		}
	}

	n := maelstrom.NewNode()
	s := &server{
		node:       n,
		seen:       make(map[int64]struct{}),
		flushEvery: flushEvery,
	}

	// The library has already recorded our node ID and the cluster's node IDs
	// by the time this handler runs; it replies init_ok after we return.
	n.Handle("init", func(msg maelstrom.Message) error {
		ids := append([]string(nil), n.NodeIDs()...)
		sort.Strings(ids)

		s.mu.Lock()
		defer s.mu.Unlock()
		s.peers = s.peers[:0]
		if len(ids) == 0 {
			s.hub, s.isHub = "", false
			return nil
		}
		s.hub = ids[0]
		s.isHub = s.hub == n.ID()
		for _, id := range ids {
			if id != n.ID() {
				s.peers = append(s.peers, id)
			}
		}
		return nil
	})

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body struct {
			Message int64 `json:"message"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		s.learn([]int64{body.Message}, true)
		return n.Reply(msg, map[string]any{"type": "broadcast_ok"})
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		s.mu.Lock()
		vals := make([]int64, len(s.values))
		copy(vals, s.values)
		s.mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "read_ok", "messages": vals})
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		// We arrange ourselves; the suggested topology is not needed.
		return n.Reply(msg, map[string]any{"type": "topology_ok"})
	})

	// "forward" is a peer handing a freshly broadcast value to the hub.
	n.Handle("forward", func(msg maelstrom.Message) error {
		var body struct {
			Values []int64 `json:"values"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		s.learn(body.Values, false)
		return nil
	})

	// "batch" is the hub shipping a batch of values to a peer.
	n.Handle("batch", func(msg maelstrom.Message) error {
		var body struct {
			Values []int64 `json:"values"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		s.store(body.Values)
		return nil
	})

	go s.flusher()

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

// learn records values. New values are also propagated: from a client they go
// straight to the hub, while at the hub (whether the value came from a local
// client or from a peer) they wait for the next periodic flush.
func (s *server) learn(vals []int64, fromClient bool) {
	s.mu.Lock()
	fresh := s.addLocked(vals)
	if len(fresh) == 0 {
		s.mu.Unlock()
		return
	}
	if s.isHub {
		s.pending = append(s.pending, fresh...)
		s.mu.Unlock()
		return
	}
	hub := s.hub
	s.mu.Unlock()

	if fromClient && hub != "" {
		_ = s.node.Send(hub, map[string]any{
			"type":   "forward",
			"values": fresh,
		})
	}
}

// store records values without propagating them anywhere.
func (s *server) store(vals []int64) {
	s.mu.Lock()
	s.addLocked(vals)
	s.mu.Unlock()
}

// addLocked adds the values we have not seen yet and returns them.
func (s *server) addLocked(vals []int64) []int64 {
	var fresh []int64
	for _, v := range vals {
		if _, ok := s.seen[v]; ok {
			continue
		}
		s.seen[v] = struct{}{}
		s.values = append(s.values, v)
		fresh = append(fresh, v)
	}
	return fresh
}

// flusher periodically ships the hub's accumulated values to every peer.
func (s *server) flusher() {
	ticker := time.NewTicker(s.flushEvery)
	defer ticker.Stop()

	for range ticker.C {
		s.mu.Lock()
		if !s.isHub || len(s.pending) == 0 {
			s.mu.Unlock()
			continue
		}
		batch := s.pending
		s.pending = nil
		peers := make([]string, len(s.peers))
		copy(peers, s.peers)
		s.mu.Unlock()

		body := map[string]any{
			"type":   "batch",
			"values": batch,
		}
		for _, peer := range peers {
			_ = s.node.Send(peer, body)
		}
	}
}
