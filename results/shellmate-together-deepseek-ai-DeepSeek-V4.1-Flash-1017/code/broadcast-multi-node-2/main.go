package main

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const (
	// Retry undelivered gossip periodically. This is mostly useful if the
	// network is partitioned for a while; the messages sent during a partition
	// can be lost, while retrying makes the nodes converge once it heals.
	retryInterval = 100 * time.Millisecond

	// A client broadcast waits this long for direct acknowledgements from the
	// other nodes. Normally acknowledgements arrive in a few milliseconds.
	// The timeout only prevents a failed node from blocking the client forever.
	broadcastAckTimeout = 1 * time.Second
)

type waiter struct {
	remaining map[string]struct{}
	done      chan struct{}
	closed    bool
}

type server struct {
	node *maelstrom.Node

	mu      sync.Mutex
	values  map[int]struct{}
	pending map[string]map[int]struct{} // peer -> values not acknowledged by peer
	waiters map[int]*waiter             // value -> broadcast waiting for fanout

	startedOnce sync.Once
}

func newServer(n *maelstrom.Node) *server {
	return &server{
		node:    n,
		values:  make(map[int]struct{}),
		pending: make(map[string]map[int]struct{}),
		waiters: make(map[int]*waiter),
	}
}

func (s *server) addPendingLocked(peer string, value int) {
	m := s.pending[peer]
	if m == nil {
		m = make(map[int]struct{})
		s.pending[peer] = m
	}
	m[value] = struct{}{}
}

func (s *server) removePendingLocked(peer string, value int) {
	if m := s.pending[peer]; m != nil {
		delete(m, value)
		if len(m) == 0 {
			delete(s.pending, peer)
		}
	}
}

// startRetries starts the periodic anti-entropy loop. It is called after init,
// when the list of node IDs is available.
func (s *server) startRetries() {
	s.startedOnce.Do(func() {
		go func() {
			ticker := time.NewTicker(retryInterval)
			defer ticker.Stop()
			for range ticker.C {
				s.flushPending()
			}
		}()
	})
}

// flushPending resends every value which has not yet been acknowledged by a
// peer. Sending duplicates is harmless because values form a set.
func (s *server) flushPending() {
	type batch struct {
		peer   string
		values []int
	}

	s.mu.Lock()
	batches := make([]batch, 0, len(s.pending))
	for peer, vals := range s.pending {
		if len(vals) == 0 {
			continue
		}
		vs := make([]int, 0, len(vals))
		for v := range vals {
			vs = append(vs, v)
		}
		batches = append(batches, batch{peer: peer, values: vs})
	}
	s.mu.Unlock()

	for _, b := range batches {
		// Keep lines comfortably below bufio.Scanner's default token limit on
		// the receiving side, even if a very large backlog accumulates.
		const maxBatch = 512
		for i := 0; i < len(b.values); i += maxBatch {
			end := i + maxBatch
			if end > len(b.values) {
				end = len(b.values)
			}
			body := map[string]any{
				"type":     "gossip",
				"messages": b.values[i:end],
			}
			if err := s.node.Send(b.peer, body); err != nil {
				log.Printf("send gossip to %s: %v", b.peer, err)
			}
		}
	}
}

func (s *server) handleBroadcast(msg maelstrom.Message) error {
	var body struct {
		Message int `json:"message"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	v := body.Message

	self := s.node.ID()
	peers := s.node.NodeIDs()

	s.mu.Lock()
	s.values[v] = struct{}{}

	w := &waiter{
		remaining: make(map[string]struct{}),
		done:      make(chan struct{}),
	}
	for _, peer := range peers {
		if peer == self {
			continue
		}
		w.remaining[peer] = struct{}{}
		s.addPendingLocked(peer, v)
	}
	s.waiters[v] = w
	s.mu.Unlock()

	// Fan the value out directly to every node. Waiting for their
	// acknowledgements before acknowledging the client guarantees that a
	// completed broadcast is already present everywhere, which also makes the
	// final reads at the end of a test observe every acknowledged value.
	s.flushPending()

	select {
	case <-w.done:
	case <-time.After(broadcastAckTimeout):
	}

	s.mu.Lock()
	if s.waiters[v] == w {
		delete(s.waiters, v)
	}
	s.mu.Unlock()

	return s.node.Reply(msg, map[string]any{"type": "broadcast_ok"})
}

func (s *server) handleGossip(msg maelstrom.Message) error {
	var body struct {
		Messages []int `json:"messages"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	self := s.node.ID()
	peers := s.node.NodeIDs()

	s.mu.Lock()
	newValues := make([]int, 0)
	seenThisMessage := make(map[int]struct{}, len(body.Messages))
	for _, v := range body.Messages {
		if _, duplicate := seenThisMessage[v]; duplicate {
			continue
		}
		seenThisMessage[v] = struct{}{}
		if _, known := s.values[v]; !known {
			s.values[v] = struct{}{}
			newValues = append(newValues, v)
		}
	}
	// Relay newly learned values to everyone except the sender. The direct
	// fanout from the original broadcaster usually makes this redundant, but
	// it helps other paths (and retries after a partition) converge too.
	for _, v := range newValues {
		for _, peer := range peers {
			if peer == self || peer == msg.Src {
				continue
			}
			s.addPendingLocked(peer, v)
		}
	}
	s.mu.Unlock()

	// Acknowledge the whole batch, including values we already knew. This lets
	// the sender stop retrying all of them.
	if err := s.node.Send(msg.Src, map[string]any{
		"type":     "gossip_ok",
		"messages": body.Messages,
	}); err != nil {
		log.Printf("send gossip ack to %s: %v", msg.Src, err)
	}

	if len(newValues) > 0 {
		s.flushPending()
	}
	return nil
}

func (s *server) handleGossipOK(msg maelstrom.Message) error {
	var body struct {
		Messages []int `json:"messages"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	s.mu.Lock()
	for _, v := range body.Messages {
		s.removePendingLocked(msg.Src, v)
		if w := s.waiters[v]; w != nil {
			delete(w.remaining, msg.Src)
			if len(w.remaining) == 0 && !w.closed {
				close(w.done)
				w.closed = true
			}
		}
	}
	s.mu.Unlock()
	return nil
}

func (s *server) handleRead(msg maelstrom.Message) error {
	s.mu.Lock()
	values := make([]int, 0, len(s.values))
	for v := range s.values {
		values = append(values, v)
	}
	s.mu.Unlock()

	return s.node.Reply(msg, map[string]any{
		"type":     "read_ok",
		"messages": values,
	})
}

func (s *server) handleTopology(msg maelstrom.Message) error {
	// The implementation uses a complete graph between nodes, which is
	// permitted regardless of the suggested topology. Still acknowledge the
	// topology message as required by the protocol.
	return s.node.Reply(msg, map[string]any{"type": "topology_ok"})
}

func (s *server) handleInit(msg maelstrom.Message) error {
	s.startRetries()
	return nil
}

func main() {
	n := maelstrom.NewNode()
	s := newServer(n)

	n.Handle("init", s.handleInit)
	n.Handle("topology", s.handleTopology)
	n.Handle("broadcast", s.handleBroadcast)
	n.Handle("read", s.handleRead)
	n.Handle("gossip", s.handleGossip)
	n.Handle("gossip_ok", s.handleGossipOK)

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
