package main

import (
	"encoding/json"
	"io"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// syncInterval controls how often each node exchanges its full set of known
// values with every other node. The broadcast values are small and the test is
// short, so shipping the whole set is cheap and makes recovery after a network
// partition essentially immediate.
const syncInterval = 100 * time.Millisecond

type syncBody struct {
	Type   string `json:"type"`
	Values []int  `json:"values"`
}

type server struct {
	n      *maelstrom.Node
	mu     sync.Mutex
	values map[int]struct{}
	peers  []string
}

func main() {
	// The Maelstrom library logs every message to stderr. With a periodic
	// anti-entropy protocol that is a lot of log lines; we do not need them.
	log.SetOutput(io.Discard)

	n := maelstrom.NewNode()
	s := &server{n: n, values: make(map[int]struct{})}

	n.Handle("init", s.handleInit)
	n.Handle("broadcast", s.handleBroadcast)
	n.Handle("read", s.handleRead)
	n.Handle("topology", s.handleTopology)
	n.Handle("gossip", s.handleGossip)
	n.Handle("sync_req", s.handleSyncReq)
	n.Handle("sync_resp", s.handleSyncResp)

	go s.syncLoop()

	if err := n.Run(); err != nil {
		log.Printf("node stopped: %s", err)
	}
}

func (s *server) handleInit(msg maelstrom.Message) error {
	// Talk to every node directly rather than following the suggested
	// topology: the nemesis partitions arbitrary links, and a full mesh lets
	// us recover through any path once the network heals.
	id := s.n.ID()
	peers := make([]string, 0, len(s.n.NodeIDs())-1)
	for _, other := range s.n.NodeIDs() {
		if other != id {
			peers = append(peers, other)
		}
	}

	s.mu.Lock()
	s.peers = peers
	s.mu.Unlock()
	return nil
}

func (s *server) handleBroadcast(msg maelstrom.Message) error {
	var body struct {
		Message int `json:"message"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	s.mu.Lock()
	_, known := s.values[body.Message]
	if !known {
		s.values[body.Message] = struct{}{}
	}
	peers := append([]string(nil), s.peers...)
	s.mu.Unlock()

	// Push a newly learned value out right away. If this message is lost to a
	// partition, the periodic full-state sync will repair it after the heal.
	if !known {
		for _, p := range peers {
			_ = s.n.Send(p, syncBody{Type: "gossip", Values: []int{body.Message}})
		}
	}

	return s.n.Reply(msg, map[string]string{"type": "broadcast_ok"})
}

func (s *server) handleRead(msg maelstrom.Message) error {
	return s.n.Reply(msg, struct {
		Type     string `json:"type"`
		Messages []int  `json:"messages"`
	}{
		Type:     "read_ok",
		Messages: s.snapshot(),
	})
}

func (s *server) handleTopology(msg maelstrom.Message) error {
	return s.n.Reply(msg, map[string]string{"type": "topology_ok"})
}

func (s *server) handleGossip(msg maelstrom.Message) error {
	var body syncBody
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	s.merge(body.Values)
	return nil
}

// handleSyncReq merges the sender's state and answers with ours. A single
// round trip therefore makes both sides converge.
func (s *server) handleSyncReq(msg maelstrom.Message) error {
	var body syncBody
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	s.merge(body.Values)
	return s.n.Send(msg.Src, syncBody{Type: "sync_resp", Values: s.snapshot()})
}

func (s *server) handleSyncResp(msg maelstrom.Message) error {
	var body syncBody
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	s.merge(body.Values)
	return nil
}

func (s *server) merge(values []int) {
	if len(values) == 0 {
		return
	}
	s.mu.Lock()
	for _, v := range values {
		s.values[v] = struct{}{}
	}
	s.mu.Unlock()
}

func (s *server) snapshot() []int {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]int, 0, len(s.values))
	for v := range s.values {
		out = append(out, v)
	}
	return out
}

// syncLoop is the anti-entropy component: every tick, send our entire value
// set to all peers, and merge the full sets they send back.
func (s *server) syncLoop() {
	ticker := time.NewTicker(syncInterval)
	defer ticker.Stop()

	for range ticker.C {
		s.mu.Lock()
		peers := append([]string(nil), s.peers...)
		values := make([]int, 0, len(s.values))
		for v := range s.values {
			values = append(values, v)
		}
		s.mu.Unlock()

		for _, p := range peers {
			_ = s.n.Send(p, syncBody{Type: "sync_req", Values: values})
		}
	}
}
