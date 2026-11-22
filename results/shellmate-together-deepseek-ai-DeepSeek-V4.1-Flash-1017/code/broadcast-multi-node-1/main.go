package main

import (
	"encoding/json"
	"log"
	"os"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const gossipTimeout = 500 * time.Millisecond

type server struct {
	node *maelstrom.Node

	mu     sync.RWMutex
	values map[int]struct{}
}

func newServer(n *maelstrom.Node) *server {
	return &server{
		node:   n,
		values: make(map[int]struct{}),
	}
}

// add records values and returns the ones that were new.
func (s *server) add(vals ...int) []int {
	s.mu.Lock()
	defer s.mu.Unlock()
	var added []int
	for _, v := range vals {
		if _, ok := s.values[v]; !ok {
			s.values[v] = struct{}{}
			added = append(added, v)
		}
	}
	return added
}

// snapshot returns a copy of all values held locally.
func (s *server) snapshot() []int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]int, 0, len(s.values))
	for v := range s.values {
		out = append(out, v)
	}
	return out
}

// peers returns every node ID in the cluster except our own.
func (s *server) peers() []string {
	ids := s.node.NodeIDs()
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		if id != s.node.ID() {
			out = append(out, id)
		}
	}
	return out
}

// sendGossip asks one peer to store values and waits (briefly) for its
// acknowledgement. The acknowledgement channel is buffered so a late reply
// can never leave a goroutine blocked (which would keep the process from
// exiting when stdin closes).
func (s *server) sendGossip(peer string, vals []int) {
	done := make(chan struct{}, 1)
	err := s.node.RPC(peer, map[string]any{
		"type":   "gossip",
		"values": vals,
	}, func(maelstrom.Message) error {
		done <- struct{}{}
		return nil
	})
	if err != nil {
		log.Printf("gossip to %s failed: %s", peer, err)
		return
	}
	select {
	case <-done:
	case <-time.After(gossipTimeout):
		log.Printf("gossip to %s timed out", peer)
	}
}

// propagate forwards values to every other node (except an optional sender)
// and waits for all of them to acknowledge, so that once a broadcast is
// acknowledged by us, every node has already stored it. It is best effort:
// errors and timeouts are logged and ignored, since the periodic anti-entropy
// loop will repair any gaps.
func (s *server) propagate(vals []int, except string) {
	if len(vals) == 0 {
		return
	}
	var wg sync.WaitGroup
	for _, peer := range s.peers() {
		if peer == except {
			continue
		}
		wg.Add(1)
		go func(peer string) {
			defer wg.Done()
			s.sendGossip(peer, vals)
		}(peer)
	}
	wg.Wait()
}

func (s *server) handleBroadcast(msg maelstrom.Message) error {
	var body struct {
		Message int `json:"message"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	added := s.add(body.Message)
	s.propagate(added, "")

	return s.node.Reply(msg, map[string]any{"type": "broadcast_ok"})
}

func (s *server) handleGossip(msg maelstrom.Message) error {
	var body struct {
		Values []int `json:"values"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	added := s.add(body.Values...)
	if len(added) > 0 {
		// Push new values onwards in the background (gossip can arrive from
		// several directions at once; duplicates are cheap to ignore).
		go s.propagate(added, msg.Src)
	}

	return s.node.Reply(msg, map[string]any{"type": "gossip_ok"})
}

func (s *server) handleRead(msg maelstrom.Message) error {
	return s.node.Reply(msg, map[string]any{
		"type":     "read_ok",
		"messages": s.snapshot(),
	})
}

func (s *server) handleTopology(msg maelstrom.Message) error {
	return s.node.Reply(msg, map[string]any{"type": "topology_ok"})
}

func (s *server) handleInit(msg maelstrom.Message) error {
	go s.antiEntropy()
	return nil
}

// antiEntropy periodically pushes the full value set to a peer, round-robin.
// The network for this workload is reliable, but this guarantees eventual
// convergence even if some one-off gossip message is lost or arrives oddly
// ordered.
func (s *server) antiEntropy() {
	peers := s.peers()
	if len(peers) == 0 {
		return
	}
	i := 0
	for {
		time.Sleep(200 * time.Millisecond)

		vals := s.snapshot()
		if len(vals) == 0 {
			continue
		}
		peer := peers[i%len(peers)]
		i++

		s.sendGossip(peer, vals)
	}
}

func main() {
	n := maelstrom.NewNode()
	s := newServer(n)

	n.Handle("broadcast", s.handleBroadcast)
	n.Handle("gossip", s.handleGossip)
	n.Handle("read", s.handleRead)
	n.Handle("topology", s.handleTopology)
	n.Handle("init", s.handleInit)

	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}
}
