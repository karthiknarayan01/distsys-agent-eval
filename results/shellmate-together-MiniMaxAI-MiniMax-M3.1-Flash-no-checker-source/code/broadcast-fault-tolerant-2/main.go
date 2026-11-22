package main

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const gossipInterval = 100 * time.Millisecond

// gossipBody is sent between nodes. It is deliberately a request without a
// msg_id: it is fire-and-forget, and periodic retries make it reliable even
// when the network is partitioned.
type gossipBody struct {
	Type     string `json:"type"`
	Messages []int  `json:"messages"`
}

type broadcastBody struct {
	Type    string `json:"type"`
	Message int    `json:"message"`
}

type readBody struct {
	Type     string `json:"type"`
	Messages []int  `json:"messages"`
}

type topologyBody struct {
	Type     string              `json:"type"`
	Topology map[string][]string `json:"topology"`
}

type nodeState struct {
	mu     sync.RWMutex
	values map[int]struct{}
	peers  []string
}

func newNodeState() *nodeState {
	return &nodeState{
		values: make(map[int]struct{}),
	}
}

func (s *nodeState) add(values []int) {
	s.mu.Lock()
	for _, value := range values {
		s.values[value] = struct{}{}
	}
	s.mu.Unlock()
}

func (s *nodeState) snapshot() ([]int, []string) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	messages := make([]int, 0, len(s.values))
	for value := range s.values {
		messages = append(messages, value)
	}
	peers := append([]string(nil), s.peers...)
	return messages, peers
}

func (s *nodeState) setPeers(peers []string) {
	s.mu.Lock()
	s.peers = append(s.peers[:0], peers...)
	s.mu.Unlock()
}

func main() {
	m := maelstrom.NewNode()
	state := newNodeState()

	// The library has already recorded the node's identity when it invokes
	// this handler. Keep a private copy of the peer list for gossip.
	m.Handle("init", func(msg maelstrom.Message) error {
		var body struct {
			NodeID  string   `json:"node_id"`
			NodeIDs []string `json:"node_ids"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		peers := make([]string, 0, len(body.NodeIDs))
		for _, id := range body.NodeIDs {
			if id != body.NodeID {
				peers = append(peers, id)
			}
		}
		state.setPeers(peers)
		return nil
	})

	m.Handle("topology", func(msg maelstrom.Message) error {
		var body topologyBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		// The topology is optional. We use an all-to-all anti-entropy mesh,
		// so a partition cannot permanently isolate any value.
		return m.Reply(msg, map[string]any{"type": "topology_ok"})
	})

	m.Handle("broadcast", func(msg maelstrom.Message) error {
		var body broadcastBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// Store before acknowledging. The periodic anti-entropy rounds will
		// deliver the value to every other node after the network heals.
		state.add([]int{body.Message})
		return m.Reply(msg, map[string]any{"type": "broadcast_ok"})
	})

	m.Handle("read", func(msg maelstrom.Message) error {
		messages, _ := state.snapshot()
		return m.Reply(msg, readBody{Type: "read_ok", Messages: messages})
	})

	m.Handle("gossip", func(msg maelstrom.Message) error {
		var body gossipBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		state.add(body.Messages)
		return nil
	})

	// Full-state anti-entropy is idempotent and eventually repairs every
	// message lost while a partition was active. It also works when the
	// partition is healed before the next round.
	stop := make(chan struct{})
	go func() {
		ticker := time.NewTicker(gossipInterval)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				messages, peers := state.snapshot()
				if len(messages) == 0 || len(peers) == 0 {
					continue
				}
				body := gossipBody{Type: "gossip", Messages: messages}
				for _, peer := range peers {
					if err := m.Send(peer, body); err != nil {
						log.Printf("gossip to %s failed: %v", peer, err)
					}
				}
			}
		}
	}()

	if err := m.Run(); err != nil {
		close(stop)
		log.Fatalf("node failed: %v", err)
	}
	close(stop)
}
