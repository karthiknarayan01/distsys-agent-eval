package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// The workload only uses integers as broadcast values. Keeping them as int64
// avoids surprises if the workload uses a value outside the range of int on
// the platform.
type broadcastRequest struct {
	Type    string `json:"type"`
	MsgID   int    `json:"msg_id"`
	Message int64  `json:"message"`
}

type replicate struct {
	Type    string `json:"type"`
	Message int64  `json:"message"`
}

type readRequest struct {
	Type  string `json:"type"`
	MsgID int    `json:"msg_id"`
}

type readResponse struct {
	Type     string  `json:"type"`
	Messages []int64 `json:"messages"`
}

type topologyRequest struct {
	Type     string              `json:"type"`
	MsgID    int                 `json:"msg_id"`
	Topology map[string][]string `json:"topology"`
}

type okResponse struct {
	Type string `json:"type"`
}

type store struct {
	mu      sync.RWMutex
	seen    map[int64]struct{}
	ordered []int64
}

func newStore() *store {
	return &store{seen: make(map[int64]struct{})}
}

// add records a value and reports whether it was new. Broadcast values are
// unique in this workload, but checking also makes the protocol idempotent.
func (s *store) add(v int64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.seen[v]; ok {
		return false
	}
	s.seen[v] = struct{}{}
	s.ordered = append(s.ordered, v)
	return true
}

func (s *store) snapshot() []int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	// Copy while holding the lock so a read is a consistent snapshot.
	out := make([]int64, len(s.ordered))
	copy(out, s.ordered)
	return out
}

func main() {
	node := maelstrom.NewNode()
	values := newStore()

	// A client starts a broadcast at one node. We replicate directly to all
	// other nodes. Besides being the minimum possible number of server
	// messages for one value (one per destination), this makes dissemination
	// take only one network hop, irrespective of where the client happened to
	// send the request.
	node.Handle("broadcast", func(msg maelstrom.Message) error {
		var req broadcastRequest
		if err := json.Unmarshal(msg.Body, &req); err != nil {
			return err
		}

		values.add(req.Message)

		// Send one copy to every other node. These are one-way messages: the
		// recipient only has to add the value to its local set.
		for _, id := range node.NodeIDs() {
			if id == node.ID() {
				continue
			}
			if err := node.Send(id, replicate{
				Type:    "replicate",
				Message: req.Message,
			}); err != nil {
				return err
			}
		}

		// Queue all replication messages before acknowledging the client.
		return node.Reply(msg, okResponse{Type: "broadcast_ok"})
	})

	// Replicas are deliberately not forwarded. The origin already sent the
	// value directly to every node, so forwarding would only amplify traffic.
	node.Handle("replicate", func(msg maelstrom.Message) error {
		var r replicate
		if err := json.Unmarshal(msg.Body, &r); err != nil {
			return err
		}
		values.add(r.Message)
		return nil
	})

	node.Handle("read", func(msg maelstrom.Message) error {
		var req readRequest
		if err := json.Unmarshal(msg.Body, &req); err != nil {
			return err
		}
		return node.Reply(msg, readResponse{
			Type:     "read_ok",
			Messages: values.snapshot(),
		})
	})

	// The topology is only a suggestion. We accept it but use the complete
	// node list above to keep dissemination to a single network hop.
	node.Handle("topology", func(msg maelstrom.Message) error {
		var req topologyRequest
		if err := json.Unmarshal(msg.Body, &req); err != nil {
			return err
		}
		return node.Reply(msg, okResponse{Type: "topology_ok"})
	})

	if err := node.Run(); err != nil {
		log.Printf("node stopped: %v", err)
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
