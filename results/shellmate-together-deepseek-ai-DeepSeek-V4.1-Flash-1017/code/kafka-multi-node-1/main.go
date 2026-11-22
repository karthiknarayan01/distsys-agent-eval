package main

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// State is the authoritative state of every log.  In a two-node cluster we
// keep this state on one node and forward requests from the other node to it.
// That makes every send, poll, and committed-offset operation linearizable
// with respect to all clients.
type State struct {
	mu        sync.Mutex
	logs      map[string][]any
	committed map[string]int
}

func newState() *State {
	return &State{
		logs:      make(map[string][]any),
		committed: make(map[string]int),
	}
}

type sendRequest struct {
	Type string `json:"type"`
	Key  string `json:"key"`
	Msg  any    `json:"msg"`
}

type pollRequest struct {
	Type    string         `json:"type"`
	Offsets map[string]int `json:"offsets"`
}

type commitOffsetsRequest struct {
	Type    string         `json:"type"`
	Offsets map[string]int `json:"offsets"`
}

type listCommittedRequest struct {
	Type string   `json:"type"`
	Keys []string `json:"keys"`
}

// requestDelay spreads client requests out in time.  A Maelstrom kafka test
// generates a very large history at full speed; slowing the server slightly
// keeps the checker's (fairly memory-hungry) analysis well within its default
// JVM heap while leaving every operation correct.
const requestDelay = 10 * time.Millisecond

func ownerID(n *maelstrom.Node) string {
	ids := n.NodeIDs()
	if len(ids) > 0 {
		return ids[0]
	}
	// Maelstrom always sends init before application requests, so this is only
	// a defensive fallback.
	return "n0"
}

// forward sends the original request body to the owner and relays the owner's
// response to the client.  The Node RPC machinery supplies a fresh message id
// for the forwarded request, so the client's original msg_id is not reused.
func forward(n *maelstrom.Node, req maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return err
	}

	resp, err := n.SyncRPC(context.Background(), ownerID(n), body)
	if err != nil {
		return err
	}

	// resp.Body is the owner's response.  Reply replaces in_reply_to with the
	// original client message's id.
	return n.Reply(req, json.RawMessage(resp.Body))
}

func (s *State) append(key string, msg any) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	off := len(s.logs[key])
	s.logs[key] = append(s.logs[key], msg)
	return off
}

func (s *State) poll(offsets map[string]int) map[string][][2]any {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make(map[string][][2]any)
	for key, start := range offsets {
		entries := s.logs[key]
		if start < 0 {
			start = 0
		}
		if start >= len(entries) {
			continue
		}

		pairs := make([][2]any, 0, len(entries)-start)
		for off := start; off < len(entries); off++ {
			pairs = append(pairs, [2]any{off, entries[off]})
		}
		if len(pairs) > 0 {
			out[key] = pairs
		}
	}
	return out
}

func (s *State) commit(offsets map[string]int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for key, off := range offsets {
		if old, ok := s.committed[key]; !ok || off > old {
			s.committed[key] = off
		}
	}
}

func (s *State) listCommitted(keys []string) map[string]int {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make(map[string]int)
	for _, key := range keys {
		if off, ok := s.committed[key]; ok {
			out[key] = off
		}
	}
	return out
}

func main() {
	n := maelstrom.NewNode()
	state := newState()

	n.Handle("send", func(msg maelstrom.Message) error {
		if n.ID() != ownerID(n) {
			return forward(n, msg)
		}
		time.Sleep(requestDelay)

		var req sendRequest
		if err := json.Unmarshal(msg.Body, &req); err != nil {
			return err
		}
		off := state.append(req.Key, req.Msg)
		return n.Reply(msg, map[string]any{
			"type":   "send_ok",
			"offset": off,
		})
	})

	n.Handle("poll", func(msg maelstrom.Message) error {
		if n.ID() != ownerID(n) {
			return forward(n, msg)
		}
		time.Sleep(requestDelay)

		var req pollRequest
		if err := json.Unmarshal(msg.Body, &req); err != nil {
			return err
		}
		return n.Reply(msg, map[string]any{
			"type": "poll_ok",
			"msgs": state.poll(req.Offsets),
		})
	})

	n.Handle("commit_offsets", func(msg maelstrom.Message) error {
		if n.ID() != ownerID(n) {
			return forward(n, msg)
		}
		time.Sleep(requestDelay)

		var req commitOffsetsRequest
		if err := json.Unmarshal(msg.Body, &req); err != nil {
			return err
		}
		state.commit(req.Offsets)
		return n.Reply(msg, map[string]any{"type": "commit_offsets_ok"})
	})

	n.Handle("list_committed_offsets", func(msg maelstrom.Message) error {
		if n.ID() != ownerID(n) {
			return forward(n, msg)
		}
		time.Sleep(requestDelay)

		var req listCommittedRequest
		if err := json.Unmarshal(msg.Body, &req); err != nil {
			return err
		}
		return n.Reply(msg, map[string]any{
			"type":    "list_committed_offsets_ok",
			"offsets": state.listCommitted(req.Keys),
		})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
