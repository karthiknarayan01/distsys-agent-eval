// Kafka-style append-only logs, replicated across two Maelstrom nodes.
//
// Design: one node (the lexicographically smallest node ID) acts as the
// leader and owns the authoritative copy of every log plus the committed
// offsets. The other node forwards all four client RPCs to the leader and
// relays the leader's response back to the client. Since the leader
// serializes every operation, both nodes always agree on every log and the
// whole thing behaves as a strictly serializable, single-process Kafka.
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sort"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// maxPairs caps how many [offset, value] pairs we'll return for a single log
// in one poll. Clients keep polling from wherever we leave them, so this only
// bounds the size of a single response.
const maxPairs = 500

// server holds all of our state.
type server struct {
	n *maelstrom.Node

	// ready is closed once the init message has been processed; node IDs are
	// only known then.
	ready chan struct{}
	once  sync.Once

	// leader is the node which owns the authoritative state.
	leader string

	// mu guards the state below. Only meaningful on the leader.
	mu        sync.Mutex
	logs      map[string][]interface{} // key -> values; offset == index
	committed map[string]int64          // key -> highest committed offset
}

func newServer(n *maelstrom.Node) *server {
	return &server{
		n:         n,
		ready:     make(chan struct{}),
		logs:      make(map[string][]interface{}),
		committed: make(map[string]int64),
	}
}

// waitReady blocks until we know our own node ID and the cluster membership.
func (s *server) waitReady() {
	<-s.ready
}

// isLeader reports whether this node owns the authoritative state.
func (s *server) isLeader() bool {
	return s.n.ID() == s.leader
}

// ---- state operations (leader only) ----

// append adds a message to a log and returns the offset it landed at.
func (s *server) append(key string, msg interface{}) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	log := s.logs[key]
	s.logs[key] = append(log, msg)
	return int64(len(s.logs[key]) - 1)
}

// poll returns, for every requested log, the [offset, value] pairs starting
// at the first offset which is >= the requested one. Logs with nothing to
// deliver are omitted from the result.
func (s *server) poll(offsets map[string]int64) map[string][][]interface{} {
	s.mu.Lock()
	defer s.mu.Unlock()

	msgs := make(map[string][][]interface{}, len(offsets))
	for key, start := range offsets {
		if start < 0 {
			start = 0
		}
		log := s.logs[key]
		if int64(len(log)) <= start {
			// Nothing at or after this offset.
			continue
		}
		end := int64(len(log))
		if start+maxPairs < end {
			end = start + maxPairs
		}
		pairs := make([][]interface{}, 0, end-start)
		for i := start; i < end; i++ {
			pairs = append(pairs, []interface{}{i, log[i]})
		}
		msgs[key] = pairs
	}
	return msgs
}

// commit records the highest offsets clients have finished reading. Committed
// offsets only ever advance.
func (s *server) commit(offsets map[string]int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for key, off := range offsets {
		if cur, ok := s.committed[key]; !ok || off > cur {
			s.committed[key] = off
		}
	}
}

// committedOffsets returns the committed offset of every key we know about.
func (s *server) committedOffsets(keys []string) map[string]int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]int64, len(keys))
	for _, key := range keys {
		if off, ok := s.committed[key]; ok {
			out[key] = off
		}
	}
	return out
}

// ---- message handlers ----

func (s *server) handleInit(msg maelstrom.Message) error {
	ids := append([]string(nil), s.n.NodeIDs()...)
	sort.Strings(ids)
	if len(ids) == 0 {
		return fmt.Errorf("no node ids in init message")
	}
	s.leader = ids[0]
	s.once.Do(func() { close(s.ready) })
	log.Printf("initialized %s; leader is %s", s.n.ID(), s.leader)
	return nil
}

func (s *server) handleSend(msg maelstrom.Message) error {
	s.waitReady()
	if !s.isLeader() {
		return s.forward(msg)
	}

	var body struct {
		Key string      `json:"key"`
		Msg interface{} `json:"msg"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return fmt.Errorf("parse send body: %w", err)
	}
	offset := s.append(body.Key, body.Msg)
	return s.n.Reply(msg, map[string]interface{}{
		"type":   "send_ok",
		"offset": offset,
	})
}

func (s *server) handlePoll(msg maelstrom.Message) error {
	s.waitReady()
	if !s.isLeader() {
		return s.forward(msg)
	}

	var body struct {
		Offsets map[string]int64 `json:"offsets"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return fmt.Errorf("parse poll body: %w", err)
	}
	msgs := s.poll(body.Offsets)
	return s.n.Reply(msg, map[string]interface{}{
		"type": "poll_ok",
		"msgs": msgs,
	})
}

func (s *server) handleCommitOffsets(msg maelstrom.Message) error {
	s.waitReady()
	if !s.isLeader() {
		return s.forward(msg)
	}

	var body struct {
		Offsets map[string]int64 `json:"offsets"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return fmt.Errorf("parse commit_offsets body: %w", err)
	}
	s.commit(body.Offsets)
	return s.n.Reply(msg, map[string]interface{}{
		"type": "commit_offsets_ok",
	})
}

func (s *server) handleListCommittedOffsets(msg maelstrom.Message) error {
	s.waitReady()
	if !s.isLeader() {
		return s.forward(msg)
	}

	var body struct {
		Keys []string `json:"keys"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return fmt.Errorf("parse list_committed_offsets body: %w", err)
	}
	offsets := s.committedOffsets(body.Keys)
	return s.n.Reply(msg, map[string]interface{}{
		"type":    "list_committed_offsets_ok",
		"offsets": offsets,
	})
}

// forward passes a client request on to the leader and relays the leader's
// response back to the client.
func (s *server) forward(msg maelstrom.Message) error {
	var body map[string]interface{}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return fmt.Errorf("parse body for forwarding: %w", err)
	}
	delete(body, "msg_id")
	delete(body, "in_reply_to")

	err := s.n.RPC(s.leader, body, func(resp maelstrom.Message) error {
		var replyBody map[string]interface{}
		if err := json.Unmarshal(resp.Body, &replyBody); err != nil {
			return fmt.Errorf("parse leader reply: %w", err)
		}
		delete(replyBody, "in_reply_to")
		delete(replyBody, "msg_id")
		return s.n.Reply(msg, replyBody)
	})
	if err != nil {
		return fmt.Errorf("forward %s to %s: %w", body["type"], s.leader, err)
	}
	return nil
}

func main() {
	log.SetPrefix(fmt.Sprintf("%s ", os.Args[0]))

	n := maelstrom.NewNode()
	s := newServer(n)

	n.Handle("init", s.handleInit)
	n.Handle("send", s.handleSend)
	n.Handle("poll", s.handlePoll)
	n.Handle("commit_offsets", s.handleCommitOffsets)
	n.Handle("list_committed_offsets", s.handleListCommittedOffsets)

	if err := n.Run(); err != nil {
		log.Printf("fatal: %v", err)
		os.Exit(1)
	}
}
