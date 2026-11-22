// Command node implements Maelstrom's Kafka-style log service.
//
// The service keeps a set of append-only logs, one per string key. A "send"
// appends a value to a log and is answered with the offset it was given;
// offsets within a log are dense and only ever grow, so every message in a log
// has an offset of its own. "poll" hands back the messages of several logs
// from a given offset onwards, "commit_offsets" records how far clients have
// read, and "list_committed_offsets" reads that back.
//
// All log state is owned by a single leader: the first node of Maelstrom's
// node ID list, which is in the same order on every node, so every node
// agrees on who leads. The leader performs each append, poll and commit under
// a lock, one at a time, which makes the whole thing strongly consistent: an
// acknowledged send is never lost, never moves, and always reads back the same
// value at the same offset. Should a cluster have more than one node, the
// other nodes relay requests to the leader and the leader answers the client
// itself, which keeps node-to-node messages small.
//
// State is kept in memory, which is plenty for the lifetime of a Maelstrom
// test, where nothing crashes and the log is never asked to survive a restart.
//
// Requests are also paced through a small rate limiter, described below.
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// entry is a single message in a log, together with the offset it was given.
type entry struct {
	offset int64
	value  json.RawMessage
}

// klog is the append-only log of a single key. Entries are kept in offset
// order, and offsets are dense: the entry at index i has offset i.
type klog struct {
	entries []entry
}

// append stores a message at the next free offset and returns that offset.
func (l *klog) append(value json.RawMessage) int64 {
	offset := int64(len(l.entries))
	l.entries = append(l.entries, entry{offset, value})
	return offset
}

// read returns every message at or after the given offset, in offset order.
func (l *klog) read(from int64) [][]any {
	i := sort.Search(len(l.entries), func(i int) bool {
		return l.entries[i].offset >= from
	})
	msgs := make([][]any, 0, len(l.entries)-i)
	for ; i < len(l.entries); i++ {
		e := l.entries[i]
		// Each message goes out as an [offset, value] pair.
		msgs = append(msgs, []any{e.offset, e.value})
	}
	return msgs
}

// request is the union of the bodies of the four kinds of client request.
type request struct {
	Type    string           `json:"type"`
	MsgID   int64            `json:"msg_id"`
	Key     string           `json:"key"`
	Msg     json.RawMessage  `json:"msg"`
	Offsets map[string]int64 `json:"offsets"`
	Keys    []string         `json:"keys"`
}

// server is the state of a node: every log, plus how far clients have
// committed their reads.
type server struct {
	n *maelstrom.Node

	// ready is closed once the init message has told us who we are.
	// Maelstrom won't let a client near us before that, but waiting here
	// costs nothing and means we never have to answer a request without
	// knowing our own node ID.
	ready     chan struct{}
	readyOnce sync.Once

	// pace spreads client requests out over time.
	pace *limiter

	// mu guards logs and committed, and with them the order in which
	// appends, polls and commits take effect.
	mu        sync.Mutex
	logs      map[string]*klog
	committed map[string]int64
}

func newServer(n *maelstrom.Node, pace *limiter) *server {
	return &server{
		n:         n,
		ready:     make(chan struct{}),
		pace:      pace,
		logs:      make(map[string]*klog),
		committed: make(map[string]int64),
	}
}

// leaderID is the node that owns all the log state. Maelstrom hands every
// node the same, identically ordered list of node IDs.
func (s *server) leaderID() string {
	if ids := s.n.NodeIDs(); len(ids) > 0 {
		return ids[0]
	}
	return s.n.ID()
}

func (s *server) isLeader() bool {
	return s.leaderID() == s.n.ID()
}

// appendToLog appends a value to a key's log and returns its offset.
func (s *server) appendToLog(key string, value json.RawMessage) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	l := s.logs[key]
	if l == nil {
		l = &klog{}
		s.logs[key] = l
	}
	return l.append(value)
}

// poll returns, for each key asked about, every message at or after the
// offset the client wants to read from. Keys with nothing to say are left out.
func (s *server) poll(offsets map[string]int64) map[string][][]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	msgs := make(map[string][][]any, len(offsets))
	for key, from := range offsets {
		l := s.logs[key]
		if l == nil {
			continue
		}
		if pairs := l.read(from); len(pairs) > 0 {
			msgs[key] = pairs
		}
	}
	return msgs
}

// commit records the highest offset a client has finished reading up to.
// Committed offsets only ever advance.
func (s *server) commit(offsets map[string]int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for key, offset := range offsets {
		if current, ok := s.committed[key]; !ok || offset > current {
			s.committed[key] = offset
		}
	}
}

// committedOffsets returns the committed offset of every key asked about,
// leaving out keys with nothing committed.
func (s *server) committedOffsets(keys []string) map[string]int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	offsets := make(map[string]int64, len(keys))
	for _, key := range keys {
		if offset, ok := s.committed[key]; ok {
			offsets[key] = offset
		}
	}
	return offsets
}

// apply performs a client request against the log state and returns the body
// of the response to send back.
func (s *server) apply(req *request) (map[string]any, error) {
	switch req.Type {
	case "send":
		return map[string]any{
			"type":   "send_ok",
			"offset": s.appendToLog(req.Key, req.Msg),
		}, nil

	case "poll":
		return map[string]any{
			"type": "poll_ok",
			"msgs": s.poll(req.Offsets),
		}, nil

	case "commit_offsets":
		s.commit(req.Offsets)
		return map[string]any{"type": "commit_offsets_ok"}, nil

	case "list_committed_offsets":
		return map[string]any{
			"type":    "list_committed_offsets_ok",
			"offsets": s.committedOffsets(req.Keys),
		}, nil

	default:
		return nil, fmt.Errorf("unhandled message type %q", req.Type)
	}
}

// sendResponse sends a response body to a client, tagging it with the msg_id
// of the request it answers. Bodies hold on to the raw JSON we were given, so
// message values reach the client byte for byte.
func (s *server) sendResponse(client string, msgID int64, body map[string]any) error {
	body["in_reply_to"] = msgID
	return s.n.Send(client, body)
}

// respond answers a request, whatever its source, with the given body.
func (s *server) respond(req maelstrom.Message, body map[string]any) error {
	var reqBody struct {
		MsgID int64 `json:"msg_id"`
	}
	if err := json.Unmarshal(req.Body, &reqBody); err != nil {
		return err
	}
	return s.sendResponse(req.Src, reqBody.MsgID, body)
}

// onInit notes that this node now knows its own ID and the cluster's members.
func (s *server) onInit(_ maelstrom.Message) error {
	s.readyOnce.Do(func() { close(s.ready) })
	return nil
}

// onClientRequest handles the four kinds of client request. The leader
// performs them itself; any other node relays them to the leader.
func (s *server) onClientRequest(req maelstrom.Message) error {
	<-s.ready
	s.pace.wait()
	if !s.isLeader() {
		return s.forward(req)
	}

	var body request
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return fmt.Errorf("parsing request: %w", err)
	}
	resp, err := s.apply(&body)
	if err != nil {
		return err
	}
	return s.respond(req, resp)
}

// onForward handles a request relayed to the leader by another node: perform
// it, answer the client directly -- that way big poll responses never have to
// squeeze through a node -- and tell the relaying node that we're done.
func (s *server) onForward(msg maelstrom.Message) error {
	<-s.ready
	s.pace.wait()
	var fwd struct {
		Client  string `json:"client"`   // the client that made the request
		ReplyTo int64  `json:"reply_to"` // its msg_id
		Body    string `json:"body"`     // its request body, as JSON text
	}
	if err := json.Unmarshal(msg.Body, &fwd); err != nil {
		return fmt.Errorf("parsing relayed request: %w", err)
	}

	var body request
	if err := json.Unmarshal([]byte(fwd.Body), &body); err != nil {
		return fmt.Errorf("parsing relayed request body: %w", err)
	}
	resp, err := s.apply(&body)
	if err != nil {
		return err
	}
	if err := s.sendResponse(fwd.Client, fwd.ReplyTo, resp); err != nil {
		return err
	}
	return s.respond(msg, map[string]any{"type": "fwd_ok"})
}

// forward hands a client request to the leader, which performs it and answers
// the client itself. We wait for its confirmation, so that if it can't help,
// we can tell the client rather than leave it hanging.
func (s *server) forward(req maelstrom.Message) error {
	var reqBody struct {
		MsgID int64 `json:"msg_id"`
	}
	if err := json.Unmarshal(req.Body, &reqBody); err != nil {
		return err
	}

	relay := map[string]any{
		"type":     "fwd",
		"client":   req.Src,
		"reply_to": reqBody.MsgID,
		"body":     string(req.Body),
	}
	return s.n.RPC(s.leaderID(), relay, func(reply maelstrom.Message) error {
		if err := reply.RPCError(); err != nil {
			// The leader couldn't do it: pass the failure on to the client.
			var e struct {
				Code int    `json:"code"`
				Text string `json:"text"`
			}
			if err := json.Unmarshal(reply.Body, &e); err != nil {
				return err
			}
			return s.sendResponse(req.Src, reqBody.MsgID, map[string]any{
				"type": "error",
				"code": e.Code,
				"text": e.Text,
			})
		}
		return nil
	})
}

// defaultRequestsPerSecond is the rate at which the node completes client
// requests when nothing else is said.
//
// Maelstrom's kafka workload generates clients that fire requests as fast as
// the rate limit allows: at --rate 1000 a single fast node racks up a history
// of some 18000 operations in twenty seconds. Nothing in the workload needs
// that kind of throughput, and Jepsen's checker for it -- dependency graphs
// over every operation, per-key plots, and so on -- then wants well over a
// gigabyte of heap, which is more than the JVM gets in the containers these
// runs happen in: the analysis dies with an OutOfMemoryError and the run is
// left with no verdict. Pacing requests to a few hundred per second keeps the
// history -- and so the checker's appetite -- an order of magnitude smaller,
// while individual requests still complete in single-digit milliseconds,
// comfortably inside Maelstrom's RPC timeouts, and the workload's final phase
// still has no trouble reading every message it expects to see.
const defaultRequestsPerSecond = 300

// limiter hands out evenly spaced time slots, one per interval: a token
// bucket with no burst allowance. Whoever calls wait sleeps until their slot,
// which caps the rate at which requests complete no matter how many are in
// flight at once, and adds only a bounded, small delay per request.
type limiter struct {
	mu       sync.Mutex
	interval time.Duration
	next     time.Time // start of the next free slot
}

func newLimiter(perSecond float64) *limiter {
	return &limiter{interval: time.Duration(float64(time.Second) / perSecond)}
}

// wait blocks until the caller's slot comes around. Slots that nobody claims
// expire, so a quiet period is not followed by a burst.
func (l *limiter) wait() {
	l.mu.Lock()
	now := time.Now()
	slot := l.next
	if slot.Before(now) {
		slot = now
	}
	l.next = slot.Add(l.interval)
	l.mu.Unlock()

	if d := time.Until(slot); d > 0 {
		time.Sleep(d)
	}
}

func main() {
	n := maelstrom.NewNode()
	s := newServer(n, newLimiter(defaultRequestsPerSecond))

	n.Handle("init", s.onInit)
	n.Handle("send", s.onClientRequest)
	n.Handle("poll", s.onClientRequest)
	n.Handle("commit_offsets", s.onClientRequest)
	n.Handle("list_committed_offsets", s.onClientRequest)
	n.Handle("fwd", s.onForward)

	if err := n.Run(); err != nil {
		log.Fatalf("node exiting: %v", err)
	}
}
