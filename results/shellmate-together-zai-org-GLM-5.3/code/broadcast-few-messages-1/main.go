// Maelstrom broadcast workload node.
//
// Strategy: keep the number of messages between nodes as small as possible
// while still spreading values quickly.
//
// All nodes agree on one "hub" node (the lexicographically smallest node id).
// The hub is connected to every other node, and every other node is connected
// only to the hub, i.e. a star.  A value therefore needs at most two hops
// (node -> hub -> nodes) to reach everybody.
//
//   - When a client gives a node a value, the node stores it and acks
//     immediately.  If it isn't the hub, it forwards the value to the hub right
//     away: one message.
//   - The hub collects values (its own and those of other nodes) and every
//     flushInterval sends everything it hasn't sent yet to every other node,
//     batched into a single message per node.
//   - Receivers de-duplicate by value, so nothing ever travels in circles.
//   - As a safety net against anything lost or missed, every node also
//     periodically hands its whole set to its neighbours (anti-entropy).
//
// With 25 nodes that's at most 24 messages per flush interval plus one message
// per client broadcast: a few messages per client operation, an order of
// magnitude below the budget, and values are everywhere within ~250 ms.
package main

import (
	"encoding/json"
	"log"
	"sort"
	"strconv"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const (
	// How often the hub pushes accumulated values down to every other node.
	flushInterval = 50 * time.Millisecond

	// How often nodes hand their whole set to their neighbours, as a safety
	// net against anything that was lost or missed.
	syncInterval = 1500 * time.Millisecond

	// Message type used between nodes.
	gossipType = "gossip"
)

// server holds the state of one node. Values are kept by their raw JSON, so
// that any kind of value can be handled, and so that equal values deduplicate.
type server struct {
	n *maelstrom.Node

	root   string          // the hub
	isRoot bool            // are we the hub?
	others []string        // every node but us
	nbrs   []string        // our gossip neighbours

	mu      sync.Mutex
	values  map[string]json.RawMessage // everything we know
	pending map[string]json.RawMessage // known but not pushed down yet (hub only)
}

func main() {
	log.SetFlags(log.Ltime | log.Lmicroseconds)

	n := maelstrom.NewNode()
	s := &server{
		n:       n,
		values:  map[string]json.RawMessage{},
		pending: map[string]json.RawMessage{},
	}

	n.Handle("init", s.handleInit)
	n.Handle("topology", s.handleTopology)
	n.Handle("broadcast", s.handleBroadcast)
	n.Handle("read", s.handleRead)
	n.Handle(gossipType, s.handleGossip)

	if err := n.Run(); err != nil {
		log.Fatalf("node %s exiting: %v", n.ID(), err)
	}
}

// ---- topology ---------------------------------------------------------

func (s *server) handleInit(msg maelstrom.Message) error {
	ids := append([]string(nil), s.n.NodeIDs()...)
	sort.Strings(ids)

	for _, id := range ids {
		if id != s.n.ID() {
			s.others = append(s.others, id)
		}
	}
	s.root = ids[0]
	s.isRoot = s.root == s.n.ID()
	if s.isRoot {
		// The hub talks to everyone.
		s.nbrs = s.others
	} else {
		// Everyone else talks only to the hub.
		s.nbrs = []string{s.root}
	}
	log.Printf("init: node %v, hub %v, neighbours %v", s.n.ID(), s.root, s.nbrs)

	go s.loop()
	return nil
}

// The suggested topology is ignored; we arrange our own star.
func (s *server) handleTopology(msg maelstrom.Message) error {
	return s.n.Reply(msg, map[string]string{"type": "topology_ok"})
}

// ---- client requests --------------------------------------------------

func (s *server) handleBroadcast(msg maelstrom.Message) error {
	var body struct {
		Message json.RawMessage `json:"message"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	fresh := s.add(body.Message)

	// Ack straight away: the value is safely stored locally.
	if err := s.n.Reply(msg, map[string]string{"type": "broadcast_ok"}); err != nil {
		return err
	}

	// Pass it to the hub immediately, so it can spread it without delay.
	if fresh && !s.isRoot {
		s.send(s.root, []json.RawMessage{body.Message})
	}
	return nil
}

func (s *server) handleRead(msg maelstrom.Message) error {
	vals := s.snapshot()
	return s.n.Reply(msg, map[string]interface{}{
		"type":     "read_ok",
		"messages": vals,
	})
}

// ---- gossip -----------------------------------------------------------

func (s *server) handleGossip(msg maelstrom.Message) error {
	var body struct {
		Messages []json.RawMessage `json:"messages"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	fresh := s.addMany(body.Messages)

	// We only ever expect gossip from the hub; if somebody else sends us
	// values, pass them on to the hub so that they still spread.
	if !s.isRoot && len(fresh) > 0 && msg.Src != s.root {
		s.send(s.root, fresh)
	}
	return nil
}

// add records a single value. Reports whether it was new to us.
func (s *server) add(v json.RawMessage) bool {
	if len(v) == 0 {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.addLocked(v)
}

func (s *server) addLocked(v json.RawMessage) bool {
	key := string(v)
	if _, ok := s.values[key]; ok {
		return false
	}
	s.values[key] = v
	if s.isRoot {
		s.pending[key] = v
	}
	return true
}

// addMany records a batch of values and returns the ones that were new.
func (s *server) addMany(vs []json.RawMessage) (fresh []json.RawMessage) {
	s.mu.Lock()
	for _, v := range vs {
		if len(v) == 0 {
			continue
		}
		if s.addLocked(v) {
			fresh = append(fresh, v)
		}
	}
	s.mu.Unlock()
	return fresh
}

// snapshot returns everything we know, in a stable order.
func (s *server) snapshot() []json.RawMessage {
	s.mu.Lock()
	vals := make([]json.RawMessage, 0, len(s.values))
	for _, v := range s.values {
		vals = append(vals, v)
	}
	s.mu.Unlock()
	sortValues(vals)
	return vals
}

// loop periodically pushes values out.
func (s *server) loop() {
	flush := time.NewTicker(flushInterval)
	defer flush.Stop()
	sync := time.NewTicker(syncInterval)
	defer sync.Stop()

	for {
		select {
		case <-flush.C:
			if s.isRoot {
				s.flush()
			}

		case <-sync.C:
			s.antiEntropy()
		}
	}
}

// flush sends everything the hub hasn't sent yet to every other node.
func (s *server) flush() {
	s.mu.Lock()
	if len(s.pending) == 0 {
		s.mu.Unlock()
		return
	}
	vals := make([]json.RawMessage, 0, len(s.pending))
	for _, v := range s.pending {
		vals = append(vals, v)
	}
	s.pending = make(map[string]json.RawMessage, len(vals))
	s.mu.Unlock()

	sortValues(vals)
	for _, dest := range s.nbrs {
		s.send(dest, vals)
	}
}

// antiEntropy hands our whole set to our neighbours, so that anything we (or
// they) might have missed is healed.
func (s *server) antiEntropy() {
	vals := s.snapshot()
	if len(vals) == 0 {
		return
	}
	for _, dest := range s.nbrs {
		s.send(dest, vals)
	}
}

// send writes one gossip message. Failures are only logged: the network is
// reliable, and the periodic anti-entropy covers us regardless.
func (s *server) send(dest string, vals []json.RawMessage) {
	if len(vals) == 0 || dest == "" || dest == s.n.ID() {
		return
	}
	body := map[string]interface{}{
		"type":     gossipType,
		"messages": vals,
	}
	if err := s.n.Send(dest, body); err != nil {
		log.Printf("send to %s: %v", dest, err)
	}
}

// ---- helpers ----------------------------------------------------------

// sortValues orders values numerically when they are numbers, so that read
// replies (and our log lines) are predictable.
func sortValues(vals []json.RawMessage) {
	sort.Slice(vals, func(i, j int) bool { return lessValue(vals[i], vals[j]) })
}

func lessValue(a, b json.RawMessage) bool {
	af, aerr := strconv.ParseFloat(string(a), 64)
	bf, berr := strconv.ParseFloat(string(b), 64)
	if aerr == nil && berr == nil {
		return af < bf
	}
	return string(a) < string(b)
}
