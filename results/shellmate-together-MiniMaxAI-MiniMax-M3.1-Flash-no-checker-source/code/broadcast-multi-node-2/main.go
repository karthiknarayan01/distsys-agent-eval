// A gossip-based broadcast node for Maelstrom.
//
// Every value handed to a node via a "broadcast" request is stored locally and
// then disseminated to the rest of the cluster:
//
//   - immediately, by forwarding the value to every peer we know about;
//   - lazily, by a periodic anti-entropy round in which each node sends a
//     randomly chosen peer the values it is not known to have yet.
//
// The combination makes propagation fast (a couple of network hops) while
// still guaranteeing eventual delivery even if individual messages are lost.
//
// The wire format between nodes is a single message type:
//
//	{"type": "gossip", "values": [<value>, ...]}
package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"log"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const (
	// gossipType is the message type used for node-to-node traffic.
	gossipType = "gossip"

	// gossipInterval is how often anti-entropy runs.
	gossipInterval = 200 * time.Millisecond

	// fullSyncEvery is how many anti-entropy rounds pass between full
	// (re)synchronisations with a random peer. Full rounds recover values
	// whose individual gossip messages were lost.
	fullSyncEvery = 10
)

// value is a broadcast payload: a canonical key used for de-duplication and
// the canonical JSON encoding of the payload itself.
type value struct {
	key string
	raw json.RawMessage
}

type server struct {
	n *maelstrom.Node

	mu sync.Mutex
	// values maps a canonical key to the canonical JSON encoding of the
	// value, so that duplicates are stored only once.
	values map[string]json.RawMessage
	// peerSent tracks, per peer, the values we believe that peer already
	// has. It is only a hint used to keep anti-entropy messages small.
	peerSent map[string]map[string]bool
	// peers are the other nodes of the cluster.
	peers []string
}

func main() {
	log.SetOutput(os.Stderr)
	log.SetPrefix("")

	n := maelstrom.NewNode()
	s := &server{
		n:        n,
		values:   make(map[string]json.RawMessage),
		peerSent: make(map[string]map[string]bool),
	}
	if err := s.run(); err != nil {
		log.Printf("fatal: %v", err)
		os.Exit(1)
	}
}

// run reads messages from stdin until EOF, dispatching each one.
func (s *server) run() error {
	scanner := bufio.NewScanner(s.n.Stdin)
	scanner.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		var msg maelstrom.Message
		if err := json.Unmarshal(line, &msg); err != nil {
			log.Printf("unmarshal message: %v", err)
			continue
		}
		s.handle(msg)
	}
	return scanner.Err()
}

func (s *server) handle(msg maelstrom.Message) {
	switch msg.Type() {
	case "init":
		s.handleInit(msg)
	case "topology":
		// The topology is only a hint; we talk to every node we know of.
		s.reply(msg, "topology_ok", nil)
	case "broadcast":
		s.handleBroadcast(msg)
	case "read":
		s.handleRead(msg)
	case gossipType:
		s.handleGossip(msg)
	default:
		log.Printf("ignoring message of unknown type %q from %s", msg.Type(), msg.Src)
	}
}

func (s *server) handleInit(msg maelstrom.Message) {
	var body maelstrom.InitMessageBody
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		log.Printf("unmarshal init body: %v", err)
		return
	}
	s.n.Init(body.NodeID, body.NodeIDs)

	peers := make([]string, 0, len(body.NodeIDs))
	for _, id := range body.NodeIDs {
		if id != body.NodeID {
			peers = append(peers, id)
		}
	}

	s.mu.Lock()
	s.peers = peers
	s.mu.Unlock()

	go s.gossipLoop()

	s.reply(msg, "init_ok", nil)
}

func (s *server) handleBroadcast(msg maelstrom.Message) {
	var body struct {
		maelstrom.MessageBody
		Message json.RawMessage `json:"message"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		log.Printf("unmarshal broadcast body: %v", err)
		s.reply(msg, "broadcast_ok", nil)
		return
	}
	if len(body.Message) == 0 {
		s.reply(msg, "broadcast_ok", nil)
		return
	}

	v, fresh := s.add(body.Message)
	if fresh {
		// Push the value out to every peer right away.
		s.sendToPeers([]value{v}, "")
	} else {
		// We already have it; make sure our peers have it too.
		s.sendMissingToPeers([]value{v})
	}

	s.reply(msg, "broadcast_ok", nil)
}

func (s *server) handleRead(msg maelstrom.Message) {
	s.mu.Lock()
	msgs := make([]json.RawMessage, 0, len(s.values))
	for _, raw := range s.values {
		msgs = append(msgs, raw)
	}
	s.mu.Unlock()

	sort.Slice(msgs, func(i, j int) bool {
		return bytes.Compare(msgs[i], msgs[j]) < 0
	})

	s.reply(msg, "read_ok", map[string]any{"messages": msgs})
}

// handleGossip merges values received from another node.
func (s *server) handleGossip(msg maelstrom.Message) {
	var body struct {
		maelstrom.MessageBody
		Values []json.RawMessage `json:"values"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		log.Printf("unmarshal gossip body: %v", err)
		return
	}
	if len(body.Values) == 0 {
		return
	}

	var fresh []value
	keys := make([]string, 0, len(body.Values))
	for _, raw := range body.Values {
		v, isNew := s.add(raw)
		if isNew {
			fresh = append(fresh, v)
		}
		keys = append(keys, v.key)
	}
	// The sender obviously has these values.
	s.markKnown(msg.Src, keys)

	if len(fresh) > 0 {
		s.sendToPeers(fresh, msg.Src)
	}
}

// add stores a value, returning it and whether it was new.
func (s *server) add(raw json.RawMessage) (value, bool) {
	key, canon, err := canonicalize(raw)
	if err != nil {
		log.Printf("canonicalize value: %v", err)
		return value{}, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.values[key]; exists {
		return value{key: key, raw: canon}, false
	}
	s.values[key] = canon
	return value{key: key, raw: canon}, true
}

// markKnown records that a peer is believed to hold the given values.
func (s *server) markKnown(peer string, keys []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	known := s.peerSent[peer]
	if known == nil {
		known = make(map[string]bool)
		s.peerSent[peer] = known
	}
	for _, k := range keys {
		if k != "" {
			known[k] = true
		}
	}
}

// sendToPeers gossips the given values to every peer except "except".
func (s *server) sendToPeers(values []value, except string) {
	s.mu.Lock()
	peers := make([]string, 0, len(s.peers))
	for _, p := range s.peers {
		if p != except {
			peers = append(peers, p)
		}
	}
	s.mu.Unlock()

	for _, p := range peers {
		s.sendGossip(p, values)
	}
}

// sendMissingToPeers gossips the given values only to the peers we believe
// are missing them.
func (s *server) sendMissingToPeers(values []value) {
	s.mu.Lock()
	var targets []string
	for _, peer := range s.peers {
		known := s.peerSent[peer]
		for _, v := range values {
			if !known[v.key] {
				targets = append(targets, peer)
				break
			}
		}
	}
	s.mu.Unlock()

	for _, peer := range targets {
		s.sendGossip(peer, values)
	}
}

// sendGossip sends values to a single peer and records that the peer has them.
func (s *server) sendGossip(peer string, values []value) {
	if len(values) == 0 {
		return
	}
	raws := make([]json.RawMessage, 0, len(values))
	keys := make([]string, 0, len(values))
	for _, v := range values {
		raws = append(raws, v.raw)
		keys = append(keys, v.key)
	}

	body := map[string]any{
		"type":   gossipType,
		"values": raws,
	}
	if err := s.n.Send(peer, body); err != nil {
		log.Printf("send gossip to %s: %v", peer, err)
		return
	}
	s.markKnown(peer, keys)
}

// gossipLoop periodically reconciles our state with a random peer.
func (s *server) gossipLoop() {
	ticker := time.NewTicker(gossipInterval)
	defer ticker.Stop()
	round := 0
	for range ticker.C {
		round++
		s.antiEntropy(round%fullSyncEvery == 0)
	}
}

// antiEntropy sends a peer the values we believe it does not have yet. On
// full rounds every known value is (re)sent, which repairs losses.
func (s *server) antiEntropy(full bool) {
	s.mu.Lock()
	if len(s.peers) == 0 {
		s.mu.Unlock()
		return
	}
	peer := s.peers[rand.Intn(len(s.peers))]
	known := s.peerSent[peer]

	var values []value
	if full {
		values = make([]value, 0, len(s.values))
		for key, raw := range s.values {
			values = append(values, value{key: key, raw: raw})
		}
	} else {
		for key, raw := range s.values {
			if !known[key] {
				values = append(values, value{key: key, raw: raw})
			}
		}
	}
	s.mu.Unlock()

	if len(values) == 0 {
		return
	}
	s.sendGossip(peer, values)
}

// reply sends a response body back to the requester.
func (s *server) reply(msg maelstrom.Message, typ string, extra map[string]any) {
	body := map[string]any{"type": typ}
	for k, v := range extra {
		body[k] = v
	}
	if err := s.n.Reply(msg, body); err != nil {
		log.Printf("reply %s: %v", typ, err)
	}
}

// canonicalize returns a stable key for a JSON value along with a canonical
// JSON encoding of that value.
func canonicalize(raw json.RawMessage) (string, json.RawMessage, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return "", nil, err
	}

	switch x := v.(type) {
	case json.Number:
		if i, err := strconv.ParseInt(x.String(), 10, 64); err == nil {
			s := strconv.FormatInt(i, 10)
			return s, json.RawMessage(s), nil
		}
		if f, err := strconv.ParseFloat(x.String(), 64); err == nil {
			s := strconv.FormatFloat(f, 'g', -1, 64)
			return s, json.RawMessage(s), nil
		}
		return x.String(), json.RawMessage(x.String()), nil
	default:
		b, err := json.Marshal(v)
		if err != nil {
			return "", nil, err
		}
		return string(b), json.RawMessage(b), nil
	}
}
