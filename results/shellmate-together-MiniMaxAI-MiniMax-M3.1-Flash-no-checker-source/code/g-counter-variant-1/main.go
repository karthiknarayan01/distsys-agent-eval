package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const (
	gossipType     = "museum-g-counter-state"
	gossipInterval = 100 * time.Millisecond
	kvInterval     = 250 * time.Millisecond
	kvTimeout      = 400 * time.Millisecond
)

type countBody struct {
	Visitors int64 `json:"visitors"`
}

type gossipBody struct {
	Type     string           `json:"type"`
	Counters map[string]int64 `json:"counters"`
}

// server holds a grow-only counter.  Each turnstile contributes one
// monotonically increasing component to the counter.  Components are merged
// with max, so exchanging state can never count a visitor twice.
type server struct {
	node *maelstrom.Node
	kv   *maelstrom.KV

	mu         sync.Mutex
	components map[string]int64
	local      int64

	persistNotify chan struct{}
	started       sync.Once
}

func newServer(node *maelstrom.Node) *server {
	return &server{
		node:          node,
		kv:            maelstrom.NewSeqKV(node),
		components:    make(map[string]int64),
		persistNotify: make(chan struct{}, 1),
	}
}

func (s *server) start() {
	s.started.Do(func() {
		go s.gossipLoop()
		go s.persistLoop()
		go s.kvRefreshLoop()
	})
}

// merge adds a remote state to our state.  It returns true if the state grew.
func (s *server) merge(remote map[string]int64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	changed := false
	for id, value := range remote {
		if value < 0 {
			continue
		}
		if value > s.components[id] {
			s.components[id] = value
			changed = true
		}
	}
	return changed
}

func (s *server) snapshot() map[string]int64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	result := make(map[string]int64, len(s.components))
	for id, value := range s.components {
		result[id] = value
	}
	return result
}

func (s *server) localValue() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.local
}

func (s *server) totalLocked() int64 {
	var total int64
	for _, value := range s.components {
		total += value
	}
	return total
}

func (s *server) total() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.totalLocked()
}

func (s *server) handleInit(msg maelstrom.Message) error {
	// The Maelstrom library has already installed our ID and the list of
	// application node IDs before invoking this handler.
	s.start()
	return nil
}

func (s *server) handleCount(msg maelstrom.Message) error {
	var body countBody
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	if body.Visitors < 0 {
		return errors.New("visitors must be non-negative")
	}

	s.mu.Lock()
	s.local += body.Visitors
	if s.local > s.components[s.node.ID()] {
		s.components[s.node.ID()] = s.local
	}
	state := make(map[string]int64, len(s.components))
	for id, value := range s.components {
		state[id] = value
	}
	s.mu.Unlock()

	// Make the increment durable in seq-kv and advertise it to the other
	// turnstiles.  These operations are deliberately independent: either
	// mechanism is enough to recover the count after a partition is healed.
	s.notifyPersist()
	s.broadcast(state)

	return s.node.Reply(msg, map[string]any{"type": "counted"})
}

func (s *server) handleTotal(msg maelstrom.Message) error {
	// Refresh from the shared store before answering.  Reads are allowed to
	// be stale, so we retain the maximum value ever observed for each
	// component.  The in-memory gossip state remains a useful fallback if
	// seq-kv is temporarily unreachable.
	s.refreshFromKV()
	value := s.total()
	return s.node.Reply(msg, map[string]any{
		"type":            "total_is",
		"visitors_so_far": value,
	})
}

func (s *server) handleGossip(msg maelstrom.Message) error {
	var body gossipBody
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	if s.merge(body.Counters) {
		// Forward newly learned state immediately.  This makes recovery after
		// a partition heal fast without relying on the periodic timer.
		s.broadcast(s.snapshot())
	}
	return nil
}

func (s *server) broadcast(state map[string]int64) {
	for _, id := range s.node.NodeIDs() {
		if id == s.node.ID() {
			continue
		}
		if err := s.node.Send(id, gossipBody{Type: gossipType, Counters: state}); err != nil {
			// A send can fail when the process is shutting down.  Gossip is
			// periodic, so there is no need to fail the client request.
			log.Printf("gossip send to %s failed: %v", id, err)
		}
	}
}

func (s *server) notifyPersist() {
	select {
	case s.persistNotify <- struct{}{}:
	default:
	}
}

func (s *server) gossipLoop() {
	ticker := time.NewTicker(gossipInterval)
	defer ticker.Stop()
	for range ticker.C {
		s.broadcast(s.snapshot())
	}
}

// persistLoop stores this turnstile's cumulative count in seq-kv.  A CAS is
// used rather than a blind write so that a delayed write can never replace a
// newer value with an older one.
func (s *server) persistLoop() {
	key := "museum:g-counter:" + s.node.ID()
	for range s.persistNotify {
		for {
			target := s.localValue()
			ctx, cancel := context.WithTimeout(context.Background(), kvTimeout)
			err := s.storeAtLeast(ctx, key, target)
			cancel()
			if err == nil {
				if s.localValue() == target {
					break
				}
				// More increments arrived while the write was in flight.
				continue
			}

			// The store may be unreachable during a partition.  Retry later;
			// the local count is still safe and gossip carries it as well.
			time.Sleep(100 * time.Millisecond)
		}
	}
}

// storeAtLeast atomically changes key to at least target.
func (s *server) storeAtLeast(ctx context.Context, key string, target int64) error {
	current, err := s.kv.ReadInt(ctx, key)
	if err == nil {
		if int64(current) >= target {
			return nil
		}
		err = s.kv.CompareAndSwap(ctx, key, current, target, false)
	} else if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
		err = s.kv.CompareAndSwap(ctx, key, nil, target, true)
	} else {
		return err
	}

	if err == nil {
		return nil
	}
	if maelstrom.ErrorCode(err) == maelstrom.PreconditionFailed {
		// Another writer installed a newer value (or our read was stale).
		// Read again and try once more.
		current, readErr := s.kv.ReadInt(ctx, key)
		if readErr == nil && int64(current) >= target {
			return nil
		}
	}
	return err
}

// kvRefreshLoop supplements gossip with values stored in seq-kv.
func (s *server) kvRefreshLoop() {
	ticker := time.NewTicker(kvInterval)
	defer ticker.Stop()
	for range ticker.C {
		s.refreshFromKV()
	}
}

func (s *server) refreshFromKV() {
	ids := s.node.NodeIDs()
	if len(ids) == 0 {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), kvTimeout)
	defer cancel()

	var wg sync.WaitGroup
	for _, id := range ids {
		id := id
		wg.Add(1)
		go func() {
			defer wg.Done()
			value, err := s.kv.ReadInt(ctx, "museum:g-counter:"+id)
			if err == nil && value >= 0 {
				s.mergeOne(id, int64(value))
			}
		}()
	}
	wg.Wait()
}

func (s *server) mergeOne(id string, value int64) {
	s.mu.Lock()
	if value > s.components[id] {
		s.components[id] = value
	}
	s.mu.Unlock()
}

func main() {
	node := maelstrom.NewNode()
	s := newServer(node)

	node.Handle("init", s.handleInit)
	node.Handle("count", s.handleCount)
	node.Handle("total", s.handleTotal)
	node.Handle(gossipType, s.handleGossip)

	if err := node.Run(); err != nil {
		log.Fatalf("node failed: %v", err)
	}
}
