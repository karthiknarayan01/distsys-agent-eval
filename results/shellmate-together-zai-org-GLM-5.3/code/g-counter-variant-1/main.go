// Museum turnstile tally: a grow-only counter replicated over four turnstiles.
//
// Each turnstile keeps the count of the visitors that came through it and only
// ever adds to that count. A "total" is answered by asking every other
// turnstile for its current count and summing them all up. Turnstiles also
// gossip their own counts to each other, so that when the network is split and
// some turnstile cannot be reached in time, its last known count can be used
// instead: the total is then a little out of date, but it never counts a
// visitor twice and never reports more than has really been counted.
package main

import (
	"encoding/json"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const (
	// How long a total waits for a fresh count from every turnstile before
	// falling back to the last counts it knows.
	readBudget = 1 * time.Second
	// Timeout for a single probe of another turnstile.
	probeTimeout = 200 * time.Millisecond
	// Pause between probes of the same turnstile.
	probeRetry = 20 * time.Millisecond
	// How often we gossip our own count to the other turnstiles.
	gossipEvery = 200 * time.Millisecond
	// How long we'll wait for the cluster to be introduced to us.
	initWait = 10 * time.Second
)

// tally is the state of one turnstile: its own count, plus the most recent
// count it has heard about from every other turnstile.
type tally struct {
	n *maelstrom.Node

	mu    sync.Mutex
	local int64            // visitors counted at this turnstile
	known map[string]int64 // last known count of every other turnstile

	readyOnce sync.Once
	ready     chan struct{} // closed once this node knows the cluster layout
}

func newTally(n *maelstrom.Node) *tally {
	return &tally{n: n, known: map[string]int64{}, ready: make(chan struct{})}
}

// initialized marks the node as knowing its own ID and the cluster members.
func (t *tally) initialized() {
	t.readyOnce.Do(func() { close(t.ready) })
}

// gate blocks until the node has been initialized, so that we never answer a
// question about a cluster we haven't been introduced to yet.
func (t *tally) gate() {
	select {
	case <-t.ready:
	case <-time.After(initWait):
	}
}

// add records visitors coming through this turnstile.
func (t *tally) add(visitors int64) {
	t.mu.Lock()
	t.local += visitors
	t.mu.Unlock()
}

// learn merges a count heard from another turnstile.
func (t *tally) learn(node string, count int64) {
	t.mu.Lock()
	if count > t.known[node] {
		t.known[node] = count
	}
	t.mu.Unlock()
}

// total sums everything we know: our own count plus the freshest count of
// every other turnstile.
func (t *tally) total() int64 {
	t.mu.Lock()
	sum := t.local
	for _, count := range t.known {
		sum += count
	}
	t.mu.Unlock()
	return sum
}

// others lists every other turnstile in the cluster.
func (t *tally) others() []string {
	ids := []string{}
	for _, id := range t.n.NodeIDs() {
		if id != t.n.ID() {
			ids = append(ids, id)
		}
	}
	return ids
}

// probe asks another turnstile for its current count. It reports whether an
// answer arrived.
func (t *tally) probe(node string) bool {
	arrived := make(chan struct{}, 1)
	if err := t.n.RPC(node, map[string]any{"type": "tally_probe"}, func(m maelstrom.Message) error {
		var body struct {
			Count int64 `json:"count"`
		}
		if err := json.Unmarshal(m.Body, &body); err != nil {
			return err
		}
		t.learn(m.Src, body.Count)
		select { // never block: a late reply just gets dropped
		case arrived <- struct{}{}:
		default:
		}
		return nil
	}); err != nil {
		return false
	}

	select {
	case <-arrived:
		return true
	case <-time.After(probeTimeout):
		return false
	}
}

// readTotal gathers as many fresh counts as it can and returns the sum of
// everything we then know.
func (t *tally) readTotal() int64 {
	t.gate()

	nodes := t.others()
	if len(nodes) == 0 {
		return t.total()
	}

	deadline := time.Now().Add(readBudget)
	var missing int64
	atomic.StoreInt64(&missing, int64(len(nodes)))
	complete := make(chan struct{})

	for _, node := range nodes {
		go func(node string) {
			for {
				if t.probe(node) {
					if atomic.AddInt64(&missing, -1) == 0 {
						close(complete)
					}
					return
				}
				if !time.Now().Before(deadline) {
					return
				}
				time.Sleep(probeRetry)
			}
		}(node)
	}

	select {
	case <-complete:
	case <-time.After(time.Until(deadline)):
	}

	return t.total()
}

// gossip periodically tells every other turnstile about our own count, which
// keeps their idea of the tally fresh even while the network is split.
func (t *tally) gossip() {
	for {
		jitter := time.Duration(rand.Int63n(int64(gossipEvery) / 4))
		time.Sleep(gossipEvery + jitter)

		t.mu.Lock()
		count := t.local
		t.mu.Unlock()

		for _, node := range t.others() {
			if err := t.n.Send(node, map[string]any{"type": "tally_push", "count": count}); err != nil {
				log.Printf("gossip to %s failed: %v", node, err)
			}
		}
	}
}

func main() {
	n := maelstrom.NewNode()
	t := newTally(n)

	// The cluster has been introduced; unblock any waiting handler.
	n.Handle("init", func(m maelstrom.Message) error {
		t.initialized()
		return nil
	})

	// Visitors came in through this turnstile.
	n.Handle("count", func(m maelstrom.Message) error {
		t.gate()
		var body struct {
			Visitors int64 `json:"visitors"`
		}
		if err := json.Unmarshal(m.Body, &body); err != nil {
			return err
		}
		// Record the visitors first, so that any acknowledged count is
		// included in every total from then on.
		t.add(body.Visitors)
		return n.Reply(m, map[string]any{"type": "counted"})
	})

	// How many visitors have come in through all the turnstiles?
	n.Handle("total", func(m maelstrom.Message) error {
		return n.Reply(m, map[string]any{
			"type":            "total_is",
			"visitors_so_far": t.readTotal(),
		})
	})

	// Another turnstile asks for our count.
	n.Handle("tally_probe", func(m maelstrom.Message) error {
		t.mu.Lock()
		count := t.local
		t.mu.Unlock()
		return n.Reply(m, map[string]any{"type": "tally_probe_ok", "count": count})
	})

	// Another turnstile volunteers its count.
	n.Handle("tally_push", func(m maelstrom.Message) error {
		var body struct {
			Count int64 `json:"count"`
		}
		if err := json.Unmarshal(m.Body, &body); err != nil {
			return err
		}
		t.learn(m.Src, body.Count)
		return nil
	})

	go t.gossip()

	if err := n.Run(); err != nil {
		log.Fatalf("node %s crashed: %v", n.ID(), err)
	}
}
