// A grow-only counter node for Maelstrom.
//
// Design: a classic G-Counter. Every node owns one component of the counter
// and is the only node that ever increases that component, so components are
// monotone and can be merged with max() without ever counting an add twice.
// The counter's value is the sum of all components.
//
// An add is applied to the local component and acknowledged immediately; the
// effect is then spread by three independent propagation paths:
//
//  1. On every add the node pushes its whole state to its peers.
//  2. An anti-entropy loop pushes the whole state to every peer every 100ms,
//     so a component survives any number of lost messages and partitions.
//  3. A backup loop mirrors every component into Maelstrom's seq-kv service
//     (one key per node, written only by its owner) and merges what it reads
//     back. Since seq-kv is sequentially consistent and only the owner writes
//     a key, a stale read can only return a value that is too small, which is
//     harmless under a max-merge.
//
// A read answers with the sum of the components the node knows; it first does
// a best-effort pull round from its peers so the answer is as fresh as the
// network allows. Maelstrom checks only the final reads, which happen after
// the partitions are healed and ten seconds of recovery time, by which time
// the anti-entropy loop has made every node's state complete.
package main

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const (
	// Per-node component keys in the shared seq-kv store.
	kvPrefix = "g-counter/"

	// Anti-entropy: push our full state to every peer this often.
	gossipEvery = 100 * time.Millisecond

	// Backup: mirror components through seq-kv this often.
	kvEvery = 250 * time.Millisecond

	// Timeout for a single seq-kv RPC.
	kvTimeout = 2 * time.Second

	// How long a read waits for its pull round before answering anyway.
	syncWaitMax = 50 * time.Millisecond
)

// syncWait tracks the replies outstanding for one pull round.
type syncWait struct {
	remaining int
	done      chan struct{}
}

type gcounter struct {
	n  *maelstrom.Node
	kv *maelstrom.KV

	mu        sync.Mutex
	counts    map[string]int64 // node id -> that node's component
	known     map[string]bool  // valid node ids
	kvWritten int64            // our own component as last written to seq-kv
	pending   map[int64]*syncWait
	nextNonce int64
}

func main() {
	n := maelstrom.NewNode()
	g := &gcounter{
		n:       n,
		kv:      maelstrom.NewSeqKV(n),
		counts:  make(map[string]int64),
		known:   make(map[string]bool),
		pending: make(map[int64]*syncWait),
	}

	// The library initializes n.ID()/n.NodeIDs() and sends init_ok after our
	// handler returns; this is where we can bootstrap our state.
	n.Handle("init", func(msg maelstrom.Message) error {
		g.start()
		return nil
	})

	n.Handle("add", func(msg maelstrom.Message) error {
		var body struct {
			Delta int64 `json:"delta"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			log.Printf("add: bad body: %v", err)
			return nil // don't kill the node over a malformed request
		}
		g.add(body.Delta)
		// Push the new state out, then acknowledge.
		g.push()
		return n.Reply(msg, map[string]any{"type": "add_ok"})
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		// Best-effort pull so the answer is as fresh as we can make it.
		g.pullPeers()
		return n.Reply(msg, map[string]any{"type": "read_ok", "value": g.sum()})
	})

	// Unsolicited state push from a peer.
	n.Handle("gossip", func(msg maelstrom.Message) error {
		var body struct {
			Counts map[string]int64 `json:"counts"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			log.Printf("gossip: bad body: %v", err)
			return nil
		}
		g.merge(body.Counts)
		return nil
	})

	// A pull request from a peer: merge what it sent, then answer with our
	// own state.
	n.Handle("sync", func(msg maelstrom.Message) error {
		var body struct {
			Nonce  int64            `json:"nonce"`
			Counts map[string]int64 `json:"counts"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			log.Printf("sync: bad body: %v", err)
			return nil
		}
		g.merge(body.Counts)
		return n.Send(msg.Src, map[string]any{
			"type":   "sync_ok",
			"nonce":  body.Nonce,
			"counts": g.snapshot(),
		})
	})

	// The answer to a pull request.
	n.Handle("sync_ok", func(msg maelstrom.Message) error {
		var body struct {
			Nonce  int64            `json:"nonce"`
			Counts map[string]int64 `json:"counts"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			log.Printf("sync_ok: bad body: %v", err)
			return nil
		}
		g.merge(body.Counts)
		g.arrive(body.Nonce)
		return nil
	})

	// Swallow RPC error messages so a peer's error can never kill this node.
	n.Handle("error", func(msg maelstrom.Message) error {
		log.Printf("ignoring error message: %s", msg.Body)
		return nil
	})

	if err := n.Run(); err != nil {
		log.Printf("run: %v", err)
	}
}

// ---------------------------------------------------------------------------
// state

// start bootstraps our state and launches the background loops.
func (g *gcounter) start() {
	g.mu.Lock()
	for _, id := range g.n.NodeIDs() {
		g.known[id] = true
		if _, ok := g.counts[id]; !ok {
			g.counts[id] = 0
		}
	}
	g.mu.Unlock()

	go g.watchdog(g.gossipLoop, "gossip")
	go g.watchdog(g.kvLoop, "seq-kv")
}

// watchdog runs a loop and recovers if it ever panics, so that no background
// failure can take the whole node down.
func (g *gcounter) watchdog(loop func(), name string) {
	for {
		func() {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("%s loop panicked: %v", name, r)
				}
			}()
			loop()
		}()
		log.Printf("%s loop exited; restarting", name)
		time.Sleep(time.Second)
	}
}

// add applies a delta to our own component.
func (g *gcounter) add(delta int64) {
	g.mu.Lock()
	id := g.n.ID()
	g.counts[id] += delta
	g.mu.Unlock()
}

// merge folds a peer's state into ours. Components are monotone, so max is
// the correct merge and can never count an add twice.
func (g *gcounter) merge(counts map[string]int64) {
	if len(counts) == 0 {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	for id, v := range counts {
		if !g.known[id] {
			continue
		}
		if v > g.counts[id] {
			g.counts[id] = v
		}
	}
}

// mergeOne merges a single component.
func (g *gcounter) mergeOne(id string, v int64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.known[id] {
		return
	}
	if v > g.counts[id] {
		g.counts[id] = v
	}
}

// snapshot returns a copy of our state.
func (g *gcounter) snapshot() map[string]int64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	out := make(map[string]int64, len(g.counts))
	for k, v := range g.counts {
		out[k] = v
	}
	return out
}

// sum returns the counter's value as we know it.
func (g *gcounter) sum() int64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	var total int64
	for _, v := range g.counts {
		total += v
	}
	return total
}

// peers returns the ids of the other nodes in the cluster.
func (g *gcounter) peers() []string {
	me := g.n.ID()
	var out []string
	for _, id := range g.n.NodeIDs() {
		if id != me {
			out = append(out, id)
		}
	}
	return out
}

// ---------------------------------------------------------------------------
// propagation

// push sends our whole state to every peer, best effort.
func (g *gcounter) push() {
	ps := g.peers()
	if len(ps) == 0 {
		return
	}
	counts := g.snapshot()
	body := map[string]any{"type": "gossip", "counts": counts}
	for _, p := range ps {
		if err := g.n.Send(p, body); err != nil {
			log.Printf("gossip to %s failed: %v", p, err)
		}
	}
}

// pullPeers asks every peer for its state and waits, for at most syncWaitMax,
// until they all answer. Used to freshen a read.
func (g *gcounter) pullPeers() {
	ps := g.peers()
	if len(ps) == 0 {
		return
	}
	counts := g.snapshot()

	g.mu.Lock()
	nonce := g.nextNonce
	g.nextNonce++
	w := &syncWait{remaining: len(ps), done: make(chan struct{})}
	g.pending[nonce] = w
	g.mu.Unlock()

	for _, p := range ps {
		if err := g.n.Send(p, map[string]any{
			"type":   "sync",
			"nonce":  nonce,
			"counts": counts,
		}); err != nil {
			log.Printf("sync to %s failed: %v", p, err)
			g.arrive(nonce) // that reply is never coming
		}
	}

	select {
	case <-w.done:
	case <-time.After(syncWaitMax):
	}

	g.mu.Lock()
	delete(g.pending, nonce)
	g.mu.Unlock()
}

// arrive records one reply to a pull round.
func (g *gcounter) arrive(nonce int64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	w, ok := g.pending[nonce]
	if !ok {
		return
	}
	w.remaining--
	if w.remaining <= 0 {
		delete(g.pending, nonce)
		close(w.done)
	}
}

// gossipLoop is the anti-entropy loop.
func (g *gcounter) gossipLoop() {
	t := time.NewTicker(gossipEvery)
	defer t.Stop()
	for range t.C {
		g.push()
	}
}

// kvLoop mirrors components through the shared seq-kv service.
func (g *gcounter) kvLoop() {
	t := time.NewTicker(kvEvery)
	defer t.Stop()
	for range t.C {
		g.kvSync()
	}
}

func (g *gcounter) kvSync() {
	ctx, cancel := context.WithTimeout(context.Background(), kvTimeout)
	defer cancel()

	// Fold in every other node's published component. A read may be stale,
	// but only the owner ever writes the key, so a stale value can only be
	// too small - never too large.
	for _, id := range g.n.NodeIDs() {
		if id == g.n.ID() {
			continue
		}
		v, err := g.kv.ReadInt(ctx, kvPrefix+id)
		if err != nil {
			continue // key doesn't exist yet, or the RPC failed
		}
		g.mergeOne(id, int64(v))
	}

	// Publish our own component if it changed since the last successful write.
	g.mu.Lock()
	mine := g.counts[g.n.ID()]
	published := g.kvWritten
	g.mu.Unlock()
	if mine == published {
		return
	}
	if err := g.kv.Write(ctx, kvPrefix+g.n.ID(), mine); err != nil {
		return // try again on the next tick
	}
	g.mu.Lock()
	if mine > g.kvWritten {
		g.kvWritten = mine
	}
	g.mu.Unlock()
}
