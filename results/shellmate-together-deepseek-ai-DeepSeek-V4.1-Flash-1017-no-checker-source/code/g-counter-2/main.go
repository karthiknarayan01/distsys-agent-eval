package main

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// app keeps a grow-only counter in the usual CRDT form: one cumulative
// counter per origin node.  A node only increments its own entry; entries are
// merged by taking the maximum.  This makes add/replication idempotent, so a
// lost or duplicated message can never cause an add to be counted twice.
type app struct {
	node *maelstrom.Node

	mu     sync.Mutex
	counts map[string]int

	startOnce sync.Once
}

func newApp() *app {
	return &app{
		node:   maelstrom.NewNode(),
		counts: make(map[string]int),
	}
}

func (a *app) merge(state map[string]int) {
	if len(state) == 0 {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	for node, count := range state {
		if count > a.counts[node] {
			a.counts[node] = count
		}
	}
}

func (a *app) snapshot() map[string]int {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make(map[string]int, len(a.counts))
	for node, count := range a.counts {
		out[node] = count
	}
	return out
}

func (a *app) value() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	total := 0
	for _, count := range a.counts {
		total += count
	}
	return total
}

// peerPull requests a peer's complete counter state. The callback is buffered
// so that a late reply after our timeout cannot leave a blocked goroutine.
type pullResult struct {
	peer  string
	state map[string]int
}

func (a *app) peerPull(peer string, timeout time.Duration, out chan<- pullResult) {
	ch := make(chan maelstrom.Message, 1)
	err := a.node.RPC(peer, map[string]any{"type": "state_request"}, func(m maelstrom.Message) error {
		select {
		case ch <- m:
		default:
		}
		return nil
	})
	if err != nil {
		return
	}

	select {
	case m := <-ch:
		var body struct {
			State map[string]int `json:"state"`
		}
		if err := json.Unmarshal(m.Body, &body); err == nil && body.State != nil {
			out <- pullResult{peer: peer, state: body.State}
		}
	case <-time.After(timeout):
	}
}

// readState performs a short pull-based anti-entropy round before answering a
// read. During a partition, unreachable peers simply time out; after the
// network is healed, the round makes the read include every acknowledged add
// from every node, even if the periodic gossip has not arrived yet.
func (a *app) readState() int {
	peers := make([]string, 0, len(a.node.NodeIDs()))
	for _, id := range a.node.NodeIDs() {
		if id != a.node.ID() {
			peers = append(peers, id)
		}
	}

	// Pull once from every peer. If any peer did not answer (for example
	// because a partition was healing), retry the outstanding peers before
	// giving up. After the final heal all peers answer in the first round.
	pending := make(map[string]bool, len(peers))
	for _, p := range peers {
		pending[p] = true
	}

	deadline := time.Now().Add(550 * time.Millisecond)
	for len(pending) > 0 {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			break
		}
		roundTimeout := remaining
		if roundTimeout > 250*time.Millisecond {
			roundTimeout = 250 * time.Millisecond
		}

		out := make(chan pullResult, len(pending))
		for p := range pending {
			go a.peerPull(p, roundTimeout, out)
		}

		roundDeadline := time.Now().Add(roundTimeout + 20*time.Millisecond)
		answered := make(map[string]bool)
		for len(answered) < len(pending) {
			left := time.Until(roundDeadline)
			if left <= 0 {
				break
			}
			select {
			case result := <-out:
				if pending[result.peer] && !answered[result.peer] {
					a.merge(result.state)
					answered[result.peer] = true
				}
			case <-time.After(left):
				left = 0
			}
			if left <= 0 {
				break
			}
		}

		for p := range answered {
			delete(pending, p)
		}
	}

	return a.value()
}

func (a *app) broadcast() {
	state := a.snapshot()
	for _, id := range a.node.NodeIDs() {
		if id == a.node.ID() {
			continue
		}
		_ = a.node.Send(id, map[string]any{
			"type":  "replicate",
			"state": state,
		})
	}
}

func (a *app) startGossip() {
	a.startOnce.Do(func() {
		go func() {
			ticker := time.NewTicker(100 * time.Millisecond)
			defer ticker.Stop()
			for range ticker.C {
				a.broadcast()
			}
		}()
	})
}

func (a *app) handleInit(msg maelstrom.Message) error {
	var body struct {
		NodeID string `json:"node_id"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	a.mu.Lock()
	if _, ok := a.counts[body.NodeID]; !ok {
		a.counts[body.NodeID] = 0
	}
	a.mu.Unlock()

	a.startGossip()
	return nil
}

func (a *app) handleAdd(msg maelstrom.Message) error {
	var body struct {
		Delta int `json:"delta"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	self := a.node.ID()
	a.mu.Lock()
	// Only the origin node updates its entry, and only by adding. Therefore
	// every acknowledged add contributes exactly once to the CRDT.
	a.counts[self] += body.Delta
	a.mu.Unlock()

	if err := a.node.Reply(msg, map[string]any{"type": "add_ok"}); err != nil {
		return err
	}

	// Push immediately as well; periodic gossip handles the rest.
	go a.broadcast()
	return nil
}

func (a *app) handleRead(msg maelstrom.Message) error {
	value := a.readState()
	return a.node.Reply(msg, map[string]any{
		"type":  "read_ok",
		"value": value,
	})
}

func (a *app) handleStateRequest(msg maelstrom.Message) error {
	return a.node.Reply(msg, map[string]any{
		"type":  "state_response",
		"state": a.snapshot(),
	})
}

func (a *app) handleReplicate(msg maelstrom.Message) error {
	var body struct {
		State map[string]int `json:"state"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	a.merge(body.State)
	return nil
}

func (a *app) handleStateResponse(msg maelstrom.Message) error {
	var body struct {
		State map[string]int `json:"state"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	a.merge(body.State)
	return nil
}

func main() {
	a := newApp()

	a.node.Handle("init", a.handleInit)
	a.node.Handle("add", a.handleAdd)
	a.node.Handle("read", a.handleRead)
	a.node.Handle("state_request", a.handleStateRequest)
	a.node.Handle("state_response", a.handleStateResponse)
	a.node.Handle("replicate", a.handleReplicate)

	if err := a.node.Run(); err != nil {
		log.Fatal(err)
	}
}
