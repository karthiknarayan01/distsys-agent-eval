package main

import (
	"encoding/json"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// gossipBody is the payload we use to pass values between nodes.
type gossipBody struct {
	Type     string `json:"type"`
	Messages []int  `json:"messages"`
}

// broadcastBody is a client broadcast request.
type broadcastBody struct {
	Type    string `json:"type"`
	Message int    `json:"message"`
}

func main() {
	n := maelstrom.NewNode()

	var mu sync.Mutex
	seen := make(map[int]struct{})
	// Peers suggested by the topology, used for the eager gossip push. The
	// periodic anti-entropy push goes to the whole cluster regardless, so the
	// values converge even if the suggested topology is sparse/disconnected.
	var peers []string

	// merge records values and returns the ones that were new to us.
	merge := func(vals []int) []int {
		mu.Lock()
		defer mu.Unlock()
		var added []int
		for _, v := range vals {
			if _, ok := seen[v]; !ok {
				seen[v] = struct{}{}
				added = append(added, v)
			}
		}
		return added
	}

	snapshot := func() []int {
		mu.Lock()
		defer mu.Unlock()
		out := make([]int, 0, len(seen))
		for v := range seen {
			out = append(out, v)
		}
		return out
	}

	// peersOrAll returns the topology neighbours, or every other node when no
	// topology has been received (init may not have happened yet at startup).
	peersOrAll := func() []string {
		mu.Lock()
		out := append([]string(nil), peers...)
		mu.Unlock()
		if len(out) > 0 {
			return out
		}
		for _, id := range n.NodeIDs() {
			if id != n.ID() {
				out = append(out, id)
			}
		}
		return out
	}

	allNodes := func() []string {
		var out []string
		for _, id := range n.NodeIDs() {
			if id != n.ID() {
				out = append(out, id)
			}
		}
		return out
	}

	sendTo := func(vals []int, targets []string) {
		if len(vals) == 0 {
			return
		}
		for _, t := range targets {
			if t == n.ID() {
				continue
			}
			n.Send(t, gossipBody{Type: "gossip", Messages: vals})
		}
	}

	// push eagerly spreads new values to the topology neighbours.
	push := func(vals []int) {
		sendTo(vals, peersOrAll())
	}

	n.Handle("gossip", func(msg maelstrom.Message) error {
		var b gossipBody
		if err := json.Unmarshal(msg.Body, &b); err != nil {
			return err
		}
		added := merge(b.Messages)
		go push(added)
		return nil
	})

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var b broadcastBody
		if err := json.Unmarshal(msg.Body, &b); err != nil {
			return err
		}
		added := merge([]int{b.Message})
		if err := n.Reply(msg, map[string]any{"type": "broadcast_ok"}); err != nil {
			return err
		}
		go push(added)
		return nil
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		return n.Reply(msg, map[string]any{
			"type":     "read_ok",
			"messages": snapshot(),
		})
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		var b struct {
			Topology map[string][]string `json:"topology"`
		}
		if err := json.Unmarshal(msg.Body, &b); err != nil {
			return err
		}
		var next []string
		if t, ok := b.Topology[n.ID()]; ok {
			for _, id := range t {
				if id != n.ID() {
					next = append(next, id)
				}
			}
		}
		mu.Lock()
		peers = next
		mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "topology_ok"})
	})

	// Periodic anti-entropy: shove the full set at every node so all replicas
	// converge quickly no matter what the topology looked like.
	go func() {
		for range time.Tick(300 * time.Millisecond) {
			go sendTo(snapshot(), allNodes())
		}
	}()

	if err := n.Run(); err != nil {
		panic(err)
	}
}
