package main

import (
	"sync"

	"github.com/jepsen-io/maelstrom/demo/go"
)

// broadcastNode is a Maelstrom node that stores every value broadcast to it
// and reports them back on read requests.
type broadcastNode struct {
	node     *maelstrom.Node
	mu       sync.Mutex
	messages []any // all acknowledged broadcast values
}

func main() {
	node := maelstrom.NewNode()

	// Register an init handler so we can react to node initialization.
	node.Handle("init", func(msg maelstrom.Message) error {
		// The library has already called node.Init for us and replied init_ok.
		return nil
	})

	// Store broadcast values and acknowledge them.
	node.Handle("broadcast", func(msg maelstrom.Message) error {
		var body maelstrom.MessageBody
		if err := msg.Body.Unmarshal(&body); err != nil {
			return err
		}

		var value any
		if err := body.Message.Unmarshal(&value); err != nil {
			return err
		}

		n := &broadcastNode{node: node}
		n.mu.Lock()
		n.messages = append(n.messages, value)
		n.mu.Unlock()

		return node.Reply(msg, maelstrom.MessageBody{Type: "broadcast_ok"})
	})

	// Report all stored values.
	node.Handle("read", func(msg maelstrom.Message) error {
		n := &broadcastNode{node: node}

		n.mu.Lock()
		msgs := make([]any, len(n.messages))
		copy(msgs, n.messages)
		n.mu.Unlock()

		return node.Reply(msg, maelstrom.MessageBody{
			Type:     "read_ok",
			Messages: msgs,
		})
	})

	// Report the cluster topology.
	node.Handle("topology", func(msg maelstrom.Message) error {
		var body maelstrom.MessageBody
		if err := msg.Body.Unmarshal(&body); err != nil {
			return err
		}

		var topo Topology
		if err := body.Topology.Unmarshal(&topo); err != nil {
			return err
		}

		return node.Reply(msg, maelstrom.MessageBody{
			Type:     "topology_ok",
			Topology: topo,
		})
	})

	node.Run()
}

// Topology maps each node id to the list of its neighbour node ids.
type Topology map[string][]string
