package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	node "github.com/jepsen-io/maelstrom/demo/go"
)

// nodeCounter maintains a per-node atomic counter for ticket generation
type nodeCounter struct {
	counter int64
}

func newNodeCounter() *nodeCounter {
	return &nodeCounter{counter: 0}
}

// init handles the init message from maelstrom and initializes the counter
func (n *nodeCounter) Init(id string, nodeIDs []string) error {
	// Counter is atomic - each node has its own memory space
	// This ensures uniqueness per node, even during network partitions
	return nil
}

// handle processes issue requests and returns replies
func (n *nodeCounter) Handle(msg node.Message) error {
	var body node.MessageBody
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		fmt.Fprintf(os.Stderr, "Error unmarshaling body: %v\n", err)
		return fmt.Errorf("bad request")
	}

	bodyType := body.Type
	
	switch bodyType {
	case "issue":
		// Atomically increment the counter to get unique ticket numbers
		n.counter = atomic.AddInt64(&n.counter, 1)
		
		fields := msg.Body.(map[string]interface{})["fields"].(map[string]interface{})
		ticket := fmt.Sprintf("%v-%d", fields["id"], n.counter)
		
		replyBody := map[string]interface{}{
			"type":         "issued",
			"in_reply_to":  int64(msg.Body.(map[string]interface{})["msg_id"].(float64)),
			"ticket":       ticket,
		}
		
		rawReply, err := json.Marshal(replyBody)
		if err != nil {
			return fmt.Errorf("failed to marshal reply: %w", err)
		}
		
		fmt.Fprintf(os.Stderr, "Issued ticket: %s for msg_id=%d\n", ticket, msg.Body.(map[string]interface{})["msg_id"])
		return nil
		
	case "init":
		// Re-register our handlers if needed
		n.Handle("issue", n.Handle)
		return node.Reply{body: map[string]interface{}{"type": "init_ok"}}
		
	default:
		return nil // Ignore unknown message types
	}
}

func main() {
	counter := newNodeCounter()
	
	// Use the Init method from the node library which will receive init messages
	n := node.NewNode().
		Init("init", counter.Init).
		Handle("issue", counter.Handle)
	
	if err := n.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "Run failed: %v\n", err)
		os.Exit(1)
	}
}
