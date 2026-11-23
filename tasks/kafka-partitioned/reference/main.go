// Command maelstrom-kafka implements the Kafka-style log across three nodes, with no shared
// store and a network that splits.
//
// The approach:
//
//   - Every log is owned by one node, picked deterministically by hashing its name over the
//     sorted node ids (ownership.go). All log writes happen on the owner, so offsets are dense
//     and grow monotonically, and a send acks from memory alone.
//   - The four message handlers (handlers.go) follow one shape: logs we own are answered
//     locally, anything else is relayed to its owner and that owner's answer comes back to the
//     client.
//   - A relay that times out is retried, and a split can lose the owner's answer after the owner
//     has already appended. So every relayed send carries a request id, and the owner answers a
//     repeat with the offset it gave the first time instead of appending again (log.go).
package main

import (
	"log"
	"os"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

var (
	node *maelstrom.Node
)

// One relay call gets this long to answer, then the caller tries again.
const rpcTimeout = 300 * time.Millisecond

// retryPause waits after a failed attempt so retries do not hammer the
// peer.
const retryPause = 10 * time.Millisecond

// giveUpAfter bounds how long a relay keeps retrying before the client
// is left without an answer.
const giveUpAfter = time.Second

func main() {
	node = maelstrom.NewNode()

	node.Handle("send", handleSend)
	node.Handle("poll", handlePoll)
	node.Handle("commit_offsets", handleCommitOffsets)
	node.Handle("list_committed_offsets", handleListCommittedOffsets)

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}
	os.Exit(0)
}
