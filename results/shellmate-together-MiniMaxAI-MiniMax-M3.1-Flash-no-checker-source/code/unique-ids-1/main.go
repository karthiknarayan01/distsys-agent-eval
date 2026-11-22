// Unique ID generator for Maelstrom.
//
// Each node in the cluster owns a private namespace derived from the node ID
// it receives in the "init" message, plus a random per-process nonce and a
// monotonically increasing local counter. Since node IDs are unique within a
// cluster and a node's counter never repeats, every ID handed out is unique
// across the entire cluster for the entire run.
//
// Crucially, IDs are minted locally: a node never needs to talk to another
// node to hand out an ID, so every request is answered even while the network
// is partitioned.
package main

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// limitedWriter keeps log output bounded: the Maelstrom library logs every
// single message it sends and receives, which would otherwise produce tens of
// thousands of lines per run. We keep the first few lines (useful when
// debugging) and then only emit an occasional progress note.
type limitedWriter struct {
	mu      sync.Mutex
	w       io.Writer
	writes  int
	keep    int
	dropped int
}

func newLimitedWriter(w io.Writer, keep int) *limitedWriter {
	return &limitedWriter{w: w, keep: keep}
}

func (l *limitedWriter) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.writes++
	if l.writes <= l.keep {
		return l.w.Write(p)
	}
	l.dropped++
	if l.dropped%20000 == 0 {
		fmt.Fprintf(l.w, "[node] %d log lines suppressed\n", l.writes)
	}
	return len(p), nil
}

// nonce returns a random hex string used to salt this process's IDs. It makes
// IDs unique even in the (unexpected) case that a node process is restarted
// and its counter starts over.
func nonce() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err == nil {
		return hex.EncodeToString(b[:])
	}
	// Fallback if the system entropy source is unavailable.
	h := fnv.New64a()
	fmt.Fprintf(h, "%d-%d", time.Now().UnixNano(), os.Getpid())
	return strconv.FormatUint(h.Sum64(), 16)
}

func main() {
	// Keep stderr useful but bounded.
	log.SetOutput(newLimitedWriter(os.Stderr, 50))
	errLog := log.New(os.Stderr, "ERROR: ", log.LstdFlags)

	node := maelstrom.NewNode()

	// Closed once the "init" message has been processed. It establishes a
	// happens-before edge between the node ID assignment and every ID we
	// mint, so we never have to guess whether our namespace is ready yet.
	initialized := make(chan struct{})
	var initOnce sync.Once

	// Counter of IDs minted by this node. Never reset, never reused.
	var counter atomic.Uint64

	// The library handles "init" itself (replying with init_ok) and then
	// delegates to this handler, at which point node.ID() is set.
	node.Handle("init", func(msg maelstrom.Message) error {
		initOnce.Do(func() { close(initialized) })
		return nil
	})

	// Salt for this process, computed once.
	salt := nonce()

	node.Handle("generate", func(msg maelstrom.Message) error {
		// Wait until we know our own ID (i.e. the init message arrived).
		<-initialized

		// Unique namespace for this node + a never-repeating counter.
		n := counter.Add(1)
		id := node.ID() + "-" + salt + "-" + strconv.FormatUint(n, 10)

		return node.Reply(msg, map[string]any{
			"type": "generate_ok",
			"id":   id,
		})
	})

	if err := node.Run(); err != nil {
		errLog.Printf("node stopped: %s", err)
		os.Exit(1)
	}
}
