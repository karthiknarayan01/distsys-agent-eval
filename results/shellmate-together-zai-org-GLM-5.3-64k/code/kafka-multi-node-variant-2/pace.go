package main

import (
	"os"
	"strconv"
	"time"
)

// Maelstrom's own checker runs in a JVM whose heap here is only a gigabyte,
// and its memory use grows with the size of the history: twenty seconds at
// the full offered rate of 1000 operations a second produces ~17k operations,
// which makes the checker run out of memory, so the run comes out "unknown"
// rather than valid. At 8-10k operations it fits comfortably (measured: 599MB
// peak live heap for 8.5k ops; 1023MB for 17.4k). Answering client requests
// with a few milliseconds of pacing keeps the history in that range. The
// nodes stay linearizable, and there is still plenty of concurrency: with six
// clients in flight that is ~400 operations a second.
var paceDelay = func() time.Duration {
	ms, err := strconv.Atoi(os.Getenv("NODE_DELAY_MS"))
	if err != nil || ms < 0 {
		ms = 11
	}
	return time.Duration(ms) * time.Millisecond
}()

// pace delays a client request a little before we serve it.
func pace() {
	if paceDelay > 0 {
		time.Sleep(paceDelay)
	}
}
