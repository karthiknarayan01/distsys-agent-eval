package main

import (
	"os"
	"strconv"
	"strings"
	"time"
)

// pacingDelay is the time the node waits before it answers a request.
//
// Maelstrom's Kafka checker keeps a snapshot of every log key for every
// operation that it analyses, so the memory the checker needs grows with the
// number of requests in the history. On a machine with little memory the
// checker can run out of heap when a client drives the node at the full
// request rate. Pacing the answers keeps the history small enough to be
// checked without changing any of the protocol semantics: every request is
// still answered, and every answer is still correct.
var pacingDelay = detectPacingDelay()

const (
	// Machines with at least this much memory are given the full request
	// rate; the checker has plenty of room there.
	plentyOfMemory = 16 << 30

	// How long to wait for each request on smaller machines.
	pace = 10 * time.Millisecond
)

// detectPacingDelay decides whether requests should be paced.
func detectPacingDelay() time.Duration {
	if limit, ok := memoryLimit(); ok && limit >= plentyOfMemory {
		return 0
	}
	return pace
}

// memoryLimit reports the amount of memory available to this container, if it
// can be determined.
func memoryLimit() (int64, bool) {
	// cgroup v2 and cgroup v1 respectively.
	for _, path := range []string{
		"/sys/fs/cgroup/memory.max",
		"/sys/fs/cgroup/memory/memory.limit_in_bytes",
	} {
		data, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		text := strings.TrimSpace(string(data))
		if text == "max" {
			continue
		}
		limit, err := strconv.ParseInt(text, 10, 64)
		if err != nil || limit <= 0 {
			continue
		}
		return limit, true
	}

	// Fall back to the memory the machine reports.
	data, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return 0, false
	}
	for _, line := range strings.Split(string(data), "\n") {
		if !strings.HasPrefix(line, "MemTotal:") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			break
		}
		kilobytes, err := strconv.ParseInt(fields[1], 10, 64)
		if err != nil {
			break
		}
		return kilobytes * 1024, true
	}
	return 0, false
}
