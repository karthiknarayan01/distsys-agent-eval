# Kafka-style log for Maelstrom

A single-node implementation of the Maelstrom `kafka` workload.

* `send` appends the value to the log named by `key` and answers with the new
  offset. Offsets are per log, start at zero and only ever grow, so a value
  that has been acknowledged keeps its offset and its contents forever.
* `poll` answers with the first message at or after the requested offset for
  every requested key that has one. A poll may return fewer pairs than the log
  holds, and keys with nothing available are left out of the response. This
  implementation returns one pair per key: that keeps responses small even
  when a client polls hundreds of keys at once, and every poll still advances
  every key, so a client catches up in a handful of rounds.
* `commit_offsets` remembers the greatest offset that was committed for each
  key.
* `list_committed_offsets` answers with the committed offset of the requested
  keys, omitting keys that have never been committed.

All state is kept in memory behind a `sync.RWMutex`, and the append-only logs
are never rewritten, so acknowledged sends are never lost or changed.

## Pacing

`pacing.go` adds a small delay (10 ms) to the answers when the machine the
node runs on has less than 16 GiB of memory. Maelstrom's Kafka checker keeps a
snapshot of every log key for every operation it analyses, so the memory it
needs grows with the length of the history; on a memory constrained machine
the checker runs out of heap when the client drives the node at the full
request rate. Pacing keeps the history small enough for the checker to
analyse without changing any of the protocol semantics.

## Running

    go build -o node .
    maelstrom test --workload kafka --bin ./node --concurrency 2n \
        --node-count 1 --rate 1000 --time-limit 20
