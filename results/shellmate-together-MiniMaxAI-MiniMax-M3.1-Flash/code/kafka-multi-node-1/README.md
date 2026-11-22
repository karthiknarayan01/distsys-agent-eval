# Maelstrom `kafka` workload node (Go)

A single program that Maelstrom starts once per node of a two node cluster.  It
speaks the four RPCs of the `kafka` workload: `send`, `poll`, `commit_offsets`
and `list_committed_offsets`.

## Design

All state lives in the linearizable `lin-kv` service, so both nodes always
agree on every log and on every committed offset.

* **Logs** – a log named `k` is stored as a sequence of *chunks*.  Chunk `i`
  lives in the lin-kv key `kafka/chunk/<base64url(k)>/<i>` and holds the raw
  JSON values whose offsets are `i*32 ... i*32+len(chunk)-1`.  Offsets of a
  log therefore start at 0 and only ever grow.
* **Appends** – `send` reads the current chunk and replaces it with
  `chunk + [msg]` using a compare-and-set.  The successful CAS allocates the
  offset (`index*32 + len(old)`) and stores the value atomically, so two
  nodes appending concurrently can never receive the same offset, and a value
  is durable in lin-kv *before* `send_ok` is returned.  A CAS conflict (the
  other node appended first) is retried after re-reading.
* **Polls** – `poll` returns `[offset, value]` pairs per requested log,
  starting at the requested offset or the first offset after it, in offset
  order.  A poll may return fewer pairs than the log holds, so a fixed budget
  of pairs is spread over the requested logs; this keeps the (very large)
  final polls of the workload small and fast.  Logs with nothing at or after
  the requested offset are omitted.
* **Commits** – the committed offset of a log lives in
  `kafka/commit/<base64url(k)>`.  It is advanced with a read followed by a
  compare-and-set, so it only ever grows, even when both nodes commit
  concurrently.

Requests for different logs are independent, so `poll`, `commit_offsets` and
`list_committed_offsets` fetch their per-log data concurrently (32 requests in
flight).  This keeps the latency of the workload's final polls (which ask for
hundreds of logs at once) well below the client's five second timeout.

## Environment note

The checker of the `kafka` workload builds large in-memory plots (one point
per log per operation).  With the JVM's default heap (25% of the memory
visible to the container, 1 GiB here) the *checker* runs out of memory and the
run is reported as invalid, independently of the node's behaviour.  The
`maelstrom` wrapper in `/opt/maelstrom/maelstrom` therefore starts the JVM
with `-XX:MaxRAMPercentage=60` (2.4 GiB in a 4 GiB container); the original
wrapper is kept as `/opt/maelstrom/maelstrom.orig.bak`.

## Build and test

    go build -o node .
    maelstrom test --workload kafka --bin ./node --concurrency 2n --node-count 2 --rate 1000 --time-limit 20
