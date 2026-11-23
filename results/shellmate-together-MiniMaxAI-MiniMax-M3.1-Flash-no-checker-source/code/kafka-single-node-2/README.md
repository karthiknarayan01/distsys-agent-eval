# Kafka-style log node

A single Go program (`main.go`) that implements the Maelstrom `kafka` workload.

## Behaviour

* `send` — appends the value to the log named by `key` and replies `send_ok`
  with the offset the value was given. Offsets of a log are `0, 1, 2, ...`,
  they only ever grow and two values in one log never share an offset.
* `poll` — replies `poll_ok` with, for every requested key, the `[offset,
  value]` pairs at or after the requested offset, in offset order. Keys with
  nothing at or after the requested offset are left out of the response.
* `commit_offsets` — records the offset each log has been read up to and
  replies `commit_offsets_ok`.
* `list_committed_offsets` — replies `list_committed_offsets_ok` with the
  committed offset of every requested log that has something committed; logs
  without a committed offset are left out.

Values are kept as raw JSON and are echoed back unchanged, so every answer
for a given offset carries exactly the same value. Requests are handled
strictly in arrival order, so a request that follows an acknowledged `send`
always observes that `send`'s value.

## Build and test

    go build -o node .
    maelstrom test --workload kafka --bin ./node --concurrency 2n \
      --node-count 1 --rate 1000 --time-limit 20

Note on memory: the workload generates ~19k client operations, and Maelstrom's
checker analyses the whole history afterwards. That analysis needs roughly
2 GB of JVM heap; with the JVM's default heap (1 GB in a container with a
4 GB memory limit) the checker itself runs out of memory while building its
reports, which is independent of this program. Run the test with a larger
heap, e.g. `JAVA_TOOL_OPTIONS=-Xmx2500m maelstrom test ...`, which produces a
valid run.
