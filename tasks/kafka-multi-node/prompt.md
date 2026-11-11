# Kafka-style log across two nodes

Write a node for a cluster of two that together keep many append-only logs, each named by a
string key, and remember how far clients have read in each. Clients send any of four kinds of
request to either node, so both must agree on every log: the same offsets for the same values,
whichever node a client asks.

`send` appends a value to a log and answers with the offset it was given. Offsets in each log
start anywhere you like but must only ever grow, and two values in one log never share an offset:

    {"type": "send", "msg_id": 1, "key": "k1", "msg": 123}
    {"type": "send_ok", "in_reply_to": 1, "offset": 1000}

`poll` asks for values from several logs, each starting at a given offset. For every log it
answers `[offset, value]` pairs in offset order, starting with the value at that offset or the
first one after it. It may answer fewer pairs than the log holds, and may leave out a log with
nothing at or after its offset:

    {"type": "poll", "msg_id": 2, "offsets": {"k1": 1000, "k2": 2000}}
    {"type": "poll_ok", "in_reply_to": 2, "msgs": {"k1": [[1000, 9], [1001, 5]], "k2": [[2000, 7]]}}

`commit_offsets` records, for each log, the offset a client has finished reading up to:

    {"type": "commit_offsets", "msg_id": 3, "offsets": {"k1": 1000, "k2": 2000}}
    {"type": "commit_offsets_ok", "in_reply_to": 3}

`list_committed_offsets` answers the committed offset of each log asked about, leaving out logs
with nothing committed:

    {"type": "list_committed_offsets", "msg_id": 4, "keys": ["k1", "k2"]}
    {"type": "list_committed_offsets_ok", "in_reply_to": 4, "offsets": {"k1": 1000, "k2": 2000}}

An acknowledged `send` must never be lost or changed: a poll that reaches past its offset must
not skip it, and every answer for that offset must give the same value. Nothing fails during
the test.

Maelstrom also runs a key/value store the nodes can use, as a service named `lin-kv`. It is
linearizable: a read always sees the latest write. The Go library talks to it with
`maelstrom.NewLinKV(node)`, which has `Read`, `ReadInt`, `Write` and `CompareAndSwap` (each takes
a `context.Context`). The nodes may also send each other messages of your own design.
