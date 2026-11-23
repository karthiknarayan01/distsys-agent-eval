# Kafka-style log across three nodes, with no shared store

Write a node for a cluster of three that together keep many append-only logs, each named by a
string key, and remember how far clients have read in each. Clients send any of four kinds of
request to any node, so all three must agree on every log: the same offsets for the same values,
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
not skip it, and every answer for that offset must give the same value. Every acknowledged send
must also show up in polls by the end of the test. A request that can't be completed may go
unanswered, but an answer must never be wrong: reply with an error Maelstrom treats as
definite, such as code 11 (temporarily unavailable), only when the request certainly had no
effect.

There is no shared store to lean on: the nodes must keep the logs between them, in messages of
your own design. During the test the network between the nodes is split and healed again and
again, every few seconds; a message sent across a split is lost without any error, and so is
its answer.
