# Grow-only counter

Write a node for a cluster of three that together keep one counter, which only ever grows.
Clients send each node two kinds of request.

`add` increases the counter by a non-negative integer:

    {"type": "add", "msg_id": 1, "delta": 3}
    {"type": "add_ok", "in_reply_to": 1}

`read` asks a node for the counter's value:

    {"type": "read", "msg_id": 2}
    {"type": "read_ok", "in_reply_to": 2, "value": 42}

Maelstrom also runs a key/value store the nodes can use, as a service named `seq-kv`. It is
sequentially consistent: every node sees the writes in the same order, but a read may return a
value from a little while ago. The Go library talks to it with `maelstrom.NewSeqKV(node)`, which
has `Read`, `ReadInt`, `Write` and `CompareAndSwap` (each takes a `context.Context`). The nodes
may also send each other messages of your own design.

During the test the network between the nodes is split and healed again and again; a message
sent across a split is lost without any error. A read may lag behind adds made elsewhere, but
once the network is healed and has settled at the end, every node's `read` must return the sum
of every acknowledged `add`. An `add` that got no answer may be counted or not, but nothing may
be counted twice.
