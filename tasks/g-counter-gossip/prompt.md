# Grow-only counter, with no shared store

Write a node for a cluster of five that together keep one counter, which only ever grows.
Clients send each node two kinds of request.

`add` increases the counter by a non-negative integer:

    {"type": "add", "msg_id": 1, "delta": 3}
    {"type": "add_ok", "in_reply_to": 1}

`read` asks a node for the counter's value:

    {"type": "read", "msg_id": 2}
    {"type": "read_ok", "in_reply_to": 2, "value": 42}

There is no shared store to lean on: the nodes must keep the counter between them, in messages
of your own design.

During the test the network between the nodes is split and healed again and again, every few
seconds; a message sent across a split is lost without any error. A node must answer `add` and
`read` even while it is cut off from the others. A read may lag behind adds made elsewhere, but
once the network is healed and has settled at the end, every node's `read` must return the sum
of every acknowledged `add`. An `add` that got no answer may be counted or not, but nothing may
be counted twice.
