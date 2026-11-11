# Visitor tally

Write a node for the four turnstiles of a museum, which together keep a running tally of
visitors. The tally only ever grows. Clients send each node two kinds of request.

`count` reports that some visitors came in through this turnstile, a non-negative integer of them:

    {"type": "count", "msg_id": 1, "visitors": 4}
    {"type": "counted", "in_reply_to": 1}

`total` asks for the tally so far, over all four turnstiles:

    {"type": "total", "msg_id": 2}
    {"type": "total_is", "in_reply_to": 2, "visitors_so_far": 118}

The nodes can share a key/value store that Maelstrom runs as a service named `seq-kv`. It is
sequentially consistent: all nodes see its writes in one order, but a read may be a little out
of date. In Go, `maelstrom.NewSeqKV(node)` gives you `Read`, `ReadInt`, `Write` and
`CompareAndSwap`, each taking a `context.Context`. Nodes may also message each other in any
format you choose.

From time to time the network splits the nodes into groups, and a message sent across a split
is dropped silently. While that lasts, a `total` may leave out visitors counted elsewhere. At the
end the network is healed and given time to settle, and then every node's `total` must equal the
sum of every acknowledged `count`. A `count` that was never answered may be included or not, but
no visitor may be counted twice.
