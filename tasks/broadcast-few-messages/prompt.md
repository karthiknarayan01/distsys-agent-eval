# Frugal broadcast across a large cluster

Write a node for a cluster of 25 that together keep every value broadcast to any of them, and
spread each value with as few messages between the nodes as you can. Clients send each node
three kinds of request.

`broadcast` gives a node a value to keep. The values are integers, and each one is sent once, to
one node:

    {"type": "broadcast", "msg_id": 1, "message": 1000}
    {"type": "broadcast_ok", "in_reply_to": 1}

`read` asks a node for every value it has, in any order:

    {"type": "read", "msg_id": 2}
    {"type": "read_ok", "in_reply_to": 2, "messages": [1000, 1001]}

`topology` suggests which nodes each node should talk to. It comes once, at the start, and you
may use it or arrange the nodes your own way:

    {"type": "topology", "msg_id": 3, "topology": {"n1": ["n2", "n3"], "n2": ["n1"], ...}}
    {"type": "topology_ok", "in_reply_to": 3}

A value broadcast to one node has to reach all of them, so the nodes must pass values on to
each other, in messages of your own design. Every value whose `broadcast` was acknowledged must
eventually appear in every node's `read`. The network between the nodes is reliable, but every
message between them takes 100 ms to arrive, and clients send 100 requests a second in all.

Being correct isn't enough here: the values must reach every node with few messages between the
nodes, and without much delay. The targets are below. The message count is the tighter of the
two; a value may take longer to spread than it would in a cluster tuned for speed.
