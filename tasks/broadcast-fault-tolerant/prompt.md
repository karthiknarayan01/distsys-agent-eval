# Broadcast through network partitions

Write a node for a cluster of five that together keep every value broadcast to any of them, even
while the network between them is partitioned. Clients send each node three kinds of request.

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
each other, in messages of your own design. During the test the network is split into groups
of nodes that can't reach each other, and later healed, again and again. A message sent across
a split is lost without any error. Clients can still reach every node, so a node keeps taking
broadcasts while it is cut off.

Every value whose `broadcast` was acknowledged must eventually appear in every node's `read`.
At the end the network is healed and given time to settle before the final reads.
