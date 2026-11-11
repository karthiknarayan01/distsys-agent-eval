# Broadcast on a single node

Write a node that keeps every value broadcast to it and reports them back when asked. Clients
send it three kinds of request.

`broadcast` gives the node a value to keep. The values are integers, and each one is sent once:

    {"type": "broadcast", "msg_id": 1, "message": 1000}
    {"type": "broadcast_ok", "in_reply_to": 1}

`read` asks for every value the node has, in any order:

    {"type": "read", "msg_id": 2}
    {"type": "read_ok", "in_reply_to": 2, "messages": [1000, 1001]}

`topology` tells the node which other nodes are its neighbours. It comes once, at the start:

    {"type": "topology", "msg_id": 3, "topology": {"n1": []}}
    {"type": "topology_ok", "in_reply_to": 3}

For now the cluster is a single node, so it has no neighbours, but the request must still be
answered. The answer to `read` must include every value whose `broadcast` was acknowledged.
