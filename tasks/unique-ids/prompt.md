# Unique IDs

Write a node that hands out IDs, where no two IDs are the same anywhere in the cluster.

A client asks any node for an ID:

    {"type": "generate", "msg_id": 2}

and the node answers with a new one:

    {"type": "generate_ok", "in_reply_to": 2, "id": "n1-17"}

An ID can be any JSON value: a string, a number, an array. The cluster has three nodes, clients
ask for about a thousand IDs a second, and the network between the nodes is partitioned from
time to time. Every request must still be answered, even by a node that can't reach any other,
and every ID must be unique across all nodes for the whole test.
