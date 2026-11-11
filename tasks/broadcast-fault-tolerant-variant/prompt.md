# Rumors through a split network

Write a node for a village of seven gossips. A rumor told to any one of them must in the end be
known to all seven. Clients send each node three kinds of request.

`tell` passes a node a rumor, which is an integer. Each rumor is told once, to one node:

    {"type": "tell", "msg_id": 1, "rumor": 42}
    {"type": "told", "in_reply_to": 1}

`recall` asks a node for every rumor it knows, in any order:

    {"type": "recall", "msg_id": 2}
    {"type": "recalled", "in_reply_to": 2, "rumors": [42, 43]}

`neighbors` arrives once, at the start, and suggests whom each node might talk to. You can follow
it or arrange things your own way:

    {"type": "neighbors", "msg_id": 3, "neighbors": {"n0": ["n1", "n2"], "n1": ["n0"], ...}}
    {"type": "neighbors_noted", "in_reply_to": 3}

The nodes must pass rumors on to each other, in messages you design. Again and again during the
test the network is split into groups of nodes that can't reach each other, and then healed. A
message sent across a split disappears without any error, and clients can still reach every
node, so a node keeps being told rumors while it is cut off.

Every rumor whose `tell` was answered must in the end show up in every node's `recall`. At the
end the network is healed and given time to settle before the last recalls.
