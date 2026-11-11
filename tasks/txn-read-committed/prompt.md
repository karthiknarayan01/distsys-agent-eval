# Read-committed transactions that stay available

Write a node for a cluster of two that together keep a store of integer keys and values, and run
transactions against it. Clients send each node one kind of request.

`txn` carries a list of operations, each a read (`r`) or a write (`w`) of one key. A read is sent
with a null value; the reply carries the same list with each read's value filled in, or null for
a key with nothing written yet. Writes come back as they were sent:

    {"type": "txn", "msg_id": 1, "txn": [["r", 1, null], ["w", 1, 6], ["w", 2, 9], ["r", 2, null]]}
    {"type": "txn_ok", "in_reply_to": 1, "txn": [["r", 1, 3], ["w", 1, 6], ["w", 2, 9], ["r", 2, 9]]}

The store must be read committed:

- A read never sees a value that the transaction writing it overwrote before it finished. If a
  transaction writes 6 and then 7 to a key, other transactions may see 7 but never 6.
- A read never sees a write from a transaction that was answered with an error.

It must also be totally available: every transaction must be answered with `txn_ok`. During the
test the network between the two nodes is split and healed again and again, and a message sent
across a split is lost without any error. Clients can still reach both nodes, so a node can't
wait on the other one before answering. Writes made on one node should reach the other once the
network allows.
