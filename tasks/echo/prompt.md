# Echo

Write a node that sends back whatever it is sent.

A client sends a request like:

    {"type": "echo", "msg_id": 1, "echo": "Please echo 35"}

Answer it with an `echo_ok` that carries the same `echo` value, and `in_reply_to` set to the
request's `msg_id`:

    {"type": "echo_ok", "in_reply_to": 1, "echo": "Please echo 35"}

The `echo` value can be any JSON value. The cluster is a single node.
