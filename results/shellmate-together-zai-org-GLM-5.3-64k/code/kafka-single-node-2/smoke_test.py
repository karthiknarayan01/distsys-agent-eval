#!/usr/bin/env python3
"""Sequential smoke test for the kafka node: send a line, read a line."""
import json, subprocess, sys

p = subprocess.Popen(["./node"], stdin=subprocess.PIPE, stdout=subprocess.PIPE, universal_newlines=True)

def req(body, expect=None):
    wrapped = {"src": "c1", "dest": "n1", "body": body}
    p.stdin.write(json.dumps(wrapped) + "\n")
    p.stdin.flush()
    resp = json.loads(p.stdout.readline())
    rb = resp["body"]
    assert rb.get("in_reply_to") == body["msg_id"], (rb, body)
    if expect:
        assert rb.get("type") == expect, (rb, expect)
    return rb

req({"type": "init", "msg_id": 1, "node_id": "n1", "node_ids": ["n1"]}, "init_ok")

o = req({"type": "send", "msg_id": 2, "key": "k1", "msg": 123}, "send_ok")
assert o["offset"] == 1000, o
o = req({"type": "send", "msg_id": 3, "key": "k1", "msg": 456}, "send_ok")
assert o["offset"] == 1001, o
o = req({"type": "send", "msg_id": 4, "key": "k2", "msg": "hi"}, "send_ok")
assert o["offset"] == 1000, o

r = req({"type": "poll", "msg_id": 5, "offsets": {"k1": 0, "k2": 1002}}, "poll_ok")
assert r["msgs"] == {"k1": [[1000, 123], [1001, 456]]}, r

r = req({"type": "poll", "msg_id": 6, "offsets": {"k3": 0, "k2": 5000}}, "poll_ok")
assert r["msgs"] == {}, r

r = req({"type": "poll", "msg_id": 7, "offsets": {"k1": 1001}}, "poll_ok")
assert r["msgs"] == {"k1": [[1001, 456]]}, r

req({"type": "commit_offsets", "msg_id": 8, "offsets": {"k1": 1001, "k2": 999}}, "commit_offsets_ok")
r = req({"type": "list_committed_offsets", "msg_id": 9, "keys": ["k1", "k2", "k3"]}, "list_committed_offsets_ok")
assert r["offsets"] == {"k1": 1001, "k2": 999}, r

# commit never goes backwards
req({"type": "commit_offsets", "msg_id": 10, "offsets": {"k1": 5}}, "commit_offsets_ok")
r = req({"type": "list_committed_offsets", "msg_id": 11, "keys": ["k1"]}, "list_committed_offsets_ok")
assert r["offsets"] == {"k1": 1001}, r

# poll exactly at the next free offset => nothing yet
r = req({"type": "poll", "msg_id": 12, "offsets": {"k1": 1002}}, "poll_ok")
assert r["msgs"] == {}, r

print("smoke test OK")
