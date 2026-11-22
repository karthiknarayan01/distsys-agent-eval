#!/usr/bin/env python3
"""Manual smoke test: run two nodes, route inter-node traffic, check txn replies
and cross-node replication."""
import json, subprocess, threading, queue, time

procs = {}
q = {}

def route(line):
    """Deliver a line from a node's stdout to its destination's stdin."""
    try:
        m = json.loads(line)
    except ValueError:
        return
    if m.get("dest") in procs:
        p = procs[m["dest"]]
        p.stdin.write(line if line.endswith("\n") else line + "\n")
        p.stdin.flush()

def start(node_id, node_ids):
    err = open(f"/tmp/{node_id}.err", "w")
    p = subprocess.Popen(['./node'], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                         stderr=err, text=True, bufsize=1)
    procs[node_id] = p
    # Fan out each node's stdout to (a) this test's queue and (b) the router.
    def reader():
        for line in p.stdout:
            q[node_id].put(line)
            route(line)
    q[node_id] = queue.Queue()
    threading.Thread(target=reader, daemon=True).start()
    send(node_id, "c-" + node_id, {"type": "init", "node_id": node_id,
                                   "node_ids": node_ids, "msg_id": 1})
    m = json.loads(q[node_id].get(timeout=5))
    assert m["body"]["type"] == "init_ok", m

def send(dest_node, src, body):
    procs[dest_node].stdin.write(json.dumps({"src": src, "dest": dest_node,
                                             "body": body}) + "\n")
    procs[dest_node].stdin.flush()

def txn(node, mops, src="c1", msg_id=[1]):
    msg_id[0] += 1
    send(node, src, {"type": "txn", "msg_id": msg_id[0], "txn": mops})
    while True:
        m = json.loads(q[node].get(timeout=5))
        if m["src"] == node and m["dest"] == src and m["body"].get("in_reply_to") == msg_id[0]:
            return m["body"]

ids = ["n1", "n2"]
start("n1", ids)
start("n2", ids)

# A read of an unset key is null.
r = txn("n1", [["r", 1, None]])
assert r["type"] == "txn_ok" and r["txn"] == [["r", 1, None]], r
print("empty read ok:", r["txn"])

# A write, read-your-own-writes, and an intermediate write that must not leak.
r = txn("n1", [["w", 1, 6], ["r", 1, None], ["w", 1, 7], ["r", 1, None], ["r", 2, None]])
assert r["txn"] == [["w", 1, 6], ["r", 1, 6], ["w", 1, 7], ["r", 1, 7], ["r", 2, None]], r
print("ryow ok:", r["txn"])

# Read before write must see the *old* value, not the one written later.
r = txn("n1", [["r", 1, None], ["w", 1, 99], ["r", 1, None]])
assert r["txn"] == [["r", 1, 7], ["w", 1, 99], ["r", 1, 99]], r
print("read-before-write ok:", r["txn"])

# The other node should converge on our writes.
def converge(node, mops, expect, timeout=5):
    deadline = time.time() + timeout
    while time.time() < deadline:
        r = txn(node, mops)
        if r["txn"] == expect:
            return
        time.sleep(0.05)
    raise AssertionError(f"{node} never saw {expect}, last={r['txn']}")

converge("n2", [["r", 1, None]], [["r", 1, 99]])
print("replication ok")

# And in the other direction.
r = txn("n2", [["w", 5, 123], ["r", 5, None]])
assert r["txn"] == [["w", 5, 123], ["r", 5, 123]], r
converge("n1", [["r", 5, None]], [["r", 5, 123]])
print("reverse replication ok")

# Big integers survive the round trip un-mangled.
big = 123456789012345678901234567890
r = txn("n1", [["w", 7, big], ["r", 7, None]])
assert r["txn"] == [["w", 7, big], ["r", 7, big]], r
converge("n2", [["r", 7, None]], [["r", 7, big]])
print("big int replication ok")

# Repeated reads within a txn are consistent.
r = txn("n2", [["r", 1, None], ["r", 1, None], ["r", 5, None]])
assert r["txn"] == [["r", 1, 99], ["r", 1, 99], ["r", 5, 123]], r
print("repeat reads ok")

for p in procs.values():
    p.kill()
print("ALL OK")
