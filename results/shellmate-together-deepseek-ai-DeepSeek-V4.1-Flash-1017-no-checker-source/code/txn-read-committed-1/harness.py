#!/usr/bin/env python3
"""Harness: runs two node processes, continuously routes messages between them
(with an optional partition), and checks convergence + read-committed
behaviour."""
import json
import queue
import subprocess
import sys
import threading
import time

NODES = ["n0", "n1"]

procs = {}
queues = {}
partitioned = threading.Event()
lock = threading.Lock()
msg_id = 0
replies = {}
replies_lock = threading.Lock()
dropped = 0
forwarded = 0


def reader(node, proc):
    for line in proc.stdout:
        line = line.strip()
        if not line:
            continue
        try:
            m = json.loads(line)
        except Exception:
            print("BAD LINE from", node, line[:200])
            continue
        queues[node].put(m)


def router():
    global dropped, forwarded
    while True:
        got = False
        for n in NODES:
            try:
                m = queues[n].get_nowait()
            except queue.Empty:
                continue
            got = True
            dest = m.get("dest")
            body = m.get("body", {})
            if dest in NODES and dest != m.get("src"):
                if partitioned.is_set():
                    dropped += 1
                    continue
                forwarded += 1
                try:
                    procs[dest].stdin.write(json.dumps(m) + "\n")
                    procs[dest].stdin.flush()
                except Exception as e:
                    print("write fail", e)
            elif "in_reply_to" in body:
                with replies_lock:
                    replies[body["in_reply_to"]] = body
        if not got:
            time.sleep(0.001)


def start():
    for n in NODES:
        p = subprocess.Popen(["./node"], stdin=subprocess.PIPE,
                             stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                             text=True, bufsize=1)
        procs[n] = p
        queues[n] = queue.Queue()
        threading.Thread(target=reader, args=(n, p), daemon=True).start()
    threading.Thread(target=router, daemon=True).start()


def send(node, body):
    body = dict(body)
    global msg_id
    with lock:
        msg_id += 1
        body["msg_id"] = msg_id
    mid = body["msg_id"]
    procs[node].stdin.write(json.dumps({"src": "client", "dest": node, "body": body}) + "\n")
    procs[node].stdin.flush()
    return mid


def wait_for(mid, timeout=10.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        with replies_lock:
            if mid in replies:
                return replies.pop(mid)
        time.sleep(0.002)
    raise RuntimeError("no reply for %d" % mid)


def txn(node, ops, timeout=10.0):
    mid = send(node, {"type": "txn", "txn": ops})
    r = wait_for(mid, timeout)
    assert r.get("type") == "txn_ok", r
    return r["txn"]


def init():
    mid = send("n0", {"type": "init", "node_id": "n0", "node_ids": NODES})
    assert wait_for(mid)["type"] == "init_ok"
    mid = send("n1", {"type": "init", "node_id": "n1", "node_ids": NODES})
    assert wait_for(mid)["type"] == "init_ok"


def read(node, key):
    r = txn(node, [["r", key, None]])
    return r[0][2]


def main():
    start()
    time.sleep(0.2)
    init()

    failures = []

    def check(name, cond, detail=""):
        print(("PASS " if cond else "FAIL ") + name, detail)
        if not cond:
            failures.append(name)

    # Split the network.
    partitioned.set()
    time.sleep(0.3)

    txn("n0", [["w", 1, 1], ["w", 2, 1]])
    txn("n1", [["w", 1, 2], ["w", 3, 2]])
    txn("n0", [["w", 1, 3]])
    # Intermediate writes: 6 then 7 on key 4, in one transaction.
    txn("n0", [["w", 4, 6], ["w", 4, 7]])

    check("n0 sees own write", read("n0", 1) == 3, read("n0", 1))
    check("n1 sees own write", read("n1", 1) == 2, read("n1", 1))
    check("n1 has not seen n0's key2", read("n1", 2) is None)
    check("n0 has not seen n1's key3", read("n0", 3) is None)
    check("intermediate write not leaked", read("n1", 4) is None)

    # Heal and let replication converge.
    partitioned.clear()
    time.sleep(1.5)

    for key in (1, 2, 3, 4):
        a = read("n0", key)
        b = read("n1", key)
        check("converged key %d" % key, a == b, (a, b))

    check("key1 is one of the partition writes", read("n0", 1) in (2, 3))
    check("key2 replicated", read("n1", 2) == 1)
    check("key3 replicated", read("n0", 3) == 2)
    check("final value of key4 (7) replicated", read("n1", 4) == 7, read("n1", 4))

    # Repeat short partition cycles: writes on both sides, then heal.
    for round_ in range(5):
        partitioned.set()
        txn("n0", [["w", 10 + round_, 100 + round_]])
        txn("n1", [["w", 20 + round_, 200 + round_]])
        time.sleep(0.05)
        partitioned.clear()
        got_n1 = None
        got_n0 = None
        deadline = time.time() + 3.0
        while time.time() < deadline:
            got_n1 = read("n1", 10 + round_)
            got_n0 = read("n0", 20 + round_)
            if got_n1 == 100 + round_ and got_n0 == 200 + round_:
                break
            time.sleep(0.05)
        check("round %d n1 got n0 write" % round_, got_n1 == 100 + round_, got_n1)
        check("round %d n0 got n1 write" % round_, got_n0 == 200 + round_, got_n0)

    # Reads must never return an intermediate write value.
    txn("n0", [["w", 99, 5], ["w", 99, 6]])
    time.sleep(0.4)
    v = read("n1", 99)
    check("key99 final value only", v == 6, v)

    print("dropped %d, forwarded %d" % (dropped, forwarded))

    for p in procs.values():
        p.kill()

    print("FAILURES:", failures if failures else "none")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
