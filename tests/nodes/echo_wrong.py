#!/usr/bin/env python3
"""A Maelstrom echo node that replies with the wrong text."""

import json
import sys

node_id = None
for line in sys.stdin:
    message = json.loads(line)
    body = message["body"]
    if body["type"] == "init":
        node_id = body["node_id"]
        reply = {"type": "init_ok"}
    else:
        reply = {"type": "echo_ok", "echo": "something else"}
    reply["in_reply_to"] = body["msg_id"]
    print(json.dumps({"src": node_id, "dest": message["src"], "body": reply}), flush=True)
