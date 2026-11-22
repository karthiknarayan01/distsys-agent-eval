#!/bin/bash
# Start node in background and feed messages
./node &
PID=$!
sleep 0.5

# init message (optional, node handles it automatically, but let's send)
echo '{"dest":"n0","body":{"type":"init","msg_id":1,"node_id":"n0","node_ids":["n0"]}}' > /proc/$PID/fd/0 2>/dev/null || echo '{"dest":"n0","body":{"type":"init","msg_id":1,"node_id":"n0","node_ids":["n0"]}}'

# Actually feed via stdin properly
kill $PID 2>/dev/null
wait $PID 2>/dev/null
