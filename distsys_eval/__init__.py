"""distsys-agent-eval: how well coding agents write fault-tolerant distributed systems code.

An agent gets a task, such as "build a node that broadcasts every message to the whole cluster,
even while the network is partitioned", and writes a Go program in a fresh container. Jepsen's
Maelstrom then runs that program as a cluster several times under injected faults. The attempt
passes only if every run is valid. Repeating attempts gives pass^k: how reliably the agent
solves the task, not just whether it can.
"""

__version__ = "0.1.0"
