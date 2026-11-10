# distsys-agent-eval

A benchmark of how well coding agents write fault-tolerant distributed systems code.

An agent gets a task, such as a broadcast node that must reach the whole cluster while the network
is partitioned, and writes a Go program in a fresh container. [Maelstrom](https://github.com/jepsen-io/maelstrom)
runs that program as a cluster several times under injected faults, and the attempt passes only if
every run is valid. No judge model is involved. Repeating attempts gives pass^k: how reliably an
agent solves a task, not just whether it can.

Status: under construction.

## Development

```sh
python -m venv .venv && . .venv/bin/activate
pip install -e ".[dev]"
ruff check . && python -m pytest -q
```

Every attempt runs in a container built from `docker/Dockerfile` (Go, a Java runtime and
Maelstrom 0.2.4). Build it once, then run the Docker tests against it:

```sh
distsys-eval build-image
SANDBOX_TEST_IMAGE=distsys-agent-eval/sandbox:0.1.0 python -m pytest -q -m docker
```
