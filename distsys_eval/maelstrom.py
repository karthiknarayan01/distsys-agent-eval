"""One Maelstrom run: test a node binary inside a sandbox and read Maelstrom's verdict.

    options = MaelstromOptions("broadcast", {"node-count": 5, "time-limit": 20, "nemesis": "partition"})
    result = await run_maelstrom(box, "/work/node", options, run_dir="runs/1")
    result.verdict    "valid", "invalid", "unknown" or "crashed"
    result.failed     which of Maelstrom's checkers said invalid, e.g. ["workload"]
    result.results    the whole verdict, for figures such as latencies and messages per operation

Maelstrom starts the binary as a cluster of nodes, plays a workload's clients against it while
it injects the faults asked for, and then checks the history it recorded. Everything it writes
goes under store/ in the directory it runs in, so each run gets a directory of its own. It has
no seed option: every run draws its own faults and timing, which is why one attempt is graded
over several runs.

The verdict:
- valid, invalid, unknown: Maelstrom's own answer, from :valid? in store/latest/results.edn
  (it also exits with 0, 1 or 2). Unknown means a checker couldn't decide, e.g. gnuplot missing.
- crashed: there is no verdict. Maelstrom stopped before writing one (a bad flag, a node that
  won't start) or ran past its timeout. `output` has the end of what it printed.

A task may give its protocol names of its own (Rename). Maelstrom then starts maelstrom-rename
as every node, which starts the real binary and renames messages between clients and nodes on
the way through (docker/rename/main.go).
"""

from __future__ import annotations

import json
import shlex
from dataclasses import dataclass, field
from typing import Any, Literal, Protocol

from distsys_eval import edn
from distsys_eval.sandbox import ExecResult

Verdict = Literal["valid", "invalid", "unknown", "crashed"]

# Maelstrom's `test` flags a task may set. --workload and --bin are set by run_maelstrom.
FLAGS = frozenset({
    "node-count", "time-limit", "rate", "concurrency", "latency", "latency-dist",
    "nemesis", "nemesis-interval", "availability", "topology", "consistency-models",
    "key-count", "max-txn-length", "max-writes-per-key", "log-stderr",
})
# Beyond the time limit: starting the JVM and the nodes, then the analysis. Checking a long history
# full of errors is the slow part: a Kafka run at 1000 requests a second takes about 3 minutes.
STARTUP_SECONDS = 600
RENAMER = "/usr/local/bin/maelstrom-rename"  # built into the sandbox image
OUTPUT_TAIL_CHARS = 4000


class Shell(Protocol):
    """Where Maelstrom runs: a Sandbox, or anything else with these two methods."""

    async def exec(self, command: str, timeout: float) -> ExecResult: ...

    async def read_file(self, path: str) -> bytes: ...


@dataclass(frozen=True)
class Rename:
    """The task's own names for Maelstrom's: message types, and fields of a message's body."""

    types: dict[str, str] = field(default_factory=dict)  # Maelstrom's name -> the task's
    fields: dict[str, str] = field(default_factory=dict)

    def as_json(self) -> str:
        return json.dumps({"types": self.types, "fields": self.fields}, sort_keys=True)


@dataclass(frozen=True)
class MaelstromOptions:
    workload: str
    flags: dict[str, str | int | float | bool] = field(default_factory=dict)
    rename: Rename | None = None

    def __post_init__(self) -> None:
        unknown = sorted(set(self.flags) - FLAGS)
        if unknown:
            raise ValueError(f"not Maelstrom test flags: {', '.join(unknown)}")

    @property
    def time_limit(self) -> float:
        return float(self.flags.get("time-limit", 60))  # Maelstrom's own default


@dataclass(frozen=True)
class MaelstromResult:
    verdict: Verdict
    failed: list[str]  # the checkers that said invalid
    undecided: list[str]  # the checkers that said unknown
    results: dict[str, Any]  # results.edn as Python; empty when the run crashed
    exit_code: int
    timed_out: bool
    output: str  # the end of what Maelstrom printed

    @property
    def valid(self) -> bool:
        return self.verdict == "valid"


async def run_maelstrom(
    shell: Shell, binary: str, options: MaelstromOptions, run_dir: str, timeout: float | None = None
) -> MaelstromResult:
    """Run Maelstrom once in `run_dir` (created if needed) against `binary`, and read its verdict."""
    where = shlex.quote(run_dir)
    command = f"mkdir -p {where} && cd {where} && {maelstrom_command(binary, options)}"
    ran = await shell.exec(command, timeout=timeout or options.time_limit + STARTUP_SECONDS)
    output = ran.output[-OUTPUT_TAIL_CHARS:]
    try:
        results = edn.loads((await shell.read_file(f"{run_dir}/store/latest/results.edn")).decode())
    except (FileNotFoundError, edn.EdnError, UnicodeDecodeError):
        results = None
    if ran.timed_out or not isinstance(results, dict):
        return MaelstromResult("crashed", [], [], {}, ran.exit_code, ran.timed_out, output)
    return MaelstromResult(
        verdict_of(results.get("valid?")),
        checkers_with(results, "invalid"),
        checkers_with(results, "unknown"),
        results,
        ran.exit_code,
        False,
        output,
    )


def maelstrom_command(binary: str, options: MaelstromOptions) -> str:
    """The `maelstrom test` command line, every value shell-quoted. With a rename, Maelstrom runs
    maelstrom-rename, told the real binary (which must be an absolute path, since Maelstrom starts
    nodes in a directory of its own) and the names through the environment."""
    parts = ["maelstrom", "test", "--workload", options.workload, "--bin", binary]
    if options.rename:
        parts = [
            f"MAELSTROM_RENAME_BIN={binary}", f"MAELSTROM_RENAME={options.rename.as_json()}",
            "maelstrom", "test", "--workload", options.workload, "--bin", RENAMER,
        ]
    for flag, value in sorted(options.flags.items()):
        if value is True:
            parts.append(f"--{flag}")
        elif value is not False:
            parts += [f"--{flag}", str(value)]
    return shlex.join(parts)


def verdict_of(valid: Any) -> Verdict:
    """Maelstrom's :valid? as a verdict: true, false, or anything else (:unknown)."""
    if valid is True:
        return "valid"
    if valid is False:
        return "invalid"
    return "unknown"


def checkers_with(results: dict[str, Any], verdict: Verdict) -> list[str]:
    """The top-level checkers (workload, availability, perf, ...) that gave this verdict."""
    return sorted(
        name for name, checker in results.items()
        if isinstance(checker, dict) and "valid?" in checker and verdict_of(checker["valid?"]) == verdict
    )
