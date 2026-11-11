"""Tasks: what an agent is asked to build, and how its attempt is tested.

    tasks = load_tasks()                  every task under tasks/, sorted by id
    task = load_task(TASKS_DIR / "echo")
    task.brief                            everything the agent is told
    task.maelstrom                        the workload and flags each grading run uses
    task.runs                             m: how many runs in a row the attempt must pass
    task.limits                           the agent's time and steps
    task.targets                          limits on messages and latency, for the tasks that have them
    task.variant_of                       the task this one is a variant of, or None

Each task is a directory under tasks/ whose name is its id:

    tasks/
      workspace.md        the container and the Go setup, the same for every task
      echo/
        task.toml         title, runs, agent limits, the Maelstrom workload and flags,
                          and optionally [targets]
        prompt.md         the problem, in the agent's terms

    title = "Echo"
    runs = 3

    [agent]
    minutes = 15
    steps = 50

    [maelstrom]
    workload = "echo"
    node-count = 1
    time-limit = 10

A task can give Maelstrom's message types and body fields names of its own, so its protocol
isn't the one an agent may remember. Its prompt uses the new names, and maelstrom-rename
translates between the two while the task is graded:

    [rename.types]
    echo = "say"
    echo_ok = "said"

    [rename.fields]
    echo = "words"

A variant is a task made from another to break recall: the same problem under new names, often
with a different cluster. It names its original, and has no reference or mutants of its own:
validate and calibrate use the original's, with the protocol's names changed in the code to
match (validate.py). So each name a variant renames as both a type and a field must get the
same new name in both.

    variant-of = "g-counter"

A task that grades efficiency too adds the limits a valid run must stay within (targets.py):

    [targets]
    msgs-per-op = 30
    median-latency-ms = 400
    max-latency-ms = 600

The loader is strict: an unknown key, a flag Maelstrom doesn't take, or a missing file is a
TaskError naming the task, so a mistake in a task fails when it loads, not halfway through a run.
"""

from __future__ import annotations

import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from distsys_eval.maelstrom import MaelstromOptions, Rename, maelstrom_command
from distsys_eval.targets import KEYS as TARGET_KEYS
from distsys_eval.targets import Targets

TASKS_DIR = Path(__file__).resolve().parent.parent / "tasks"

# The contract between an attempt and the grader: the agent leaves a Go program in the working
# directory, the grader builds it with BUILD and Maelstrom runs BINARY as every node.
BUILD = "go build -o node ."
BINARY = "./node"
WORKSPACE_BINARY = "/work/node"  # the same, as maelstrom-rename needs it: an absolute path

TASK_KEYS = {"title", "runs", "agent", "maelstrom", "targets", "rename", "variant-of"}
AGENT_KEYS = {"minutes", "steps"}


class TaskError(ValueError):
    """A task directory that isn't a valid task."""


@dataclass(frozen=True)
class AgentLimits:
    minutes: float  # wall-clock time the agent may spend before it is stopped
    steps: int  # model replies the agent may make; one reply may run several tools


@dataclass(frozen=True)
class Task:
    id: str
    title: str
    prompt: str  # prompt.md: the problem alone
    brief: str  # what the agent is told: the workspace, the problem, and how it's graded
    maelstrom: MaelstromOptions
    runs: int
    limits: AgentLimits
    path: Path
    targets: Targets = Targets()
    variant_of: str | None = None


def load_tasks(tasks_dir: Path = TASKS_DIR) -> list[Task]:
    """Every task in `tasks_dir`, sorted by id. Raises TaskError on the first bad one."""
    return [load_task(path) for path in sorted(tasks_dir.iterdir()) if path.is_dir()]


def load_task(path: Path) -> Task:
    """The task in directory `path`; workspace.md is read from the directory above it."""
    task_id = path.name
    try:
        config = tomllib.loads(_read(path / "task.toml"))
    except tomllib.TOMLDecodeError as e:
        raise TaskError(f"{task_id}: task.toml: {e}") from e
    _only(task_id, "task.toml", config, TASK_KEYS)
    title = _get(task_id, config, "title", str)
    runs = _get(task_id, config, "runs", int)
    if runs < 1:
        raise TaskError(f"{task_id}: runs must be at least 1, not {runs}")
    limits = agent_limits(task_id, _get(task_id, config, "agent", dict))
    maelstrom = maelstrom_options(task_id, _get(task_id, config, "maelstrom", dict), config.get("rename"))
    limits_on_runs = targets(task_id, config.get("targets", {}))
    original = variant_of(task_id, config.get("variant-of"), path.parent, maelstrom)
    prompt = _read(path / "prompt.md").strip()
    if not prompt:
        raise TaskError(f"{task_id}: prompt.md is empty")
    workspace = _read(path.parent / "workspace.md").strip()
    told = brief(workspace, prompt, maelstrom, runs, limits, limits_on_runs)
    return Task(task_id, title, prompt, told, maelstrom, runs, limits, path, limits_on_runs, original)


def agent_limits(task_id: str, table: dict[str, Any]) -> AgentLimits:
    """The [agent] table: how long the agent may work, and how many tool calls it may make."""
    _only(task_id, "[agent]", table, AGENT_KEYS)
    minutes = _get(task_id, table, "minutes", (int, float))
    steps = _get(task_id, table, "steps", int)
    if minutes <= 0 or steps < 1:
        raise TaskError(f"{task_id}: [agent] minutes and steps must be positive")
    return AgentLimits(float(minutes), steps)


def maelstrom_options(task_id: str, table: dict[str, Any], rename_table: Any) -> MaelstromOptions:
    """The [maelstrom] table: the workload, and the rest as `maelstrom test` flags; and the
    [rename] table, if there is one."""
    flags = dict(table)
    workload = flags.pop("workload", None)
    if not isinstance(workload, str):
        raise TaskError(f"{task_id}: [maelstrom] needs a workload")
    names = None if rename_table is None else rename(task_id, rename_table)
    try:
        return MaelstromOptions(workload, flags, names)
    except ValueError as e:
        raise TaskError(f"{task_id}: [maelstrom] {e}") from e


def rename(task_id: str, table: Any) -> Rename:
    """The [rename] table: [rename.types] and [rename.fields], each mapping Maelstrom's names to
    the task's. Each must be one to one, so a reply can be renamed back."""
    if not isinstance(table, dict):
        raise TaskError(f"{task_id}: rename must be a table")
    _only(task_id, "[rename]", table, {"types", "fields"})
    maps = {}
    for part in ("types", "fields"):
        names = table.get(part, {})
        if not isinstance(names, dict) or not all(
            isinstance(v, str) and v for v in names.values()
        ):
            raise TaskError(f"{task_id}: [rename.{part}] must map names to names")
        if len(set(names.values())) != len(names):
            raise TaskError(f"{task_id}: [rename.{part}] renames two names to the same one")
        maps[part] = dict(names)
    if not maps["types"] and not maps["fields"]:
        raise TaskError(f"{task_id}: [rename] renames nothing")
    return Rename(maps["types"], maps["fields"])


def targets(task_id: str, table: Any) -> Targets:
    """The [targets] table: positive limits on a valid run's figures. Absent means none."""
    if not isinstance(table, dict):
        raise TaskError(f"{task_id}: targets must be a table")
    _only(task_id, "[targets]", table, set(TARGET_KEYS))
    limits = {}
    for key, field in TARGET_KEYS.items():
        if key in table:
            limit = _get(task_id, table, key, (int, float))
            if limit <= 0:
                raise TaskError(f"{task_id}: [targets] {key} must be positive")
            limits[field] = float(limit)
    return Targets(**limits)


def variant_of(task_id: str, value: Any, tasks_dir: Path, maelstrom: MaelstromOptions) -> str | None:
    """variant-of: an existing task with a reference, whose names this one's rename can be
    written into the original's code without a name meaning two things."""
    if value is None:
        return None
    if not isinstance(value, str) or not (tasks_dir / value / "reference").is_dir():
        raise TaskError(f"{task_id}: variant-of must name a task with a reference/")
    if maelstrom.rename:
        types, fields = maelstrom.rename.types, maelstrom.rename.fields
        clashes = sorted(name for name in set(types) & set(fields) if types[name] != fields[name])
        if clashes:
            raise TaskError(f"{task_id}: a variant must rename {', '.join(clashes)} the same as a type "
                            "and as a field")
    return value


def brief(
    workspace: str,
    prompt: str,
    maelstrom: MaelstromOptions,
    runs: int,
    limits: AgentLimits,
    targets: Targets,
) -> str:
    """What the agent is told. The grading section is generated from the task itself, so it
    always shows the agent's limits and the exact command the attempt will be tested with."""
    times = "once" if runs == 1 else f"{runs} times"
    grading = (
        "## How it's graded\n\n"
        f"You have {limits.minutes:g} minutes and {limits.steps} steps, a step being one reply from you, "
        "however many tools it runs. If either runs out, you are stopped where you are.\n\n"
        f"When you stop, your program is built with `{BUILD}` and tested {times} with:\n\n"
        f"    {maelstrom_command(WORKSPACE_BINARY if maelstrom.rename else BINARY, maelstrom)}\n\n"
        "The runs are independent, each with its own timing and faults. The attempt passes only if it "
        "builds and every run is valid."
    )
    if maelstrom.rename:
        grading += (
            "\n\nMaelstrom itself uses other names for these messages. `maelstrom-rename` stands in "
            "for your program, starts it, and translates every message between a client and a node, "
            "so test with the command above as it is."
        )
    if targets:
        grading += "\n\n" + targets_section(targets)
    return f"{workspace}\n\n{prompt}\n\n{grading}\n"


def targets_section(targets: Targets) -> str:
    """The targets in the agent's terms, each with where Maelstrom reports it in results.edn."""
    lines = []
    if targets.msgs_per_op is not None:
        lines.append(
            f"- At most {targets.msgs_per_op:g} messages between nodes per client operation "
            "(`:net :servers :msgs-per-op`)."
        )
    if targets.median_latency_ms is not None:
        lines.append(
            f"- A median latency of at most {targets.median_latency_ms:g} ms, from a broadcast being "
            "acknowledged to its value being in every read (`:workload :stable-latencies` at 0.5)."
        )
    if targets.max_latency_ms is not None:
        lines.append(
            f"- A maximum latency of at most {targets.max_latency_ms:g} ms (`:stable-latencies` at 1)."
        )
    return (
        "A valid run must also meet these targets, read from Maelstrom's `store/latest/results.edn`:\n\n"
        + "\n".join(lines)
    )


def _read(path: Path) -> str:
    try:
        return path.read_text()
    except FileNotFoundError as e:
        raise TaskError(f"{path.parent.name}: missing {path.name}") from e


def _only(task_id: str, where: str, table: dict[str, Any], allowed: set[str]) -> None:
    """Refuse keys the format doesn't have, so a misspelled one isn't silently ignored."""
    unknown = sorted(set(table) - allowed)
    if unknown:
        raise TaskError(f"{task_id}: {where} has unknown keys: {', '.join(unknown)}")


def _get(task_id: str, table: dict[str, Any], key: str, kind: type | tuple[type, ...]) -> Any:
    """A required value of the given type. A TOML true is not accepted as a number."""
    value = table.get(key)
    if value is None or isinstance(value, bool) or not isinstance(value, kind):
        raise TaskError(f"{task_id}: {key} is missing or has the wrong type")
    return value
