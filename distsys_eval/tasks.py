"""Tasks: what an agent is asked to build, and how its attempt is tested.

    tasks = load_tasks()                  every task under tasks/, sorted by id
    task = load_task(TASKS_DIR / "echo")
    task.brief                            everything the agent is told
    task.maelstrom                        the workload and flags each grading run uses
    task.runs                             m: how many runs in a row the attempt must pass
    task.limits                           the agent's time and steps

Each task is a directory under tasks/ whose name is its id:

    tasks/
      workspace.md        the container and the Go setup, the same for every task
      echo/
        task.toml         title, runs, agent limits, the Maelstrom workload and flags
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

The loader is strict: an unknown key, a flag Maelstrom doesn't take, or a missing file is a
TaskError naming the task, so a mistake in a task fails when it loads, not halfway through a run.
"""

from __future__ import annotations

import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from distsys_eval.maelstrom import MaelstromOptions, maelstrom_command

TASKS_DIR = Path(__file__).resolve().parent.parent / "tasks"

# The contract between an attempt and the grader: the agent leaves a Go program in the working
# directory, the grader builds it with BUILD and Maelstrom runs BINARY as every node.
BUILD = "go build -o node ."
BINARY = "./node"

TASK_KEYS = {"title", "runs", "agent", "maelstrom"}
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
    maelstrom = maelstrom_options(task_id, _get(task_id, config, "maelstrom", dict))
    prompt = _read(path / "prompt.md").strip()
    if not prompt:
        raise TaskError(f"{task_id}: prompt.md is empty")
    workspace = _read(path.parent / "workspace.md").strip()
    told = brief(workspace, prompt, maelstrom, runs, limits)
    return Task(task_id, title, prompt, told, maelstrom, runs, limits, path)


def agent_limits(task_id: str, table: dict[str, Any]) -> AgentLimits:
    """The [agent] table: how long the agent may work, and how many tool calls it may make."""
    _only(task_id, "[agent]", table, AGENT_KEYS)
    minutes = _get(task_id, table, "minutes", (int, float))
    steps = _get(task_id, table, "steps", int)
    if minutes <= 0 or steps < 1:
        raise TaskError(f"{task_id}: [agent] minutes and steps must be positive")
    return AgentLimits(float(minutes), steps)


def maelstrom_options(task_id: str, table: dict[str, Any]) -> MaelstromOptions:
    """The [maelstrom] table: the workload, and the rest as `maelstrom test` flags."""
    flags = dict(table)
    workload = flags.pop("workload", None)
    if not isinstance(workload, str):
        raise TaskError(f"{task_id}: [maelstrom] needs a workload")
    try:
        return MaelstromOptions(workload, flags)
    except ValueError as e:
        raise TaskError(f"{task_id}: [maelstrom] {e}") from e


def brief(
    workspace: str, prompt: str, maelstrom: MaelstromOptions, runs: int, limits: AgentLimits
) -> str:
    """What the agent is told. The grading section is generated from the task itself, so it
    always shows the agent's limits and the exact command the attempt will be tested with."""
    times = "once" if runs == 1 else f"{runs} times"
    grading = (
        "## How it's graded\n\n"
        f"You have {limits.minutes:g} minutes and {limits.steps} steps, a step being one reply from you, "
        "however many tools it runs. If either runs out, you are stopped where you are.\n\n"
        f"When you stop, your program is built with `{BUILD}` and tested {times} with:\n\n"
        f"    {maelstrom_command(BINARY, maelstrom)}\n\n"
        "The runs are independent, each with its own timing and faults. The attempt passes only if it "
        "builds and every run is valid."
    )
    return f"{workspace}\n\n{prompt}\n\n{grading}\n"


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
