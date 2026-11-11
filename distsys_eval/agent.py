"""Agents: what attempts a task, and what it reports back.

    agent = ShellmateAgent.from_provider("ollama")
    async with Sandbox.start() as box:
        run = await agent.attempt(task, box, transcript=Path("attempts/echo-1.jsonl"))
        grade = await grade_attempt(box, task)
    run.stop          "finished", or why it was stopped, e.g. "out-of-steps"
    run.steps         model replies used, out of task.limits.steps

An agent is given the task's brief and a workspace: the sandbox's working directory, reached
only through exec, read_file and write_file. It works until it says it is done or runs out of
the task's steps or time. The grader then builds whatever it left there; the agent never sees
the grade.

How an attempt ended is recorded, not judged: an attempt stopped for running out of steps is
still graded, since the code it left may already be right. A model that couldn't be reached
(model-error) is different, because it says nothing about the agent: the runner counts it as an
infrastructure error, as it does Docker failing, which raises SandboxError out of attempt().
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Protocol

from distsys_eval.maelstrom import Shell
from distsys_eval.tasks import Task

StopReason = Literal["finished", "out-of-steps", "out-of-time", "truncated", "model-error"]


class Workspace(Shell, Protocol):
    """Where the agent works: a Sandbox, or anything else that can also write a file."""

    async def write_file(self, path: str, content: str | bytes) -> None: ...


@dataclass(frozen=True)
class AgentRun:
    """How one attempt went, from the agent's side."""

    stop: StopReason
    steps: int  # model replies
    tool_calls: int
    input_tokens: int
    output_tokens: int
    seconds: float
    detail: str = ""  # what stopped it, when it didn't finish on its own


class Agent(Protocol):
    name: str  # what the results call it, e.g. "shellmate/ollama/qwen3.5:9b"

    async def attempt(self, task: Task, workspace: Workspace, transcript: Path) -> AgentRun:
        """Work on the task in the workspace until done or out of budget, writing a transcript
        of everything it did to `transcript`."""
        ...
