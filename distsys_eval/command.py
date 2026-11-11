"""A command-line coding agent, run inside the sandbox by a shell command you give.

    agent = CommandAgent("claude-code", 'claude -p "$(cat "$BRIEF")" --dangerously-skip-permissions')
    run = await agent.attempt(task, box, transcript=Path("transcripts/echo-1.jsonl"))

The task's brief is written to a file outside the working directory, and the command is run in
the working directory with BRIEF set to that file's path. The command is the agent: it may run
whatever it likes in the container until it exits or the task's time runs out. Its output, as
the command printed it, is the transcript.

What the harness can't see, it doesn't claim. A command's steps and tokens are its own business,
so they are recorded as 0, and the task's step limit isn't enforced: only its time is. A command
that exits with an error is taken to mean the agent's own tooling failed (a missing API key, a
model that can't be reached), so the attempt is a model-error and isn't scored, as with any
agent whose model can't be reached.

Such an agent usually calls its model over the network, so its sandbox needs one (`distsys-eval
run --network`), and it needs its own image: the sandbox image with the agent installed on top.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from pathlib import Path

from distsys_eval.agent import AgentRun, Workspace
from distsys_eval.tasks import Task

BRIEF_PATH = "/tmp/brief.md"  # outside the working directory, so it isn't taken for code
DETAIL_CHARS = 500  # of the command's output, kept when it fails


@dataclass(frozen=True)
class CommandAgent:
    name: str  # what the results call it, e.g. "claude-code"
    command: str  # run with sh in the working directory, with BRIEF set
    brief_path: str = BRIEF_PATH

    async def attempt(self, task: Task, workspace: Workspace, transcript: Path) -> AgentRun:
        """Write the brief, run the command for at most the task's minutes, and record what it
        printed."""
        await workspace.write_file(self.brief_path, task.brief)
        started = time.monotonic()
        seconds_allowed = task.limits.minutes * 60
        ran = await workspace.exec(f"export BRIEF={self.brief_path}; {self.command}", timeout=seconds_allowed)
        seconds = time.monotonic() - started
        write_transcript(transcript, self, task, ran.output, ran.exit_code)
        if ran.timed_out:
            stopped = f"stopped after {task.limits.minutes:g} minutes"
            return AgentRun("out-of-time", 0, 0, 0, 0, seconds, stopped)
        if ran.exit_code != 0:
            detail = f"the command exited {ran.exit_code}: {ran.output[-DETAIL_CHARS:].strip()}"
            return AgentRun("model-error", 0, 0, 0, 0, seconds, detail)
        return AgentRun("finished", 0, 0, 0, 0, seconds)


def write_transcript(path: Path, agent: CommandAgent, task: Task, output: str, exit_code: int) -> None:
    """A first line saying what was attempted and with what command, then what it printed."""
    lines = [
        {"type": "attempt", "agent": agent.name, "task": task.id, "command": agent.command,
         "limits": {"minutes": task.limits.minutes}},
        {"type": "output", "exit_code": exit_code, "text": output},
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(line, ensure_ascii=False) + "\n" for line in lines))
