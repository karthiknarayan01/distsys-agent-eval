"""The Shellmate agent: Shellmate's agent loop, given tools that work inside the sandbox.

    agent = ShellmateAgent.from_provider("deepseek")
    run = await agent.attempt(task, box, transcript)

Shellmate (github.com/karthiknarayan01/Shellmate) runs one turn of a tool-using model: the
brief goes in as the user's message, and the model calls tools until it replies without one,
which ends the attempt. Its own tools work on the host's files, so here it gets three that go
through the workspace instead:

    shell        run a command in the working directory, with a timeout
    read_file    read a file
    write_file   create or overwrite a file

Every call is approved without asking: the container is what keeps the host safe.

The task's limits become Shellmate's: steps is its max_steps, so one step is one model reply
however many tools it calls, and the time limit covers the whole turn, which is cancelled
wherever it is when the time runs out.

Shellmate records each step in its event log. When the attempt ends, the log is written out as
the transcript, one JSON object per line, after a first line that says what was attempted: the
agent, the task, its limits and the system prompt.
"""

from __future__ import annotations

import asyncio
import json
import tempfile
import time
from dataclasses import asdict
from pathlib import Path
from typing import Any

from runtime.approval import AutoApprove
from runtime.events import Event, EventLog
from runtime.migrate import migrate
from runtime.model import ModelClient, ModelError
from runtime.providers import DEFAULT_MODELS, make_model
from runtime.session import CompactionFailed, MaxStepsReached, ReplyTruncated, Session
from runtime.tools import Tool, ToolRegistry

from distsys_eval.agent import AgentRun, StopReason, Workspace
from distsys_eval.sandbox import ExecResult, SandboxError
from distsys_eval.tasks import Task

SYSTEM_PROMPT = (
    "You are a software engineer working alone in a Linux container, with a shell and tools to "
    "read and write files. The user's message describes a program to build. Write it, build it "
    "and test it with the tools. Nobody will answer questions, so make reasonable choices "
    "yourself. When the program is finished, reply with a short summary and no tool call: that "
    "ends your attempt, and the program is graded as you left it."
)

SHELL_SECONDS = 120  # a command's timeout when the model doesn't give one
MAX_SHELL_SECONDS = 600
MAX_OUTPUT_CHARS = 20_000  # what one tool result may put in the conversation


class ShellmateAgent:
    def __init__(self, model: ModelClient, name: str):
        self.model = model
        self.name = name

    @classmethod
    def from_provider(cls, provider: str, model: str | None = None) -> ShellmateAgent:
        """Shellmate with one of its providers: "ollama", "deepseek" or "anthropic"."""
        client = make_model(provider, model)
        return cls(client, f"shellmate/{provider}/{model or DEFAULT_MODELS[provider]}")

    async def attempt(self, task: Task, workspace: Workspace, transcript: Path) -> AgentRun:
        """Run Shellmate on the task's brief until it finishes or a limit stops it, then write
        the transcript. Raises SandboxError if the sandbox broke under it."""
        with tempfile.TemporaryDirectory(prefix="shellmate-") as scratch:
            db = str(Path(scratch) / "events.db")
            migrate(db)
            with EventLog(db) as log:
                broken = asyncio.get_running_loop().create_future()
                session = Session.create(
                    log, self.model, ToolRegistry(workspace_tools(workspace, broken)),
                    approver=AutoApprove(), system_prompt=SYSTEM_PROMPT, max_steps=task.limits.steps,
                )
                started = time.monotonic()
                stop, detail = await run_turn(session, task, broken)
                seconds = time.monotonic() - started
                events = log.load(session.session_id)
        write_transcript(transcript, self.name, task, events)
        return tally(events, stop, detail, seconds)


def workspace_tools(workspace: Workspace, broken: asyncio.Future) -> list[Tool]:
    """shell, read_file and write_file, each going through the workspace."""

    async def shell(command: str, timeout_seconds: int = SHELL_SECONDS) -> str:
        timeout = min(timeout_seconds, MAX_SHELL_SECONDS)
        return shell_result(await guarded(broken, workspace.exec(command, timeout)), timeout)

    async def read_file(path: str) -> str:
        return clip((await guarded(broken, workspace.read_file(path))).decode(errors="replace"))

    async def write_file(path: str, content: str) -> str:
        await guarded(broken, workspace.write_file(path, content))
        return f"wrote {len(content)} characters to {path}"

    return [
        Tool(
            name="shell",
            description=(
                "Run a shell command (sh -c) in the working directory and get its exit code and "
                f"output. It is stopped after timeout_seconds (default {SHELL_SECONDS}, at most "
                f"{MAX_SHELL_SECONDS}). There is no network."
            ),
            input_schema={
                "type": "object",
                "properties": {
                    "command": {"type": "string"},
                    "timeout_seconds": {"type": "integer", "minimum": 1, "maximum": MAX_SHELL_SECONDS},
                },
                "required": ["command"],
                "additionalProperties": False,
            },
            fn=shell,
        ),
        Tool(
            name="read_file",
            description="Read a text file. A relative path is under the working directory.",
            input_schema={
                "type": "object",
                "properties": {"path": {"type": "string"}},
                "required": ["path"],
                "additionalProperties": False,
            },
            fn=read_file,
        ),
        Tool(
            name="write_file",
            description=(
                "Create or overwrite a text file with `content`, creating its directories. "
                "A relative path is under the working directory."
            ),
            input_schema={
                "type": "object",
                "properties": {"path": {"type": "string"}, "content": {"type": "string"}},
                "required": ["path", "content"],
                "additionalProperties": False,
            },
            fn=write_file,
        ),
    ]


async def guarded(broken: asyncio.Future, call: Any) -> Any:
    """Await a workspace call. If the sandbox fails, mark the attempt broken before re-raising:
    the model would only see an error result, and the attempt has to stop instead."""
    try:
        return await call
    except SandboxError as e:
        if not broken.done():
            broken.set_exception(e)
        raise


def shell_result(result: ExecResult, timeout: int) -> str:
    """What the model sees of a command: how it ended, then its output."""
    status = f"timed out after {timeout}s" if result.timed_out else f"exit code {result.exit_code}"
    return f"{status}\n{clip(result.output)}" if result.output else status


def clip(text: str) -> str:
    """Keep the start and the end of a long output; the end is usually where an error is."""
    if len(text) <= MAX_OUTPUT_CHARS:
        return text
    half = MAX_OUTPUT_CHARS // 2
    return f"{text[:half]}\n[... {len(text) - 2 * half} characters cut ...]\n{text[-half:]}"


async def run_turn(session: Session, task: Task, broken: asyncio.Future) -> tuple[StopReason, str]:
    """The one turn, under the task's time limit: how it ended, and what stopped it.

    `broken` is set by a tool when the sandbox fails; the turn is then cancelled and the
    SandboxError raised, since nothing the model does afterwards could work.
    """
    turn = asyncio.ensure_future(session.run_turn(task.brief))
    seconds = task.limits.minutes * 60
    await asyncio.wait({turn, broken}, timeout=seconds, return_when=asyncio.FIRST_COMPLETED)
    if not turn.done():
        turn.cancel()
        await asyncio.gather(turn, return_exceptions=True)
    session.cancel()  # closes the turn in the log if it was left open; otherwise does nothing
    if broken.done():
        raise broken.exception()
    if turn.cancelled():
        return "out-of-time", f"stopped after {task.limits.minutes:g} minutes"
    try:
        turn.result()
    except MaxStepsReached:
        return "out-of-steps", f"stopped after {task.limits.steps} steps"
    except (ReplyTruncated, CompactionFailed) as e:
        return "truncated", str(e)
    except ModelError as e:
        return "model-error", str(e)
    return "finished", ""


def write_transcript(path: Path, agent: str, task: Task, events: list[Event]) -> None:
    """A first line saying what was attempted, then every event of the session, one per line."""
    header = {
        "type": "attempt", "agent": agent, "task": task.id,
        "limits": asdict(task.limits), "system": SYSTEM_PROMPT,
    }
    lines = [header] + [{"seq": e.seq, "type": e.type, "ts": e.ts, "payload": e.payload} for e in events]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(line, ensure_ascii=False) + "\n" for line in lines))


def tally(events: list[Event], stop: StopReason, detail: str, seconds: float) -> AgentRun:
    """Count the steps, tool calls and tokens in the session's events."""
    replies = [e.payload for e in events if e.type == "assistant_message"]
    usages = [e.payload["usage"] for e in events if e.type in ("assistant_message", "compaction")]
    return AgentRun(
        stop=stop,
        steps=len(replies),
        tool_calls=sum(len(reply["tool_calls"]) for reply in replies),
        input_tokens=sum(usage["input_tokens"] for usage in usages),
        output_tokens=sum(usage["output_tokens"] for usage in usages),
        seconds=seconds,
        detail=detail,
    )
