"""The runner: n attempts by one agent at each task, each in a fresh sandbox, graded as it ends.

    results = await run(agent, tasks, attempts=5, out=Path("results/qwen"), jobs=2)

One attempt:

    start a sandbox
    the agent works in it          -> transcripts/<task>-<n>.jsonl
    copy out what it left          -> code/<task>-<n>/
    grade it in the same sandbox   -> one line in results.jsonl
    remove the sandbox

Attempts run `jobs` at a time, and each is written to results.jsonl as soon as it ends. So a
run that is stopped and started again with the same output directory carries on: attempts
already recorded are kept, and only missing ones and errors are run again. The directory
belongs to one agent; running another into it is refused, so results are never mixed.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from contextlib import AbstractAsyncContextManager
from dataclasses import replace
from pathlib import Path

from distsys_eval.agent import Agent, AgentRun
from distsys_eval.grader import Grade, grade_attempt
from distsys_eval.results import RESULTS_FILE, AttemptResult, append, load
from distsys_eval.sandbox import Sandbox, SandboxConfig, SandboxError
from distsys_eval.tasks import Task

SandboxFactory = Callable[[], AbstractAsyncContextManager[Sandbox]]
ResultSink = Callable[[AttemptResult], None]


class RunnerError(RuntimeError):
    """The output directory holds another agent's results."""


def sandboxes(image: str | None = None, network: str = "none", env: tuple[str, ...] = ()) -> SandboxFactory:
    """Fresh sandboxes from `image` (or the default image), on `network`, given the host's
    values of the environment variables named in `env`."""
    config = SandboxConfig(network=network, env=env, **({"image": image} if image else {}))
    return lambda: Sandbox.start(config)


async def run(
    agent: Agent,
    tasks: list[Task],
    attempts: int,
    out: Path,
    jobs: int = 1,
    sandbox: SandboxFactory | None = None,
    on_result: ResultSink | None = None,
) -> list[AttemptResult]:
    """Run every attempt not yet recorded in `out`, `jobs` at a time, and return all of them.
    `on_result` is told about each attempt as it is recorded."""
    sandbox = sandbox or sandboxes()
    results_file = out / RESULTS_FILE
    pending = attempts_to_run(agent, tasks, attempts, load(results_file))
    limit = asyncio.Semaphore(jobs)

    async def one(task: Task, number: int) -> None:
        async with limit:
            result = await attempt(agent, task, number, out, sandbox)
        append(results_file, result)
        if on_result:
            on_result(result)

    await asyncio.gather(*(one(task, number) for task, number in pending))
    return load(results_file)


def attempts_to_run(
    agent: Agent, tasks: list[Task], attempts: int, recorded: list[AttemptResult]
) -> list[tuple[Task, int]]:
    """Every (task, attempt number) without a scored result yet. Raises RunnerError if the
    results recorded so far are another agent's."""
    others = sorted({r.agent for r in recorded} - {agent.name})
    if others:
        raise RunnerError(f"the output directory holds results from {', '.join(others)}, not {agent.name}")
    done = {r.key for r in recorded if r.outcome != "error"}
    return [(t, n) for t in tasks for n in range(1, attempts + 1) if (t.id, n) not in done]


async def attempt(agent: Agent, task: Task, number: int, out: Path, sandbox: SandboxFactory) -> AttemptResult:
    """One attempt from start to finish. Docker failing at any point makes it an error."""
    name = f"{task.id}-{number}"
    transcript, code = f"transcripts/{name}.jsonl", f"code/{name}"
    base = AttemptResult(task.id, number, agent.name, "error", runs_required=task.runs)
    run = None
    try:
        async with sandbox() as box:
            run = await agent.attempt(task, box, out / transcript)
            await box.copy_out(".", out / code)
            if run.stop == "model-error":
                return record(base, run, transcript, code, error=f"model: {run.detail}")
            started = time.monotonic()
            grade = await grade_attempt(box, task)
            return record(base, run, transcript, code, grade, time.monotonic() - started)
    except SandboxError as e:
        return record(base, run, transcript if run else None, None, error=f"sandbox: {e}")


def record(
    base: AttemptResult,
    run: AgentRun | None,
    transcript: str | None,
    code: str | None,
    grade: Grade | None = None,
    grade_seconds: float = 0.0,
    error: str | None = None,
) -> AttemptResult:
    """The result line: how the agent's work went, then how it was graded."""
    fields: dict = {"transcript": transcript, "code": code, "error": error}
    if run is not None:
        fields |= {
            "stop": run.stop, "steps": run.steps, "tool_calls": run.tool_calls,
            "input_tokens": run.input_tokens, "output_tokens": run.output_tokens,
            "agent_seconds": round(run.seconds, 1),
        }
    if grade is not None:
        fields |= {
            "outcome": grade.outcome, "category": grade.category,
            "runs": [r.verdict for r in grade.runs], "grade_seconds": round(grade_seconds, 1),
        }
        if grade.outcome == "error":
            fields["error"] = "maelstrom couldn't decide a run"
    return replace(base, **fields)
