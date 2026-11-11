"""Grading one attempt: build the program the agent left, then test it with Maelstrom m times.

    grade = await grade_attempt(box, task)
    grade.outcome     "pass", "fail" or "error"
    grade.category    why it failed, e.g. "fails-under-faults"; None unless it failed
    grade.runs        the Maelstrom runs, in order

The attempt passes only if it builds and every one of the task's m runs is valid and meets the
task's targets, if it has any. Grading stops at the first run that doesn't, since the attempt
can't pass after that.

A failed attempt gets one category:
- doesnt-build: `go build` failed or took too long.
- crashed: a run ended with no verdict, e.g. the node exits at startup.
- fails-without-faults: wrong even on a calm network.
- fails-under-faults: right on a calm network, wrong once faults are injected.
- too-slow: valid, but over the task's message or latency targets. `grade.missed` says which.

The last two are told apart by one extra run with the task's faults switched off, made only when
a run with faults failed. One calm run can be lucky, so fails-under-faults means "the calm run
passed", not proof that the program is right without faults.

An error is a run Maelstrom couldn't decide (unknown). It says nothing about the program, so
it isn't counted against the agent. Docker failing raises SandboxError, as everywhere else.
"""

from __future__ import annotations

import shlex
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Literal

from distsys_eval.maelstrom import MaelstromOptions, MaelstromResult, Shell, run_maelstrom
from distsys_eval.sandbox import ExecResult, Sandbox, SandboxConfig
from distsys_eval.targets import missed
from distsys_eval.tasks import Task

Outcome = Literal["pass", "fail", "error"]
Category = Literal["doesnt-build", "crashed", "fails-without-faults", "fails-under-faults", "too-slow"]

# Outside the agent's working directory, so nothing the agent left there can stand in for the
# grader's own binary or Maelstrom's results.
GRADE_DIR = "/tmp/grade"
BUILD_SECONDS = 300
FAULT_FLAGS = ("nemesis", "nemesis-interval")
BUILD_OUTPUT_LINES = 20


@dataclass(frozen=True)
class Grade:
    outcome: Outcome
    category: Category | None
    build: ExecResult
    runs: list[MaelstromResult]  # up to the first that wasn't valid
    calm_run: MaelstromResult | None = None  # the run without faults, made only to diagnose a failure
    missed: list[str] = field(default_factory=list)  # the targets the last run missed, if too-slow

    @property
    def passed(self) -> bool:
        return self.outcome == "pass"


async def grade_directory(task: Task, source: Path, image: str | None = None) -> Grade:
    """Grade a local directory as if an agent had left it: copy it into a fresh sandbox and grade."""
    async with Sandbox.start(SandboxConfig(image=image) if image else None) as box:
        await box.copy_in(source)
        return await grade_attempt(box, task)


async def grade_attempt(shell: Shell, task: Task, grade_dir: str = GRADE_DIR) -> Grade:
    """Build the program in the shell's working directory and run the task's m Maelstrom runs."""
    build, binary = await build_attempt(shell, grade_dir)
    if not build.ok:
        return Grade("fail", "doesnt-build", build, [])
    runs = []
    for number in range(1, task.runs + 1):
        runs.append(await run_maelstrom(shell, binary, task.maelstrom, f"{grade_dir}/run-{number}"))
        if not runs[-1].valid:
            break
        misses = missed(task.targets, runs[-1].results)
        if misses:
            return Grade("fail", "too-slow", build, runs, missed=misses)
    last = runs[-1]
    if last.valid:
        return Grade("pass", None, build, runs)
    if last.verdict == "unknown":
        return Grade("error", None, build, runs)
    if last.verdict == "crashed":
        return Grade("fail", "crashed", build, runs)
    return await diagnose(shell, binary, task, grade_dir, Grade("fail", "fails-without-faults", build, runs))


async def build_attempt(shell: Shell, grade_dir: str = GRADE_DIR) -> tuple[ExecResult, str]:
    """`go build` the working directory into grade_dir; the result, and where the binary went."""
    binary = f"{grade_dir}/node"
    build = await shell.exec(f"go build -o {shlex.quote(binary)} .", timeout=BUILD_SECONDS)
    return build, binary


async def diagnose(shell: Shell, binary: str, task: Task, grade_dir: str, failed: Grade) -> Grade:
    """An invalid run: if the task injects faults, run once without them to see whether the
    faults are what broke it. With no faults to remove, the failure stands as it is."""
    calm = without_faults(task.maelstrom)
    if calm is None:
        return failed
    calm_run = await run_maelstrom(shell, binary, calm, f"{grade_dir}/without-faults")
    category = "fails-under-faults" if calm_run.valid else "fails-without-faults"
    return replace(failed, category=category, calm_run=calm_run)


def without_faults(options: MaelstromOptions) -> MaelstromOptions | None:
    """The same test with no nemesis, or None if it had none."""
    if not any(flag in options.flags for flag in FAULT_FLAGS):
        return None
    flags = {flag: value for flag, value in options.flags.items() if flag not in FAULT_FLAGS}
    return MaelstromOptions(options.workload, flags, options.rename)


def describe(task: Task, grade: Grade) -> str:
    """The grade as a few lines for a terminal: the outcome, then each run's verdict."""
    valid = sum(run.valid for run in grade.runs)
    lines = [f"{task.id}: {grade.outcome}"
             + (f" ({grade.category})" if grade.category else "")
             + (f", {valid} of {task.runs} runs valid" if grade.build.ok else "")]
    if not grade.build.ok:
        lines.append("go build failed:" if not grade.build.timed_out else "go build timed out:")
        lines += ["  " + line for line in grade.build.output.strip().splitlines()[-BUILD_OUTPUT_LINES:]]
    for number, run in enumerate(grade.runs, 1):
        lines.append(f"  run {number}: {run_line(run)}")
    if grade.missed:
        lines.append(f"  too slow: {'; '.join(grade.missed)}")
    if grade.calm_run:
        lines.append(f"  without faults: {run_line(grade.calm_run)}")
    return "\n".join(lines)


def run_line(run: MaelstromResult) -> str:
    """valid, invalid (workload), unknown (perf), or crashed (timed out)."""
    if run.verdict == "invalid":
        return f"invalid ({', '.join(run.failed)})" if run.failed else "invalid"
    if run.verdict == "unknown":
        return f"unknown ({', '.join(run.undecided)})" if run.undecided else "unknown"
    if run.verdict == "crashed":
        return "crashed (timed out)" if run.timed_out else f"crashed (Maelstrom exited {run.exit_code})"
    return run.verdict
