"""The results of a benchmark run: one JSON line per attempt, in results.jsonl.

    append(path, result)         after each attempt, so nothing finished is lost
    load(path)                   every attempt, the latest line for each
    print(line(result))          "echo #1: fail (fails-under-faults), 14 steps in 3m05s"

    results/shellmate-ollama-qwen3.5-9b/
      results.jsonl
      transcripts/echo-1.jsonl   what the agent did in attempt 1 at echo
      code/echo-1/               what it left in the working directory

An attempt's outcome is the grader's: pass, or fail with a category. Or it is `error`: the
attempt says nothing about the agent, because the harness let it down. Docker failed, the model
couldn't be reached, or Maelstrom couldn't decide a run. An error isn't scored, and a run that
is started again tries that attempt again. The new line is appended, and the latest line for
an attempt is the one that counts.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Literal

from distsys_eval.grader import Category

Outcome = Literal["pass", "fail", "error"]
RESULTS_FILE = "results.jsonl"


@dataclass(frozen=True)
class AttemptResult:
    task: str
    attempt: int  # 1 to n
    agent: str
    outcome: Outcome
    category: Category | None = None  # why it failed
    error: str | None = None  # what went wrong outside the agent, when the outcome is error
    stop: str | None = None  # how the agent stopped (agent.StopReason); None if it never ran
    steps: int = 0
    tool_calls: int = 0
    input_tokens: int = 0
    output_tokens: int = 0
    agent_seconds: float = 0.0
    grade_seconds: float = 0.0
    runs: list[str] = field(default_factory=list)  # the verdicts of the grading runs, in order
    runs_required: int = 0  # the task's m when it was graded
    transcript: str | None = None  # relative to the results directory
    code: str | None = None

    @property
    def key(self) -> tuple[str, int]:
        return self.task, self.attempt


def append(path: Path, result: AttemptResult) -> None:
    """Add one line to the results file, creating it if needed."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a") as f:
        f.write(json.dumps(asdict(result)) + "\n")


def load(path: Path) -> list[AttemptResult]:
    """Every attempt in the file, the latest line for each, in the order they were first
    recorded. A missing file has none."""
    if not path.exists():
        return []
    latest: dict[tuple[str, int], AttemptResult] = {}
    for text in path.read_text().splitlines():
        if text.strip():
            result = AttemptResult(**json.loads(text))
            latest[result.key] = result
    return list(latest.values())


def line(result: AttemptResult) -> str:
    """One attempt on one line: how it came out, and what the agent spent on it."""
    if result.outcome == "error":
        how = f"error, not scored ({result.error})"
    else:
        how = result.outcome + (f" ({result.category})" if result.category else "")
    spent = ""
    if result.stop is not None:
        minutes, seconds = divmod(round(result.agent_seconds), 60)
        spent = f", {result.steps} steps in {minutes}m{seconds:02d}s"
        if result.stop != "finished":
            spent += f", {result.stop}"
    return f"{result.task} #{result.attempt}: {how}{spent}"


def totals(results: list[AttemptResult]) -> str:
    """How many attempts passed, failed and errored."""
    count = {outcome: sum(r.outcome == outcome for r in results) for outcome in ("pass", "fail", "error")}
    return (
        f"{len(results)} attempts: {count['pass']} passed, {count['fail']} failed, "
        f"{count['error']} {plural(count['error'], 'error')} (not scored)"
    )


def plural(count: int, word: str) -> str:
    return word if count == 1 else word + "s"
