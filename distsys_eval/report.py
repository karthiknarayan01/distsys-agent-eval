"""The report: how reliably an agent solved each task, why its attempts failed, and what it spent.

    report = summarize(load(Path("results/qwen/results.jsonl")), {"unique-ids-variant": "unique-ids"})
    print(render(report))                 a table for the terminal
    json.dumps(as_json(report))           the same numbers for other tools

pass^k is the chance that k attempts drawn at random from a task's n all pass. With c of the n
passing, it is C(c, k) / C(n, k), so pass^1 is the plain pass rate and pass^n is 1 only if every
attempt passed. It falls as k grows unless the agent is consistent, which is the point: it
measures whether an agent can be relied on, not whether it can get lucky. A task's value is
given for every k up to its n; the overall value for each k is the mean over tasks, given only
for the k every task reaches, so each mean is over the same tasks.

Only scored attempts count: an error says nothing about the agent, so it is left out and
counted on its own.

Each failed attempt is given one reason. If the agent was stopped (out of steps, out of time,
or its reply cut off at the model's output cap), that is the reason, since it never handed in a
finished program. Otherwise the reason is the grader's category.

A variant is the same problem as its original under new names (tasks.py). When both were run,
the report sets their pass^1 side by side: an agent that does much worse on the variant was
likely recalling the public solution rather than solving the problem.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from math import comb
from statistics import mean
from typing import Any

from distsys_eval.results import AttemptResult, plural

STOPPED = ("out-of-steps", "out-of-time", "truncated")  # agent stop reasons that are failure reasons
TABLE_KS = 8  # beyond this many attempts, the table shows k = 1, 2, 4, 8, ... and n


@dataclass(frozen=True)
class TaskSummary:
    task: str
    attempts: int  # scored: passed or failed
    passed: int
    errors: int  # not scored
    pass_hat: dict[int, float]  # k -> pass^k, for k = 1 to attempts
    failures: Counter[str]  # reason -> how many failed attempts
    steps: float  # means over scored attempts
    minutes: float
    input_tokens: int  # totals over scored attempts
    output_tokens: int


@dataclass(frozen=True)
class AgentReport:
    agent: str
    tasks: list[TaskSummary]
    overall: dict[int, float]  # k -> mean pass^k over tasks, for every k all tasks reach
    failures: Counter[str]
    variants: list[tuple[str, str]] = field(default_factory=list)  # (original, variant), both run


def summarize(results: list[AttemptResult], variant_of: dict[str, str] | None = None) -> AgentReport:
    """The report for one agent's results, tasks in the order they were first recorded.
    `variant_of` maps each variant to its original. Raises ValueError if the results are empty
    or come from more than one agent."""
    agents = {r.agent for r in results}
    if len(agents) != 1:
        raise ValueError(f"expected one agent's results, found {len(agents)}")
    by_task: dict[str, list[AttemptResult]] = {}
    for result in results:
        by_task.setdefault(result.task, []).append(result)
    tasks = [task_summary(task, attempts) for task, attempts in by_task.items()]
    reach = min(t.attempts for t in tasks)
    overall = {k: mean(t.pass_hat[k] for t in tasks) for k in range(1, reach + 1)}
    failures = sum((t.failures for t in tasks), Counter())
    variants = [
        (original, variant) for variant, original in (variant_of or {}).items()
        if original in by_task and variant in by_task
    ]
    return AgentReport(agents.pop(), tasks, overall, failures, variants)


def task_summary(task: str, results: list[AttemptResult]) -> TaskSummary:
    """pass^k, failure reasons, and what the agent spent, over one task's scored attempts."""
    scored = [r for r in results if r.outcome != "error"]
    n, c = len(scored), sum(r.outcome == "pass" for r in scored)
    return TaskSummary(
        task=task,
        attempts=n,
        passed=c,
        errors=len(results) - n,
        pass_hat={k: pass_hat(n, c, k) for k in range(1, n + 1)},
        failures=Counter(failure_reason(r) for r in scored if r.outcome == "fail"),
        steps=mean(r.steps for r in scored) if scored else 0.0,
        minutes=mean(r.agent_seconds for r in scored) / 60 if scored else 0.0,
        input_tokens=sum(r.input_tokens for r in scored),
        output_tokens=sum(r.output_tokens for r in scored),
    )


def pass_hat(n: int, c: int, k: int) -> float:
    """The chance that k of n attempts, c of which passed, drawn without replacement all pass."""
    return comb(c, k) / comb(n, k)


def failure_reason(result: AttemptResult) -> str:
    """Why a failed attempt failed: the agent being stopped, or else the grader's category."""
    if result.stop in STOPPED:
        return result.stop
    return result.category or "unknown"


def render(report: AgentReport) -> str:
    """The report as a table: a row per task and an overall row, then the failure reasons."""
    ks = table_ks(max((t.attempts for t in report.tasks), default=0))
    header = ["task", "passed"] + [f"pass^{k}" for k in ks] + ["steps", "minutes", "tokens in / out"]
    rows = [header]
    for t in report.tasks:
        rows.append(
            [t.task, f"{t.passed}/{t.attempts}"]
            + [f"{t.pass_hat[k]:.2f}" if k in t.pass_hat else "" for k in ks]
            + [f"{t.steps:.0f}", f"{t.minutes:.1f}", f"{tokens(t.input_tokens)} / {tokens(t.output_tokens)}"]
        )
    rows.append(["overall", ""] + [f"{report.overall[k]:.2f}" if k in report.overall else "" for k in ks])
    errors = sum(t.errors for t in report.tasks)
    lines = [report.agent + (f"  ({errors} {plural(errors, 'error')} not scored)" if errors else ""), ""]
    lines += table(rows)
    if report.failures:
        lines += ["", "why attempts failed"]
        width = max(map(len, report.failures))
        lines += [f"  {reason:<{width}}  {count}" for reason, count in report.failures.most_common()]
    if report.variants:
        lines += ["", "originals and their variants"]
        lines += ["  " + line for line in table(variant_rows(report))]
    return "\n".join(lines)


def variant_rows(report: AgentReport) -> list[list[str]]:
    """A row per original and variant pair: each one's pass^1, and the change from one to the other."""
    pass_1 = {t.task: t.pass_hat.get(1, 0.0) for t in report.tasks}
    rows = [["original", "pass^1", "variant", "pass^1", "change"]]
    for original, variant in report.variants:
        before, after = pass_1[original], pass_1[variant]
        rows.append([original, f"{before:.2f}", variant, f"{after:.2f}", f"{after - before:+.2f}"])
    return rows


def table_ks(n: int) -> list[int]:
    """Which k to show for up to n attempts: all of them, or for many, 1, 2, 4, 8, ... and n."""
    if n <= TABLE_KS:
        return list(range(1, n + 1))
    ks = [1 << i for i in range(n.bit_length()) if 1 << i < n]
    return ks + [n]


def table(rows: list[list[str]]) -> list[str]:
    """Rows as left-aligned columns, two spaces apart."""
    widths = [max(len(row[i]) for row in rows if i < len(row)) for i in range(len(rows[0]))]
    return ["  ".join(cell.ljust(w) for cell, w in zip(row, widths, strict=False)).rstrip() for row in rows]


def tokens(count: int) -> str:
    """A token count the way people say it: 812, 45k, 1.2M."""
    if count >= 1_000_000:
        return f"{count / 1_000_000:.1f}M"
    if count >= 1_000:
        return f"{count / 1_000:.0f}k"
    return str(count)


def as_json(report: AgentReport) -> dict[str, Any]:
    """The report's numbers, every k included, for other tools to read."""
    return {
        "agent": report.agent,
        "overall": {"pass_hat": report.overall, "failures": dict(report.failures)},
        "variants": [{"original": original, "variant": variant} for original, variant in report.variants],
        "tasks": [
            {
                "task": t.task, "attempts": t.attempts, "passed": t.passed, "errors": t.errors,
                "pass_hat": t.pass_hat, "failures": dict(t.failures), "mean_steps": t.steps,
                "mean_minutes": t.minutes, "input_tokens": t.input_tokens, "output_tokens": t.output_tokens,
            }
            for t in report.tasks
        ],
    }
