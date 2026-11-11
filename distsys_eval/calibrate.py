"""calibrate: choose each task's m, the number of Maelstrom runs an attempt must pass.

    results = await calibrate(load_tasks(), trials=20, jobs=3)
    print(summary(results))
    write(results[0])            calibration.toml, and runs = m in task.toml

One Maelstrom run may miss a bug: the faults and timing it draws may not hit it. So each mutant
is built once and run `trials` times on its own, and the share of runs that catch it estimates
p, the chance one run catches it. m runs then catch it with probability 1 - (1 - p)^m, and the
task's m is the smallest that reaches TARGET for every one of its mutants.

p comes from a sample, so a mutant caught in 20 of 20 runs is not taken to be caught every time:
m is worked out from the lower end of p's 95% confidence interval (Wilson's), which for 20 of 20
is 0.88 and gives m = 3. More trials narrow the interval and can lower m.

The reference is run the same way. A correct solution should never fail, so its failures are
reported as they are: they mean the task is flaky, which no choice of m fixes.

A run fails as grading would fail it: invalid, or valid but over the task's targets. A run
Maelstrom couldn't decide (unknown) is left out of the counts, and the summary says which of
its checkers couldn't decide and why, since many such runs leave too few to count. A run with no
verdict
(crashed) counts as a catch for a mutant and as a failure for the reference.
"""

from __future__ import annotations

import asyncio
import math
import re
import tempfile
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path

from distsys_eval.grader import GRADE_DIR, build_attempt
from distsys_eval.maelstrom import MaelstromResult, Shell, run_maelstrom
from distsys_eval.sandbox import ExecResult, Sandbox, SandboxConfig
from distsys_eval.targets import missed
from distsys_eval.tasks import Task
from distsys_eval.validate import Subject, materialize, subjects

TARGET = 0.99  # the chance that m runs catch each mutant
Z_95 = 1.645  # one-sided 95% normal quantile, for the lower confidence bound
MAX_RUNS = 20  # beyond this a mutant is too rarely caught to be useful; fix the task instead


@dataclass(frozen=True)
class Measurement:
    """How one subject came out over its trials."""

    subject: Subject
    build: ExecResult
    failed: int  # runs that weren't valid or missed a target: catches, for a mutant
    decided: int  # runs with a verdict to count (all but unknown)
    undecided: Counter[str] = field(default_factory=Counter)  # why the other runs were unknown

    @property
    def built(self) -> bool:
        return self.build.ok


@dataclass(frozen=True)
class Calibration:
    """One task's measurements and the m they call for."""

    task: Task
    trials: int
    reference: Measurement
    mutants: list[Measurement]

    @property
    def runs(self) -> int | None:
        """The smallest m that catches every mutant often enough, or None if some mutant is
        caught too rarely (or never built) for any m up to MAX_RUNS."""
        needed = [runs_needed(m.failed, m.decided) if m.built else None for m in self.mutants]
        if not needed or None in needed:
            return None
        return max(needed)


async def calibrate(
    tasks: list[Task], trials: int, image: str | None = None, jobs: int = 1
) -> list[Calibration]:
    """Measure every task's reference and mutants, `jobs` sandboxes at a time."""
    limit = asyncio.Semaphore(jobs)

    async def measure(subject: Subject) -> Measurement:
        async with limit:
            return await measure_in_sandbox(subject, trials, image)

    results = []
    for task in tasks:
        reference, *mutants = await asyncio.gather(*(measure(s) for s in subjects(task)))
        results.append(Calibration(task, trials, reference, mutants))
    return results


async def measure_in_sandbox(subject: Subject, trials: int, image: str | None) -> Measurement:
    """Set the subject's code up in a fresh sandbox and measure it there."""
    with tempfile.TemporaryDirectory(prefix=f"{subject.task.id}-{subject.name}-") as scratch:
        source = Path(scratch) / "src"
        materialize(subject, source)
        async with Sandbox.start(SandboxConfig(image=image) if image else None) as box:
            await box.copy_in(source)
            return await measure(box, subject, trials)


async def measure(shell: Shell, subject: Subject, trials: int, grade_dir: str = GRADE_DIR) -> Measurement:
    """Build once, then run Maelstrom `trials` times, each run on its own: none stops the rest."""
    build, binary = await build_attempt(shell, grade_dir)
    if not build.ok:
        return Measurement(subject, build, 0, 0)
    failed = decided = 0
    undecided: Counter[str] = Counter()
    for trial in range(1, trials + 1):
        run_dir = f"{grade_dir}/trial-{trial}"
        run = await run_maelstrom(shell, binary, subject.task.maelstrom, run_dir)
        await shell.exec(f"rm -rf {run_dir}", timeout=60)  # keep the container's disk small
        if run.verdict == "unknown":
            undecided[why_undecided(run)] += 1
            continue
        decided += 1
        failed += not run.valid or bool(missed(subject.task.targets, run.results))
    return Measurement(subject, build, failed, decided, undecided)


def why_undecided(run: MaelstromResult) -> str:
    """Which checkers couldn't decide a run, each with the part of it that couldn't, or the
    anomalies it names: "perf (latency-graph)", "workload (cycle-search-timeout)"."""
    reasons = []
    for name in run.undecided or ["?"]:
        checker = run.results.get(name, {})
        parts = [key for key, value in checker.items() if isinstance(value, dict) and undecided(value)]
        parts += [str(anomaly) for anomaly in checker.get("anomaly-types") or []]
        reasons.append(name + (f" ({', '.join(parts)})" if parts else ""))
    return ", ".join(reasons)


def undecided(checker: dict) -> bool:
    return checker.get("valid?") not in (True, False)


def runs_needed(caught: int, trials: int, target: float = TARGET) -> int | None:
    """The smallest m with 1 - (1 - p)^m >= target, for p at the lower 95% bound of
    caught / trials. None if that m is above MAX_RUNS."""
    p = wilson_lower(caught, trials)
    if p <= 0:
        return None
    if p >= 1:
        return 1
    m = math.ceil(math.log(1 - target) / math.log(1 - p))
    return m if m <= MAX_RUNS else None


def wilson_lower(successes: int, trials: int, z: float = Z_95) -> float:
    """The lower end of the Wilson score interval for a proportion: a cautious estimate of the
    true rate behind successes out of trials, which stays below 1 even when every trial succeeded."""
    if trials == 0:
        return 0.0
    p = successes / trials
    centre = p + z * z / (2 * trials)
    spread = z * math.sqrt(p * (1 - p) / trials + z * z / (4 * trials * trials))
    return max(0.0, (centre - spread) / (1 + z * z / trials))


def summary(results: list[Calibration]) -> str:
    """Per task: m now and the m measured, then a line per subject with its counts."""
    lines = []
    for result in results:
        m = result.runs
        chosen = f"m = {m}" if m else "no m is enough"
        lines.append(
            f"{result.task.id}: {chosen} (task.toml has {result.task.runs}), {result.trials} trials each"
        )
        ref = result.reference
        lines.append(f"  reference: {subject_line(ref, 'failed')}")
        for mutant in result.mutants:
            needs = runs_needed(mutant.failed, mutant.decided) if mutant.built else None
            counts = subject_line(mutant, "caught")
            lines.append(f"  {mutant.subject.name}: {counts}, needs m = {needs or '-'}")
    return "\n".join(lines)


def subject_line(measurement: Measurement, verb: str) -> str:
    if not measurement.built:
        return "doesn't build"
    line = f"{verb} in {measurement.failed} of {measurement.decided} runs"
    if measurement.undecided:
        reasons = ", ".join(f"{count} {why}" for why, count in measurement.undecided.most_common())
        line += f"; undecided: {reasons}"
    return line


def write(result: Calibration) -> None:
    """Record the counts in calibration.toml and set runs = m in task.toml. Raises ValueError
    if there is no m to set: the task needs fixing first."""
    m = result.runs
    if m is None or result.reference.failed or not result.reference.built:
        raise ValueError(f"{result.task.id}: not calibrated: {summary([result])}")
    lines = [
        "# Written by distsys-eval calibrate: how often one Maelstrom run of this task failed each",
        "# subject. task.toml's runs is the smallest m that catches every mutant 99% of the time.",
        f"trials = {result.trials}",
        f"runs = {m}",
        "",
        "[reference]",
        f"failed = {result.reference.failed}",
        f"decided = {result.reference.decided}",
    ]
    for mutant in result.mutants:
        lines += ["", f'[mutants."{mutant.subject.name}"]',
                  f"caught = {mutant.failed}", f"decided = {mutant.decided}"]
    (result.task.path / "calibration.toml").write_text("\n".join(lines) + "\n")
    set_runs(result.task.path / "task.toml", m)


def set_runs(task_toml: Path, m: int) -> None:
    """Change the runs = line in task.toml, keeping the rest of the file as it is."""
    text, count = re.subn(r"^runs = \d+$", f"runs = {m}", task_toml.read_text(), flags=re.MULTILINE)
    if count != 1:
        raise ValueError(f"{task_toml}: expected one runs = line, found {count}")
    task_toml.write_text(text)
