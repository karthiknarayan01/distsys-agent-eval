"""validate: check that each task's grading tells good code from bad, before any agent is scored.

    checks = await validate(load_tasks(), jobs=3)
    print(summary(checks))       one line per reference and mutant
    all(check.ok for check in checks)

Each task carries code whose grade is already known:

    tasks/unique-ids/
      reference/                 a correct solution: must pass
      mutants/
        no-node-bits.patch       the reference with one bug: must fail
        ask-the-leader.patch

A mutant is a patch to the reference, starting with a few lines on what the bug is. It must fail
for a reason other than not building: a mutant that doesn't compile tests the compiler, not the
task. A reference that fails means the task asks for more than a correct solution can give; a
mutant that passes means the task's runs are too short, too calm or too few to catch that bug.

A variant (tasks.py) borrows its original's reference and mutants. Each is set up as for the
original, and then every Go string literal that is one of the protocol's names is replaced with
the variant's name for it: "generate" becomes "issue" in a handler, in a reply's type, and in a
struct tag alike. So a variant is checked with code that speaks its protocol, and a mutant keeps
its one bug.
"""

from __future__ import annotations

import asyncio
import re
import shutil
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from distsys_eval.grader import Grade, grade_directory
from distsys_eval.tasks import Task


class ValidateError(RuntimeError):
    """A task's reference or mutant couldn't be set up: missing, or a patch that doesn't apply."""


@dataclass(frozen=True)
class Subject:
    """One piece of known code: a task's reference, or one of its mutants."""

    task: Task
    name: str  # "reference", or the mutant's file name without .patch
    expect: Literal["pass", "fail"]
    patch: Path | None  # None for the reference
    about: str  # what the mutant's bug is; empty for the reference


@dataclass(frozen=True)
class Check:
    subject: Subject
    grade: Grade

    @property
    def ok(self) -> bool:
        """The reference passed, or the mutant failed for a reason other than not building."""
        if self.subject.expect == "pass":
            return self.grade.passed
        return self.grade.outcome == "fail" and self.grade.category != "doesnt-build"


async def validate(tasks: list[Task], image: str | None = None, jobs: int = 1) -> list[Check]:
    """Grade every task's reference and mutants, `jobs` at a time, in the order they're listed."""
    limit = asyncio.Semaphore(jobs)

    async def check(subject: Subject) -> Check:
        async with limit:
            return Check(subject, await grade_subject(subject, image))

    return list(await asyncio.gather(*(check(s) for task in tasks for s in subjects(task))))


def subjects(task: Task) -> list[Subject]:
    """The task's reference and its mutants, or for a variant its original's. Raises
    ValidateError if there is no reference."""
    source = code_dir(task)
    if not (source / "reference").is_dir():
        raise ValidateError(f"{task.id}: no reference/ directory")
    reference = Subject(task, "reference", "pass", None, "")
    mutants = [
        Subject(task, patch.stem, "fail", patch, about(patch))
        for patch in sorted((source / "mutants").glob("*.patch"))
    ]
    return [reference, *mutants]


async def grade_subject(subject: Subject, image: str | None) -> Grade:
    """Write the subject's code into a scratch directory and grade it in a fresh sandbox."""
    with tempfile.TemporaryDirectory(prefix=f"{subject.task.id}-{subject.name}-") as scratch:
        source = Path(scratch) / "src"
        materialize(subject, source)
        return await grade_directory(subject.task, source, image)


def code_dir(task: Task) -> Path:
    """Where the task's reference and mutants are: its own directory, or its original's."""
    return task.path.parent / task.variant_of if task.variant_of else task.path


def materialize(subject: Subject, dest: Path) -> None:
    """Copy the reference to `dest`, apply a mutant's patch there, and for a variant, rename."""
    shutil.copytree(code_dir(subject.task) / "reference", dest)
    if subject.patch is not None:
        apply_patch(subject, dest)
    rename = subject.task.maelstrom.rename
    if subject.task.variant_of and rename:
        rename_in_code(dest, {**rename.types, **rename.fields})


def apply_patch(subject: Subject, dest: Path) -> None:
    """git apply the subject's patch in dest."""
    applied = subprocess.run(
        ["git", "apply", str(subject.patch.resolve())], cwd=dest, capture_output=True, text=True
    )
    if applied.returncode != 0:
        problem = applied.stderr.strip()
        raise ValidateError(f"{subject.task.id}: {subject.patch.name} doesn't apply: {problem}")


def rename_in_code(directory: Path, names: dict[str, str]) -> None:
    """Replace every Go string literal that is exactly one of `names` with its new name, in
    each .go file under directory."""
    literal = re.compile('"(' + "|".join(map(re.escape, names)) + ')"')
    for path in directory.rglob("*.go"):
        path.write_text(literal.sub(lambda m: f'"{names[m.group(1)]}"', path.read_text()))


def about(patch: Path) -> str:
    """The text before the diff: what the mutant's bug is, on one line."""
    lines = []
    for line in patch.read_text().splitlines():
        if line.startswith("diff --git"):
            break
        lines.append(line.strip())
    return " ".join(line for line in lines if line)


def summary(checks: list[Check]) -> str:
    """One line per check: task, subject, the grade it got, and whether that was the right one."""
    width = max((len(c.subject.task.id) + len(c.subject.name) + 1 for c in checks), default=0)
    lines = []
    for check in checks:
        grade = check.grade
        got = grade.outcome + (f" ({grade.category})" if grade.category else "")
        label = f"{check.subject.task.id}/{check.subject.name}"
        lines.append(f"{'ok' if check.ok else 'WRONG':<5}  {label:<{width}}  {got}")
        if not check.ok and check.subject.about:
            lines.append(f"       the bug: {check.subject.about}")
    wrong = sum(not check.ok for check in checks)
    lines.append(f"{len(checks) - wrong} of {len(checks)} as expected")
    return "\n".join(lines)
