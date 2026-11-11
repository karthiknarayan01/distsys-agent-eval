"""validate: the shipped references and mutants are in order, and checks are judged and
summarised right. Grading itself is faked here; the CI sandbox job runs the real thing."""

import asyncio
import shutil
from dataclasses import replace

import pytest

from distsys_eval import validate as validate_module
from distsys_eval.grader import Grade
from distsys_eval.sandbox import ExecResult
from distsys_eval.tasks import TASKS_DIR, load_task, load_tasks
from distsys_eval.validate import Check, Subject, ValidateError, materialize, subjects, summary, validate

ECHO = load_task(TASKS_DIR / "echo")
BUILT = ExecResult(0, "", False, False)


def grade(outcome, category=None):
    return Grade(outcome, category, BUILT, [])


# The shipped references and mutants


@pytest.mark.parametrize("task", load_tasks(), ids=lambda task: task.id)
def test_every_task_has_a_reference_and_at_least_two_mutants(task):
    reference, *mutants = subjects(task)
    assert reference.name == "reference" and reference.expect == "pass"
    assert len(mutants) >= 2
    assert all(mutant.expect == "fail" and mutant.about for mutant in mutants)


@pytest.mark.skipif(not shutil.which("git"), reason="applying a mutant needs git")
@pytest.mark.parametrize(
    "subject", [s for task in load_tasks() for s in subjects(task)[1:]], ids=lambda s: f"{s.task.id}/{s.name}"
)
def test_every_mutant_applies_and_changes_the_reference(subject, tmp_path):
    materialize(subject, tmp_path / "src")
    reference = subject.task.path / "reference"
    changed = [path.name for path in (tmp_path / "src").iterdir()
               if path.read_bytes() != (reference / path.name).read_bytes()]
    assert changed, "the patch applied but changed nothing"


def test_the_about_line_is_the_text_before_the_diff():
    (mutant,) = [s for s in subjects(ECHO) if s.name == "echo-from-wrong-field"]
    assert mutant.about == (
        "Copies the payload from a field named message instead of echo, so every reply echoes null."
    )


# Set-up errors


def test_a_task_with_no_reference_is_refused(tmp_path):
    with pytest.raises(ValidateError, match="echo: no reference/"):
        subjects(replace(ECHO, path=tmp_path))


@pytest.mark.skipif(not shutil.which("git"), reason="applying a mutant needs git")
def test_a_patch_that_doesnt_apply_is_refused(tmp_path):
    patch = tmp_path / "stale.patch"
    patch.write_text("A stale mutant.\n\ndiff --git a/main.go b/main.go\n--- a/main.go\n+++ b/main.go\n"
                     "@@ -1 +1 @@\n-not in the file\n+anything\n")
    with pytest.raises(ValidateError, match="stale.patch doesn't apply"):
        materialize(Subject(ECHO, "stale", "fail", patch, "A stale mutant."), tmp_path / "src")


# Judging and summarising


def test_a_reference_must_pass_and_a_mutant_must_fail_for_a_real_reason():
    reference = Subject(ECHO, "reference", "pass", None, "")
    mutant = Subject(ECHO, "bug", "fail", None, "A bug.")
    assert Check(reference, grade("pass")).ok
    assert not Check(reference, grade("fail", "crashed")).ok
    assert Check(mutant, grade("fail", "fails-without-faults")).ok
    assert Check(mutant, grade("fail", "crashed")).ok
    assert not Check(mutant, grade("pass")).ok
    assert not Check(mutant, grade("fail", "doesnt-build")).ok
    assert not Check(mutant, grade("error")).ok


def test_summary_marks_what_came_out_wrong_and_says_why_the_mutant_should_fail():
    checks = [
        Check(Subject(ECHO, "reference", "pass", None, ""), grade("pass")),
        Check(Subject(ECHO, "bug", "fail", None, "Drops the reply."), grade("pass")),
    ]
    assert summary(checks) == (
        "ok     echo/reference  pass\n"
        "WRONG  echo/bug        pass\n"
        "       the bug: Drops the reply.\n"
        "1 of 2 as expected"
    )


async def test_validate_grades_every_subject_at_most_jobs_at_a_time(monkeypatch):
    running, most = 0, 0

    async def fake_grade_subject(subject, image):
        nonlocal running, most
        running += 1
        most = max(most, running)
        await asyncio.sleep(0.01)
        running -= 1
        return grade("pass" if subject.expect == "pass" else "fail", None if subject.expect == "pass"
                     else "fails-without-faults")

    monkeypatch.setattr(validate_module, "grade_subject", fake_grade_subject)
    tasks = load_tasks()
    checks = await validate(tasks, "sandbox:test", jobs=2)
    first = [s.name for s in subjects(tasks[0])]
    assert [c.subject.name for c in checks][: len(first)] == first  # in the order they're listed
    assert all(check.ok for check in checks) and most == 2


