"""calibrate: the arithmetic that turns catch counts into m, measuring a subject over many runs
against a scripted shell, and writing the result."""

import re
from dataclasses import replace
from pathlib import Path

import pytest

from distsys_eval.calibrate import (
    Calibration,
    Measurement,
    measure,
    runs_needed,
    set_runs,
    summary,
    wilson_lower,
    write,
)
from distsys_eval.sandbox import ExecResult
from distsys_eval.tasks import TASKS_DIR, load_task
from distsys_eval.validate import Subject
from tests.test_grader import GradingShell

ECHO = load_task(TASKS_DIR / "echo")
BUILT = ExecResult(0, "", False, False)
BROKEN = ExecResult(1, "syntax error", False, False)
REFERENCE = Subject(ECHO, "reference", "pass", None, "")
MUTANT = Subject(ECHO, "bug", "fail", None, "A bug.")


# The arithmetic


def test_the_lower_bound_stays_below_a_perfect_score():
    assert wilson_lower(20, 20) == pytest.approx(0.881, abs=0.001)
    assert wilson_lower(100, 100) == pytest.approx(0.974, abs=0.001)
    assert wilson_lower(0, 20) == 0.0 and wilson_lower(0, 0) == 0.0
    assert wilson_lower(10, 20) < 0.5


def test_runs_needed():
    assert runs_needed(20, 20) == 3  # 1 - (1 - 0.881)^3 = 0.998; two runs give 0.986
    assert runs_needed(100, 100) == 2
    assert runs_needed(18, 20) == 4
    assert runs_needed(10, 20) == 12
    assert runs_needed(2, 20) is None  # would need far more than MAX_RUNS
    assert runs_needed(0, 20) is None


# Measuring


async def test_measure_counts_every_run_and_leaves_out_undecided_ones():
    shell = GradingShell(["valid", "invalid", None, "unknown", "invalid"])
    measured = await measure(shell, MUTANT, trials=5)
    assert (measured.failed, measured.decided) == (3, 4)
    runs = [c for c in shell.commands if "maelstrom test" in c]
    assert len(runs) == 5 and all("--bin /tmp/grade/node" in c for c in runs)
    assert "rm -rf /tmp/grade/trial-5" in shell.commands


async def test_a_subject_that_doesnt_build_runs_nothing():
    shell = GradingShell([], build=BROKEN)
    measured = await measure(shell, MUTANT, trials=5)
    assert not measured.built and measured.decided == 0
    assert not any("maelstrom" in c for c in shell.commands)


# Choosing m and reporting it


def calibration(reference_failed=0, caught=(20, 20), mutant_built=True):
    reference = Measurement(REFERENCE, BUILT, reference_failed, 20)
    mutants = [Measurement(replace(MUTANT, name=f"bug-{i}"), BUILT if mutant_built else BROKEN, c, 20)
               for i, c in enumerate(caught)]
    return Calibration(ECHO, 20, reference, mutants)


def test_a_task_needs_the_largest_m_any_of_its_mutants_needs():
    assert calibration(caught=(20, 20)).runs == 3
    assert calibration(caught=(20, 10)).runs == 12
    assert calibration(caught=(20, 2)).runs is None
    assert calibration(mutant_built=False).runs is None


def test_summary():
    assert summary([calibration(reference_failed=1, caught=(20, 10))]) == (
        "echo: m = 12 (task.toml has 3), 20 trials each\n"
        "  reference: failed in 1 of 20 runs\n"
        "  bug-0: caught in 20 of 20 runs, needs m = 3\n"
        "  bug-1: caught in 10 of 20 runs, needs m = 12"
    )


# Writing


def copy_task(tmp_path: Path):
    path = tmp_path / "echo"
    path.mkdir()
    (path / "task.toml").write_text((ECHO.path / "task.toml").read_text())
    return replace(ECHO, path=path)


def test_write_records_the_counts_and_sets_runs(tmp_path):
    task = copy_task(tmp_path)
    result = replace(calibration(caught=(20, 10)), task=task)
    write(result)
    toml = (task.path / "task.toml").read_text()
    assert re.search(r"^runs = 12$", toml, re.MULTILINE)
    assert toml.replace("runs = 12", "runs = 3") == (ECHO.path / "task.toml").read_text()
    recorded = (task.path / "calibration.toml").read_text()
    assert "trials = 20\nruns = 12\n" in recorded
    assert '[mutants."bug-1"]\ncaught = 10\ndecided = 20' in recorded


def test_write_refuses_a_flaky_reference_or_a_task_with_no_m(tmp_path):
    task = copy_task(tmp_path)
    with pytest.raises(ValueError, match="not calibrated"):
        write(replace(calibration(reference_failed=1), task=task))
    with pytest.raises(ValueError, match="not calibrated"):
        write(replace(calibration(caught=(0, 20)), task=task))
    assert not (task.path / "calibration.toml").exists()


def test_set_runs_needs_exactly_one_runs_line(tmp_path):
    toml = tmp_path / "task.toml"
    toml.write_text('title = "x"\n')
    with pytest.raises(ValueError, match="found 0"):
        set_runs(toml, 3)
