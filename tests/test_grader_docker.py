"""Grading in the real sandbox image: the shipped echo task against nodes whose answer is known.
Run when SANDBOX_TEST_IMAGE is the built sandbox image; skipped otherwise."""

import os
import shutil
from dataclasses import replace
from pathlib import Path

import pytest

from distsys_eval.grader import grade_directory
from distsys_eval.tasks import TASKS_DIR, load_task

IMAGE = os.environ.get("SANDBOX_TEST_IMAGE")
NODES = Path(__file__).parent / "nodes"
ECHO = load_task(TASKS_DIR / "echo")
pytestmark = [
    pytest.mark.docker,
    pytest.mark.skipif(not IMAGE, reason="set SANDBOX_TEST_IMAGE to the sandbox image to run these"),
]


async def test_a_correct_echo_node_passes_the_echo_task():
    grade = await grade_directory(ECHO, NODES / "echo-go", IMAGE)
    assert grade.passed, [run.output for run in grade.runs]
    assert len(grade.runs) == ECHO.runs


async def test_a_wrong_echo_node_fails_without_faults():
    grade = await grade_directory(replace(ECHO, runs=1), NODES / "echo-go-wrong", IMAGE)
    assert (grade.outcome, grade.category) == ("fail", "fails-without-faults")
    assert grade.runs[0].failed == ["workload"]


async def test_code_that_doesnt_compile_doesnt_build(tmp_path):
    shutil.copy(NODES / "echo-go" / "go.mod", tmp_path)
    (tmp_path / "main.go").write_text("package main\n\nfunc main() {\n")
    grade = await grade_directory(ECHO, tmp_path, IMAGE)
    assert (grade.outcome, grade.category) == ("fail", "doesnt-build")
    assert "main.go" in grade.build.output
