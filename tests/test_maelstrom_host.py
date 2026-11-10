"""Maelstrom for real, on this machine, against the Python echo nodes. Skipped unless
`maelstrom` (and Java and gnuplot) are on PATH; CI runs the same checks in the sandbox image."""

import shutil
from pathlib import Path

import pytest

from distsys_eval.maelstrom import MaelstromOptions, run_maelstrom
from tests.fake_shell import HostShell

NODES = Path(__file__).parent / "nodes"
ECHO = MaelstromOptions("echo", {"node-count": 1, "time-limit": 3})
pytestmark = pytest.mark.skipif(not shutil.which("maelstrom"), reason="maelstrom isn't on PATH")


async def test_a_correct_node_is_valid(tmp_path):
    result = await run_maelstrom(HostShell(tmp_path), str(NODES / "echo.py"), ECHO, run_dir="runs/1")
    assert result.verdict == "valid", result.output


async def test_a_wrong_node_is_invalid(tmp_path):
    result = await run_maelstrom(HostShell(tmp_path), str(NODES / "echo_wrong.py"), ECHO, run_dir="runs/1")
    assert result.verdict == "invalid" and result.failed == ["workload"]


async def test_runs_in_separate_directories_keep_separate_verdicts(tmp_path):
    shell = HostShell(tmp_path)
    good = await run_maelstrom(shell, str(NODES / "echo.py"), ECHO, run_dir="runs/good")
    bad = await run_maelstrom(shell, str(NODES / "echo_wrong.py"), ECHO, run_dir="runs/bad")
    assert (good.verdict, bad.verdict) == ("valid", "invalid")
