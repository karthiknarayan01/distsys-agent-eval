"""One Maelstrom run, against a scripted shell: the command line, and reading the verdict."""

import shlex
from pathlib import Path

import pytest

from distsys_eval.maelstrom import STARTUP_SECONDS, MaelstromOptions, maelstrom_command, run_maelstrom
from distsys_eval.sandbox import ExecResult
from tests.fake_shell import ScriptedShell

FIXTURES = Path(__file__).parent / "fixtures" / "maelstrom"
RESULTS = "runs/1/store/latest/results.edn"


def finished(code: int, fixture: str | None) -> ScriptedShell:
    files = {RESULTS: (FIXTURES / f"{fixture}.edn").read_bytes()} if fixture else {}
    return ScriptedShell(ExecResult(code, "...Maelstrom's log...", False, False), files)


def test_the_command_line():
    options = MaelstromOptions("broadcast", {"time-limit": 20, "node-count": 5, "nemesis": "partition",
                                             "log-stderr": True, "topology": False})
    assert shlex.split(maelstrom_command("/work/node", options)) == [
        "maelstrom", "test", "--workload", "broadcast", "--bin", "/work/node",
        "--log-stderr", "--nemesis", "partition", "--node-count", "5", "--time-limit", "20",
    ]


def test_values_are_shell_quoted():
    command = maelstrom_command("/work/my node", MaelstromOptions("echo", {"nemesis": "partition; rm -rf /"}))
    assert shlex.split(command)[5] == "/work/my node"
    assert shlex.split(command)[-1] == "partition; rm -rf /"


def test_unknown_flags_are_refused():
    with pytest.raises(ValueError, match="node-cuont"):
        MaelstromOptions("echo", {"node-cuont": 3})


async def test_it_runs_in_its_own_directory_with_time_to_spare():
    shell = finished(0, "valid")
    await run_maelstrom(shell, "/work/node", MaelstromOptions("echo", {"time-limit": 10}), run_dir="runs/1")
    ((command, timeout),) = shell.commands
    assert command.startswith("mkdir -p runs/1 && cd runs/1 && maelstrom test")
    assert timeout == 10 + STARTUP_SECONDS


async def test_a_valid_run():
    result = await run_maelstrom(finished(0, "valid"), "/work/node", MaelstromOptions("echo"), "runs/1")
    assert result.valid and result.verdict == "valid"
    assert result.failed == [] and result.undecided == []
    assert result.results["availability"]["ok-fraction"] == 1.0


async def test_an_invalid_run_names_the_checker_that_failed():
    result = await run_maelstrom(finished(1, "invalid"), "/work/node", MaelstromOptions("echo"), "runs/1")
    assert result.verdict == "invalid" and not result.valid
    assert result.failed == ["workload"]


async def test_a_run_no_checker_could_decide():
    result = await run_maelstrom(finished(2, "unknown"), "/work/node", MaelstromOptions("echo"), "runs/1")
    assert result.verdict == "unknown"
    assert result.undecided == ["perf"]


async def test_no_results_file_is_a_crash():
    result = await run_maelstrom(finished(255, None), "/work/node", MaelstromOptions("echo"), "runs/1")
    assert result.verdict == "crashed" and result.results == {}
    assert result.output == "...Maelstrom's log..."


async def test_a_timed_out_run_is_a_crash_even_with_a_results_file():
    shell = finished(124, "valid")
    shell.result = ExecResult(124, "", True, False)
    result = await run_maelstrom(shell, "/work/node", MaelstromOptions("echo"), "runs/1")
    assert result.verdict == "crashed" and result.timed_out
