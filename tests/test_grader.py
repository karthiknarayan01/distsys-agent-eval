"""Grading an attempt, against a scripted shell: building, the m runs, and the failure category."""

import re
from dataclasses import replace
from pathlib import Path

from distsys_eval.grader import describe, grade_attempt, without_faults
from distsys_eval.maelstrom import MaelstromOptions, Rename
from distsys_eval.sandbox import ExecResult
from distsys_eval.targets import Targets
from distsys_eval.tasks import TASKS_DIR, load_task

FIXTURES = Path(__file__).parent / "fixtures" / "maelstrom"
ECHO = load_task(TASKS_DIR / "echo")  # no faults, m = 3
UNIQUE_IDS = load_task(TASKS_DIR / "unique-ids")  # partitions, m = 3
BUILT = ExecResult(0, "", False, False)


class GradingShell:
    """Answers `go build` with `build`, each Maelstrom run with the next of `verdicts`, and
    anything else with success. A verdict is a recorded results file ("valid", "invalid",
    "unknown"), or None for a run that writes none."""

    def __init__(self, verdicts: list[str | None], build: ExecResult = BUILT):
        self.build = build
        self.verdicts = list(verdicts)
        self.files: dict[str, bytes] = {}
        self.commands: list[str] = []

    async def exec(self, command: str, timeout: float) -> ExecResult:
        self.commands.append(command)
        if command.startswith("go build"):
            return self.build
        if "maelstrom test" not in command:
            return BUILT  # housekeeping, such as removing a run's directory
        run_dir = re.match(r"mkdir -p (\S+) ", command).group(1)
        verdict = self.verdicts.pop(0)
        if verdict:
            self.files[f"{run_dir}/store/latest/results.edn"] = (FIXTURES / f"{verdict}.edn").read_bytes()
        return ExecResult({"valid": 0, "invalid": 1, "unknown": 2}.get(verdict, 255), "", False, False)

    async def read_file(self, path: str) -> bytes:
        if path not in self.files:
            raise FileNotFoundError(path)
        return self.files[path]

    def maelstrom_commands(self) -> list[str]:
        return [command for command in self.commands if "maelstrom test" in command]


async def test_every_run_valid_is_a_pass():
    shell = GradingShell(["valid"] * 3)
    grade = await grade_attempt(shell, ECHO)
    assert grade.passed and grade.category is None
    assert [run.verdict for run in grade.runs] == ["valid"] * 3
    assert shell.commands[0] == "go build -o /tmp/grade/node ."
    assert [re.match(r"mkdir -p (\S+)", c).group(1) for c in shell.maelstrom_commands()] == [
        "/tmp/grade/run-1", "/tmp/grade/run-2", "/tmp/grade/run-3",
    ]
    assert all("--bin /tmp/grade/node" in c for c in shell.maelstrom_commands())


async def test_a_build_failure_runs_nothing():
    shell = GradingShell([], build=ExecResult(1, "main.go:3: syntax error", False, False))
    grade = await grade_attempt(shell, ECHO)
    assert (grade.outcome, grade.category, grade.runs) == ("fail", "doesnt-build", [])
    assert shell.maelstrom_commands() == []


async def test_grading_stops_at_the_first_invalid_run():
    shell = GradingShell(["valid", "invalid"])
    grade = await grade_attempt(shell, ECHO)
    assert grade.outcome == "fail"
    assert [run.verdict for run in grade.runs] == ["valid", "invalid"]


async def test_with_no_faults_to_remove_an_invalid_run_fails_without_faults():
    grade = await grade_attempt(GradingShell(["invalid"]), ECHO)
    assert grade.category == "fails-without-faults" and grade.calm_run is None


async def test_a_valid_run_within_the_targets_counts_as_valid():
    task = replace(ECHO, targets=Targets(msgs_per_op=1))  # the echo results show 0 between nodes
    assert (await grade_attempt(GradingShell(["valid"] * 3), task)).passed


async def test_a_valid_run_over_a_target_is_too_slow_and_stops_grading():
    task = replace(ECHO, targets=Targets(msgs_per_op=1, max_latency_ms=600))  # echo has no latencies
    shell = GradingShell(["valid"] * 3)
    grade = await grade_attempt(shell, task)
    assert (grade.outcome, grade.category, len(grade.runs)) == ("fail", "too-slow", 1)
    assert grade.missed == ["max-latency-ms wasn't measured"]
    assert len(shell.maelstrom_commands()) == 1
    assert describe(task, grade).splitlines()[-1] == "  too slow: max-latency-ms wasn't measured"


async def test_a_run_with_no_verdict_is_a_crash():
    grade = await grade_attempt(GradingShell(["valid", None]), ECHO)
    assert (grade.outcome, grade.category) == ("fail", "crashed")


async def test_an_undecided_run_is_an_error_not_a_failure():
    grade = await grade_attempt(GradingShell(["unknown"]), ECHO)
    assert (grade.outcome, grade.category) == ("error", None)


async def test_valid_once_the_faults_are_off_fails_under_faults():
    shell = GradingShell(["invalid", "valid"])
    grade = await grade_attempt(shell, UNIQUE_IDS)
    assert (grade.outcome, grade.category) == ("fail", "fails-under-faults")
    assert len(grade.runs) == 1 and grade.calm_run.valid
    faulted, calm = shell.maelstrom_commands()
    assert "--nemesis partition" in faulted
    assert "mkdir -p /tmp/grade/without-faults" in calm and "--nemesis" not in calm


async def test_still_invalid_without_faults_fails_without_faults():
    grade = await grade_attempt(GradingShell(["valid", "invalid", "invalid"]), UNIQUE_IDS)
    assert grade.category == "fails-without-faults"
    assert len(grade.runs) == 2 and not grade.calm_run.valid


def test_without_faults_keeps_everything_but_the_nemesis():
    options = MaelstromOptions("broadcast", {"node-count": 5, "nemesis": "partition", "nemesis-interval": 3})
    assert without_faults(options) == MaelstromOptions("broadcast", {"node-count": 5})
    assert without_faults(MaelstromOptions("echo", {"node-count": 1})) is None


def test_without_faults_keeps_the_renamed_protocol():
    rename = Rename({"broadcast": "spread"})
    options = MaelstromOptions("broadcast", {"nemesis": "partition"}, rename)
    assert without_faults(options).rename == rename


async def test_describe_a_pass_and_a_diagnosed_failure():
    assert describe(ECHO, await grade_attempt(GradingShell(["valid"] * 3), ECHO)) == (
        "echo: pass, 3 of 3 runs valid\n  run 1: valid\n  run 2: valid\n  run 3: valid"
    )
    grade = await grade_attempt(GradingShell(["invalid", "valid"]), UNIQUE_IDS)
    assert describe(UNIQUE_IDS, grade) == (
        "unique-ids: fail (fails-under-faults), 0 of 3 runs valid\n"
        "  run 1: invalid (workload)\n"
        "  without faults: valid"
    )


async def test_describe_a_build_failure_shows_the_compiler_output():
    build = ExecResult(1, "# node\n./main.go:3:1: syntax error\n", False, False)
    text = describe(ECHO, await grade_attempt(GradingShell([], build=build), ECHO))
    assert text == "echo: fail (doesnt-build)\ngo build failed:\n  # node\n  ./main.go:3:1: syntax error"
