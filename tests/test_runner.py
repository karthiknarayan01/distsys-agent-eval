"""The runner, with a scripted agent and a scripted sandbox: which attempts run, what each
records, and carrying on after a stop."""

import asyncio
from contextlib import asynccontextmanager
from dataclasses import replace

import pytest

from distsys_eval.agent import AgentRun
from distsys_eval.results import RESULTS_FILE, AttemptResult, append, load
from distsys_eval.runner import RunnerError, run
from distsys_eval.sandbox import SandboxError
from distsys_eval.tasks import TASKS_DIR, load_task
from tests.test_grader import GradingShell

ECHO = load_task(TASKS_DIR / "echo")
UNIQUE_IDS = load_task(TASKS_DIR / "unique-ids")
FINISHED = AgentRun("finished", steps=5, tool_calls=4, input_tokens=900, output_tokens=90, seconds=12.34)


class Box(GradingShell):
    """A sandbox that grades with the given verdicts and holds the files the agent wrote."""

    async def write_file(self, path, content):
        self.files[path] = content.encode() if isinstance(content, str) else content

    async def copy_out(self, source, dest):
        dest.mkdir(parents=True, exist_ok=True)
        for path, content in self.files.items():
            if not path.startswith("/"):
                (dest / path).write_bytes(content)


def sandbox(verdicts=("valid",) * 3, broken=False):
    """A sandbox factory: each sandbox grades with `verdicts`, or fails to start if `broken`."""

    @asynccontextmanager
    async def start():
        if broken:
            raise SandboxError("couldn't start a container")
        yield Box(list(verdicts))

    return start


class Agent:
    """Writes main.go and a transcript, and returns `outcome`. Tracks how many attempts ran at once."""

    def __init__(self, outcome=FINISHED, name="shellmate/fake"):
        self.name = name
        self.outcome = outcome
        self.tried: list[str] = []
        self.running = self.most_at_once = 0

    async def attempt(self, task, workspace, transcript):
        self.tried.append(task.id)
        self.running += 1
        self.most_at_once = max(self.most_at_once, self.running)
        await asyncio.sleep(0.01)
        self.running -= 1
        await workspace.write_file("main.go", "package main\n")
        transcript.parent.mkdir(parents=True, exist_ok=True)
        transcript.write_text("{}\n")
        return self.outcome


async def test_every_attempt_is_run_graded_and_recorded(tmp_path):
    agent = Agent()
    results = await run(agent, [ECHO, UNIQUE_IDS], 2, tmp_path, sandbox=sandbox())

    assert [(r.task, r.attempt, r.outcome) for r in results] == [
        ("echo", 1, "pass"), ("echo", 2, "pass"), ("unique-ids", 1, "pass"), ("unique-ids", 2, "pass"),
    ]
    first = results[0]
    assert (first.stop, first.steps, first.tool_calls, first.agent_seconds) == ("finished", 5, 4, 12.3)
    assert (first.runs, first.runs_required) == (["valid"] * 3, 3)
    assert (tmp_path / first.transcript).exists()
    assert (tmp_path / first.code / "main.go").read_text() == "package main\n"
    assert load(tmp_path / RESULTS_FILE) == results


async def test_a_failed_attempt_records_why(tmp_path):
    [result] = await run(Agent(), [ECHO], 1, tmp_path, sandbox=sandbox(["valid", "invalid"]))
    assert (result.outcome, result.category) == ("fail", "fails-without-faults")
    assert result.runs == ["valid", "invalid"]


async def test_an_agent_out_of_steps_is_still_graded(tmp_path):
    agent = Agent(replace(FINISHED, stop="out-of-steps"))
    [result] = await run(agent, [ECHO], 1, tmp_path, sandbox=sandbox())
    assert (result.outcome, result.stop) == ("pass", "out-of-steps")


async def test_a_model_error_is_an_error_and_isnt_graded(tmp_path):
    agent = Agent(replace(FINISHED, stop="model-error", detail="Ollama isn't running"))
    [result] = await run(agent, [ECHO], 1, tmp_path, sandbox=sandbox(verdicts=[]))
    assert (result.outcome, result.error, result.runs) == ("error", "model: Ollama isn't running", [])


async def test_a_run_maelstrom_couldnt_decide_is_an_error(tmp_path):
    [result] = await run(Agent(), [ECHO], 1, tmp_path, sandbox=sandbox(["unknown"]))
    assert (result.outcome, result.error) == ("error", "maelstrom couldn't decide a run")


async def test_a_sandbox_that_wont_start_is_an_error(tmp_path):
    [result] = await run(Agent(), [ECHO], 1, tmp_path, sandbox=sandbox(broken=True))
    assert (result.outcome, result.error) == ("error", "sandbox: couldn't start a container")
    assert (result.stop, result.transcript, result.code) == (None, None, None)


async def test_a_second_run_does_only_whats_missing_and_retries_errors(tmp_path):
    results_file = tmp_path / RESULTS_FILE
    append(results_file, AttemptResult("echo", 1, "shellmate/fake", "fail", category="crashed"))
    append(results_file, AttemptResult("echo", 2, "shellmate/fake", "error", error="sandbox: gone"))
    agent = Agent()

    results = await run(agent, [ECHO], 3, tmp_path, sandbox=sandbox())

    assert agent.tried == ["echo", "echo"]  # attempts 2 and 3
    assert [(r.attempt, r.outcome) for r in results] == [(1, "fail"), (2, "pass"), (3, "pass")]


async def test_another_agents_results_are_never_mixed_in(tmp_path):
    append(tmp_path / RESULTS_FILE, AttemptResult("echo", 1, "shellmate/other", "pass"))
    with pytest.raises(RunnerError, match="shellmate/other"):
        await run(Agent(), [ECHO], 1, tmp_path, sandbox=sandbox())


async def test_attempts_run_jobs_at_a_time(tmp_path):
    agent = Agent()
    await run(agent, [ECHO, UNIQUE_IDS], 3, tmp_path, jobs=2, sandbox=sandbox())
    assert agent.most_at_once == 2


async def test_each_attempt_is_reported_as_it_is_recorded(tmp_path):
    seen = []
    await run(Agent(), [ECHO], 2, tmp_path, sandbox=sandbox(), on_result=seen.append)
    assert [r.attempt for r in seen] == [1, 2]
