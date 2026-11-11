"""The command agent: it hands the command the brief, runs it under the task's time, and turns
how it exited into how the attempt ended. Run on this machine through HostShell."""

import json
from dataclasses import replace

from distsys_eval.command import CommandAgent
from distsys_eval.tasks import TASKS_DIR, AgentLimits, load_task
from tests.fake_shell import HostShell

ECHO = load_task(TASKS_DIR / "echo")


def agent(tmp_path, command: str) -> CommandAgent:
    return CommandAgent("cli/fake", command, brief_path=str(tmp_path / "brief.md"))


async def test_the_command_reads_the_brief_and_works_in_the_working_directory(tmp_path):
    work = tmp_path / "work"
    work.mkdir()
    run = await agent(tmp_path, 'head -1 "$BRIEF" && echo package main > main.go').attempt(
        ECHO, HostShell(work), tmp_path / "t.jsonl"
    )
    assert run.stop == "finished" and run.steps == 0 and run.input_tokens == 0
    assert (work / "main.go").read_text() == "package main\n"
    header, output = [json.loads(line) for line in (tmp_path / "t.jsonl").read_text().splitlines()]
    assert header["agent"] == "cli/fake" and header["task"] == "echo" and "head -1" in header["command"]
    assert output == {"type": "output", "exit_code": 0, "text": "# Your workspace\n"}


async def test_running_past_the_tasks_minutes_is_out_of_time(tmp_path):
    quick = replace(ECHO, limits=AgentLimits(minutes=0.01, steps=1))
    run = await agent(tmp_path, "sleep 5").attempt(quick, HostShell(tmp_path), tmp_path / "t.jsonl")
    assert run.stop == "out-of-time" and "0.01 minutes" in run.detail


async def test_a_command_that_fails_is_a_model_error_with_the_end_of_its_output(tmp_path):
    run = await agent(tmp_path, "echo ANTHROPIC_API_KEY is not set; exit 3").attempt(
        ECHO, HostShell(tmp_path), tmp_path / "t.jsonl"
    )
    assert run.stop == "model-error"
    assert run.detail == "the command exited 3: ANTHROPIC_API_KEY is not set"
