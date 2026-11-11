"""The Shellmate agent against a scripted model, working in a directory on this machine."""

import json
from dataclasses import replace

import pytest
from runtime.model import ModelError

from distsys_eval.sandbox import ExecResult, SandboxError
from distsys_eval.shellmate import MAX_OUTPUT_CHARS, SYSTEM_PROMPT, ShellmateAgent, clip, shell_result
from distsys_eval.tasks import TASKS_DIR, AgentLimits, load_task
from tests.fake_model import ScriptedModel, call, say
from tests.fake_shell import HostShell

ECHO = load_task(TASKS_DIR / "echo")


def limited(minutes: float = 5, steps: int = 10):
    return replace(ECHO, limits=AgentLimits(minutes, steps))


def read_transcript(path):
    return [json.loads(line) for line in path.read_text().splitlines()]


async def test_an_attempt_that_finishes(tmp_path):
    work = tmp_path / "work"
    work.mkdir()
    model = ScriptedModel(
        call("write_file", path="src/hello.txt", content="hi\n"),
        call("shell", command="cat src/hello.txt; exit 3"),
        say("done"),
    )
    agent = ShellmateAgent(model, "shellmate/fake")
    run = await agent.attempt(limited(), HostShell(work), tmp_path / "t.jsonl")

    assert (run.stop, run.steps, run.tool_calls) == ("finished", 3, 2)
    assert (run.input_tokens, run.output_tokens) == (300, 30)
    assert (work / "src" / "hello.txt").read_text() == "hi\n"
    system, messages, tools = model.requests[-1]
    assert system == SYSTEM_PROMPT
    assert messages[0].text == ECHO.brief
    assert [t.name for t in tools] == ["shell", "read_file", "write_file"]
    assert messages[-1].text == "exit code 3\nhi\n"


async def test_the_transcript_says_what_was_attempted_then_holds_every_event(tmp_path):
    agent = ShellmateAgent(ScriptedModel(call("shell", command="true"), say("done")), "shellmate/fake")
    await agent.attempt(limited(), HostShell(tmp_path), tmp_path / "runs" / "t.jsonl")

    header, *events = read_transcript(tmp_path / "runs" / "t.jsonl")
    assert header == {
        "type": "attempt", "agent": "shellmate/fake", "task": "echo",
        "limits": {"minutes": 5, "steps": 10}, "system": SYSTEM_PROMPT,
    }
    assert [e["type"] for e in events] == [
        "user_message", "assistant_message", "tool_started", "tool_finished", "assistant_message",
    ]
    assert events[0]["payload"]["text"] == ECHO.brief


async def test_a_model_that_never_stops_runs_out_of_steps(tmp_path):
    agent = ShellmateAgent(ScriptedModel(call("shell", command="true")), "shellmate/fake")
    run = await agent.attempt(limited(steps=4), HostShell(tmp_path), tmp_path / "t.jsonl")
    assert (run.stop, run.steps, run.tool_calls) == ("out-of-steps", 4, 4)
    assert run.detail == "stopped after 4 steps"


async def test_a_turn_still_running_at_the_time_limit_is_cut_off(tmp_path):
    agent = ShellmateAgent(ScriptedModel(call("shell", command="sleep 5")), "shellmate/fake")
    run = await agent.attempt(limited(minutes=0.01), HostShell(tmp_path), tmp_path / "t.jsonl")

    assert run.stop == "out-of-time"
    assert run.seconds < 3
    *_, finished, stopped = read_transcript(tmp_path / "t.jsonl")
    assert finished["payload"]["interrupted"] is True  # the sleep that was running
    assert stopped["payload"] == {"reason": "cancelled"}


async def test_a_model_that_cant_be_reached_is_a_model_error(tmp_path):
    agent = ShellmateAgent(ScriptedModel(ModelError("503 from the server")), "shellmate/fake")
    run = await agent.attempt(limited(), HostShell(tmp_path), tmp_path / "t.jsonl")
    assert (run.stop, run.steps, run.detail) == ("model-error", 0, "503 from the server")


async def test_a_broken_sandbox_stops_the_attempt_and_is_raised(tmp_path):
    class Broken(HostShell):
        async def exec(self, command, timeout):
            raise SandboxError("the container is gone")

    model = ScriptedModel(call("shell", command="ls"), say("done"))
    agent = ShellmateAgent(model, "shellmate/fake")
    with pytest.raises(SandboxError, match="the container is gone"):
        await agent.attempt(limited(), Broken(tmp_path), tmp_path / "t.jsonl")
    assert len(model.requests) == 1  # the model wasn't asked again after the tool broke


async def test_a_file_that_isnt_there_is_an_error_the_model_sees(tmp_path):
    model = ScriptedModel(call("read_file", path="nope.go"), say("done"))
    agent = ShellmateAgent(model, "shellmate/fake")
    run = await agent.attempt(limited(), HostShell(tmp_path), tmp_path / "t.jsonl")
    assert run.stop == "finished"
    result = model.requests[-1][1][-1]
    assert result.is_error and "FileNotFoundError" in result.text


def test_shell_result_says_how_the_command_ended():
    assert shell_result(ExecResult(0, "", False, False), 120) == "exit code 0"
    assert shell_result(ExecResult(124, "partial\n", True, False), 30) == "timed out after 30s\npartial\n"


def test_clip_keeps_the_start_and_the_end():
    text = "a" * MAX_OUTPUT_CHARS + "the error"
    clipped = clip(text)
    assert clipped.startswith("aaa") and clipped.endswith("the error")
    assert "characters cut" in clipped
    assert clip("short") == "short"
