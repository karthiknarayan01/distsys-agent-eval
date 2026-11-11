"""The Shellmate agent's tools in the real sandbox: an attempt that writes the echo reference
through them is graded like any other. Run when SANDBOX_TEST_IMAGE is the built sandbox image."""

import os

import pytest

from distsys_eval.grader import grade_attempt
from distsys_eval.sandbox import Sandbox, SandboxConfig
from distsys_eval.shellmate import ShellmateAgent
from distsys_eval.tasks import TASKS_DIR, load_task
from tests.fake_model import ScriptedModel, call, say

IMAGE = os.environ.get("SANDBOX_TEST_IMAGE")
ECHO = load_task(TASKS_DIR / "echo")
pytestmark = [
    pytest.mark.docker,
    pytest.mark.skipif(not IMAGE, reason="set SANDBOX_TEST_IMAGE to the sandbox image to run these"),
]


async def test_code_written_through_the_tools_is_graded(tmp_path):
    reference = TASKS_DIR / "echo" / "reference"
    writes = [call("write_file", path=f.name, content=f.read_text()) for f in sorted(reference.iterdir())]
    model = ScriptedModel(*writes, call("shell", command="go build -o node . && echo built"), say("done"))
    agent = ShellmateAgent(model, "shellmate/fake")

    async with Sandbox.start(SandboxConfig(image=IMAGE)) as box:
        run = await agent.attempt(ECHO, box, tmp_path / "t.jsonl")
        grade = await grade_attempt(box, ECHO)

    assert run.stop == "finished"
    assert model.requests[-1][1][-1].text == "exit code 0\nbuilt\n"
    assert grade.passed, [r.output for r in grade.runs]
