"""One attempt through the runner in the real sandbox image, with a scripted Shellmate model.
Run when SANDBOX_TEST_IMAGE is the built sandbox image; skipped otherwise."""

import os

import pytest

from distsys_eval.runner import run, sandboxes
from distsys_eval.shellmate import ShellmateAgent
from distsys_eval.tasks import TASKS_DIR, load_task
from tests.fake_model import ScriptedModel, call, say

IMAGE = os.environ.get("SANDBOX_TEST_IMAGE")
ECHO = load_task(TASKS_DIR / "echo")
pytestmark = [
    pytest.mark.docker,
    pytest.mark.skipif(not IMAGE, reason="set SANDBOX_TEST_IMAGE to the sandbox image to run these"),
]


async def test_an_attempt_is_run_graded_and_its_code_kept(tmp_path):
    reference = TASKS_DIR / "echo" / "reference"
    writes = [call("write_file", path=f.name, content=f.read_text()) for f in sorted(reference.iterdir())]
    agent = ShellmateAgent(ScriptedModel(*writes, say("done")), "shellmate/fake")

    [result] = await run(agent, [ECHO], 1, tmp_path, sandbox=sandboxes(IMAGE))

    assert (result.outcome, result.stop, result.runs) == ("pass", "finished", ["valid"] * ECHO.runs)
    assert (tmp_path / result.code / "main.go").read_text() == (reference / "main.go").read_text()
    assert (tmp_path / result.transcript).read_text().startswith('{"type": "attempt"')
