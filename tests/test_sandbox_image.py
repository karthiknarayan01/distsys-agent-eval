"""The real sandbox image: a Go node builds with no network, and Maelstrom grades it there.
Run when SANDBOX_TEST_IMAGE is the built sandbox image; skipped otherwise, or when the image
has no Go."""

import os
from pathlib import Path

import pytest

from distsys_eval.maelstrom import MaelstromOptions, run_maelstrom
from distsys_eval.sandbox import Sandbox, SandboxConfig

IMAGE = os.environ.get("SANDBOX_TEST_IMAGE")
NODES = Path(__file__).parent / "nodes"
pytestmark = [
    pytest.mark.docker,
    pytest.mark.skipif(not IMAGE, reason="set SANDBOX_TEST_IMAGE to the sandbox image to run these"),
]
ECHO = MaelstromOptions("echo", {"node-count": 1, "time-limit": 5})


async def build(box: Sandbox, node: str) -> None:
    if not (await box.exec("command -v go", timeout=10)).ok:
        pytest.skip(f"{IMAGE} has no Go: not the sandbox image")
    await box.copy_in(NODES / node, node)
    built = await box.exec(f"cd {node} && go build -o node .", timeout=300)
    assert built.ok, built.output


async def test_a_correct_node_builds_offline_and_maelstrom_says_valid():
    async with Sandbox.start(SandboxConfig(image=IMAGE)) as box:
        await build(box, "echo-go")
        result = await run_maelstrom(box, "/work/echo-go/node", ECHO, run_dir="runs/1")
    assert result.verdict == "valid", result.output


async def test_a_wrong_node_is_invalid_on_the_workload():
    async with Sandbox.start(SandboxConfig(image=IMAGE)) as box:
        await build(box, "echo-go-wrong")
        result = await run_maelstrom(box, "/work/echo-go-wrong/node", ECHO, run_dir="runs/1")
    assert result.verdict == "invalid" and result.failed == ["workload"], result.output


async def test_a_binary_that_isnt_there_is_a_crash():
    async with Sandbox.start(SandboxConfig(image=IMAGE)) as box:
        if not (await box.exec("command -v maelstrom", timeout=10)).ok:
            pytest.skip(f"{IMAGE} has no Maelstrom: not the sandbox image")
        result = await run_maelstrom(box, "/work/missing", ECHO, run_dir="runs/1")
    assert result.verdict == "crashed"


async def test_the_tools_maelstroms_checkers_run_are_installed():
    """Without them a checker can't decide, and every run of the tasks that need them is unknown."""
    async with Sandbox.start(SandboxConfig(image=IMAGE)) as box:
        if not (await box.exec("command -v maelstrom", timeout=10)).ok:
            pytest.skip(f"{IMAGE} has no Maelstrom: not the sandbox image")
        for tool in ("gnuplot", "dot"):
            assert (await box.exec(f"command -v {tool}", timeout=10)).ok, f"{tool} is missing"
