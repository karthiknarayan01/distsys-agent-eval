"""The sandbox against a real Docker daemon. These need an image with a POSIX shell and
coreutils (`timeout`, `tar`), named in SANDBOX_TEST_IMAGE; without it they are skipped.
CI runs them against the real sandbox image, which also runs the toolchain checks."""

import asyncio
import os
import subprocess
import time

import pytest

from distsys_eval.sandbox import Sandbox, SandboxConfig

IMAGE = os.environ.get("SANDBOX_TEST_IMAGE")
pytestmark = [
    pytest.mark.docker,
    pytest.mark.skipif(not IMAGE, reason="set SANDBOX_TEST_IMAGE to an image to run these"),
]


def sandbox():
    return Sandbox.start(SandboxConfig(image=IMAGE, cpus=1, memory="512m"))


async def test_a_command_returns_its_output_and_exit_code():
    async with sandbox() as box:
        ok = await box.exec("echo out; echo err >&2", timeout=30)
        failing = await box.exec("exit 7", timeout=30)
    assert ok.ok and ok.output == "out\nerr\n"
    assert failing.exit_code == 7 and not failing.timed_out


async def test_a_command_past_its_timeout_is_stopped_with_its_children():
    async with sandbox() as box:
        started = time.monotonic()
        result = await box.exec("sleep 60 & sleep 60", timeout=1)
        elapsed = time.monotonic() - started
        running = await box.exec("cat /proc/[0-9]*/cmdline", timeout=10)
    assert result.timed_out and elapsed < 20
    assert "sleep\x0060" not in running.output  # neither sleep is left behind


async def test_files_round_trip(tmp_path):
    (tmp_path / "src").mkdir()
    (tmp_path / "src" / "main.go").write_text("package main\n")
    async with sandbox() as box:
        await box.write_file("notes/plan.txt", "one\ntwo\n")
        assert await box.read_file("notes/plan.txt") == b"one\ntwo\n"
        with pytest.raises(FileNotFoundError):
            await box.read_file("missing.txt")
        await box.copy_in(tmp_path, "project")
        assert (await box.exec("cat project/src/main.go", timeout=10)).output == "package main\n"
        await box.copy_out("project", tmp_path / "back")
    assert (tmp_path / "back" / "src" / "main.go").read_text() == "package main\n"


async def test_there_is_no_network():
    async with sandbox() as box:
        result = await box.exec("ls /sys/class/net", timeout=10)
    assert result.output.split() == ["lo"]


async def test_the_container_is_gone_afterwards():
    async with sandbox() as box:
        container = box.container_id
    listed = subprocess.run(["docker", "ps", "-aq", "--no-trunc"], capture_output=True, text=True).stdout
    assert container not in listed


async def test_sandboxes_are_isolated_from_each_other():
    async with sandbox() as first, sandbox() as second:
        await first.write_file("mine.txt", "first")
        with pytest.raises(FileNotFoundError):
            await second.read_file("mine.txt")


async def test_several_commands_can_run_at_once():
    async with sandbox() as box:
        started = time.monotonic()
        results = await asyncio.gather(*(box.exec("sleep 1", timeout=30) for _ in range(4)))
        elapsed = time.monotonic() - started
    assert all(r.ok for r in results) and elapsed < 4
