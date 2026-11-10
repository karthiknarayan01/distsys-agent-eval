"""The sandbox's docker calls, checked against a fake docker: how a container is started,
that it is always removed, and how commands and files are passed in."""

import asyncio

import pytest

from distsys_eval.sandbox import TIMED_OUT, DockerResult, Sandbox, SandboxConfig, SandboxError, read_capped
from tests.fake_docker import FakeDocker, failed


async def test_a_container_has_no_network_and_limits():
    docker = FakeDocker()
    async with Sandbox.start(SandboxConfig(image="img", cpus=1.5, memory="2g", pids=256), docker) as box:
        assert box.container_id == "container-1"
    run = docker.calls[0][0]
    assert run[0] == "run"
    assert run[run.index("--network") + 1] == "none"
    assert run[run.index("--cpus") + 1] == "1.5"
    assert run[run.index("--memory") + 1] == "2g"
    assert run[run.index("--pids-limit") + 1] == "256"
    assert "--init" in run and run[-3] == "img"


async def test_the_container_is_removed_when_the_block_ends():
    docker = FakeDocker()
    async with Sandbox.start(SandboxConfig(), docker):
        pass
    assert docker.calls[-1][0] == ("rm", "--force", "container-1")


async def test_the_container_is_removed_after_an_exception():
    docker = FakeDocker()
    with pytest.raises(ValueError):
        async with Sandbox.start(SandboxConfig(), docker):
            raise ValueError("boom")
    assert docker.commands() == ["run", "rm"]


async def test_the_container_is_removed_after_a_cancellation():
    docker = FakeDocker()
    inside = asyncio.Event()

    async def use():
        async with Sandbox.start(SandboxConfig(), docker):
            inside.set()
            await asyncio.sleep(10)

    task = asyncio.create_task(use())
    await inside.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert docker.commands() == ["run", "rm"]


async def test_a_container_that_wont_start_is_an_error():
    docker = FakeDocker({"run": failed("no such image: img")})
    with pytest.raises(SandboxError, match="no such image"):
        async with Sandbox.start(SandboxConfig(image="img"), docker):
            pass
    assert docker.commands() == ["run", "rm"]


async def test_a_command_runs_under_a_timeout_in_the_working_directory():
    docker = FakeDocker({"exec": DockerResult(3, b"compile error\n", "", False)})
    async with Sandbox.start(SandboxConfig(workdir="/work"), docker) as box:
        result = await box.exec("go build .", timeout=30)
    args = docker.calls[1][0]
    assert args[args.index("--workdir") + 1] == "/work"
    assert args[args.index("timeout") + 3] == "30"
    assert args[-1].endswith("go build .")
    assert (result.exit_code, result.output) == (3, "compile error\n")
    assert not result.timed_out and not result.ok


async def test_a_command_stopped_by_its_timeout_says_so():
    docker = FakeDocker({"exec": DockerResult(TIMED_OUT, b"", "", False)})
    async with Sandbox.start(SandboxConfig(), docker) as box:
        result = await box.exec("sleep 100", timeout=1)
    assert result.timed_out and not result.ok


async def test_docker_failing_to_run_a_command_is_an_error():
    docker = FakeDocker({"exec": failed("Error response from daemon: container is not running", 125)})
    async with Sandbox.start(SandboxConfig(), docker) as box:
        with pytest.raises(SandboxError, match="not running"):
            await box.exec("go build .", timeout=30)


async def test_a_file_is_written_through_stdin():
    docker = FakeDocker()
    async with Sandbox.start(SandboxConfig(), docker) as box:
        await box.write_file("src/main.go", "package main\n")
    args, stdin = docker.calls[1]
    assert args[-1] == "src/main.go" and stdin == b"package main\n"


async def test_reading_a_missing_file():
    docker = FakeDocker({"exec": failed("cat: nope: No such file or directory")})
    async with Sandbox.start(SandboxConfig(), docker) as box:
        with pytest.raises(FileNotFoundError, match="nope"):
            await box.read_file("nope")


async def test_long_output_keeps_its_start_and_end():
    stream = asyncio.StreamReader()
    stream.feed_data(b"a" * 1000 + b"b" * 1000)
    stream.feed_eof()
    out, cut = await read_capped(stream, 100)
    assert cut
    assert out.startswith(b"a" * 50) and out.endswith(b"b" * 50)
    assert b"[... 1900 bytes cut ...]" in out

    short = asyncio.StreamReader()
    short.feed_data(b"hello")
    short.feed_eof()
    assert await read_capped(short, 100) == (b"hello", False)

