"""Sandbox: one throwaway Docker container that an attempt is written and graded in.

    async with Sandbox.start(SandboxConfig(image=IMAGE)) as box:
        await box.write_file("main.go", source)
        result = await box.exec("go build -o node .", timeout=120)
        result.exit_code, result.output, result.timed_out
    # the container is gone here, however the block was left

The container has no network and limited CPU, memory and processes, so nothing an agent runs
can reach the host, the internet or another attempt. Everything goes through the `docker`
command line, so the only thing the harness needs is Docker itself.

Two kinds of failure are kept apart. A command that fails inside the container is a result:
its exit code and output come back in an ExecResult. Docker itself failing (no daemon, a
container that won't start) raises SandboxError: the attempt couldn't run at all, so the
runner counts it as an infrastructure error, not as the agent's failure.
"""

from __future__ import annotations

import asyncio
import io
import tarfile
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path

IMAGE = "distsys-agent-eval/sandbox:0.1.0"
DOCKERFILE_DIR = Path(__file__).resolve().parent.parent / "docker"

MAX_OUTPUT_BYTES = 64 * 1024  # what one command may return; the middle of longer output is cut
KEEP_ALIVE_SECONDS = 24 * 3600  # the container's main process just waits; attempts are far shorter
KILL_GRACE_SECONDS = 5  # after a timeout, how long a command gets to exit before it is killed
DOCKER_TIMEOUT_SECONDS = 120  # for docker's own calls: starting, copying, removing
TIMED_OUT = 124  # the exit status `timeout` gives a command it had to stop
DOCKER_FAILED = 125  # the exit status `docker exec` gives when docker itself failed


class SandboxError(RuntimeError):
    """Docker failed, so the sandbox couldn't do what was asked."""


@dataclass(frozen=True)
class SandboxConfig:
    image: str = IMAGE
    cpus: float = 2.0
    memory: str = "4g"
    pids: int = 1024
    workdir: str = "/work"


@dataclass(frozen=True)
class ExecResult:
    exit_code: int
    output: str  # stdout and stderr together, in the order they were written
    timed_out: bool
    truncated: bool  # the output was longer than MAX_OUTPUT_BYTES and its middle was cut

    @property
    def ok(self) -> bool:
        return self.exit_code == 0 and not self.timed_out


class Sandbox:
    """A running container. Made only by `Sandbox.start`, which also removes it."""

    def __init__(self, container_id: str, config: SandboxConfig, docker: Docker):
        self.container_id = container_id
        self.config = config
        self._docker = docker

    @classmethod
    @asynccontextmanager
    async def start(
        cls, config: SandboxConfig | None = None, docker: Docker | None = None
    ) -> AsyncIterator[Sandbox]:
        """Start a container and yield it; it is removed when the block is left, even by an
        exception or a cancellation. Raises SandboxError if it can't be started."""
        config = config or SandboxConfig()
        docker = docker or Docker()
        name = f"distsys-eval-{uuid.uuid4().hex[:12]}"
        started = await docker.run(
            "run", "--detach", "--init", "--name", name,
            "--network", "none",
            "--cpus", str(config.cpus), "--memory", config.memory, "--pids-limit", str(config.pids),
            "--workdir", config.workdir,
            config.image, "sleep", str(KEEP_ALIVE_SECONDS),
        )
        if started.exit_code != 0:
            await docker.run("rm", "--force", name)  # a half-created container
            raise SandboxError(f"couldn't start a container from {config.image}: {started.error}")
        box = cls(started.stdout.strip(), config, docker)
        try:
            yield box
        finally:
            await asyncio.shield(box._remove())

    async def exec(self, command: str, timeout: float) -> ExecResult:
        """Run a shell command in the working directory. After `timeout` seconds it is stopped
        (and everything it started, which shares its process group), and the result says so.
        Raises SandboxError if docker couldn't run it at all, e.g. the container is gone."""
        ran = await self._docker.run(
            "exec", "--workdir", self.config.workdir, self.container_id,
            "timeout", "-k", str(KILL_GRACE_SECONDS), str(timeout), "sh", "-c", f"exec 2>&1; {command}",
            timeout=timeout + KILL_GRACE_SECONDS + DOCKER_TIMEOUT_SECONDS,
        )
        if ran.exit_code == DOCKER_FAILED:
            raise SandboxError(f"docker couldn't run the command: {ran.error}")
        return ExecResult(ran.exit_code, ran.stdout, ran.exit_code == TIMED_OUT, ran.truncated)

    async def write_file(self, path: str, content: str | bytes) -> None:
        """Write a file, creating its directories; a relative path is under the working directory."""
        data = content.encode() if isinstance(content, str) else content
        wrote = await self._docker.run(
            "exec", "--interactive", "--workdir", self.config.workdir, self.container_id,
            "sh", "-c", 'd="${1%/*}"; if [ "$d" != "$1" ]; then mkdir -p "$d"; fi; cat > "$1"', "sh", path,
            stdin=data,
        )
        if wrote.exit_code != 0:
            raise SandboxError(f"couldn't write {path}: {wrote.error}")

    async def read_file(self, path: str) -> bytes:
        """A file's contents. Raises FileNotFoundError if it isn't there or can't be read."""
        read = await self._docker.run(
            "exec", "--workdir", self.config.workdir, self.container_id, "cat", "--", path, limit=None,
        )
        if read.exit_code != 0:
            raise FileNotFoundError(f"{path}: {read.error}")
        return read.stdout_bytes

    async def copy_in(self, source: Path, dest: str = ".") -> None:
        """Copy a local directory's contents into `dest`, owned by the container's user."""
        archive = io.BytesIO()
        with tarfile.open(fileobj=archive, mode="w") as tar:
            tar.add(source, arcname=".")
        copied = await self._docker.run(
            "exec", "--interactive", "--workdir", self.config.workdir, self.container_id,
            "sh", "-c", 'mkdir -p "$1" && tar -x -C "$1"', "sh", dest,
            stdin=archive.getvalue(),
        )
        if copied.exit_code != 0:
            raise SandboxError(f"couldn't copy {source} into {dest}: {copied.error}")

    async def copy_out(self, source: str, dest: Path) -> None:
        """Copy a directory in the container into a local directory, which is created."""
        copied = await self._docker.run(
            "exec", "--workdir", self.config.workdir, self.container_id, "tar", "-c", "-C", source, ".",
            limit=None,
        )
        if copied.exit_code != 0:
            raise SandboxError(f"couldn't copy {source} out: {copied.error}")
        dest.mkdir(parents=True, exist_ok=True)
        with tarfile.open(fileobj=io.BytesIO(copied.stdout_bytes)) as tar:
            tar.extractall(dest, filter="data")

    async def _remove(self) -> None:
        removed = await self._docker.run("rm", "--force", self.container_id)
        if removed.exit_code != 0:
            raise SandboxError(f"couldn't remove container {self.container_id}: {removed.error}")


class Docker:
    """Runs the docker command line. Tests swap in a fake with the same `run`."""

    def __init__(self, binary: str = "docker"):
        self.binary = binary

    async def run(
        self,
        *args: str,
        stdin: bytes | None = None,
        timeout: float = DOCKER_TIMEOUT_SECONDS,
        limit: int | None = MAX_OUTPUT_BYTES,
    ) -> DockerResult:
        """`docker *args`, run to the end. Stdout is cut to `limit` bytes (None: kept whole).
        Raises SandboxError if docker can't be run or doesn't finish in `timeout` seconds."""
        try:
            process = await asyncio.create_subprocess_exec(
                self.binary, *args,
                stdin=asyncio.subprocess.PIPE if stdin is not None else asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
        except FileNotFoundError as e:
            raise SandboxError(f"{self.binary} isn't installed") from e
        try:
            (out, cut), (err, _) = await asyncio.wait_for(_communicate(process, stdin, limit), timeout)
        except TimeoutError as e:
            raise SandboxError(f"docker {args[0]} didn't finish in {timeout:.0f}s") from e
        finally:
            if process.returncode is None:  # timed out or cancelled
                process.kill()
                await process.wait()
        return DockerResult(process.returncode, out, err.decode(errors="replace"), cut)


@dataclass(frozen=True)
class DockerResult:
    exit_code: int
    stdout_bytes: bytes
    stderr: str
    truncated: bool

    @property
    def stdout(self) -> str:
        return self.stdout_bytes.decode(errors="replace")

    @property
    def error(self) -> str:
        """What to say about a failed call: stderr, or stdout when stderr is empty."""
        return self.stderr.strip() or self.stdout.strip()


async def _communicate(
    process: asyncio.subprocess.Process, stdin: bytes | None, limit: int | None
) -> tuple[tuple[bytes, bool], tuple[bytes, bool]]:
    """Feed stdin, read stdout (cut to `limit`) and stderr, and wait for the process to exit."""

    async def feed() -> None:
        if stdin is None:
            return
        try:
            process.stdin.write(stdin)
            await process.stdin.drain()
            process.stdin.close()
        except (BrokenPipeError, ConnectionResetError):
            pass  # the process exited without reading it all; its exit code says why

    _, out, err = await asyncio.gather(
        feed(), read_capped(process.stdout, limit), read_capped(process.stderr, MAX_OUTPUT_BYTES)
    )
    await process.wait()
    return out, err


async def read_capped(stream: asyncio.StreamReader, limit: int | None) -> tuple[bytes, bool]:
    """Everything in the stream and False, or if it's longer than `limit`: its first and last
    halves with a marker between them, and True. The stream is always read to its end, so the
    process writing it never blocks."""
    if limit is None:
        return await stream.read(), False
    half = limit // 2
    head = bytearray()
    tail = bytearray()
    cut = 0
    while chunk := await stream.read(65536):
        room = max(half - len(head), 0)
        head += chunk[:room]
        tail += chunk[room:]
        if len(tail) > half:
            cut += len(tail) - half
            del tail[: len(tail) - half]
    if not cut:
        return bytes(head + tail), False
    return bytes(head) + f"\n[... {cut} bytes cut ...]\n".encode() + bytes(tail), True
