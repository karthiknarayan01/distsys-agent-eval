"""Stand-ins for a Sandbox: a scripted one, and one that runs commands on this machine."""

from __future__ import annotations

import asyncio
from pathlib import Path

from distsys_eval.sandbox import ExecResult


class ScriptedShell:
    """Answers exec with a fixed result and read_file from a dict of files."""

    def __init__(self, result: ExecResult, files: dict[str, bytes] | None = None):
        self.result = result
        self.files = files or {}
        self.commands: list[tuple[str, float]] = []

    async def exec(self, command: str, timeout: float) -> ExecResult:
        self.commands.append((command, timeout))
        return self.result

    async def read_file(self, path: str) -> bytes:
        if path not in self.files:
            raise FileNotFoundError(path)
        return self.files[path]


class HostShell:
    """Runs commands with sh in a directory on this machine. No isolation: tests only."""

    def __init__(self, root: Path):
        self.root = root

    async def exec(self, command: str, timeout: float) -> ExecResult:
        process = await asyncio.create_subprocess_exec(
            "sh", "-c", f"exec 2>&1; {command}", cwd=self.root, stdout=asyncio.subprocess.PIPE
        )
        try:
            out, _ = await asyncio.wait_for(process.communicate(), timeout)
        except TimeoutError:
            process.kill()
            await process.wait()
            return ExecResult(124, "", True, False)
        return ExecResult(process.returncode, out.decode(errors="replace"), False, False)

    async def read_file(self, path: str) -> bytes:
        return (self.root / path).read_bytes()

    async def write_file(self, path: str, content: str | bytes) -> None:
        target = self.root / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(content.encode() if isinstance(content, str) else content)
