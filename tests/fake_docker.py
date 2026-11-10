"""A stand-in for sandbox.Docker: records each call and answers from a script."""

from __future__ import annotations

from distsys_eval.sandbox import DockerResult


class FakeDocker:
    def __init__(self, answers: dict[str, DockerResult] | None = None):
        self.answers = answers or {}
        self.calls: list[tuple[tuple[str, ...], bytes | None]] = []

    async def run(self, *args, stdin=None, timeout=None, limit=None) -> DockerResult:
        self.calls.append((args, stdin))
        return self.answers.get(args[0], ok("container-1\n" if args[0] == "run" else ""))

    def commands(self) -> list[str]:
        return [args[0] for args, _ in self.calls]


def ok(stdout: str = "") -> DockerResult:
    return DockerResult(0, stdout.encode(), "", False)


def failed(stderr: str, code: int = 1) -> DockerResult:
    return DockerResult(code, b"", stderr, False)
