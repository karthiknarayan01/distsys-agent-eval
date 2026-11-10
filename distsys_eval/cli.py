"""The distsys-eval command.

    distsys-eval --version
    distsys-eval build-image      build the sandbox image every attempt runs in
"""

from __future__ import annotations

import argparse
import subprocess

from distsys_eval import __version__
from distsys_eval.sandbox import DOCKERFILE_DIR, IMAGE


def main(argv: list[str] | None = None) -> int:
    """Parse the arguments and run the chosen command; the exit status is returned."""
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command is None:
        parser.print_help()
        return 0
    return args.run(args)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="distsys-eval",
        description="Benchmark coding agents on distributed systems tasks graded by Maelstrom.",
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    commands = parser.add_subparsers(dest="command", metavar="command")
    build = commands.add_parser("build-image", help="build the sandbox image every attempt runs in")
    build.add_argument("--tag", default=IMAGE, help=f"image name (default {IMAGE})")
    build.set_defaults(run=build_image)
    return parser


def build_image(args: argparse.Namespace) -> int:
    """`docker build` the sandbox image, showing docker's own progress."""
    return subprocess.run(["docker", "build", "--tag", args.tag, str(DOCKERFILE_DIR)]).returncode
