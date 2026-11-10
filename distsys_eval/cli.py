"""The distsys-eval command.

    distsys-eval --version

Each part of the harness adds its own subcommand here as it lands.
"""

from __future__ import annotations

import argparse

from distsys_eval import __version__


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
    parser.add_subparsers(dest="command", metavar="command")
    return parser
