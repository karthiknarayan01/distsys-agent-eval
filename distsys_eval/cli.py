"""The distsys-eval command.

    distsys-eval --version
    distsys-eval build-image      build the sandbox image every attempt runs in
    distsys-eval tasks            list the tasks
    distsys-eval tasks echo       show what the agent is told for one task
"""

from __future__ import annotations

import argparse
import subprocess
import sys

from distsys_eval import __version__
from distsys_eval.sandbox import DOCKERFILE_DIR, IMAGE
from distsys_eval.tasks import TASKS_DIR, load_tasks


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
    tasks = commands.add_parser("tasks", help="list the tasks, or show one task's brief")
    tasks.add_argument("task", nargs="?", help="a task id: show what the agent is told")
    tasks.set_defaults(run=show_tasks)
    return parser


def build_image(args: argparse.Namespace) -> int:
    """`docker build` the sandbox image, showing docker's own progress."""
    return subprocess.run(["docker", "build", "--tag", args.tag, str(DOCKERFILE_DIR)]).returncode


def show_tasks(args: argparse.Namespace) -> int:
    """One line per task, or with a task id, that task's brief."""
    tasks = {task.id: task for task in load_tasks(TASKS_DIR)}
    if args.task is None:
        width = max(map(len, tasks), default=0)
        for task in tasks.values():
            print(f"{task.id:<{width}}  {task.maelstrom.workload:<12}  m={task.runs}  {task.title}")
        return 0
    if args.task not in tasks:
        print(f"no task {args.task!r}; the tasks are: {', '.join(tasks)}", file=sys.stderr)
        return 1
    print(tasks[args.task].brief, end="")
    return 0
