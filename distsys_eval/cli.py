"""The distsys-eval command.

    distsys-eval --version
    distsys-eval build-image      build the sandbox image every attempt runs in
    distsys-eval tasks            list the tasks
    distsys-eval tasks echo       show what the agent is told for one task
    distsys-eval grade echo DIR   grade a Go program in DIR as an attempt at a task
    distsys-eval validate         check that references pass and mutants fail
    distsys-eval calibrate        measure how often one run catches each mutant, and choose m
    distsys-eval run --provider ollama
                                  n attempts by an agent at every task, graded, into results/
    distsys-eval run --command CMD --name NAME --image IMG --network --env API_KEY
                                  the same for a command-line agent installed in IMG
    distsys-eval report DIR       pass^k per task, why attempts failed, and what they cost
"""

from __future__ import annotations

import argparse
import asyncio
import json
import subprocess
import sys
from pathlib import Path

from distsys_eval import __version__, runner
from distsys_eval import calibrate as calibration
from distsys_eval import report as reporting
from distsys_eval.agent import Agent
from distsys_eval.command import CommandAgent
from distsys_eval.grader import describe, grade_directory
from distsys_eval.results import RESULTS_FILE, line, load, totals
from distsys_eval.sandbox import DOCKERFILE_DIR, IMAGE
from distsys_eval.shellmate import ShellmateAgent
from distsys_eval.tasks import TASKS_DIR, load_tasks
from distsys_eval.validate import summary, validate


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
    grade = commands.add_parser("grade", help="grade a directory as an attempt at a task")
    grade.add_argument("task", help="the task id")
    grade.add_argument("dir", type=Path, help="a directory holding a Go program")
    grade.add_argument("--image", default=IMAGE, help=f"sandbox image (default {IMAGE})")
    grade.set_defaults(run=grade_command)
    check = commands.add_parser("validate", help="check that each task's references pass and mutants fail")
    check.add_argument("tasks", nargs="*", help="task ids (default: every task)")
    check.add_argument("--jobs", type=int, default=1, help="sandboxes to run at once (default 1)")
    check.add_argument("--image", default=IMAGE, help=f"sandbox image (default {IMAGE})")
    check.set_defaults(run=validate_command)
    calibrate = commands.add_parser("calibrate", help="measure each mutant's catch rate per run and choose m")
    calibrate.add_argument("tasks", nargs="*", help="task ids (default: every task)")
    calibrate.add_argument("--trials", type=int, default=20, help="runs per subject (default 20)")
    calibrate.add_argument("--jobs", type=int, default=1, help="sandboxes to run at once (default 1)")
    calibrate.add_argument("--image", default=IMAGE, help=f"sandbox image (default {IMAGE})")
    calibrate.add_argument("--write", action="store_true", help="record the counts and set m")
    calibrate.set_defaults(run=calibrate_command)
    attempts = commands.add_parser("run", help="run an agent's attempts at the tasks and grade them")
    attempts.add_argument("tasks", nargs="*", help="task ids (default: every task)")
    which = attempts.add_mutually_exclusive_group(required=True)
    which.add_argument("--provider", choices=["ollama", "deepseek", "anthropic"],
                       help="run Shellmate with this model provider")
    which.add_argument("--command", dest="agent_command",
                       help="run this command-line agent in the sandbox; the brief is at $BRIEF")
    attempts.add_argument("--model", help="the provider's model (default: Shellmate's for that provider)")
    attempts.add_argument("--name", help="what the results call a --command agent (required with it)")
    attempts.add_argument("--network", action="store_true", help="give the sandbox a network, for --command")
    attempts.add_argument("--env", action="append", default=[], metavar="NAME",
                          help="pass this environment variable into the sandbox (repeatable)")
    attempts.add_argument("--attempts", type=int, default=5, help="attempts per task (default 5)")
    attempts.add_argument("--out", type=Path, help="results directory (default results/<agent>)")
    attempts.add_argument("--jobs", type=int, default=1, help="attempts to run at once (default 1)")
    attempts.add_argument("--image", default=IMAGE, help=f"sandbox image (default {IMAGE})")
    attempts.set_defaults(run=run_command)
    report = commands.add_parser("report", help="summarize the results of one or more runs")
    report.add_argument("dirs", nargs="+", type=Path, help="results directories, one per agent")
    report.add_argument("--json", action="store_true", help="print JSON instead of tables")
    report.set_defaults(run=report_command)
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


def grade_command(args: argparse.Namespace) -> int:
    """Grade the directory in a fresh sandbox and print the result; exits 0 only on a pass."""
    tasks = {task.id: task for task in load_tasks(TASKS_DIR)}
    if args.task not in tasks:
        print(f"no task {args.task!r}; the tasks are: {', '.join(tasks)}", file=sys.stderr)
        return 2
    if not args.dir.is_dir():
        print(f"{args.dir} isn't a directory", file=sys.stderr)
        return 2
    grade = asyncio.run(grade_directory(tasks[args.task], args.dir, args.image))
    print(describe(tasks[args.task], grade))
    return 0 if grade.passed else 1


def validate_command(args: argparse.Namespace) -> int:
    """Grade every reference and mutant and print how each came out; exits 0 only if all were
    graded as expected."""
    chosen = pick_tasks(args.tasks)
    if chosen is None:
        return 2
    checks = asyncio.run(validate(chosen, args.image, args.jobs))
    print(summary(checks))
    return 0 if all(check.ok for check in checks) else 1


def calibrate_command(args: argparse.Namespace) -> int:
    """Measure every reference and mutant, print the counts and the m they call for, and with
    --write record them. Exits 1 if some task has no m: a mutant caught too rarely, or a flaky
    reference."""
    chosen = pick_tasks(args.tasks)
    if chosen is None:
        return 2
    results = asyncio.run(calibration.calibrate(chosen, args.trials, args.image, args.jobs))
    print(calibration.summary(results))
    usable = [r for r in results if r.runs and r.reference.built and not r.reference.failed]
    if args.write:
        for result in usable:
            calibration.write(result)
    return 0 if len(usable) == len(results) else 1


def run_command(args: argparse.Namespace) -> int:
    """Run every attempt not yet in the results directory, printing each as it ends, then the
    totals. Started again with the same directory, it carries on where it stopped."""
    chosen = pick_tasks(args.tasks)
    if chosen is None:
        return 2
    agent = pick_agent(args)
    if agent is None:
        return 2
    out = args.out or Path("results") / agent.name.replace("/", "-").replace(":", "-")
    print(f"{agent.name}: {args.attempts} attempts at {len(chosen)} tasks, into {out}")
    try:
        results = asyncio.run(runner.run(
            agent, chosen, args.attempts, out, args.jobs,
            runner.sandboxes(args.image, "bridge" if args.network else "none", tuple(args.env)),
            on_result=lambda result: print(line(result), flush=True),
        ))
    except runner.RunnerError as e:
        print(e, file=sys.stderr)
        return 2
    print(totals(results))
    return 0


def pick_agent(args: argparse.Namespace) -> Agent | None:
    """The agent the run options ask for, or None (after saying why) if it can't be made."""
    if args.agent_command:
        if not args.name:
            print("--command needs --name, to tell its results apart", file=sys.stderr)
            return None
        return CommandAgent(args.name, args.agent_command)
    try:
        return ShellmateAgent.from_provider(args.provider, args.model)
    except (RuntimeError, ValueError) as e:  # e.g. an API key that isn't set
        print(e, file=sys.stderr)
        return None


def report_command(args: argparse.Namespace) -> int:
    """A report for each results directory, as tables or as one JSON list."""
    variant_of = {task.id: task.variant_of for task in load_tasks(TASKS_DIR) if task.variant_of}
    reports = []
    for directory in args.dirs:
        results = load(directory / RESULTS_FILE)
        if not results:
            print(f"no results in {directory}", file=sys.stderr)
            return 2
        reports.append(reporting.summarize(results, variant_of))
    if args.json:
        print(json.dumps([reporting.as_json(r) for r in reports], indent=2))
    else:
        print("\n\n".join(reporting.render(r) for r in reports))
    return 0


def pick_tasks(task_ids: list[str]) -> list | None:
    """The named tasks, or every task when none is named; None (after saying so) if a name is
    not a task."""
    tasks = {task.id: task for task in load_tasks(TASKS_DIR)}
    unknown = [task_id for task_id in task_ids if task_id not in tasks]
    if unknown:
        print(f"no task {', '.join(map(repr, unknown))}; the tasks are: {', '.join(tasks)}", file=sys.stderr)
        return None
    return [tasks[task_id] for task_id in task_ids] or list(tasks.values())
