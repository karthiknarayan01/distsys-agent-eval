"""Merge the results of many eval jobs into one results directory per agent.

    python -m distsys_eval.merge OUT JOB_DIR [JOB_DIR ...]

Each job directory holds what one CI job uploaded: either results.tgz (a tarball of its
results/ directory) or that directory's contents unpacked, one subdirectory per agent with a
results.jsonl and the attempts' transcripts and code. Jobs are given oldest first. When a later
job ran a task an earlier one also ran for the same agent, the later job's attempts at that
task replace the earlier ones: that is how a task is re-run after the harness let it down.

Everything else in a job (transcripts, code, Maelstrom's output, logs) is copied over, later
jobs overwriting earlier files of the same name.
"""

from __future__ import annotations

import shutil
import sys
import tarfile
import tempfile
from pathlib import Path

from distsys_eval.results import RESULTS_FILE, AttemptResult, append, load

Attempts = dict[tuple[str, str], list[AttemptResult]]  # (agent directory, task) -> its attempts


def merge(out: Path, jobs: list[Path]) -> dict[str, list[AttemptResult]]:
    """Merge `jobs`, oldest first, into `out`/<agent>/. Returns each agent's attempts."""
    attempts: Attempts = {}
    with tempfile.TemporaryDirectory() as scratch:
        for i, job in enumerate(jobs):
            root = unpacked(job, Path(scratch) / str(i))
            for agent_dir in sorted(p for p in root.iterdir() if p.is_dir()):
                by_task: dict[str, list[AttemptResult]] = {}
                for result in load(agent_dir / RESULTS_FILE):
                    by_task.setdefault(result.task, []).append(result)
                for task, results in by_task.items():
                    attempts[(agent_dir.name, task)] = results
                copy_tree(agent_dir, out / agent_dir.name)
            for loose in (p for p in root.iterdir() if p.is_file() and p.name != "results.tgz"):
                (out / "logs").mkdir(parents=True, exist_ok=True)
                shutil.copy2(loose, out / "logs" / loose.name)
    return write_results(out, attempts)


def unpacked(job: Path, scratch: Path) -> Path:
    """The job's results directory: its tarball extracted into `scratch`, or the job itself."""
    tarball = job / "results.tgz"
    if not tarball.exists():
        return job
    scratch.mkdir(parents=True)
    with tarfile.open(tarball) as tar:
        tar.extractall(scratch, filter="data")
    inner = scratch / "results"
    return inner if inner.is_dir() else scratch


def copy_tree(source: Path, dest: Path) -> None:
    """Copy everything in `source` but its results file into `dest`, overwriting."""
    shutil.copytree(source, dest, dirs_exist_ok=True, ignore=shutil.ignore_patterns(RESULTS_FILE))


def write_results(out: Path, attempts: Attempts) -> dict[str, list[AttemptResult]]:
    """Write each agent's results file afresh, tasks in name order."""
    by_agent: dict[str, list[AttemptResult]] = {}
    for (agent_dir, _task), results in sorted(attempts.items()):
        by_agent.setdefault(agent_dir, []).extend(sorted(results, key=lambda r: r.attempt))
    for agent_dir, results in by_agent.items():
        path = out / agent_dir / RESULTS_FILE
        path.unlink(missing_ok=True)
        for result in results:
            append(path, result)
    return by_agent


def main(argv: list[str]) -> int:
    if len(argv) < 2:
        print(__doc__.strip().splitlines()[2], file=sys.stderr)
        return 2
    merged = merge(Path(argv[0]), [Path(p) for p in argv[1:]])
    for agent_dir, results in merged.items():
        print(f"{agent_dir}: {len(results)} attempts")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
