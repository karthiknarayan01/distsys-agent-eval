"""Merging CI jobs' results: one directory per agent, re-run tasks replacing their earlier attempts."""

import tarfile
from pathlib import Path

from distsys_eval.merge import merge
from distsys_eval.results import RESULTS_FILE, AttemptResult, append, load

AGENT = "shellmate-ollama-qwen3.5-9b"


def job(root: Path, name: str, *results: AttemptResult, packed: bool = False) -> Path:
    """A job directory holding `results` and a transcript for each, packed as results.tgz or not."""
    results_dir = root / name / "results" if packed else root / name
    for r in results:
        append(results_dir / AGENT / RESULTS_FILE, r)
        transcript = results_dir / AGENT / "transcripts" / f"{r.task}-{r.attempt}.jsonl"
        transcript.parent.mkdir(parents=True, exist_ok=True)
        transcript.write_text(f"{name} {r.outcome}\n")
    if not packed:
        return root / name
    with tarfile.open(root / name / "results.tgz", "w:gz") as tar:
        tar.add(results_dir, arcname="results")
    return root / name


def attempt(task: str, n: int, outcome: str) -> AttemptResult:
    return AttemptResult(task=task, attempt=n, agent="shellmate/ollama/qwen3.5:9b", outcome=outcome)


def test_jobs_for_different_tasks_are_combined(tmp_path):
    first = job(tmp_path, "a", attempt("echo", 1, "pass"), attempt("echo", 2, "fail"))
    second = job(tmp_path, "b", attempt("g-counter", 1, "pass"), packed=True)
    merge(tmp_path / "out", [first, second])
    merged = load(tmp_path / "out" / AGENT / RESULTS_FILE)
    assert [(r.task, r.attempt, r.outcome) for r in merged] == [
        ("echo", 1, "pass"), ("echo", 2, "fail"), ("g-counter", 1, "pass"),
    ]
    assert (tmp_path / "out" / AGENT / "transcripts" / "g-counter-1.jsonl").read_text() == "b pass\n"


def test_a_rerun_task_replaces_every_earlier_attempt_at_it(tmp_path):
    old = job(tmp_path, "old", attempt("unique-ids", 1, "fail"), attempt("unique-ids", 2, "error"),
              attempt("echo", 1, "pass"))
    rerun = job(tmp_path, "rerun", attempt("unique-ids", 1, "pass"), attempt("unique-ids", 2, "pass"),
                packed=True)
    merge(tmp_path / "out", [old, rerun])
    merged = load(tmp_path / "out" / AGENT / RESULTS_FILE)
    assert [(r.task, r.attempt, r.outcome) for r in merged] == [
        ("echo", 1, "pass"), ("unique-ids", 1, "pass"), ("unique-ids", 2, "pass"),
    ]
    assert (tmp_path / "out" / AGENT / "transcripts" / "unique-ids-2.jsonl").read_text() == "rerun pass\n"


def test_files_beside_the_agents_go_in_logs(tmp_path):
    first = job(tmp_path, "a", attempt("echo", 1, "pass"))
    (first / "ollama-qwen3.5-9b-echo.log").write_text("loaded\n")
    merge(tmp_path / "out", [first])
    assert (tmp_path / "out" / "logs" / "ollama-qwen3.5-9b-echo.log").read_text() == "loaded\n"
