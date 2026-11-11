"""The results file: appending attempts, reading back the latest of each, and printing them."""

from distsys_eval.results import AttemptResult, append, line, load, totals


def result(task="echo", attempt=1, outcome="pass", **fields) -> AttemptResult:
    return AttemptResult(task, attempt, "shellmate/fake", outcome, **fields)


def test_what_is_appended_is_loaded_back(tmp_path):
    path = tmp_path / "out" / "results.jsonl"
    first = result(runs=["valid"] * 3, runs_required=3, stop="finished", steps=4, agent_seconds=61.5)
    append(path, first)
    append(path, result(attempt=2, outcome="fail", category="crashed"))
    assert load(path) == [first, result(attempt=2, outcome="fail", category="crashed")]


def test_the_latest_line_for_an_attempt_is_the_one_that_counts(tmp_path):
    path = tmp_path / "results.jsonl"
    append(path, result(attempt=1, outcome="error", error="sandbox: no daemon"))
    append(path, result(attempt=2))
    append(path, result(attempt=1, outcome="fail", category="doesnt-build"))
    assert [(r.attempt, r.outcome) for r in load(path)] == [(1, "fail"), (2, "pass")]


def test_a_missing_file_has_no_results(tmp_path):
    assert load(tmp_path / "results.jsonl") == []


def test_line():
    assert line(result(stop="finished", steps=14, agent_seconds=185)) == "echo #1: pass, 14 steps in 3m05s"
    stopped = result(
        outcome="fail", category="fails-under-faults", stop="out-of-time", steps=40, agent_seconds=1800
    )
    assert line(stopped) == "echo #1: fail (fails-under-faults), 40 steps in 30m00s, out-of-time"
    assert line(result(outcome="error", error="sandbox: no daemon")) == (
        "echo #1: error, not scored (sandbox: no daemon)"
    )


def test_totals():
    results = [result(), result(attempt=2, outcome="fail"), result(attempt=3, outcome="error")]
    assert totals(results) == "3 attempts: 1 passed, 1 failed, 1 errors (not scored)"
