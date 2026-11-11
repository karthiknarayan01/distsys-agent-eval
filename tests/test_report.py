"""The report: pass^k, which attempts count, failure reasons, and how it's printed."""

import json

import pytest

from distsys_eval.report import as_json, failure_reason, pass_hat, render, summarize, table_ks, tokens
from distsys_eval.results import AttemptResult

AGENT = "shellmate/fake"


def attempt(task, number, outcome, category=None, stop="finished", steps=10, seconds=120.0, error=None):
    return AttemptResult(
        task, number, AGENT, outcome, category=category, error=error, stop=stop, steps=steps,
        input_tokens=10_000, output_tokens=500, agent_seconds=seconds,
    )


def test_pass_hat_is_the_chance_k_random_attempts_all_pass():
    assert [pass_hat(5, 3, k) for k in range(1, 6)] == [0.6, 0.3, 0.1, 0.0, 0.0]
    assert pass_hat(4, 4, 4) == 1.0
    assert pass_hat(4, 0, 1) == 0.0


def test_errors_are_left_out_and_counted_apart():
    report = summarize([
        attempt("echo", 1, "pass"),
        attempt("echo", 2, "error", stop=None, steps=0, error="sandbox: gone"),
        attempt("echo", 3, "fail", "crashed"),
    ])
    [echo] = report.tasks
    assert (echo.attempts, echo.passed, echo.errors) == (2, 1, 1)
    assert echo.pass_hat == {1: 0.5, 2: 0.0}
    assert (echo.steps, echo.minutes, echo.input_tokens) == (10, 2.0, 20_000)


def test_overall_is_the_mean_over_tasks_for_every_k_all_of_them_reach():
    report = summarize(
        [attempt("echo", n, "pass") for n in (1, 2, 3)]
        + [attempt("unique-ids", 1, "pass"), attempt("unique-ids", 2, "fail", "fails-under-faults")]
    )
    assert report.overall == {1: (1.0 + 0.5) / 2, 2: (1.0 + 0.0) / 2}


def test_a_stopped_agent_is_the_reason_before_the_graders_category():
    assert failure_reason(attempt("echo", 1, "fail", "doesnt-build", stop="out-of-time")) == "out-of-time"
    assert failure_reason(attempt("echo", 1, "fail", "doesnt-build")) == "doesnt-build"
    report = summarize([
        attempt("echo", 1, "fail", "doesnt-build"),
        attempt("echo", 2, "fail", "crashed", stop="out-of-steps"),
        attempt("echo", 3, "fail", "doesnt-build"),
    ])
    assert report.failures.most_common() == [("doesnt-build", 2), ("out-of-steps", 1)]


def test_one_agents_results_at_a_time():
    other = AttemptResult("echo", 2, "shellmate/other", "pass")
    with pytest.raises(ValueError, match="one agent"):
        summarize([attempt("echo", 1, "pass"), other])


def test_render():
    report = summarize([
        attempt("echo", 1, "pass", steps=6, seconds=60),
        attempt("echo", 2, "pass", steps=8, seconds=120),
        attempt("broadcast-single-node", 1, "fail", "fails-without-faults", steps=30, seconds=600),
        attempt("broadcast-single-node", 2, "error", stop=None, steps=0, error="model: down"),
    ])
    assert render(report) == "\n".join([
        "shellmate/fake  (1 error not scored)",
        "",
        "task                   passed  pass^1  pass^2  steps  minutes  tokens in / out",
        "echo                   2/2     1.00    1.00    7      1.5      20k / 1k",
        "broadcast-single-node  0/1     0.00            30     10.0     10k / 500",
        "overall                        0.50",
        "",
        "why attempts failed",
        "  fails-without-faults  1",
    ])


def test_an_original_and_its_variant_are_set_side_by_side():
    results = [
        attempt("unique-ids", 1, "pass"), attempt("unique-ids", 2, "pass"),
        attempt("unique-ids-variant", 1, "pass"), attempt("unique-ids-variant", 2, "fail", "crashed"),
        attempt("echo", 1, "pass"),
    ]
    report = summarize(results, {"unique-ids-variant": "unique-ids", "g-counter-variant": "g-counter"})
    assert report.variants == [("unique-ids", "unique-ids-variant")]
    assert render(report).splitlines()[-3:] == [
        "originals and their variants",
        "  original    pass^1  variant             pass^1  change",
        "  unique-ids  1.00    unique-ids-variant  0.50    -0.50",
    ]
    assert as_json(report)["variants"] == [{"original": "unique-ids", "variant": "unique-ids-variant"}]


def test_without_variants_there_is_no_comparison():
    assert "variants" not in render(summarize([attempt("echo", 1, "pass")]))


def test_many_attempts_show_k_in_powers_of_two():
    assert table_ks(5) == [1, 2, 3, 4, 5]
    assert table_ks(16) == [1, 2, 4, 8, 16]
    assert table_ks(20) == [1, 2, 4, 8, 16, 20]


def test_tokens():
    assert [tokens(n) for n in (812, 45_300, 1_240_000)] == ["812", "45k", "1.2M"]


def test_as_json_holds_every_k():
    report = summarize([attempt("echo", n, "pass" if n < 3 else "fail", "crashed") for n in (1, 2, 3)])
    data = json.loads(json.dumps(as_json(report)))
    assert data["overall"]["pass_hat"] == {"1": 2 / 3, "2": 1 / 3, "3": 0.0}
    assert data["tasks"][0]["failures"] == {"crashed": 1}
