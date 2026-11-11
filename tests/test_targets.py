"""Targets: reading a run's message and latency figures, and saying which limits it went over."""

from distsys_eval.targets import Targets, measured, missed

RESULTS = {
    "net": {"servers": {"msgs-per-op": 23.4567}, "clients": {"msg-count": 4000}},
    "workload": {"stable-latencies": {0: 0, 0.5: 238, 0.95: 341, 0.99: 353, 1: 361}},
}


def test_the_figures_come_from_the_servers_and_the_stable_latencies():
    assert measured(RESULTS) == {"msgs-per-op": 23.5, "median-latency-ms": 238.0, "max-latency-ms": 361.0}


def test_figures_the_results_lack_are_left_out():
    assert measured({"net": {"servers": {}}, "workload": {"valid?": True}}) == {}
    assert measured({}) == {}


def test_a_run_within_every_target_misses_none():
    assert missed(Targets(msgs_per_op=30, median_latency_ms=400, max_latency_ms=600), RESULTS) == []


def test_each_target_gone_over_is_named_with_the_figure():
    assert missed(Targets(msgs_per_op=20, median_latency_ms=400, max_latency_ms=300), RESULTS) == [
        "msgs-per-op was 23.5, over 20",
        "max-latency-ms was 361, over 300",
    ]


def test_a_target_the_run_never_measured_is_missed():
    assert missed(Targets(median_latency_ms=400), {"net": {"servers": {"msgs-per-op": 2.0}}}) == [
        "median-latency-ms wasn't measured"
    ]


def test_no_targets_means_nothing_to_miss():
    assert not Targets()
    assert missed(Targets(), {}) == []
