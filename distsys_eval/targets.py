"""Performance targets: how many messages and how much latency a valid run may cost.

    targets = Targets(msgs_per_op=30, median_latency_ms=400, max_latency_ms=600)
    missed(targets, run.results)    ["msgs-per-op was 34.1, over 30"], or [] if all are met

Some tasks ask for more than a correct answer: a broadcast that reaches every node, but quickly
and without flooding the network. Maelstrom measures both in every run's results, and a task's
[targets] table puts limits on them. A run that Maelstrom calls valid but that misses a target
fails all the same, as too-slow.

The figures, all from results.edn:
- msgs-per-op: messages sent between nodes for each client operation (:net :servers :msgs-per-op).
  Client requests and replies aren't counted, so it measures only the nodes' own traffic.
- median and max latency: how long after a broadcast was acknowledged its value showed up in every
  read from then on, in milliseconds (:workload :stable-latencies, at 0.5 and 1).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

# The [targets] keys, and the field each one sets.
KEYS = {
    "msgs-per-op": "msgs_per_op",
    "median-latency-ms": "median_latency_ms",
    "max-latency-ms": "max_latency_ms",
}


@dataclass(frozen=True)
class Targets:
    """Upper limits on a valid run's figures. None means no limit."""

    msgs_per_op: float | None = None
    median_latency_ms: float | None = None
    max_latency_ms: float | None = None

    def __bool__(self) -> bool:
        return any(getattr(self, field) is not None for field in KEYS.values())


def missed(targets: Targets, results: dict[str, Any]) -> list[str]:
    """Each target the run's results miss, as a sentence. A figure the results don't have counts
    as missed: a target can't be met by a run that never measured it."""
    figures = measured(results)
    misses = []
    for key, field in KEYS.items():
        limit = getattr(targets, field)
        if limit is None:
            continue
        value = figures.get(key)
        if value is None:
            misses.append(f"{key} wasn't measured")
        elif value > limit:
            misses.append(f"{key} was {value:g}, over {limit:g}")
    return misses


def measured(results: dict[str, Any]) -> dict[str, float]:
    """The figures the targets limit, from one run's results.edn; any it lacks are left out."""
    figures = {}
    msgs = dig(results, "net", "servers", "msgs-per-op")
    if isinstance(msgs, (int, float)):
        figures["msgs-per-op"] = round(float(msgs), 1)
    latencies = dig(results, "workload", "stable-latencies")
    if isinstance(latencies, dict):
        for key, quantile in (("median-latency-ms", 0.5), ("max-latency-ms", 1)):
            if isinstance(latencies.get(quantile), (int, float)):
                figures[key] = float(latencies[quantile])
    return figures


def dig(table: Any, *keys: str) -> Any:
    """table[k1][k2]..., or None as soon as a level is missing or isn't a map."""
    for key in keys:
        if not isinstance(table, dict):
            return None
        table = table.get(key)
    return table
