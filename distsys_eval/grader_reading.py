"""Did reading the grader help? Pass rates of attempts that read Maelstrom's checker source
against those that didn't, per agent.

    python -m distsys_eval.grader_reading results/*/

An attempt read the grader if any of its tool calls opened Maelstrom's jar or named a Clojure
source file: unzip, jar tf/xf, Python's zipfile, maelstrom.jar or ".clj" in a command or path.
Attempts the harness let down (errors) are left out, and so are attempts cut off before their
first step, which never had the chance to read anything.
"""

from __future__ import annotations

import json
import sys
from dataclasses import dataclass
from pathlib import Path

from distsys_eval.results import RESULTS_FILE, load

SIGNS = ("maelstrom.jar", ".clj", "unzip", "jar tf", "jar xf", "zipfile")


@dataclass(frozen=True)
class Split:
    agent: str
    read: tuple[int, int]  # (passed, attempts) among attempts that read the grader
    did_not: tuple[int, int]  # the same among attempts that didn't


def split(results_dir: Path) -> Split:
    """Count `results_dir`'s scored attempts by whether they read the grader and whether they passed."""
    counts = {True: [0, 0], False: [0, 0]}
    agent = ""
    for result in load(results_dir / RESULTS_FILE):
        agent = result.agent
        if result.outcome == "error" or (result.stop == "truncated" and result.steps == 0):
            continue
        read = read_grader(results_dir / result.transcript) if result.transcript else False
        counts[read][0] += result.outcome == "pass"
        counts[read][1] += 1
    return Split(agent, tuple(counts[True]), tuple(counts[False]))


def read_grader(transcript: Path) -> bool:
    """Whether any tool call in `transcript` opened Maelstrom's jar or named a checker source."""
    for line in transcript.read_text().splitlines():
        event = json.loads(line)
        if event.get("type") != "assistant_message":
            continue
        for call in event["payload"].get("tool_calls") or []:
            arguments = json.dumps(call.get("arguments"))
            if any(sign in arguments for sign in SIGNS):
                return True
    return False


def table(splits: list[Split]) -> str:
    """One line per agent, then everyone together."""
    rows = [("agent", "read the grader", "didn't")]
    rows += [(s.agent, rate(s.read), rate(s.did_not)) for s in splits]
    everyone_read = tuple(map(sum, zip(*(s.read for s in splits), strict=True)))
    everyone_did_not = tuple(map(sum, zip(*(s.did_not for s in splits), strict=True)))
    rows.append(("all", rate(everyone_read), rate(everyone_did_not)))
    widths = [max(len(row[i]) for row in rows) for i in range(3)]
    lines = ("  ".join(cell.ljust(w) for cell, w in zip(row, widths, strict=True)) for row in rows)
    return "\n".join(line.rstrip() for line in lines)


def rate(passed_attempts: tuple[int, int]) -> str:
    """"7/8 (88%)", or "-" for no attempts."""
    passed, attempts = passed_attempts
    return f"{passed}/{attempts} ({passed / attempts:.0%})" if attempts else "-"


def main(dirs: list[str]) -> int:
    print(table([split(Path(d)) for d in dirs]))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))

