"""The pass^k chart: a well-formed SVG with a line and a label per agent, labels kept apart, and
a refusal when there are more agents than hues."""

import xml.etree.ElementTree as ET
from collections import Counter

import pytest

from distsys_eval.chart import LABEL_GAP, LIGHT, pass_k_chart, short_name
from distsys_eval.report import AgentReport

SVG = "{http://www.w3.org/2000/svg}"


def report(agent: str, overall: dict[int, float]) -> AgentReport:
    return AgentReport(agent, [], overall, Counter())


def parse(svg: str) -> ET.Element:
    return ET.fromstring(svg)


def test_a_line_and_a_marker_per_k_for_each_agent():
    svg = parse(pass_k_chart([report("shellmate/ollama/qwen3.5:9b", {1: 0.5, 2: 0.2, 3: 0.1}),
                              report("claude-code", {1: 0.8, 2: 0.7, 3: 0.65})]))
    assert [p.get("class") for p in svg.iter(f"{SVG}path")] == ["s0", "s1"]
    assert len(list(svg.iter(f"{SVG}circle"))) == 6
    texts = [t.text for t in svg.iter(f"{SVG}text")]
    assert "shellmate/ollama/qwen3.5:9b" in texts  # the legend has the full name
    assert "qwen3.5:9b 0.10" in texts and "claude-code 0.65" in texts  # the line ends, the short one
    titles = [t.text for t in svg.iter(f"{SVG}title")]
    assert "claude-code: pass^2 = 0.70" in titles


def test_labels_of_lines_that_end_together_are_kept_apart():
    svg = parse(pass_k_chart([report("a", {1: 0.5, 2: 0.4}), report("b", {1: 0.5, 2: 0.4})]))
    ends = [float(t.get("y")) for t in svg.iter(f"{SVG}text") if t.text in ("a 0.40", "b 0.40")]
    assert abs(ends[1] - ends[0]) >= LABEL_GAP


def test_names_are_escaped():
    svg = pass_k_chart([report("me/<odd>&name", {1: 1.0})])
    assert "&lt;odd&gt;&amp;name" in svg
    parse(svg)


def test_more_agents_than_hues_are_refused():
    with pytest.raises(ValueError, match="at most 8 agents"):
        pass_k_chart([report(f"a{i}", {1: 0.5}) for i in range(len(LIGHT) + 1)])


def test_the_short_name_is_the_last_part():
    assert short_name("shellmate/deepseek/deepseek-chat") == "deepseek-chat"
    assert short_name("claude-code") == "claude-code"
