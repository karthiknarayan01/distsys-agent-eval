"""A chart of pass^k for the README: one line per agent, k along the bottom.

    svg = pass_k_chart([summarize(load(path)) for path in paths])
    Path("results/pass-k.svg").write_text(svg)

Each agent's line is its overall pass^k for k = 1, 2, ..., as far as all its tasks reach. A line
that holds up as k grows is an agent that can be relied on; one that falls away solved tasks now
and then. The chart is plain SVG, with no script, so it shows wherever the README does, and it
follows the reader's light or dark mode. The numbers are also in the report's table, which is
where a reader goes for exact values.
"""

from __future__ import annotations

from html import escape

from distsys_eval.report import AgentReport

# Categorical hues in a fixed order, light and dark (validated for adjacent lines in both modes).
LIGHT = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#6250d6", "#e34948"]
DARK = ["#3987e5", "#d95926", "#199e70", "#c98500", "#d55181", "#008300", "#9085e9", "#e66767"]

WIDTH, HEIGHT = 720, 400
LEFT, RIGHT, TOP, BOTTOM = 56, 170, 64, 48  # room for the y labels, the line labels, the title and legend
TICKS = (0.0, 0.25, 0.5, 0.75, 1.0)
CHAR_WIDTH = 6.6  # roughly, for 13px text: enough to space the legend
LABEL_GAP = 16  # the least distance between two line-end labels


def pass_k_chart(reports: list[AgentReport]) -> str:
    """The SVG. Raises ValueError for more agents than there are hues: past that, a line could
    only be told apart by its label, so the chart should be split instead."""
    if len(reports) > len(LIGHT):
        raise ValueError(f"at most {len(LIGHT)} agents fit one chart, not {len(reports)}")
    ks = max((max(r.overall, default=0) for r in reports), default=0)
    x, y = scales(max(ks, 1))
    parts = [style(len(reports)), title_and_legend(reports), grid(y, ks, x)]
    parts += [line(report, i, x, y) for i, report in enumerate(reports)]
    parts += [end_labels(reports, x, y)]
    body = "\n".join(parts)
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {WIDTH} {HEIGHT}" '
        f'width="{WIDTH}" height="{HEIGHT}" role="img" aria-label="Overall pass^k by agent">\n'
        f"{body}\n</svg>\n"
    )


def scales(ks: int):
    """Functions from k and from pass^k to x and y on the canvas."""
    plot_width, plot_height = WIDTH - LEFT - RIGHT, HEIGHT - TOP - BOTTOM

    def x(k: int) -> float:
        return LEFT + (plot_width * (k - 1) / (ks - 1) if ks > 1 else plot_width / 2)

    def y(value: float) -> float:
        return TOP + plot_height * (1 - value)

    return x, y


def style(agents: int) -> str:
    """Colors as classes, with the dark steps for a dark reader."""
    light = "".join(f".s{i}{{stroke:{c};fill:{c}}}" for i, c in enumerate(LIGHT[:agents]))
    dark = "".join(f".s{i}{{stroke:{c};fill:{c}}}" for i, c in enumerate(DARK[:agents]))
    return (
        "<style>"
        "svg{font:13px system-ui,-apple-system,'Segoe UI',sans-serif}"
        ".bg{fill:#fcfcfb}.ink{fill:#0b0b0b}.muted{fill:#52514e}.grid{stroke:#e4e3de;stroke-width:1}"
        f".axis{{stroke:#a3a29c;stroke-width:1}}{light}.mark{{stroke:#fcfcfb;stroke-width:2}}"
        "@media (prefers-color-scheme: dark){"
        ".bg{fill:#1a1a19}.ink{fill:#f0efec}.muted{fill:#c3c2b7}.grid{stroke:#33332f}"
        f".axis{{stroke:#5f5e58}}{dark}.mark{{stroke:#1a1a19}}}}"
        "</style>\n"
        f'<rect class="bg" width="{WIDTH}" height="{HEIGHT}" rx="8"/>'
    )


def title_and_legend(reports: list[AgentReport]) -> str:
    """The title, and a legend row under it: a swatch and the agent's name for each line."""
    parts = [f'<text class="ink" x="{LEFT}" y="28" font-size="16" font-weight="600">Overall pass^k</text>']
    cursor = LEFT
    for i, report in enumerate(reports):
        parts.append(f'<rect class="s{i}" x="{cursor}" y="40" width="12" height="12" rx="3"/>')
        parts.append(f'<text class="muted" x="{cursor + 18}" y="50">{escape(report.agent)}</text>')
        cursor += 18 + CHAR_WIDTH * len(report.agent) + 20
    return "\n".join(parts)


def grid(y, ks: int, x) -> str:
    """Horizontal lines at quarter steps with their values, and k along the bottom."""
    parts = []
    for value in TICKS:
        cls, at = ("axis" if value == 0 else "grid"), f"{y(value):.1f}"
        parts.append(f'<line class="{cls}" x1="{LEFT}" x2="{WIDTH - RIGHT}" y1="{at}" y2="{at}"/>')
        label = f'x="{LEFT - 10}" y="{y(value) + 4:.1f}" text-anchor="end"'
        parts.append(f'<text class="muted" {label}>{value:.2f}</text>')
    for k in range(1, ks + 1):
        at, below = f"{x(k):.1f}", HEIGHT - BOTTOM + 20
        parts.append(f'<text class="muted" x="{at}" y="{below}" text-anchor="middle">{k}</text>')
    parts.append(
        f'<text class="muted" x="{(LEFT + WIDTH - RIGHT) / 2:.1f}" y="{HEIGHT - 8}" text-anchor="middle">'
        "k: attempts drawn that must all pass</text>"
    )
    return "\n".join(parts)


def line(report: AgentReport, index: int, x, y) -> str:
    """One agent's line, and a marker on each k with its value as a tooltip."""
    points = sorted(report.overall.items())
    if not points:
        return ""
    path = " ".join(f"{'M' if i == 0 else 'L'}{x(k):.1f},{y(v):.1f}" for i, (k, v) in enumerate(points))
    parts = [f'<path class="s{index}" d="{path}" fill="none" stroke-width="2" stroke-linejoin="round" '
             'style="fill:none"/>']
    for k, value in points:
        parts.append(
            f'<circle class="s{index} mark" cx="{x(k):.1f}" cy="{y(value):.1f}" r="4.5">'
            f"<title>{escape(report.agent)}: pass^{k} = {value:.2f}</title></circle>"
        )
    return "\n".join(parts)


def end_labels(reports: list[AgentReport], x, y) -> str:
    """Each agent's short name and last value beside the end of its line, nudged apart so that
    lines ending close together don't print over each other. The legend has the full names."""
    ends = []
    for report in reports:
        if report.overall:
            k, value = max(report.overall.items())
            ends.append([y(value) + 4, x(k) + 12, f"{escape(short_name(report.agent))} {value:.2f}"])
    ends.sort()
    for above, below in zip(ends, ends[1:], strict=False):
        below[0] = max(below[0], above[0] + LABEL_GAP)
    return "\n".join(
        f'<text class="ink" x="{at_x:.1f}" y="{at_y:.1f}">{text}</text>' for at_y, at_x, text in ends
    )


def short_name(agent: str) -> str:
    """The last part of an agent's name, which tells the lines apart: "qwen3.5:9b" for
    "shellmate/ollama/qwen3.5:9b"."""
    return agent.rsplit("/", 1)[-1]
