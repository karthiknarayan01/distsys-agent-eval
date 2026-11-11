"""Tasks: the ones shipped in tasks/ load and say the right things, and a bad task is refused
with a message naming it."""

import re
from pathlib import Path

import pytest

from distsys_eval.maelstrom import Rename
from distsys_eval.sandbox import DOCKERFILE_DIR
from distsys_eval.targets import Targets
from distsys_eval.tasks import TASKS_DIR, TaskError, load_task, load_tasks

GOOD_TOML = """\
title = "Echo"
runs = 2

[agent]
minutes = 15
steps = 50

[maelstrom]
workload = "echo"
node-count = 1
time-limit = 10
"""


# The tasks shipped in tasks/


def test_shipped_tasks_load():
    tasks = load_tasks()
    assert [task.id for task in tasks] == [
        "broadcast-fault-tolerant", "broadcast-few-messages", "broadcast-low-latency", "broadcast-multi-node",
        "broadcast-single-node", "echo", "g-counter", "kafka-multi-node", "kafka-single-node",
        "txn-read-committed", "unique-ids",
    ]
    assert all(task.runs >= 1 and task.prompt and task.title for task in tasks)


def test_unique_ids_is_tested_under_partitions():
    task = load_task(TASKS_DIR / "unique-ids")
    assert task.maelstrom.workload == "unique-ids"
    assert task.maelstrom.flags["nemesis"] == "partition"
    assert task.maelstrom.flags["availability"] == "total"


def test_brief_shows_the_limits_and_the_exact_grading_command():
    task = load_task(TASKS_DIR / "unique-ids")
    assert task.brief.startswith("# Your workspace")
    assert task.prompt in task.brief
    assert "You have 30 minutes and 100 steps" in task.brief
    assert "built with `go build -o node .` and tested 3 times" in task.brief
    assert (
        "maelstrom test --workload unique-ids --bin ./node --availability total --concurrency 2n "
        "--nemesis partition --node-count 3 --rate 1000 --time-limit 30"
    ) in task.brief


def test_an_efficiency_task_tells_the_agent_its_targets():
    task = load_task(TASKS_DIR / "broadcast-low-latency")
    assert task.targets == Targets(msgs_per_op=30, median_latency_ms=400, max_latency_ms=600)
    assert "A valid run must also meet these targets" in task.brief
    assert "At most 30 messages between nodes per client operation" in task.brief
    assert "A median latency of at most 400 ms" in task.brief
    assert "A maximum latency of at most 600 ms" in task.brief


def test_a_task_without_targets_says_nothing_about_them():
    task = load_task(TASKS_DIR / "echo")
    assert not task.targets and "targets" not in task.brief


def test_workspace_names_the_library_version_the_image_has():
    """The go get line agents are given must match what the image put in the module cache."""
    dockerfile = (DOCKERFILE_DIR / "Dockerfile").read_text()
    (image_version,) = re.findall(r"ARG MAELSTROM_GO_VERSION=(\S+)", dockerfile)
    workspace = (TASKS_DIR / "workspace.md").read_text()
    assert f"go get github.com/jepsen-io/maelstrom/demo/go@{image_version}" in workspace


# Refused tasks


def make_task(root: Path, toml: str = GOOD_TOML, prompt: str | None = "# Echo\n\nEcho it.") -> Path:
    (root / "workspace.md").write_text("# Your workspace")
    path = root / "echo"
    path.mkdir()
    (path / "task.toml").write_text(toml)
    if prompt is not None:
        (path / "prompt.md").write_text(prompt)
    return path


def test_a_good_task_loads(tmp_path):
    task = load_task(make_task(tmp_path))
    assert (task.id, task.title, task.runs) == ("echo", "Echo", 2)
    assert (task.limits.minutes, task.limits.steps) == (15.0, 50)
    assert task.maelstrom.flags == {"node-count": 1, "time-limit": 10}
    assert "tested 2 times" in task.brief
    assert task.targets == Targets()


RENAME = """
[rename.types]
echo = "say"
echo_ok = "said"

[rename.fields]
echo = "words"
"""


def test_a_renamed_protocol_loads_and_the_brief_shows_how_to_test_it(tmp_path):
    task = load_task(make_task(tmp_path, GOOD_TOML + RENAME))
    assert task.maelstrom.rename == Rename({"echo": "say", "echo_ok": "said"}, {"echo": "words"})
    assert "MAELSTROM_RENAME_BIN=/work/node" in task.brief
    assert "--bin /usr/local/bin/maelstrom-rename" in task.brief
    assert "Maelstrom itself uses other names for these messages" in task.brief


def test_a_task_without_a_rename_tests_its_binary_directly(tmp_path):
    task = load_task(make_task(tmp_path))
    assert task.maelstrom.rename is None
    assert "--bin ./node" in task.brief and "maelstrom-rename" not in task.brief


def test_targets_load_as_numbers(tmp_path):
    toml = GOOD_TOML + "\n[targets]\nmsgs-per-op = 20\nmax-latency-ms = 2000.5\n"
    task = load_task(make_task(tmp_path, toml))
    assert task.targets == Targets(msgs_per_op=20.0, max_latency_ms=2000.5)


@pytest.mark.parametrize(
    ("change", "message"),
    [
        (("runs = 2", "runs = 0"), "runs must be at least 1"),
        (("runs = 2", 'runs = "2"'), "runs is missing or has the wrong type"),
        (("runs = 2", "runs = true"), "runs is missing or has the wrong type"),
        (('title = "Echo"\n', ""), "title is missing"),
        (("runs = 2", "runs = 2\nseed = 1"), "task.toml has unknown keys: seed"),
        (("steps = 50", "steps = 50\nturns = 9"), "[agent] has unknown keys: turns"),
        (("steps = 50", "steps = 0"), "minutes and steps must be positive"),
        (('workload = "echo"\n', ""), "[maelstrom] needs a workload"),
        (("node-count = 1", "nodes = 1"), "not Maelstrom test flags: nodes"),
        (("runs = 2", "runs = = 2"), "task.toml:"),
        (("runs = 2", "runs = 2\ntargets = 3"), "targets must be a table"),
        (("runs = 2", "runs = 2\nrename = 3"), "rename must be a table"),
        (("[agent]", "[rename]\nnames = {}\n\n[agent]"), "[rename] has unknown keys: names"),
        (("[agent]", "[rename]\ntypes = {}\n\n[agent]"), "[rename] renames nothing"),
        (("[agent]", '[rename.types]\necho = 1\n\n[agent]'), "[rename.types] must map names to names"),
        (("[agent]", '[rename.types]\na = "x"\nb = "x"\n\n[agent]'), "renames two names to the same one"),
        (("[agent]", "[targets]\nlatency = 1\n\n[agent]"), "[targets] has unknown keys: latency"),
        (("[agent]", "[targets]\nmsgs-per-op = 0\n\n[agent]"), "[targets] msgs-per-op must be positive"),
        (("[agent]", '[targets]\nmsgs-per-op = "9"\n\n[agent]'), "msgs-per-op is missing or has the wrong"),
    ],
)
def test_a_bad_task_toml_is_refused(tmp_path, change, message):
    path = make_task(tmp_path, GOOD_TOML.replace(*change))
    with pytest.raises(TaskError, match=re.escape(message)) as error:
        load_task(path)
    assert str(error.value).startswith("echo: ")


def test_a_missing_prompt_is_refused(tmp_path):
    with pytest.raises(TaskError, match="echo: missing prompt.md"):
        load_task(make_task(tmp_path, prompt=None))


def test_an_empty_prompt_is_refused(tmp_path):
    with pytest.raises(TaskError, match="echo: prompt.md is empty"):
        load_task(make_task(tmp_path, prompt="\n"))


def test_a_missing_workspace_is_refused(tmp_path):
    path = make_task(tmp_path)
    (tmp_path / "workspace.md").unlink()
    with pytest.raises(TaskError, match="missing workspace.md"):
        load_task(path)
