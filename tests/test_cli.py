"""The command line: it starts, prints its version, and shows help with no command."""

import json

import pytest

from distsys_eval import __version__
from distsys_eval.cli import main
from distsys_eval.tasks import load_tasks


def test_version(capsys):
    with pytest.raises(SystemExit) as exit:
        main(["--version"])
    assert exit.value.code == 0
    assert capsys.readouterr().out.strip() == f"distsys-eval {__version__}"


def test_no_command_shows_help(capsys):
    assert main([]) == 0
    assert "usage: distsys-eval" in capsys.readouterr().out


def test_build_image_runs_docker_build(monkeypatch):
    calls = []
    monkeypatch.setattr("subprocess.run", lambda argv: calls.append(argv) or type("R", (), {"returncode": 0}))
    assert main(["build-image", "--tag", "sandbox:test"]) == 0
    (argv,) = calls
    assert argv[:4] == ["docker", "build", "--tag", "sandbox:test"]
    assert argv[4].endswith("docker")


def test_tasks_lists_every_task(capsys):
    assert main(["tasks"]) == 0
    lines = capsys.readouterr().out.splitlines()
    assert [line.split()[0] for line in lines] == [task.id for task in load_tasks()]
    echo = next(line for line in lines if line.startswith("echo "))
    assert "m=3" in echo and "Echo" in echo


def test_tasks_shows_one_brief(capsys):
    assert main(["tasks", "echo"]) == 0
    out = capsys.readouterr().out
    assert out.startswith("# Your workspace") and "maelstrom test --workload echo" in out


def test_tasks_unknown_id(capsys):
    assert main(["tasks", "nope"]) == 1
    assert "no task 'nope'" in capsys.readouterr().err


def test_grade_prints_the_grade_and_exits_0_on_a_pass(monkeypatch, capsys, tmp_path):
    from distsys_eval.grader import Grade
    from distsys_eval.sandbox import ExecResult

    seen = []

    async def fake_grade_directory(task, source, image):
        seen.append((task.id, source, image))
        return Grade("pass", None, ExecResult(0, "", False, False), [])

    monkeypatch.setattr("distsys_eval.cli.grade_directory", fake_grade_directory)
    assert main(["grade", "echo", str(tmp_path), "--image", "sandbox:test"]) == 0
    assert seen == [("echo", tmp_path, "sandbox:test")]
    assert capsys.readouterr().out.startswith("echo: pass")


def test_grade_refuses_an_unknown_task_or_a_missing_directory(capsys, tmp_path):
    assert main(["grade", "nope", str(tmp_path)]) == 2
    assert main(["grade", "echo", str(tmp_path / "missing")]) == 2
    err = capsys.readouterr().err
    assert "no task 'nope'" in err and "isn't a directory" in err


def test_validate_exits_1_when_a_check_comes_out_wrong(monkeypatch, capsys):
    from distsys_eval.grader import Grade
    from distsys_eval.sandbox import ExecResult
    from distsys_eval.validate import Check

    async def fake_validate(tasks, image, jobs):
        (task,) = tasks
        from distsys_eval.validate import subjects

        return [Check(subject, Grade("pass", None, ExecResult(0, "", False, False), []))
                for subject in subjects(task)]

    monkeypatch.setattr("distsys_eval.cli.validate", fake_validate)
    assert main(["validate", "echo"]) == 1
    out = capsys.readouterr().out
    assert "ok     echo/reference" in out and "WRONG" in out


def test_validate_refuses_an_unknown_task(capsys):
    assert main(["validate", "nope"]) == 2
    assert "no task 'nope'" in capsys.readouterr().err


def test_calibrate_writes_only_with_write_and_exits_1_when_a_task_has_no_m(monkeypatch, capsys):
    from tests.test_calibrate import calibration

    written = []

    async def fake_calibrate(tasks, trials, image, jobs):
        assert trials == 7
        return [calibration(caught=(20, 20)), calibration(caught=(0, 20))]

    monkeypatch.setattr("distsys_eval.calibrate.calibrate", fake_calibrate)
    monkeypatch.setattr("distsys_eval.calibrate.write", written.append)
    assert main(["calibrate", "echo", "--trials", "7"]) == 1
    assert written == []
    assert main(["calibrate", "echo", "--trials", "7", "--write"]) == 1
    assert len(written) == 1 and written[0].runs == 3
    assert "no m is enough" in capsys.readouterr().out


def test_run_says_so_when_the_provider_cant_be_used(monkeypatch, capsys):
    monkeypatch.delenv("DEEPSEEK_API_KEY", raising=False)
    assert main(["run", "--provider", "deepseek"]) == 2
    assert "DEEPSEEK_API_KEY is not set" in capsys.readouterr().err


def test_run_prints_each_attempt_then_the_totals(monkeypatch, capsys):
    from distsys_eval.results import AttemptResult

    async def fake_run(agent, tasks, attempts, out, jobs, sandbox, on_result):
        assert (agent.name, attempts, jobs) == ("shellmate/ollama/qwen3.5:4b", 2, 1)
        assert out.as_posix() == "results/shellmate-ollama-qwen3.5-4b"
        results = [AttemptResult("echo", n, agent.name, "pass") for n in (1, 2)]
        for result in results:
            on_result(result)
        return results

    monkeypatch.setattr("distsys_eval.runner.run", fake_run)
    assert main(["run", "echo", "--provider", "ollama", "--model", "qwen3.5:4b", "--attempts", "2"]) == 0
    assert capsys.readouterr().out.splitlines()[1:] == [
        "echo #1: pass", "echo #2: pass", "2 attempts: 2 passed, 0 failed, 0 errors (not scored)",
    ]


def test_run_a_command_agent_with_a_network_and_its_key(monkeypatch, capsys):
    seen = {}

    def fake_sandboxes(image, network, env):
        seen["sandbox"] = (image, network, env)
        return None

    async def fake_run(agent, tasks, attempts, out, jobs, sandbox, on_result):
        seen["agent"] = (agent.name, agent.command, out.as_posix())
        return []

    monkeypatch.setattr("distsys_eval.runner.sandboxes", fake_sandboxes)
    monkeypatch.setattr("distsys_eval.runner.run", fake_run)
    argv = ["run", "echo", "--command", 'claude -p "$(cat "$BRIEF")"', "--name", "claude-code",
            "--image", "mine", "--network", "--env", "ANTHROPIC_API_KEY"]
    assert main(argv) == 0
    assert seen["sandbox"] == ("mine", "bridge", ("ANTHROPIC_API_KEY",))
    assert seen["agent"] == ("claude-code", 'claude -p "$(cat "$BRIEF")"', "results/claude-code")


def test_a_command_agent_needs_a_name(capsys):
    assert main(["run", "--command", "true"]) == 2
    assert "--command needs --name" in capsys.readouterr().err


def test_report_prints_a_table_or_json(tmp_path, capsys):
    from distsys_eval.results import AttemptResult, append

    append(tmp_path / "results.jsonl", AttemptResult("echo", 1, "shellmate/fake", "pass"))
    assert main(["report", str(tmp_path)]) == 0
    assert capsys.readouterr().out.startswith("shellmate/fake\n")
    assert main(["report", str(tmp_path), "--json"]) == 0
    assert json.loads(capsys.readouterr().out)[0]["agent"] == "shellmate/fake"


def test_report_says_so_when_a_directory_has_no_results(tmp_path, capsys):
    assert main(["report", str(tmp_path)]) == 2
    assert "no results in" in capsys.readouterr().err
