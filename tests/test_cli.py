"""The command line: it starts, prints its version, and shows help with no command."""

import pytest

from distsys_eval import __version__
from distsys_eval.cli import main


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
    assert [line.split()[0] for line in lines] == ["broadcast-single-node", "echo", "unique-ids"]
    assert "m=3" in lines[1] and "Echo" in lines[1]


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
