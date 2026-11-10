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
