"""eval.sh, in its dry run: the options become the distsys-eval commands it would run."""

import shutil
import subprocess
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parent.parent / "eval.sh"
pytestmark = pytest.mark.skipif(not shutil.which("bash"), reason="eval.sh needs bash")


def dry_run(*args: str) -> list[str]:
    ran = subprocess.run(["bash", str(SCRIPT), "--dry-run", *args], stdin=subprocess.DEVNULL,
                         capture_output=True, text=True, check=True)
    return ran.stdout.splitlines()[1:]  # after the line saying what it will run


def test_each_model_is_run_with_its_provider_then_everything_is_reported():
    assert dry_run("--models", "qwen3.5:4b,deepseek", "--attempts", "3", "--jobs", "2",
                   "--tasks", "echo unique-ids") == [
        "distsys-eval run --attempts 3 --jobs 2 --provider ollama --model qwen3.5:4b echo unique-ids",
        "distsys-eval run --attempts 3 --jobs 2 --provider deepseek echo unique-ids",
        "distsys-eval report results/<each agent> --chart results/pass-k.svg > results/report.txt",
    ]


def test_without_a_terminal_it_asks_nothing_and_takes_the_defaults():
    (run, _report) = dry_run()
    assert run.startswith("distsys-eval run --attempts 5 --jobs 1 --provider ollama --model qwen3.5:")


def test_a_bad_option_is_refused():
    ran = subprocess.run(["bash", str(SCRIPT), "--model", "x"], stdin=subprocess.DEVNULL,
                         capture_output=True, text=True)
    assert ran.returncode == 1 and "Unknown option --model" in ran.stderr
