"""Stripping the checkers' source from Maelstrom's jar: compiled namespaces lose their source,
everything else is kept byte for byte."""

import importlib.util
import zipfile
from pathlib import Path

SCRIPT = Path(__file__).parent.parent / "docker" / "strip_checker_source.py"
spec = importlib.util.spec_from_file_location("strip_checker_source", SCRIPT)
strip_checker_source = importlib.util.module_from_spec(spec)
spec.loader.exec_module(strip_checker_source)


def jar(path, files):
    with zipfile.ZipFile(path, "w") as z:
        for name, content in files.items():
            z.writestr(name, content)
    return path


def test_source_goes_only_where_the_namespace_is_compiled(tmp_path):
    path = jar(tmp_path / "maelstrom.jar", {
        "maelstrom/workload/kafka.clj": "(ns maelstrom.workload.kafka)",
        "maelstrom/workload/kafka__init.class": b"\xca\xfe",
        "jepsen/tests/cycle.clj": "(ns jepsen.tests.cycle)",  # not compiled: Maelstrom needs it
        "data_readers.clj": "{}",
        "META-INF/MANIFEST.MF": "Main-Class: maelstrom.core\n",
    })

    assert strip_checker_source.strip(path) == 1

    with zipfile.ZipFile(path) as z:
        assert sorted(z.namelist()) == [
            "META-INF/MANIFEST.MF", "data_readers.clj", "jepsen/tests/cycle.clj",
            "maelstrom/workload/kafka__init.class",
        ]
        assert z.read("maelstrom/workload/kafka__init.class") == b"\xca\xfe"
