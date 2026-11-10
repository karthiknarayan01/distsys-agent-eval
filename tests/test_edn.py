"""Reading EDN: each kind of value Maelstrom prints, and its real results files."""

from pathlib import Path

import pytest

from distsys_eval.edn import EdnError, Keyword, Symbol, loads

FIXTURES = Path(__file__).parent / "fixtures" / "maelstrom"


@pytest.mark.parametrize("text, value", [
    ("42", 42), ("-7", -7), ("1.5", 1.5), ("2e3", 2000.0), ("12N", 12), ("1.25M", 1.25), ("5/2", 2.5),
    ("nil", None), ("true", True), ("false", False),
    ('"say \\"hi\\"\\n"', 'say "hi"\n'), ('"\\u00e9"', "é"),
    ("\\a", "a"), ("\\newline", "\n"),
    ("[1 2, 3]", [1, 2, 3]), ("(1 (2))", [1, [2]]), ("#{1 2}", [1, 2]),
    ("{:a 1, :b {:c [true]}}", {"a": 1, "b": {"c": [True]}}),
    ("{[1 2] :x}", {(1, 2): "x"}),
    ("; a comment\n[1 #_2 3]", [1, 3]),
    ('#inst "2026-11-10T00:00:00Z"', "2026-11-10T00:00:00Z"),
    ("#object[java.io.File 0x1 \"/tmp\"]", [Symbol("java.io.File"), Symbol("0x1"), "/tmp"]),
])
def test_values(text, value):
    assert loads(text) == value


def test_keywords_and_symbols_are_strings_that_keep_their_kind():
    value = loads("[:unknown unknown \"unknown\"]")
    assert value == ["unknown", "unknown", "unknown"]
    assert [type(v) for v in value] == [Keyword, Symbol, str]


@pytest.mark.parametrize("text", ["[1 2", "{:a}", "]", '"open', "1 2", ""])
def test_bad_text(text):
    with pytest.raises(EdnError):
        loads(text)


@pytest.mark.parametrize(
    "name, valid", [("valid", True), ("invalid", False), ("unknown", Keyword("unknown"))]
)
def test_real_results_files(name, valid):
    results = loads((FIXTURES / f"{name}.edn").read_text())
    assert results["valid?"] == valid
    assert results["stats"]["by-f"]["echo"]["count"] > 0
