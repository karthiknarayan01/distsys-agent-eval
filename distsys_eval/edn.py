"""A reader for EDN, the Clojure data format Maelstrom writes its verdict in.

    loads('{:valid? true, :stats {:count 25}}')  ->  {"valid?": True, "stats": {"count": 25}}

Maelstrom's results.edn is one large map. This turns it into plain Python: maps become dicts,
vectors and lists become lists, keywords and symbols become strings (Keyword and Symbol, so a
keyword value such as :unknown can still be told apart from a string), and numbers stay numbers.
It reads everything Clojure prints in those files, including what plain EDN lacks: ratios
(5/2), big numbers (12N, 1.5M) and tagged values such as #object[...], which become the value
after the tag. Only reading is supported.
"""

from __future__ import annotations

import re
from fractions import Fraction
from typing import Any


class Keyword(str):
    """:valid? is Keyword("valid?")."""


class Symbol(str):
    """A bare word such as java.lang.IllegalStateException."""


class EdnError(ValueError):
    """The text isn't EDN this reader understands."""


def loads(text: str) -> Any:
    """The single value in `text`."""
    reader = _Reader(text)
    value = reader.read()
    reader.skip_space()
    if reader.pos != len(text):
        raise EdnError(f"unexpected text after the value at offset {reader.pos}")
    return value


_DELIMITERS = set("()[]{}\";")
_NUMBER = re.compile(r"[+-]?\d+(\.\d+)?([eE][+-]?\d+)?[NM]?$|[+-]?\d+/\d+$")
_CLOSING = {"(": ")", "[": "]", "{": "}"}


class _Reader:
    def __init__(self, text: str):
        self.text = text
        self.pos = 0

    def read(self) -> Any:
        """The value starting at the current position."""
        self.skip_space()
        if self.pos >= len(self.text):
            raise EdnError("unexpected end of input")
        char = self.text[self.pos]
        if char in "([{":
            self.pos += 1
            items = self.read_until(_CLOSING[char])
            return _pairs(items) if char == "{" else items
        if char == "#":
            return self.read_dispatch()
        if char == '"':
            return self.read_string()
        if char == "\\":
            return self.read_char()
        if char in ")]}":
            raise EdnError(f"unexpected {char!r} at offset {self.pos}")
        return _atom(self.read_token())

    def read_until(self, closing: str) -> list[Any]:
        items = []
        while True:
            self.skip_space()
            if self.pos >= len(self.text):
                raise EdnError(f"missing {closing!r}")
            if self.text[self.pos] == closing:
                self.pos += 1
                return items
            items.append(self.read())

    def read_dispatch(self) -> Any:
        """What follows a #: a set, a discarded form, or a tagged value."""
        self.pos += 1
        following = self.text[self.pos : self.pos + 1]
        if following == "{":
            self.pos += 1
            return self.read_until("}")  # a set, as a list: its members may be unhashable
        if following == "_":
            self.pos += 1
            self.read()  # #_ discards the next value
            return self.read()
        self.read_token()  # the tag, e.g. inst or object; the value after it is kept
        return self.read()

    def read_string(self) -> str:
        self.pos += 1
        out = []
        while self.pos < len(self.text):
            char = self.text[self.pos]
            if char == '"':
                self.pos += 1
                return "".join(out)
            if char == "\\":
                self.pos += 1
                escaped = self.text[self.pos : self.pos + 1]
                if escaped == "u":
                    out.append(chr(int(self.text[self.pos + 1 : self.pos + 5], 16)))
                    self.pos += 4
                else:
                    out.append({"n": "\n", "t": "\t", "r": "\r"}.get(escaped, escaped))
            else:
                out.append(char)
            self.pos += 1
        raise EdnError("unterminated string")

    def read_char(self) -> str:
        self.pos += 1
        token = self.read_token() or self.text[self.pos - 1]
        named = {"newline": "\n", "space": " ", "tab": "\t", "return": "\r"}
        return named.get(token, token[:1])

    def read_token(self) -> str:
        start = self.pos
        while self.pos < len(self.text):
            char = self.text[self.pos]
            if char.isspace() or char == "," or char in _DELIMITERS:
                break
            self.pos += 1
        return self.text[start : self.pos]

    def skip_space(self) -> None:
        """Whitespace, commas and ; comments."""
        while self.pos < len(self.text):
            char = self.text[self.pos]
            if char == ";":
                end = self.text.find("\n", self.pos)
                self.pos = len(self.text) if end == -1 else end
            elif char.isspace() or char == ",":
                self.pos += 1
            else:
                return


def _pairs(items: list[Any]) -> dict[Any, Any]:
    if len(items) % 2:
        raise EdnError("a map needs an even number of forms")
    return {_hashable(items[i]): items[i + 1] for i in range(0, len(items), 2)}


def _hashable(key: Any) -> Any:
    """Map keys are usually keywords; a vector key becomes a tuple."""
    return tuple(_hashable(k) for k in key) if isinstance(key, list) else key


def _atom(token: str) -> Any:
    """A keyword, number, nil, boolean or symbol."""
    if token.startswith(":"):
        return Keyword(token[1:])
    if token in ("nil", "true", "false"):
        return {"nil": None, "true": True, "false": False}[token]
    if _NUMBER.match(token):
        if "/" in token:
            return float(Fraction(token))
        number = token.rstrip("NM")
        is_float = any(c in number for c in ".eE") or token.endswith("M")
        return float(number) if is_float else int(number)
    return Symbol(token)
