"""Remove the Clojure source of Maelstrom's checkers from its jar, keeping the compiled classes.

    python3 strip_checker_source.py /opt/maelstrom/lib/maelstrom.jar

A source file is removed only when its namespace's compiled loader (<path>__init.class) is in
the jar beside it, so Maelstrom loads exactly what it loaded before. That covers every Maelstrom
workload and the Jepsen and Elle checkers they use. Used to build a sandbox where an agent can't
read how its program will be graded.
"""

from __future__ import annotations

import sys
import zipfile
from pathlib import Path

SOURCE_SUFFIXES = (".clj", ".cljc")


def compiled_sources(names: set[str]) -> set[str]:
    """The source files in `names` whose namespace is also there compiled."""
    return {
        name for name in names
        if name.endswith(SOURCE_SUFFIXES) and name.rsplit(".", 1)[0] + "__init.class" in names
    }


def strip(jar: Path) -> int:
    """Rewrite `jar` without its compiled namespaces' sources. Returns how many were removed."""
    stripped = jar.with_suffix(".stripped")
    with zipfile.ZipFile(jar) as source:
        drop = compiled_sources(set(source.namelist()))
        with zipfile.ZipFile(stripped, "w", zipfile.ZIP_DEFLATED) as out:
            for item in source.infolist():
                if item.filename not in drop:
                    out.writestr(item, source.read(item.filename))
    stripped.replace(jar)
    return len(drop)


if __name__ == "__main__":
    print(f"removed {strip(Path(sys.argv[1]))} source files")
