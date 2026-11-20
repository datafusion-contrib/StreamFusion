"""Validate explicit runtime owners without renaming durable deployment identities."""

import csv
from pathlib import Path
import re


ROOT = Path(__file__).resolve().parents[1]
MODES = {"batch", "stream", "shared"}


def rows(root, name):
    with (root / "dev/runtime-layout" / name).open(encoding="utf-8") as stream:
        return list(csv.DictReader(stream, delimiter="\t"))


def validate(root=ROOT):
    operators = rows(root, "operators.tsv")
    declared = {row["class"] for row in operators}
    source = root / "streamfusion-core/src/main/java"
    actual = {".".join(path.relative_to(source).with_suffix("").parts)
              for path in (source / "tech/streamfusion/operator").rglob("*.java")
              if path.name != "package-info.java"}
    if len(declared) != len(operators) or declared != actual:
        raise ValueError(f"Operator owners differ: missing={actual-declared}, stale={declared-actual}")
    if any(row["mode"] not in MODES or not row["family"] or not row["identity"] for row in operators):
        raise ValueError("Every runtime owner requires a mode, family and identity contract")
    previous, current = set(), set()
    for row in rows(root, "test-relocations.tsv"):
        if row["previous_class"] in previous or row["current_class"] in current:
            raise ValueError("Test relocation identities must be unique")
        previous.add(row["previous_class"])
        current.add(row["current_class"])
        if row["mode"] not in {"batch", "stream", "common"}:
            raise ValueError("Invalid test execution mode")
        if row["owner"] not in {"streamfusion-core", "streamfusion-runtime"}:
            raise ValueError("Invalid test source owner")
        if any(not re.fullmatch(r"[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)+", row[key])
               for key in ("previous_class", "current_class")):
            raise ValueError("Invalid test class name")
        if row["owner"] == "streamfusion-runtime":
            prefix = f"tech.streamfusion.runtime.{row['mode']}."
            if not row["current_class"].startswith(prefix):
                raise ValueError("Test package must match its execution mode")
        source = root / row["owner"] / "src/test/java"
        old = source / (row["previous_class"].replace(".", "/") + ".java")
        new = source / (row["current_class"].replace(".", "/") + ".java")
        if old.exists() or not new.is_file():
            raise ValueError(f"Test must have one current source owner: {row}")
        package = row["current_class"].rsplit(".", 1)[0]
        if not re.search(r"^package " + re.escape(package) + r";",
                         new.read_text(encoding="utf-8"), re.M):
            raise ValueError(f"Java package must follow its source directory: {new}")
    if previous & current:
        raise ValueError("Historical test names must map directly to current owners")
    return len(operators), len(current)


if __name__ == "__main__":
    operators, tests = validate()
    print(f"Verified {operators} runtime owners and {tests} test/fixture relocations")
