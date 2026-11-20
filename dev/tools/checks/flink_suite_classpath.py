#!/usr/bin/env python3
"""Reject a source-suite classpath containing payloads from another Flink line."""

import argparse
import os
from pathlib import Path
import zipfile


def validate(classpath, line):
    if line not in {"1.18", "2.2"}:
        raise ValueError(f"Unsupported Flink line: {line}")
    suffix = "-flink1.18" if line == "1.18" else ""
    modules = set()
    for item in classpath.strip().split(os.pathsep):
        path = Path(item)
        with zipfile.ZipFile(path) as archive:
            try:
                manifest = archive.read("META-INF/MANIFEST.MF").decode("utf-8")
            except KeyError:
                manifest = ""
        manifest = manifest.replace("\r\n", "\n").replace("\n ", "")
        fields = dict(entry.split(": ", 1) for entry in manifest.splitlines() if ": " in entry)
        module = fields.get("StreamFusion-Module")
        actual_line = fields.get("StreamFusion-Flink-Line")
        if not module and not actual_line and not path.name.startswith("streamfusion-"):
            continue
        if (
            actual_line != line
            or not module
            or not module.startswith("streamfusion-")
            or (line == "1.18" and not module.endswith(suffix))
            or (line == "2.2" and "-flink" in module)
        ):
            raise ValueError(
                f"{path}: payload identity {module!r} / Flink {actual_line!r}, expected Flink {line}"
            )
        if module in modules:
            raise ValueError(f"Duplicate StreamFusion payload: {module}")
        modules.add(module)
    core = "streamfusion-core" + suffix
    if core not in modules:
        raise ValueError(f"Suite classpath is missing {core}")
    return modules


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("classpath", type=Path)
    parser.add_argument("line", choices=["1.18", "2.2"])
    args = parser.parse_args()
    try:
        modules = validate(args.classpath.read_text(), args.line)
    except (OSError, ValueError, zipfile.BadZipFile) as error:
        parser.exit(2, f"Cannot use the Flink suite classpath: {error}\n")
    print(f"Verified {len(modules)} source-suite payloads for Flink {args.line}")


if __name__ == "__main__":
    main()
