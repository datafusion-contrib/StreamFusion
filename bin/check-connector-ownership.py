#!/usr/bin/env python3
"""Check that a connector JAR contains exactly its module-local Java types and SPI files."""

import argparse
from pathlib import Path
import zipfile


def check(module, artifact, line):
    roots = [module / "src/main/java", module / f"src/main/java-flink{line}"]
    expected = set()
    for root in roots:
        for source in root.rglob("*.java"):
            entry = source.relative_to(root).with_suffix(".class").as_posix()
            if entry in expected:
                raise ValueError(f"duplicate production source: {entry}")
            expected.add(entry)
    if not expected:
        raise ValueError(f"no module-local production sources in {module}")

    with zipfile.ZipFile(artifact) as jar:
        entries = jar.namelist()
        # Inner classes have the same source owner as their enclosing top-level type.
        classes = {entry for entry in entries if entry.endswith(".class")}
        owners = {entry.split("$", 1)[0] + ".class" if "$" in entry else entry
                  for entry in classes}
        missing, unexpected = expected - classes, owners - expected
        if missing or unexpected:
            raise ValueError(f"{artifact}: missing classes {sorted(missing)}; "
                             f"unexpected classes {sorted(unexpected)}")
        if len(entries) != len(set(entries)):
            raise ValueError(f"duplicate ZIP entries in {artifact}")

        resources = module / "src/main/resources"
        services = {path.relative_to(resources).as_posix(): path.read_bytes()
                    for path in (resources / "META-INF/services").glob("*") if path.is_file()}
        bundled = {entry: jar.read(entry) for entry in entries
                   if entry.startswith("META-INF/services/") and not entry.endswith("/")}
        if bundled != services:
            raise ValueError(f"{artifact}: service providers differ from module resources")
    return len(expected)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("module", type=Path)
    parser.add_argument("artifact", type=Path)
    parser.add_argument("--flink-line", choices=("1.18", "2.2"), required=True)
    args = parser.parse_args()
    try:
        count = check(args.module, args.artifact, args.flink_line)
    except (OSError, ValueError, zipfile.BadZipFile) as error:
        parser.exit(1, f"{error}\n")
    print(f"{args.artifact}: {count} production types and module SPI ownership verified")


if __name__ == "__main__":
    main()
