#!/usr/bin/env python3
"""Check binary/source JARs against module-local Java inputs and SPI files."""

import argparse
from pathlib import Path
import zipfile


def source_files(module, line):
    roots = [module / "src/main/java", module / f"src/main/java-flink{line}"]
    expected = {}
    for root in roots:
        for source in root.rglob("*.java"):
            entry = source.relative_to(root).as_posix()
            if entry in expected:
                raise ValueError(f"duplicate production source: {entry}")
            expected[entry] = source
    if not expected:
        raise ValueError(f"no module-local production sources in {module}")
    return expected


def unique_entries(jar, artifact):
    entries = jar.namelist()
    if len(entries) != len(set(entries)):
        raise ValueError(f"duplicate ZIP entries in {artifact}")
    return entries


def check(module, artifact, line):
    expected = {Path(entry).with_suffix(".class").as_posix()
                for entry in source_files(module, line)}

    with zipfile.ZipFile(artifact) as jar:
        entries = unique_entries(jar, artifact)
        # Inner classes have the same source owner as their enclosing top-level type.
        classes = {entry for entry in entries if entry.endswith(".class")}
        owners = {entry.split("$", 1)[0] + ".class" if "$" in entry else entry
                  for entry in classes}
        missing, unexpected = expected - classes, owners - expected
        if missing or unexpected:
            raise ValueError(f"{artifact}: missing classes {sorted(missing)}; "
                             f"unexpected classes {sorted(unexpected)}")
        resources = module / "src/main/resources"
        services = {path.relative_to(resources).as_posix(): path.read_bytes()
                    for path in (resources / "META-INF/services").glob("*") if path.is_file()}
        bundled = {entry: jar.read(entry) for entry in entries
                   if entry.startswith("META-INF/services/") and not entry.endswith("/")}
        if bundled != services:
            raise ValueError(f"{artifact}: service providers differ from module resources")
    return len(expected)


def check_sources(module, artifact, line):
    expected = source_files(module, line)
    with zipfile.ZipFile(artifact) as jar:
        entries = unique_entries(jar, artifact)
        sources = {entry for entry in entries if entry.endswith(".java")}
        missing, unexpected = expected.keys() - sources, sources - expected.keys()
        if missing or unexpected:
            raise ValueError(f"{artifact}: missing sources {sorted(missing)}; "
                             f"unexpected sources {sorted(unexpected)}")
        binaries = {entry for entry in entries
                    if entry.endswith((".class", ".so", ".dylib", ".dll"))}
        if binaries:
            raise ValueError(f"{artifact}: executable/native payload in sources: {sorted(binaries)}")
        changed = [entry for entry, source in expected.items()
                   if jar.read(entry) != source.read_bytes()]
        if changed:
            raise ValueError(f"{artifact}: source contents differ: {sorted(changed)}")
    return len(expected)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("module", type=Path)
    parser.add_argument("artifact", type=Path)
    parser.add_argument("--flink-line", choices=("1.18", "2.2"), required=True)
    parser.add_argument("--sources-jar", type=Path)
    args = parser.parse_args()
    try:
        count = check(args.module, args.artifact, args.flink_line)
        if args.sources_jar is not None:
            check_sources(args.module, args.sources_jar, args.flink_line)
    except (OSError, ValueError, zipfile.BadZipFile) as error:
        parser.exit(1, f"{error}\n")
    source_check = ", exact source attachment" if args.sources_jar is not None else ""
    print(f"{args.artifact}: {count} production types, module SPI ownership{source_check} verified")


if __name__ == "__main__":
    main()
