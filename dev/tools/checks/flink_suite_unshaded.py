#!/usr/bin/env python3
"""Reject a prepared source suite whose unshaded Maven artifacts were overwritten."""

import argparse
import hashlib
from pathlib import Path
import xml.etree.ElementTree as ET


MODULES = ("flink-sql-parser", "flink-table-calcite-bridge", "flink-table-planner")


def digest(path):
    result = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            result.update(chunk)
    return result.digest()


def validate(root, repository, version):
    namespace = {"m": "http://maven.apache.org/POM/4.0.0"}
    for module in MODULES:
        pom = root / f"{module}-{version}-effective.pom"
        model = ET.parse(pom).getroot()
        coordinates = [model.findtext(f"m:{field}", namespaces=namespace)
                       for field in ("groupId", "artifactId", "version")]
        if any(not value or "${" in value for value in coordinates):
            raise ValueError(f"Unresolved source-suite coordinates: {pom}")
        group, artifact, actual_version = coordinates
        if group != "org.apache.flink" or actual_version != version:
            raise ValueError(f"Unexpected source-suite coordinates: {coordinates}")
        installed = repository / group.replace(".", "/") / artifact / version / f"{artifact}-{version}"
        for expected, actual in ((pom, Path(str(installed) + ".pom")),
                                 (root / f"{module}-{version}-unshaded.jar",
                                  Path(str(installed) + ".jar"))):
            if digest(expected) != digest(actual):
                raise ValueError(f"The unshaded source-suite artifact was overwritten: {actual}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    parser.add_argument("repository", type=Path)
    parser.add_argument("version")
    args = parser.parse_args()
    try:
        validate(args.root, args.repository, args.version)
    except (OSError, ValueError, ET.ParseError) as error:
        parser.exit(2, f"Cannot publish the prepared source suite: {error}\n")
    print("Verified all three unshaded source-suite JARs and Maven POMs")


if __name__ == "__main__":
    main()
