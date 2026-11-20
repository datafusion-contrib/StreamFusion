#!/usr/bin/env python3
"""Check packaged Linux libraries against the official Flink image's glibc floor."""

import re
import subprocess
import sys
import tempfile
from pathlib import Path
import zipfile


def validate_versions(output):
    versions = set(re.findall(r"Name: (GLIBC_\S+)", output))
    if not versions:
        raise ValueError("No glibc version requirements found")
    for version in sorted(versions):
        suffix = version.removeprefix("GLIBC_")
        if not re.fullmatch(r"[0-9]+(?:\.[0-9]+)+", suffix):
            raise ValueError(f"Unsupported glibc requirement: {version}")
        if tuple(map(int, suffix.split("."))) > (2, 35):
            raise ValueError(f"Requires {version}; deployment baseline is GLIBC_2.35")


def main():
    for filename in sys.argv[1:]:
        with zipfile.ZipFile(filename) as archive, tempfile.TemporaryDirectory() as directory:
            for entry in archive.namelist():
                if "/linux/" not in entry or not entry.endswith(".so"):
                    continue
                library = Path(directory) / "library.so"
                library.write_bytes(archive.read(entry))
                output = subprocess.check_output(
                    ["readelf", "--version-info", str(library)], text=True
                )
                try:
                    validate_versions(output)
                except ValueError as error:
                    raise SystemExit(f"{filename}!/{entry}: {error}") from error
    print("Packaged Linux native libraries fit the glibc 2.35 deployment baseline")


if __name__ == "__main__":
    main()
