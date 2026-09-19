#!/usr/bin/env python3
"""Reject Linux JNI libraries that reserve TLS at process startup."""

from pathlib import Path
import re
import subprocess
import sys
import tempfile
import zipfile


def check_library(path):
    output = subprocess.check_output(["readelf", "--wide", "--dynamic", "--relocs", str(path)], text=True)
    if re.search(r"\b(?:STATIC_TLS|R_X86_64_TPOFF(?:32|64)|R_AARCH64_TLS_TPREL(?:32|64))\b", output):
        raise ValueError("requires static TLS; JNI libraries must support loading after JVM startup")
    if "Dynamic section" not in output:
        raise ValueError("missing ELF dynamic section")


def check_archive(filename):
    with zipfile.ZipFile(filename) as archive, tempfile.TemporaryDirectory() as directory:
        for entry in archive.namelist():
            if "/linux/" not in entry or not entry.endswith(".so"):
                continue
            library = Path(directory) / "library.so"
            library.write_bytes(archive.read(entry))
            try:
                check_library(library)
            except ValueError as error:
                raise ValueError(f"{filename}!/{entry}: {error}") from error


if __name__ == "__main__":
    try:
        for filename in sys.argv[1:]:
            check_archive(filename)
    except ValueError as error:
        raise SystemExit(str(error)) from error
