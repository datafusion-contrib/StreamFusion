#!/usr/bin/env python3
"""Resolve CI dependencies with bounded retries before any build or test lifecycle."""

import argparse
from pathlib import Path
import subprocess
import sys
import time


ROOT = Path(__file__).resolve().parents[1]
GOAL = "org.apache.maven.plugins:maven-dependency-plugin:3.11.0:go-offline"
DELAYS = (5, 15)
TIMEOUT_SECONDS = 600


def resolve(options):
    command = [options.maven, "-B", "-ntp", "-U", "-s", str(ROOT / "dev/flink-suite/settings.xml")]
    if options.profile:
        command.append("-P" + options.profile)
    if options.projects:
        command.extend(["-pl", options.projects])
    if options.also_make:
        command.append("-am")
    if options.pom:
        command.extend(["-f", str(options.pom)])
    if options.repository:
        command.append("-Dmaven.repo.local=" + str(options.repository))
    command.append(GOAL)
    attempts = len(DELAYS) + 1
    for attempt in range(attempts):
        print(f"Resolving Maven dependencies (attempt {attempt + 1}/{attempts})", flush=True)
        try:
            status = subprocess.run(command, timeout=TIMEOUT_SECONDS).returncode
        except subprocess.TimeoutExpired:
            status = 124
        except OSError as error:
            print(f"Cannot start Maven dependency resolution: {error}", file=sys.stderr)
            return 1
        if status == 0:
            return 0
        if attempt < len(DELAYS):
            time.sleep(DELAYS[attempt])
    return status


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", default="")
    parser.add_argument("--projects")
    parser.add_argument("--also-make", action="store_true")
    parser.add_argument("--pom", type=Path)
    parser.add_argument("--repository", type=Path)
    parser.add_argument("--maven", default="mvn")
    return resolve(parser.parse_args())


if __name__ == "__main__":
    sys.exit(main())
