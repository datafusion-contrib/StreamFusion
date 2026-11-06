#!/usr/bin/env python3
"""Resolve CI dependencies with bounded retries before any build or test lifecycle."""

import argparse
from pathlib import Path
import subprocess
import sys
import time


ROOT = Path(__file__).resolve().parents[1]
ARTIFACT_POM = ROOT / "dev/flink-suite/runtime-dependencies/pom.xml"
GOAL = "org.apache.maven.plugins:maven-dependency-plugin:3.11.0:go-offline"
ARTIFACT_GOAL = "org.apache.maven.plugins:maven-dependency-plugin:3.11.0:get"
MODEL_GOAL = "org.apache.maven.plugins:maven-help-plugin:3.5.1:evaluate"
DELAYS = (5, 15)
TIMEOUT_SECONDS = 600
TRANSPORT_OPTIONS = [
    # Hosted Maven and upstream wrappers span both Resolver generations and Wagon.
    "-Daether.connector.requestTimeout=60000",
    "-Daether.connector.http.retryHandler.count=5",
    "-Daether.connector.http.retryHandler.serviceUnavailable=429,500,502,503,504",
    "-Daether.transport.http.requestTimeout=60000",
    "-Daether.transport.http.retryHandler.count=5",
    "-Daether.transport.http.retryHandler.serviceUnavailable=429,500,502,503,504",
    "-Dmaven.wagon.rto=60000",
    "-Dmaven.wagon.http.retryHandler.count=5",
]


def resolve(options):
    settings = str(ROOT / "dev/flink-suite/settings.xml")
    command = [options.maven, "-B", "-ntp", "-U", "-s", settings, "-gs", settings,
               *TRANSPORT_OPTIONS]
    if options.artifact:
        # Build plugins and test providers can resolve original artifact graphs outside
        # the host project's dependency management.
        command.extend(["-f", str(ARTIFACT_POM)])
    elif options.profile:
        command.append("-P" + options.profile)
    if options.projects and not options.artifact:
        command.extend(["-pl", options.projects])
    if options.also_make and not options.artifact:
        command.append("-am")
    if options.pom and not options.artifact:
        command.extend(["-f", str(options.pom)])
    if options.repository:
        command.append("-Dmaven.repo.local=" + str(options.repository))
    goal = ([MODEL_GOAL, "-Dexpression=project.id", "-DforceStdout"]
            if options.model_only else [GOAL])
    commands = ([command + [ARTIFACT_GOAL, "-Dartifact=" + artifact, "-Dtransitive=true"]
                 for artifact in options.artifact] if options.artifact else [command + goal])
    for invocation in commands:
        status = resolve_command(invocation)
        if status:
            return status
    return 0


def resolve_command(command):
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
    resolution = parser.add_mutually_exclusive_group()
    resolution.add_argument("--artifact", action="append", default=[],
                            help="Resolve an artifact's original transitive graph in an independent POM")
    resolution.add_argument("--model-only", action="store_true",
                            help="Resolve project models and build extensions before compiling an upstream reactor")
    return resolve(parser.parse_args())


if __name__ == "__main__":
    sys.exit(main())
