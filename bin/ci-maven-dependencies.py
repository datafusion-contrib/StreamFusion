#!/usr/bin/env python3
"""Resolve CI dependencies with bounded retries before any build or test lifecycle."""

import argparse
from pathlib import Path
import random
import subprocess
import sys
import time


ROOT = Path(__file__).resolve().parents[1]
ARTIFACT_POM = ROOT / "dev/flink-suite/runtime-dependencies/pom.xml"
GOAL = "org.apache.maven.plugins:maven-dependency-plugin:3.11.0:go-offline"
PROJECT_GOAL = "org.apache.maven.plugins:maven-dependency-plugin:3.11.0:resolve"
ARTIFACT_GOAL = "org.apache.maven.plugins:maven-dependency-plugin:3.11.0:get"
MODEL_GOAL = "org.apache.maven.plugins:maven-help-plugin:3.5.1:evaluate"
GRAPH_GOAL = "org.apache.maven.plugins:maven-dependency-plugin:3.11.0:tree"
DELAYS = (15, 45)
TIMEOUT_SECONDS = 600
TRANSPORT_OPTIONS = [
    # Hosted Maven and upstream wrappers span both Resolver generations and Wagon.
    "-Daether.connector.requestTimeout=60000",
    "-Daether.connector.http.retryHandler.count=2",
    "-Daether.connector.http.retryHandler.serviceUnavailable=429,500,502,503,504",
    "-Daether.transport.http.requestTimeout=60000",
    "-Daether.transport.http.retryHandler.count=2",
    "-Daether.transport.http.retryHandler.serviceUnavailable=429,500,502,503,504",
    "-Dmaven.wagon.rto=60000",
    "-Dmaven.wagon.http.retryHandler.count=2",
]


def resolve(options):
    settings = str(ROOT / "dev/flink-suite/settings.xml")
    command = [options.maven, "-B", "-ntp", "-s", settings, "-gs", settings,
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
    command.extend("-D" + definition for definition in getattr(options, "define", ()))
    goal = ([MODEL_GOAL, "-Dexpression=project.id", "-DforceStdout"]
            if options.model_only else [GOAL])
    if getattr(options, "graph", False):
        goal = [GRAPH_GOAL, "-DoutputType=json", "-DoutputFile=target/ci-maven-graph.json"]
    if getattr(options, "resolve_project", False):
        # go-offline visits dependencies separately from Maven's lifecycle mediation.
        goal.append(PROJECT_GOAL)
    transitive = "false" if getattr(options, "no_transitive", False) else "true"
    commands = ([command + [ARTIFACT_GOAL, "-Dartifact=" + artifact, "-Dtransitive=" + transitive]
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
            # Only a failed resolution needs to bypass Maven's negative lookup cache.
            invocation = [command[0], "-U", *command[1:]] if attempt else command
            status = subprocess.run(invocation, timeout=TIMEOUT_SECONDS).returncode
        except subprocess.TimeoutExpired:
            status = 124
        except OSError as error:
            print(f"Cannot start Maven dependency resolution: {error}", file=sys.stderr)
            return 1
        if status == 0:
            return 0
        if attempt < len(DELAYS):
            time.sleep(random.uniform(DELAYS[attempt], DELAYS[attempt] * 1.5))
    return status


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", default="")
    parser.add_argument("--projects")
    parser.add_argument("--also-make", action="store_true")
    parser.add_argument("--pom", type=Path)
    parser.add_argument("--repository", type=Path)
    parser.add_argument("--maven", default="mvn")
    parser.add_argument("--define", action="append", default=[], metavar="PROPERTY=VALUE",
                        help="Set the same model property used by the eventual Maven build")
    parser.add_argument("--no-transitive", action="store_true",
                        help="Fetch exact artifacts from an already mediated dependency closure")
    resolution = parser.add_mutually_exclusive_group()
    resolution.add_argument("--artifact", action="append", default=[],
                            help="Resolve an artifact's original transitive graph in an independent POM")
    resolution.add_argument("--model-only", action="store_true",
                            help="Resolve project models and build extensions before compiling an upstream reactor")
    resolution.add_argument("--resolve-project", action="store_true",
                            help="Prepare plugins and the exact mediated classpath of a standalone project")
    resolution.add_argument("--graph", action="store_true",
                            help="Record the mediated dependency graph without compiling reactor artifacts")
    return resolve(parser.parse_args())


if __name__ == "__main__":
    sys.exit(main())
