#!/usr/bin/env python3
"""Resolve CI dependencies with bounded retries before any build or test lifecycle."""

import argparse
from pathlib import Path
import random
import re
import subprocess
import sys
import time


ROOT = Path(__file__).resolve().parents[3]
ARTIFACT_POM = ROOT / "dev/flink-suite/runtime-dependencies/pom.xml"
GOAL = "org.apache.maven.plugins:maven-dependency-plugin:3.11.0:go-offline"
PROJECT_GOAL = "org.apache.maven.plugins:maven-dependency-plugin:3.11.0:resolve"
PLUGIN_GOAL = "org.apache.maven.plugins:maven-dependency-plugin:3.11.0:resolve-plugins"
ARTIFACT_GOAL = "org.apache.maven.plugins:maven-dependency-plugin:3.11.0:get"
MODEL_GOAL = "org.apache.maven.plugins:maven-help-plugin:3.5.1:evaluate"
GRAPH_GOAL = "org.apache.maven.plugins:maven-dependency-plugin:3.11.0:tree"
DELAYS = (15, 45)
TIMEOUT_SECONDS = 600
TRANSPORT_OPTIONS = [
    # Hosted Maven and upstream wrappers span both Resolver generations and Wagon.
    "-Daether.connector.requestTimeout=60000",
    "-Daether.connector.http.retryHandler.count=2",
    "-Daether.connector.http.retryHandler.interval=15000",
    "-Daether.connector.http.retryHandler.serviceUnavailable=429,500,502,503,504",
    "-Daether.transport.http.requestTimeout=60000",
    "-Daether.transport.http.retryHandler.count=2",
    "-Daether.transport.http.retryHandler.interval=15000",
    "-Daether.transport.http.retryHandler.serviceUnavailable=429,500,502,503,504",
    "-Dmaven.wagon.rto=60000",
    "-Dmaven.wagon.http.retryHandler.count=2",
]


def test_runtime_arguments(arguments):
    """Accept the consumer's model options, never a goal that could run tests on retry."""
    arguments = list(arguments)
    result = []
    index = 0
    while index < len(arguments):
        argument = arguments[index]
        if argument in ("-s", "-gs", "-f", "-pl", "-P"):
            if index + 1 == len(arguments) or arguments[index + 1].startswith("-"):
                raise ValueError(f"Missing value for Maven option {argument}")
            result.extend(arguments[index:index + 2])
            index += 2
            continue
        if argument != "-o":
            if argument not in ("-B", "-ntp", "-e") and not argument.startswith(("-D", "-P")):
                raise ValueError(f"Not a Maven test-runtime model option: {argument}")
            result.append(argument)
        index += 1
    return result


def resolve(options):
    settings = str(getattr(options, "settings", None) or ROOT / "dev/flink-suite/settings.xml")
    runtime = getattr(options, "test_runtime", None)
    if runtime is not None:
        arguments = test_runtime_arguments(runtime)
        command = [options.maven, "-B", "-ntp", *TRANSPORT_OPTIONS]
        for flag in ("-s", "-gs"):
            if flag not in arguments:
                command.extend([flag, settings])
        command.extend(arguments)
        # Keep plugin filters out of project resolution: they would hide test dependencies.
        for goals in ([PROJECT_GOAL], [PLUGIN_GOAL, "-DincludeGroupIds=org.apache.maven.plugins",
                                      "-DincludeArtifactIds=maven-surefire-plugin"]):
            status = resolve_command(command + goals, offline_first=options.offline_first)
            if status:
                return status
        return 0
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
    if getattr(options, "plugin_help", False):
        # Loading a help mojo uses Maven's actual plugin realm, including overrides.
        # dependency:resolve-plugins also visits the plugin's unmediated graph.
        if any(not re.fullmatch(r"[\w.-]+:[\w.-]+", plugin) for plugin in options.plugin_help):
            raise ValueError("Plugin help requires GROUP_ID:ARTIFACT_ID coordinates")
        goal = [plugin + ":help" for plugin in options.plugin_help]
    transitive = "false" if getattr(options, "no_transitive", False) else "true"
    commands = ([command + [ARTIFACT_GOAL, "-Dartifact=" + artifact, "-Dtransitive=" + transitive]
                 for artifact in options.artifact] if options.artifact else [command + goal])
    for invocation in commands:
        status = resolve_command(invocation, offline_first=getattr(options, "offline_first", False))
        if status:
            return status
    return 0


def resolve_command(command, *, offline_first=False):
    if offline_first:
        try:
            cached = subprocess.run([command[0], "-o", *command[1:]], timeout=TIMEOUT_SECONDS,
                                    stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
        except subprocess.TimeoutExpired:
            pass
        except OSError as error:
            print(f"Cannot start Maven dependency resolution: {error}", file=sys.stderr)
            return 1
        else:
            if cached.returncode == 0:
                print("Maven dependencies already available offline", flush=True)
                return 0
    attempts = len(DELAYS) + 1
    for attempt in range(attempts):
        print(f"Resolving Maven dependencies (attempt {attempt + 1}/{attempts})", flush=True)
        try:
            # Only a failed resolution needs to bypass Maven's negative lookup cache.
            invocation = [command[0], "-U", *command[1:]] if attempt else command
            status = subprocess.run(invocation, timeout=TIMEOUT_SECONDS).returncode
        except subprocess.TimeoutExpired:
            status = 124
            print(f"Maven dependency resolution timed out after {TIMEOUT_SECONDS} seconds",
                  file=sys.stderr, flush=True)
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
    parser.add_argument("--settings", type=Path,
                        help="Use this settings file for both user and global settings")
    parser.add_argument("--define", action="append", default=[], metavar="PROPERTY=VALUE",
                        help="Set the same model property used by the eventual Maven build")
    parser.add_argument("--offline-first", action="store_true",
                        help="Reuse a complete cache for pinned releases before attempting downloads")
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
    resolution.add_argument("--plugin-help", nargs="+", metavar="GROUP_ID:ARTIFACT_ID",
                            help="Load declared build plugins through dependency-only help goals before compilation")
    resolution.add_argument("--test-runtime", nargs=argparse.REMAINDER,
                            help="Resolve the exact consumer classpath and Surefire plugin; remaining arguments must be Maven options")
    options = parser.parse_args()
    try:
        return resolve(options)
    except ValueError as error:
        parser.error(str(error))


if __name__ == "__main__":
    sys.exit(main())
