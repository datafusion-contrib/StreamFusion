#!/usr/bin/env python3
"""Compare runtime CI JVM compilation with unchanged SQL and rescaling assertions."""

import argparse
from collections import Counter
import json
import os
from pathlib import Path
import resource
import shutil
import subprocess
import time
import xml.etree.ElementTree as ET


ROOT = Path(__file__).resolve().parents[1]
CLASSES = {
    "FlinkJvmOverSqlHarnessTest": (181, {"2.2": 0, "1.18": 2}),
    "FlinkDecimalNegateAbsSqlHarnessTest": (13, {"2.2": 0, "1.18": 0}),
    "FlinkStringCollectionSqlHarnessTest": (80, {"2.2": 0, "1.18": 54}),
    "StatefulSqlRescaleTest": (31, {"2.2": 2, "1.18": 2}),
}


def outcomes(reports, suffix, line):
    cases = Counter()
    totals = Counter()
    classes = {}
    for report in reports.glob(f"TEST-*-{suffix}.xml"):
        suite = ET.parse(report).getroot()
        name = suite.get("name").removesuffix(f"({suffix})")
        if name in classes:
            raise ValueError(f"Duplicate runtime profile class: {name}")
        counts = {key: int(suite.get(key, 0)) for key in ("tests", "failures", "errors", "skipped")}
        totals.update(counts)
        classes[name] = {**counts, "seconds": float(suite.get("time", 0))}
        observed = Counter()
        for case in suite.iter("testcase"):
            status = next((key for key in ("failure", "error", "skipped")
                           if case.find(key) is not None), "passed")
            cases[(case.get("classname").removesuffix(f"({suffix})"), case.get("name"), status)] += 1
            observed[status] += 1
        if (sum(observed.values()) != counts["tests"]
                or observed["failure"] != counts["failures"]
                or observed["error"] != counts["errors"]
                or observed["skipped"] != counts["skipped"]):
            raise ValueError(f"Report counts do not match test invocations: {name}")
    expected = {"tech.streamfusion." + name for name in CLASSES}
    if set(classes) != expected:
        raise ValueError(f"Changed runtime profile class selection: {set(classes) ^ expected}")
    for name, result in classes.items():
        tests, skipped = CLASSES[name.rsplit(".", 1)[-1]]
        if (result["tests"] != tests or result["skipped"] != skipped[line]
                or result["errors"] or result["failures"]):
            raise ValueError(f"Incomplete runtime profile: {name}: {result}")
    if sum(cases.values()) != totals["tests"]:
        raise ValueError("Report totals do not match the test invocation inventory")
    return cases, dict(totals), classes


def profile(output, repetitions, line):
    if "TieredStopAtLevel" in os.environ.get("JAVA_TOOL_OPTIONS", ""):
        raise ValueError("Remove inherited TieredStopAtLevel before the default trial")
    output.mkdir(parents=True, exist_ok=False)
    temporary = output / "tmp"
    temporary.mkdir()
    reports = ROOT / "streamfusion-runtime/target/surefire-reports"
    trials = []
    expected = None
    try:
        for pair in range(repetitions):
            for mode in (("default", "startup") if pair % 2 == 0 else ("startup", "default")):
                suffix = f"runtime-profile-{line}-{pair}-{mode}"
                options = os.environ.get("JAVA_TOOL_OPTIONS", "")
                options += f' -Djava.io.tmpdir="{temporary}"'
                if mode == "startup":
                    options += " -XX:TieredStopAtLevel=1"
                environment = {**os.environ, "JAVA_TOOL_OPTIONS": options, "TMPDIR": str(temporary)}
                command = ["mvn", "-B", "-ntp", "-pl", "streamfusion-runtime"]
                if line == "1.18":
                    command.append("-Pflink-1.18")
                command.extend([f"-Dtest={','.join(CLASSES)}", "-Dsf.testForks=2",
                                "-Dsf.testReuseForks=false", "-Dsf.extraJvmArgs=-Xmx4g",
                                f"-Dsurefire.reportNameSuffix={suffix}", "surefire:test@default-test"])
                log = output / (suffix + ".log")
                before = resource.getrusage(resource.RUSAGE_CHILDREN)
                start = time.monotonic()
                with log.open("w") as stream:
                    result = subprocess.run(command, cwd=ROOT, stdout=stream,
                                            stderr=subprocess.STDOUT, env=environment)
                elapsed = time.monotonic() - start
                after = resource.getrusage(resource.RUSAGE_CHILDREN)
                if result.returncode:
                    raise RuntimeError(f"Runtime profile failed: {log}")
                cases, counts, classes = outcomes(reports, suffix, line)
                if expected is not None and cases != expected:
                    raise ValueError(f"Changed test invocations or outcomes: {log}")
                expected = cases
                trial = {"mode": mode, "pair": pair + 1, "line": line,
                         "elapsed_seconds": elapsed,
                         "cpu_seconds": after.ru_utime + after.ru_stime - before.ru_utime - before.ru_stime,
                         "outcomes": counts, "classes": classes}
                trials.append(trial)
                (output / "summary.json").write_text(json.dumps(trials, indent=2) + "\n")
                print(f"{mode} {pair + 1}: {elapsed:.2f}s elapsed, {trial['cpu_seconds']:.2f}s CPU, "
                      f"{counts['tests'] - counts['skipped']} passed, {counts['skipped']} existing skips", flush=True)
                log.unlink()
                for report in reports.glob(f"*-{suffix}.*"):
                    report.unlink()
    finally:
        shutil.rmtree(temporary)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True, help="New directory for concise trial evidence")
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--line", choices=("2.2", "1.18"), required=True)
    args = parser.parse_args()
    if args.repetitions < 2:
        parser.error("Use at least two pairs to alternate execution order")
    profile(args.output.resolve(), args.repetitions, args.line)


if __name__ == "__main__":
    main()
