#!/usr/bin/env python3
"""Compare runtime CI JVM compilation with unchanged SQL and rescaling assertions."""

import argparse
from collections import Counter
import json
import os
from pathlib import Path
import resource
import re
import shutil
import subprocess
import time
import xml.etree.ElementTree as ET

import ci_jfr


ROOT = Path(__file__).resolve().parents[1]
CLASSES = {
    "FlinkJvmOverSqlHarnessTest": (181, {"2.2": 0, "1.18": 2}),
    "FlinkDecimalNegateAbsSqlHarnessTest": (13, {"2.2": 0, "1.18": 0}),
    "FlinkStringCollectionSqlHarnessTest": (80, {"2.2": 0, "1.18": 54}),
    "StatefulSqlRescaleTest": (31, {"2.2": 2, "1.18": 2}),
}
HOTSPOTS = {
    "planner.CanonicalGroupDistinctMergeTest": (120, {"2.2": 0}),
    "planner.CanonicalManagedUdfKeyPrecisionTest": (532, {"2.2": 0}),
    "planner.CanonicalMiniBatchGroupStateTest": (290, {"2.2": 0}),
    "FlinkRecursiveScalarSqlHarnessTest": (1134, {"2.2": 0}),
    "FlinkRecursiveTableFunctionSqlHarnessTest": (683, {"2.2": 0}),
}


def outcomes(reports, suffix, line, selection=CLASSES):
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
    expected = {"tech.streamfusion." + name for name in selection}
    if set(classes) != expected:
        raise ValueError(f"Changed runtime profile class selection: {set(classes) ^ expected}")
    for name, result in classes.items():
        tests, skipped = selection[name.removeprefix("tech.streamfusion.")]
        if (result["tests"] != tests or result["skipped"] != skipped[line]
                or result["errors"] or result["failures"]):
            raise ValueError(f"Incomplete runtime profile: {name}: {result}")
    if sum(cases.values()) != totals["tests"]:
        raise ValueError("Report totals do not match the test invocation inventory")
    return cases, dict(totals), classes


def profile(output, repetitions, line, parallel=False, jfr=False, hotspots=False, slow_mix=False):
    if "TieredStopAtLevel" in os.environ.get("JAVA_TOOL_OPTIONS", ""):
        raise ValueError("Remove inherited TieredStopAtLevel before the default trial")
    output.mkdir(parents=True, exist_ok=False)
    temporary = output / "tmp"
    temporary.mkdir()
    reports = ROOT / "streamfusion-runtime/target/surefire-reports"
    trials = []
    expected = None
    selection = HOTSPOTS if hotspots or slow_mix else CLASSES
    modes = ("current",) if hotspots else (("startup", "parallel") if parallel else ("default", "startup"))
    try:
        java_home = Path(os.environ["JAVA_HOME"]) if jfr else None
        java_major = int(re.search(r'^JAVA_VERSION="(\d+)', (java_home / "release").read_text(), re.M)[1]) if hotspots else None
        if jfr:
            ci_jfr.configuration(java_home, temporary / "runtime.jfc")
        for pair in range(repetitions):
            for mode in (modes if pair % 2 == 0 else tuple(reversed(modes))):
                suffix = f"runtime-profile-{line}-{pair}-{mode}"
                options = os.environ.get("JAVA_TOOL_OPTIONS", "")
                options += f' -Djava.io.tmpdir="{temporary}"'
                if mode != "default":
                    options += " -XX:TieredStopAtLevel=1"
                environment = {**os.environ, "JAVA_TOOL_OPTIONS": options, "TMPDIR": str(temporary)}
                command = ["mvn", "-B", "-ntp", "-pl", "streamfusion-runtime"]
                if line == "1.18":
                    command.append("-Pflink-1.18")
                forks, heap = (4, "2g") if mode == "parallel" or hotspots and java_major >= 17 else (2, "4g")
                extra = f"-Xmx{heap}"
                if jfr:
                    recording_dir = output / "jfr" / mode
                    recording_dir.mkdir(parents=True)
                    extra += (f" -XX:StartFlightRecording=filename={recording_dir},"
                              f"settings={temporary / 'runtime.jfc'},dumponexit=true")
                command.extend([f"-Dtest={','.join('tech.streamfusion.' + name for name in selection)}", f"-Dsf.testForks={forks}",
                                "-Dsf.testReuseForks=false", f"-Dsf.extraJvmArgs={extra}",
                                f"-Dsurefire.reportNameSuffix={suffix}", "surefire:test@default-test"])
                if hotspots:
                    fixture_directory = temporary / "fixture-timings"
                    command.append(f"-Dsf.fixtureTimingsDirectory={fixture_directory}")
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
                cases, counts, classes = outcomes(reports, suffix, line, selection)
                if expected is not None and cases != expected:
                    raise ValueError(f"Changed test invocations or outcomes: {log}")
                expected = cases
                trial = {"mode": mode, "pair": pair + 1, "line": line,
                         "forks": forks, "heap": heap, "instrumented": jfr,
                         "elapsed_seconds": elapsed,
                         "cpu_seconds": after.ru_utime + after.ru_stime - before.ru_utime - before.ru_stime,
                         "outcomes": counts, "classes": classes}
                if jfr:
                    trial["jfr"] = ci_jfr.summarize(list(recording_dir.glob("*.jfr")), java_home, len(selection))
                if hotspots:
                    files = list(fixture_directory.glob("*.txt"))
                    if len(files) != len(selection):
                        raise ValueError(f"Expected {len(selection)} fixture summaries, found {len(files)}")
                    trial["fixture_timings"] = [{"name": m[1], "calls": int(m[2]), "seconds": int(m[3]) / 1e9}
                                               for file in files
                                               for m in re.finditer(r"\[ci-fixture-timing\] (\S+) calls=(\d+) nanoseconds=(\d+)", file.read_text())]
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
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument("--parallel", action="store_true", help="Compare two 4 GiB vs four 2 GiB fresh C1 forks")
    modes.add_argument("--jfr", action="store_true", help="Capture one instrumented default/C1 pair; not timing evidence")
    modes.add_argument("--hotspots", action="store_true", help="Attribute five measured slow classes under current CI settings")
    parser.add_argument("--slow-mix", action="store_true", help="Use the five measured slow classes for paired comparisons")
    args = parser.parse_args()
    if (args.hotspots or args.slow_mix) and args.line != "2.2":
        parser.error("The measured hotspot inventory requires Flink 2.2")
    if args.repetitions < 2 and not (args.jfr or args.hotspots):
        parser.error("Use at least two pairs to alternate execution order")
    if (args.jfr or args.hotspots) and args.repetitions != 1:
        parser.error("Use --repetitions 1 for the separate JFR attribution pass")
    profile(args.output.resolve(), args.repetitions, args.line, args.parallel, args.jfr or args.hotspots, args.hotspots, args.slow_mix)


if __name__ == "__main__":
    main()
