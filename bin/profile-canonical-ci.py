#!/usr/bin/env python3
"""Compare cold-worker JVM settings with identical canonical handoff assertions."""

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
SELECTION = ("CanonicalRegularJoinJobTest#regularStateContinuesAcrossCleanEngineProcesses"
             "+nativeRegularCheckpointResumesBeforeCanonicalSwitch,CanonicalManagedUdfDecimalKeyJobTest")


def profile(output, repetitions):
    output.mkdir(parents=True, exist_ok=False)
    temporary = output / "tmp"
    temporary.mkdir()
    reports = ROOT / "streamfusion-runtime/target/surefire-reports"
    trials = []
    expected = None
    try:
        for index in range(repetitions):
            # Reverse the order on alternate pairs to expose warm filesystem/cache effects.
            modes = ("default", "startup") if index % 2 == 0 else ("startup", "default")
            for mode in modes:
                suffix = f"ci-profile-{index}-{mode}"
                options = os.environ.get("JAVA_TOOL_OPTIONS", "")
                options += f" -Djava.io.tmpdir={temporary}"
                if mode == "startup":
                    options += " -XX:TieredStopAtLevel=1"
                command = ["mvn", "-B", "-ntp", "-pl", "streamfusion-runtime",
                           f"-Dtest={SELECTION}", "-Dstreamfusion.test.regularJoinJobSmoke=true",
                           "-Dsf.canonical.wideKeys.job.smoke=true", "-Dsf.testForks=2",
                           "-Dsf.testReuseForks=false", "-Dsf.extraJvmArgs=-Xmx2g",
                           f"-Dsurefire.reportNameSuffix={suffix}", "surefire:test@default-test"]
                before = resource.getrusage(resource.RUSAGE_CHILDREN)
                start = time.monotonic()
                log = output / (suffix + ".log")
                with log.open("w") as stream:
                    result = subprocess.run(command, cwd=ROOT, stdout=stream, stderr=subprocess.STDOUT,
                                            env={**os.environ, "JAVA_TOOL_OPTIONS": options})
                elapsed = time.monotonic() - start
                after = resource.getrusage(resource.RUSAGE_CHILDREN)
                cases = Counter()
                totals = Counter()
                classes = {}
                for report in reports.glob(f"TEST-*-{suffix}.xml"):
                    suite = ET.parse(report).getroot()
                    for key in ("tests", "failures", "errors", "skipped"):
                        totals[key] += int(suite.get(key, 0))
                    classes[suite.get("name").removesuffix(f"({suffix})")] = float(suite.get("time", 0))
                    cases.update((case.get("classname").removesuffix(f"({suffix})"), case.get("name"))
                                 for case in suite.iter("testcase"))
                trial = {"mode": mode, "pair": index + 1, "elapsed_seconds": elapsed,
                         "cpu_seconds": after.ru_utime + after.ru_stime - before.ru_utime - before.ru_stime,
                         "outcomes": dict(totals), "classes": classes, "exit_code": result.returncode}
                trials.append(trial)
                (output / "summary.json").write_text(json.dumps(trials, indent=2) + "\n")
                if (result.returncode or totals["tests"] != 5 or totals["failures"]
                        or totals["errors"] or totals["skipped"] or expected is not None and cases != expected):
                    raise RuntimeError(f"Incomplete or inconsistent canonical profile: {log}")
                expected = cases
                print(f"{mode} pair {index + 1}: {elapsed:.2f}s elapsed, {trial['cpu_seconds']:.2f}s CPU, five cases passed", flush=True)
                log.unlink()
                for report in reports.glob(f"*-{suffix}.*"):
                    report.unlink()
    finally:
        shutil.rmtree(temporary)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True, help="New directory for concise trial evidence")
    parser.add_argument("--repetitions", type=int, default=3)
    args = parser.parse_args()
    if args.repetitions < 2:
        parser.error("Use at least two pairs to alternate execution order")
    profile(args.output.resolve(), args.repetitions)


if __name__ == "__main__":
    main()
