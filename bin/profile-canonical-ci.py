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


def profile(output, repetitions, cds=False):
    if "TieredStopAtLevel" in os.environ.get("JAVA_TOOL_OPTIONS", ""):
        raise ValueError("Remove inherited TieredStopAtLevel so the default trial uses normal compilation")
    if os.environ.get("SF_CANONICAL_CDS_DIR"):
        raise ValueError("Remove inherited SF_CANONICAL_CDS_DIR so the default trial has no worker archive")
    output.mkdir(parents=True, exist_ok=False)
    temporary = output / "tmp"
    temporary.mkdir()
    reports = ROOT / "streamfusion-runtime/target/surefire-reports"
    trials = []
    expected = None
    schedule = [("cds", -1, "preparation")] if cds else []
    for index in range(repetitions):
        modes = ("default", "startup", "cds") if cds else ("default", "startup")
        if index % 2:
            modes = tuple(reversed(modes))
        schedule.extend((mode, index, "measurement") for mode in modes)
    try:
        for mode, index, phase in schedule:
            suffix = f"ci-profile-{index}-{mode}-{phase}"
            options = os.environ.get("JAVA_TOOL_OPTIONS", "")
            options += f' -Djava.io.tmpdir="{temporary}"'
            if mode != "default":
                options += " -XX:TieredStopAtLevel=1"
            environment = {**os.environ, "JAVA_TOOL_OPTIONS": options}
            if mode == "cds":
                environment["SF_CANONICAL_CDS_DIR"] = str(temporary / "archives")
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
                                        env=environment)
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
            trial = {"mode": mode, "pair": index + 1, "phase": phase, "elapsed_seconds": elapsed,
                     "cpu_seconds": after.ru_utime + after.ru_stime - before.ru_utime - before.ru_stime,
                     "outcomes": dict(totals), "classes": classes, "exit_code": result.returncode}
            if mode == "cds":
                cache = Path(environment["SF_CANONICAL_CDS_DIR"])
                archives = list(cache.glob("*.jsa"))
                trial["archives"] = len(archives)
                trial["archive_bytes"] = sum(path.stat().st_size for path in archives)
                trial["classpath_bytes"] = sum(path.stat().st_size for path in cache.glob("*.jar"))
            trials.append(trial)
            (output / "summary.json").write_text(json.dumps(trials, indent=2) + "\n")
            if (result.returncode or totals["tests"] != 5 or totals["failures"]
                    or totals["errors"] or totals["skipped"] or expected is not None and cases != expected
                    or mode == "cds" and trial["archives"] < 2):
                raise RuntimeError(f"Incomplete or inconsistent canonical profile: {log}")
            expected = cases
            print(f"{mode} {phase} {index + 1}: {elapsed:.2f}s elapsed, {trial['cpu_seconds']:.2f}s CPU, five cases passed", flush=True)
            log.unlink()
            for report in reports.glob(f"*-{suffix}.*"):
                report.unlink()
    finally:
        shutil.rmtree(temporary)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True, help="New directory for concise trial evidence")
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--cds", action="store_true", help="Also measure job-local class metadata archives")
    args = parser.parse_args()
    if args.repetitions < 2:
        parser.error("Use at least two pairs to alternate execution order")
    if args.cds:
        java_home = os.environ.get("JAVA_HOME")
        java = str(Path(java_home) / "bin/java") if java_home else "java"
        supported = subprocess.run([java, "-Xshare:on", "-version"], capture_output=True, text=True)
        if supported.returncode:
            parser.error("This JVM cannot use class-data sharing; omit --cds to measure default/first-tier compilation")
    profile(args.output.resolve(), args.repetitions, args.cds)


if __name__ == "__main__":
    main()
