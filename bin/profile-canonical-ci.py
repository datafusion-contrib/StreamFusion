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
import tempfile
import time
import xml.etree.ElementTree as ET


ROOT = Path(__file__).resolve().parents[1]
SELECTION = ("CanonicalRegularJoinJobTest#regularStateContinuesAcrossCleanEngineProcesses"
             "+nativeRegularCheckpointResumesBeforeCanonicalSwitch,CanonicalManagedUdfDecimalKeyJobTest")
PARALLEL_CLASSES = (
    "CanonicalGroupArithmeticIntJobTest", "CanonicalHiddenExtremaJobTest",
    "CanonicalGroupTypedRescaleJobTest", "CanonicalGroupTime0ExtremaJobTest",
    "CanonicalGroupDateExtremaJobTest", "CanonicalGroupBooleanExtremaJobTest",
    "CanonicalGlobalHiddenExtremaJobTest", "CanonicalGroupTime3ExtremaJobTest",
)


def profile(output, repetitions, cds=False, parallel=False, gc=False):
    if "TieredStopAtLevel" in os.environ.get("JAVA_TOOL_OPTIONS", ""):
        raise ValueError("Remove inherited TieredStopAtLevel so the default trial uses normal compilation")
    if os.environ.get("SF_CANONICAL_CDS_DIR"):
        raise ValueError("Remove inherited SF_CANONICAL_CDS_DIR so the default trial has no worker archive")
    if gc and "GC" in os.environ.get("JAVA_TOOL_OPTIONS", ""):
        raise ValueError("Remove inherited garbage-collector settings before comparing collectors")
    output.mkdir(parents=True, exist_ok=False)
    temporary = output / "tmp"
    temporary.mkdir()
    reports = ROOT / "streamfusion-runtime/target/surefire-reports"
    trials = []
    expected = None
    preparations = ("gc-g1", "gc-serial") if gc else ("cds-two" if parallel else "cds",)
    schedule = [(mode, -1, "preparation") for mode in preparations] if cds else []
    for index in range(repetitions):
        modes = (("gc-g1", "gc-serial") if gc else ("cds-two", "cds-four") if parallel else
                 ("default", "startup", "cds") if cds else ("default", "startup"))
        if index % 2:
            modes = tuple(reversed(modes))
        schedule.extend((mode, index, "measurement") for mode in modes)
    try:
        for mode, index, phase in schedule:
            suffix = f"ci-profile-{index}-{mode}-{phase}"
            options = os.environ.get("JAVA_TOOL_OPTIONS", "")
            options += f' -Djava.io.tmpdir="{temporary}"'
            if mode != "default":
                options += " -XX:TieredStopAtLevel=1 -XX:ReservedCodeCacheSize=512m"
            if gc:
                options += " -XX:+UseSerialGC" if mode == "gc-serial" else " -XX:+UseG1GC"
            environment = {**os.environ, "JAVA_TOOL_OPTIONS": options}
            use_archive = gc or mode in ("cds", "cds-two", "cds-four")
            if use_archive:
                environment["SF_CANONICAL_CDS_DIR"] = str(
                    temporary / (mode if gc else "archives"))
            if parallel:
                environment["SF_KAFKA_INTEGRATION"] = "true"
            forks, heap = (4, "1g") if gc or mode == "cds-four" else (2, "2g")
            selection = ",".join(PARALLEL_CLASSES) if parallel else SELECTION
            command = ["mvn", "-B", "-ntp", "-pl", "streamfusion-runtime",
                       f"-Dtest={selection}", f"-Dsf.testForks={forks}",
                       "-Dsf.testReuseForks=false", f"-Dsf.extraJvmArgs=-Xmx{heap}",
                       f"-Dsurefire.reportNameSuffix={suffix}", "surefire:test@default-test"]
            if parallel:
                command.append("-Pcanonical-kafka")
            else:
                command.extend(["-Dstreamfusion.test.regularJoinJobSmoke=true",
                                "-Dsf.canonical.wideKeys.job.smoke=true"])
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
            class_outcomes = {}
            for report in reports.glob(f"TEST-*-{suffix}.xml"):
                suite = ET.parse(report).getroot()
                name = suite.get("name").removesuffix(f"({suffix})")
                if name in classes:
                    raise RuntimeError(f"Duplicate profile report: {name}")
                for key in ("tests", "failures", "errors", "skipped"):
                    totals[key] += int(suite.get(key, 0))
                classes[name] = float(suite.get("time", 0))
                class_outcomes[name] = {key: int(suite.get(key, 0)) for key in ("tests", "failures", "errors", "skipped")}
                cases.update((case.get("classname").removesuffix(f"({suffix})"), case.get("name"))
                             for case in suite.iter("testcase"))
            trial = {"mode": mode, "pair": index + 1, "phase": phase, "forks": forks, "heap": heap, "elapsed_seconds": elapsed,
                     "cpu_seconds": after.ru_utime + after.ru_stime - before.ru_utime - before.ru_stime,
                     "outcomes": dict(totals), "classes": classes, "class_outcomes": class_outcomes, "exit_code": result.returncode}
            if use_archive:
                cache = Path(environment["SF_CANONICAL_CDS_DIR"])
                archives = list(cache.glob("*.jsa"))
                trial["archives"] = len(archives)
                trial["archive_bytes"] = sum(path.stat().st_size for path in archives)
                trial["classpath_bytes"] = sum(path.stat().st_size for path in cache.glob("*.jar"))
            trials.append(trial)
            (output / "summary.json").write_text(json.dumps(trials, indent=2) + "\n")
            expected_classes = {"tech.streamfusion." + name: 8 for name in PARALLEL_CLASSES} if parallel else {
                "tech.streamfusion.CanonicalRegularJoinJobTest": 4,
                "tech.streamfusion.CanonicalManagedUdfDecimalKeyJobTest": 1}
            expected_tests = sum(expected_classes.values())
            if (result.returncode or totals["tests"] != expected_tests or totals["failures"]
                    or totals["errors"] or totals["skipped"] or expected is not None and cases != expected
                    or len(cases) != expected_tests
                    or {name: counts["tests"] for name, counts in class_outcomes.items()} != expected_classes
                    or use_archive and trial["archives"] < 2):
                raise RuntimeError(f"Incomplete or inconsistent canonical profile: {log}")
            expected = cases
            print(f"{mode} {phase} {index + 1}: {elapsed:.2f}s elapsed, {trial['cpu_seconds']:.2f}s CPU, {expected_tests} cases passed", flush=True)
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
    comparison = parser.add_mutually_exclusive_group()
    comparison.add_argument("--parallel", action="store_true", help="Compare two vs four fresh forks with prepared worker metadata")
    comparison.add_argument("--gc", action="store_true", help="Compare G1 and Serial GC with current startup settings and separate prepared archives")
    args = parser.parse_args()
    if args.parallel or args.gc:
        args.cds = True
    if args.repetitions < 2:
        parser.error("Use at least two pairs to alternate execution order")
    if args.cds:
        java_home = os.environ.get("JAVA_HOME")
        java = str(Path(java_home) / "bin/java") if java_home else "java"
        with tempfile.TemporaryDirectory(prefix="cds-capability-", dir=args.output.resolve().parent) as directory:
            temporary = Path(directory)
            classes = temporary / "classes.list"
            classes.write_text("java/lang/Object\n")
            empty = temporary / "empty"
            empty.mkdir()
            supported = subprocess.run(
                [java, "-Xshare:dump", f"-XX:SharedClassListFile={classes}",
                 f"-XX:SharedArchiveFile={temporary / 'probe.jsa'}", "-cp", str(empty)],
                capture_output=True, text=True, timeout=120)
        if supported.returncode:
            parser.error("This JVM cannot use class-data sharing; omit --cds to measure default/first-tier compilation")
    profile(args.output.resolve(), args.repetitions, args.cds, args.parallel, args.gc)


if __name__ == "__main__":
    main()
