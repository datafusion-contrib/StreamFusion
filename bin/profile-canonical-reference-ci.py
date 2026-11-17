#!/usr/bin/env python3
"""Compare identical canonical handoffs with and without repeated stock oracle execution."""

import argparse
import json
import os
from pathlib import Path
import platform
import resource
import shutil
import subprocess
import time
import xml.etree.ElementTree as ET

import ci_java_build


ROOT = Path(__file__).resolve().parents[1]


def profile(output, repetitions, line):
    if os.environ.get("SF_CANONICAL_CDS_DIR") or os.environ.get("SF_CANONICAL_REFERENCE_DIR"):
        raise ValueError("Remove inherited canonical caches; the comparison controls their lifetimes.")
    java_home = Path(os.environ["JAVA_HOME"])
    output.mkdir(parents=True, exist_ok=False)
    temporary = output / "tmp"
    temporary.mkdir()
    trials = []
    try:
        profiles = ("flink-1.18," if line == "1.18" else "") + "canonical-kafka"
        setup = ["mvn", "-B", "-ntp", "-P" + profiles, "-pl", "streamfusion-runtime",
                 "-Dtest=CanonicalReferenceCacheTest", "-Dsf.testForks=1", "-Dsf.extraJvmArgs=-Xmx1g",
                 *ci_java_build.runtime_test_arguments()]
        environment = {**os.environ, "TMPDIR": str(temporary),
                       "JAVA_TOOL_OPTIONS": os.environ.get("JAVA_TOOL_OPTIONS", "")
                       + f' -XX:TieredStopAtLevel=1 -XX:ReservedCodeCacheSize=512m -Djava.io.tmpdir="{temporary}"'}
        log = output / "setup.log"
        with log.open("w") as stream:
            subprocess.run(setup, cwd=ROOT, env=environment, stdout=stream, stderr=subprocess.STDOUT, check=True)
        report = ROOT / "streamfusion-runtime/target/surefire-reports/TEST-tech.streamfusion.CanonicalReferenceCacheTest.xml"
        suite = ET.parse(report).getroot()
        if (int(suite.get("tests", 0)) != 5
                or any(int(suite.get(key, 0)) for key in ("errors", "failures", "skipped"))):
            raise ValueError("Reference cache controls did not pass in full")
        properties = {p.get("name"): p.get("value") for p in suite.find("properties")}
        classpath = properties["surefire.test.class.path"]
        classes = temporary / "classes"
        subprocess.run([str(java_home / "bin/javac"), "--release", "11", "-cp", classpath,
                        "-d", str(classes), str(ROOT / "dev/ci/CanonicalReferenceProfile.java")],
                       cwd=ROOT, env=environment, check=True, capture_output=True, text=True)
        log.unlink()
        command = [str(java_home / "bin/java"), "-Xmx1g",
                   "-Djava.library.path=" + str(ROOT / "native/target/debug"),
                   "-Dstreamfusion.native.development=true", "-Darrow.enable_unsafe_memory_access=true"]
        command += ["--add-opens=java.base/" + package + "=ALL-UNNAMED" for package in (
            "java.nio", "java.lang", "java.util", "java.io", "java.net", "java.util.concurrent",
            "java.util.concurrent.atomic", "java.lang.reflect", "sun.nio.ch")]
        command += ["-cp", str(classes) + os.pathsep + classpath, "tech.streamfusion.CanonicalReferenceProfile"]
        schedule = [(0, False, "preparation")]
        for pair in range(1, repetitions + 1):
            schedule.extend((pair, cached, "measurement") for cached in
                            ([False, True] if pair % 2 else [True, False]))
        for pair, cached, phase in schedule:
            for family in ("materializer", "count"):
                name = f"{phase}-{pair}-{cached}-{family}"
                fixture, references = temporary / name, temporary / (name + "-references")
                env = {**environment, "SF_CANONICAL_CDS_DIR": str(temporary / "cds")}
                if cached:
                    env["SF_CANONICAL_REFERENCE_DIR"] = str(references)
                log = output / (name + ".log")
                before = resource.getrusage(resource.RUSAGE_CHILDREN)
                start = time.monotonic()
                with log.open("w") as stream:
                    result = subprocess.run([*command, str(fixture), family], cwd=ROOT, env=env,
                                            stdout=stream, stderr=subprocess.STDOUT)
                after = resource.getrusage(resource.RUSAGE_CHILDREN)
                cases = sorted(line for line in log.read_text().splitlines() if line.startswith("CANONICAL_CASE "))
                reference_workers = sum(p.parent.name == "baseline" for p in fixture.rglob("*.log"))
                handoff_workers = sum(p.parent.name == "switched" for p in fixture.rglob("*.log"))
                trial = {"pair": pair, "cached": cached, "phase": phase, "family": family,
                         "elapsed_seconds": time.monotonic() - start,
                         "cpu_seconds": after.ru_utime + after.ru_stime - before.ru_utime - before.ru_stime,
                         "exit_code": result.returncode, "invocations": cases,
                         "reference_workers": reference_workers, "handoff_workers": handoff_workers}
                trials.append(trial)
                (output / "summary.json").write_text(json.dumps({"flink_line": line,
                    "platform": platform.platform(), "java": properties["java.runtime.version"],
                    "host_logical_processors": os.cpu_count(),
                    "worker_gc": "Serial" if "-XX:+UseSerialGC" in environment["JAVA_TOOL_OPTIONS"] else "JVM default",
                    "trials": trials}, indent=2) + "\n")
                identity = [f"CANONICAL_CASE {family} {first} {backend} passed"
                            for first in ("true", "false") for backend in ("hashmap", "rocksdb")]
                if (result.returncode or cases != sorted(identity) or handoff_workers != 12
                        or reference_workers != (1 if cached else 4) * (3 if family == "count" else 1)):
                    raise ValueError(f"Incomplete assertions or changed worker boundaries: {log}")
                print(f"{name}: {trial['elapsed_seconds']:.2f}s; four cases, "
                      f"{reference_workers} reference / {handoff_workers} handoff workers", flush=True)
                shutil.rmtree(fixture)
                if references.exists():
                    shutil.rmtree(references)
                log.unlink()
    finally:
        shutil.rmtree(temporary)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--line", choices=("1.18", "2.2"), default="2.2")
    args = parser.parse_args()
    if args.repetitions < 2:
        parser.error("Use at least two pairs to alternate trial order.")
    profile(args.output.resolve(), args.repetitions, args.line)


if __name__ == "__main__":
    main()
