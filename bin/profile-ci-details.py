#!/usr/bin/env python3
"""Attribute remaining CI phases and diagnose missing recovery accumulators."""

import argparse
from collections import Counter
import importlib.util
import json
import os
from pathlib import Path
import re
import resource
import shutil
import subprocess
import time
import xml.etree.ElementTree as ET

import ci_jfr

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("runtime_profile", Path(__file__).with_name("profile-runtime-ci.py"))
runtime_profile = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runtime_profile)
SUITES = {
    "recursive": ("streamfusion-runtime", {
        "FlinkRecursiveScalarSqlHarnessTest": (1134, {"1.18": 0, "2.2": 0}),
        "FlinkRecursiveTableFunctionSqlHarnessTest": (683, {"1.18": 0, "2.2": 0}),
    }),
    "fluss": ("streamfusion-fluss", {"FlussSqlTest": (29, {"1.18": 0, "2.2": 0})}),
    "recovery": ("streamfusion-runtime", {"FlinkBinaryCollectionSqlHarnessTest": (54, {"1.18": 28, "2.2": 0})}),
}


def profile(output, line, suite):
    output.mkdir(parents=True, exist_ok=False)
    temporary = output / "tmp"
    temporary.mkdir()
    java_home = Path(os.environ["JAVA_HOME"])
    ci_jfr.configuration(java_home, temporary / "details.jfc")
    module, selection = SUITES[suite]
    reports = ROOT / module / "target/surefire-reports"
    heaps = ["2g", "4g", "4g", "2g"] if suite == "recovery" else ["2g"]
    trials = []
    failed = False
    expected_cases = None
    try:
        for index, heap in enumerate(heaps):
            suffix = f"details-{line}-{suite}-{index}"
            recorded = output / "jfr" / str(index)
            recorded.mkdir(parents=True)
            fixtures = temporary / f"fixtures-{index}"
            environment = {**os.environ, "TMPDIR": str(temporary),
                           "JAVA_TOOL_OPTIONS": f'-XX:TieredStopAtLevel=1 -Djava.io.tmpdir="{temporary}"'}
            profiles = (["ci-diagnostics"] if suite == "recovery" else []) + (["flink-1.18"] if line == "1.18" else [])
            extra = (f"-Xmx{heap} -XX:StartFlightRecording=filename={recorded},"
                     f"settings={temporary / 'details.jfc'},dumponexit=true "
                     f"-XX:ErrorFile={output / 'hs_err_pid%p.log'} "
                     f"-Xlog:gc*:file={output / ('gc-' + str(index) + '-%p.log')}")
            command = ["mvn", "-B", "-ntp"] + (["-P" + ",".join(profiles)] if profiles else []) + ["-pl", module,
                       f"-Dtest={','.join('tech.streamfusion.' + name for name in selection)}",
                       "-Dsf.testForks=4", "-Dsf.testReuseForks=false", f"-Dsf.extraJvmArgs={extra}",
                       f"-Dsf.fixtureTimingsDirectory={fixtures}", "-Dsf.fixturePlanCache=true",
                       "-Dsf.recoveryResults.profile=true", "-Dorg.slf4j.simpleLogger.defaultLogLevel=error",
                       f"-Dsurefire.reportNameSuffix={suffix}", "surefire:test@default-test"]
            log = output / (suffix + ".log")
            start = time.monotonic()
            cpu = resource.getrusage(resource.RUSAGE_CHILDREN)
            with log.open("w") as stream:
                result = subprocess.run(command, cwd=ROOT, env=environment, stdout=stream, stderr=subprocess.STDOUT)
            elapsed = time.monotonic() - start
            after = resource.getrusage(resource.RUSAGE_CHILDREN)
            trial = {"suite": suite, "line": line, "heap": heap, "forks": 4,
                     "instrumented": True, "elapsed_seconds": elapsed,
                     "cpu_seconds": after.ru_utime + after.ru_stime - cpu.ru_utime - cpu.ru_stime,
                     "exit_code": result.returncode}
            try:
                cases, counts, classes = runtime_profile.outcomes(reports, suffix, line, selection)
                if expected_cases is not None and cases != expected_cases:
                    raise ValueError("Individual outcomes changed between diagnostic trials")
                expected_cases = cases
                trial.update({"validated": True, "outcomes": counts, "classes": classes})
            except ValueError as error:
                failed = True
                trial.update({"validated": False, "validation_error": str(error)[:1200]})
                counts = Counter()
                for path in reports.glob(f"TEST-*-{suffix}.xml"):
                    report = ET.parse(path).getroot()
                    counts.update({k: int(report.get(k, 0)) for k in ["tests", "errors", "failures", "skipped"]})
                trial["outcomes"] = dict(counts)
            raw = log.read_text()
            trial["recovery_messages"] = [line[:1500] for line in raw.splitlines()
                                           if "[recovery-result]" in line or "[recovery-capture]" in line]
            trial["fixture_timings"] = [
                {"name": m[1], "calls": int(m[2]), "seconds": int(m[3]) / 1e9}
                for path in fixtures.glob("*.txt")
                for m in re.finditer(r"\[ci-fixture-timing\] (\S+) calls=(\d+) nanoseconds=(\d+)", path.read_text())]
            trials.append(trial)
            (output / "summary.json").write_text(json.dumps(trials, indent=2) + "\n")
            trial["jfr"] = ci_jfr.summarize(list(recorded.glob("*.jfr")), java_home, len(selection))
            (output / "summary.json").write_text(json.dumps(trials, indent=2) + "\n")
            if result.returncode:
                selected = [(line if len(line) <= 1500 else line[:750] + " ... " + line[-750:]) for line in raw.splitlines() if any(
                    word in line for word in ["[ERROR]", "OutOfMemory", "accumulator", "Accumulator", "[recovery-"])]
                (output / (suffix + ".failure.txt")).write_text("\n".join(selected[-160:]) + "\n")
                failed = True
            log.unlink()
            for path in reports.glob(f"*-{suffix}.*"):
                path.unlink()
            print(f"{suite} trial {index + 1}, heap={heap}: {trial['outcomes']}, validated={trial['validated']}", flush=True)
    finally:
        shutil.rmtree(temporary)
        if suite == "fluss":
            plans = ROOT / "streamfusion-fluss/target/fluss-sql-plans"
            if plans.exists():
                shutil.rmtree(plans)
    if failed:
        raise RuntimeError("A diagnostic trial failed; inspect the retained summary and condensed failure evidence")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--suite", choices=SUITES, required=True)
    parser.add_argument("--line", choices=("1.18", "2.2"), required=True)
    args = parser.parse_args()
    profile(args.output.resolve(), args.line, args.suite)


if __name__ == "__main__":
    main()
