"""Reuse Paimon's compiled inputs while executing both format configurations separately."""

import argparse
from collections import Counter
import json
import os
from pathlib import Path
import resource
import subprocess
import sys
import time
import xml.etree.ElementTree as ET


ROOT = Path(__file__).resolve().parents[1]


def outcomes(module, suffix):
    counts = Counter()
    reports = list((ROOT / module / "target/surefire-reports").glob(f"TEST-*-{suffix}.xml"))
    for report in reports:
        suite = ET.parse(report).getroot()
        cases = list(suite.iter("testcase"))
        actual = {"tests": len(cases), **{
            field: sum(case.find(element) is not None for case in cases)
            for field, element in (("failures", "failure"), ("errors", "error"), ("skipped", "skipped"))}}
        if actual != {field: int(suite.get(field, 0)) for field in actual}:
            raise ValueError(f"Malformed {suffix} outcomes: {report}")
        counts.update(actual)
    if not counts["tests"] or counts["tests"] <= counts["skipped"] or counts["failures"] or counts["errors"]:
        raise ValueError(f"Incomplete {suffix} test execution: {dict(counts)}")
    return dict(counts)


def run(module, profile, summary=None):
    base = ["mvn", "-B", "-ntp"]
    if profile:
        base.append("-P" + profile)
    base.extend(["-pl", module, "-am", "-Dsf.runtime.tests.skip=true", "-Dnative.build.skip=true"])
    if module != "streamfusion-paimon":
        return subprocess.run([*base, "test"], cwd=ROOT).returncode
    trials = []

    def execute(phase, options, environment=None):
        before = resource.getrusage(resource.RUSAGE_CHILDREN)
        start = time.monotonic()
        result = subprocess.run([*base, *options], cwd=ROOT, env=environment)
        after = resource.getrusage(resource.RUSAGE_CHILDREN)
        trial = {"phase": phase, "exit_code": result.returncode,
                 "elapsed_seconds": time.monotonic() - start,
                 "cpu_seconds": after.ru_utime + after.ru_stime - before.ru_utime - before.ru_stime}
        trials.append(trial)
        return trial

    def save():
        if summary:
            summary.parent.mkdir(parents=True, exist_ok=True)
            summary.write_text(json.dumps(trials, indent=2) + "\n")

    preparation = execute("compile", ["test", "-DskipTests=true"])
    save()
    if preparation["exit_code"]:
        return preparation["exit_code"]
    failed = False
    for format in ("parquet", "orc"):
        suffix = "paimon-" + format
        for previous in (ROOT / module / "target/surefire-reports").glob(f"TEST-*-{suffix}.xml"):
            previous.unlink()
        trial = execute(format, ["test", "-DskipTests=false", "-Dsf.ci.precompiledJava=true",
                                 "-Dsf.protobuf.checkStaleness=true", f"-Dsurefire.reportNameSuffix={suffix}"],
                        {**os.environ, "SF_PAIMON_FILE_FORMAT": format})
        if trial["exit_code"]:
            failed = True
        else:
            try:
                trial["outcomes"] = outcomes(module, suffix)
            except (ValueError, ET.ParseError) as error:
                failed = True
                trial["validation_error"] = str(error)
                print(error, file=sys.stderr)
        save()
    return int(failed)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--module", required=True)
    parser.add_argument("--profile", default="")
    parser.add_argument("--summary", type=Path)
    args = parser.parse_args()
    sys.exit(run(args.module, args.profile, args.summary))


if __name__ == "__main__":
    main()
