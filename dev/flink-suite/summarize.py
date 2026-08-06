#!/usr/bin/env python3
"""Summarize Flink Surefire XML reports after an upstream StreamFusion run."""

from __future__ import annotations

import argparse
import pathlib
import sys
import xml.etree.ElementTree as ET


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("reports", type=pathlib.Path)
    parser.add_argument("--xfail", action="append", default=[])
    args = parser.parse_args()

    files = sorted(args.reports.rglob("TEST-*.xml"))
    if not files:
        print("No Surefire XML reports found.")
        return 2

    tests = failures = errors = skipped = 0
    problems: list[tuple[str, str, str, str]] = []
    expected: list[tuple[str, str, str, str]] = []
    malformed: list[tuple[pathlib.Path, str]] = []

    for report in files:
        try:
            suite = ET.parse(report).getroot()
        except (ET.ParseError, OSError) as exc:
            malformed.append((report, str(exc)))
            continue

        tests += int(suite.attrib.get("tests", 0))
        failures += int(suite.attrib.get("failures", 0))
        errors += int(suite.attrib.get("errors", 0))
        skipped += int(suite.attrib.get("skipped", 0))
        for case in suite.findall("testcase"):
            problem = case.find("failure")
            kind = "failure"
            if problem is None:
                problem = case.find("error")
                kind = "error"
            if problem is None:
                continue
            detail = (problem.attrib.get("message") or problem.text or "").strip()
            detail = " ".join(detail.split())[:800]
            item = (
                case.attrib.get("classname", suite.attrib.get("name", "unknown")),
                case.attrib.get("name", "unknown"),
                kind,
                detail,
            )
            key = f"{item[0]}#{item[1]}"
            (expected if key in args.xfail else problems).append(item)

    expected_failures = sum(kind == "failure" for _, _, kind, _ in expected)
    expected_errors = sum(kind == "error" for _, _, kind, _ in expected)
    unexpected_failures = failures - expected_failures
    unexpected_errors = errors - expected_errors

    print("# StreamFusion upstream Flink suite")
    print()
    print(f"- Reports: {len(files)}")
    print(f"- Tests: {tests}")
    print(f"- Passed: {tests - failures - errors - skipped}")
    print(f"- Failures: {unexpected_failures}")
    print(f"- Errors: {unexpected_errors}")
    print(f"- Expected upstream failures: {len(expected)}")
    print(f"- Skipped: {skipped}")

    if malformed:
        print(f"- Malformed reports: {len(malformed)}")

    if problems:
        print()
        print("## Issues")
        for class_name, test_name, kind, detail in problems:
            print()
            print(f"- `{class_name}#{test_name}` ({kind})")
            if detail:
                print(f"  - {detail}")

    if malformed:
        print()
        print("## Malformed reports")
        for report, detail in malformed:
            print(f"- `{report}`: {detail}")

    if expected:
        print()
        print("## Expected upstream failures")
        for class_name, test_name, kind, detail in expected:
            print(f"- `{class_name}#{test_name}` ({kind})")
            if detail:
                print(f"  - {detail}")

    return 1 if unexpected_failures or unexpected_errors or malformed else 0


if __name__ == "__main__":
    sys.exit(main())
