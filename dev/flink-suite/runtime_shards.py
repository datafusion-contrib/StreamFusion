"""Partition unchanged planner ITCase classes, then require complete runtime coverage."""

import argparse
from collections import Counter
import csv
import hashlib
import json
from pathlib import Path
import xml.etree.ElementTree as ET

from summarize import execution_contracts


SHARDS = 4


def record_baseline(reports, output):
    rows = {}
    for path in sorted(reports.rglob("TEST-*.xml")):
        suite = ET.parse(path).getroot()
        name = suite.attrib["name"]
        if not name.startswith("org.apache.flink.") or not name.endswith("ITCase"):
            continue
        if name in rows:
            raise ValueError(f"Duplicate runtime report: {name}")
        if suite.findall("testcase/failure") or suite.findall("testcase/error"):
            raise ValueError(f"Cannot record a failing runtime class: {name}")
        rows[name] = (float(suite.attrib["time"]), len(suite.findall("testcase")))
    if not rows:
        raise ValueError("No runtime reports to record")
    with output.open("w") as target:
        writer = csv.writer(target, delimiter="\t", lineterminator="\n")
        writer.writerow(("class", "seconds", "tests"))
        for name, (seconds, tests) in sorted(rows.items()):
            writer.writerow((name, f"{seconds:.3f}", tests))


def make_plan(classes, baseline, contracts=None):
    candidates = {
        path.relative_to(classes).with_suffix("").as_posix().replace("/", ".")
        for path in classes.rglob("*ITCase.class")
        if "$" not in path.name
    }
    with baseline.open() as source:
        timings = {row["class"]: row for row in csv.DictReader(source, delimiter="\t")}
    missing = timings.keys() - candidates
    if missing:
        raise ValueError(f"Previously executed runtime classes are missing: {sorted(missing)}")
    if not candidates:
        raise ValueError("No compiled runtime ITCase classes found")
    if contracts:
        required = {test.split("#", 1)[0] for test in execution_contracts(contracts)
                    if test.startswith("org.apache.flink.")}
        if required - candidates:
            raise ValueError(f"Contracted runtime classes are missing: {sorted(required - candidates)}")
    durations = {name: float(timings.get(name, {}).get("seconds", 30)) for name in candidates}
    groups = [[] for _ in range(SHARDS)]
    totals = [0.0] * SHARDS
    for name in sorted(candidates, key=lambda name: (-durations[name], name)):
        shard = min(range(SHARDS), key=lambda index: (totals[index], index))
        groups[shard].append(name)
        totals[shard] += durations[name]
    return {
        "schema_version": 1,
        "shards": [sorted(group) for group in groups],
        "estimated_seconds": totals,
        "expected_cases": {name: int(row["tests"]) for name, row in sorted(timings.items())},
    }


def read_plan(path):
    plan = json.loads(path.read_text())
    groups = plan["shards"]
    counts = Counter(name for group in groups for name in group)
    if len(groups) != SHARDS or any(not group for group in groups):
        raise ValueError("Expected four nonempty runtime shards")
    if any(count != 1 for count in counts.values()):
        raise ValueError("A runtime class belongs to multiple shards")
    if plan["expected_cases"].keys() - counts.keys():
        raise ValueError("Runtime plan omits previously executed classes")
    return plan


def plan_digest(plan):
    return hashlib.sha256(json.dumps(plan, sort_keys=True).encode()).hexdigest()


def verify(plan, shard, audit):
    selected = set(plan["shards"][shard - 1])
    if audit["validation_problems"]:
        raise ValueError("Execution audit contains validation failures")
    counts = Counter(case["test"].split("#", 1)[0] for case in audit["testcases"])
    unexpected = counts.keys() - selected
    if unexpected:
        raise ValueError(f"Tests executed outside this shard: {sorted(unexpected)}")
    for name in sorted(selected & plan["expected_cases"].keys()):
        expected = plan["expected_cases"][name]
        if counts[name] != expected:
            raise ValueError(f"{name}: expected {expected} test cases including skips, got {counts[name]}")
    return {"plan": plan_digest(plan), "shard": shard, "classes": sorted(selected), "cases": counts}


def aggregate(plan, reports):
    by_shard = {}
    for path in reports.rglob("shard-coverage.json"):
        coverage = json.loads(path.read_text())
        shard = coverage["shard"]
        if shard in by_shard or shard not in range(1, SHARDS + 1):
            raise ValueError(f"Duplicate or invalid shard: {shard}")
        if coverage["plan"] != plan_digest(plan):
            raise ValueError(f"Shard {shard} used a different runtime plan")
        if coverage["classes"] != plan["shards"][shard - 1]:
            raise ValueError(f"Shard {shard} changed its test selection")
        counts = coverage["cases"]
        if counts.keys() - set(coverage["classes"]):
            raise ValueError(f"Shard {shard} executed another shard's tests")
        for name in coverage["classes"]:
            if name in plan["expected_cases"] and counts.get(name, 0) != plan["expected_cases"][name]:
                raise ValueError(f"Shard {shard} lost test invocations for {name}")
        by_shard[shard] = coverage
    if set(by_shard) != set(range(1, SHARDS + 1)):
        raise ValueError(f"Missing runtime shards: {sorted(set(range(1, SHARDS + 1)) - by_shard.keys())}")
    return sum(sum(report["cases"].values()) for report in by_shard.values())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    record = commands.add_parser("record")
    record.add_argument("--reports", type=Path, required=True)
    record.add_argument("--output", type=Path, required=True)
    create = commands.add_parser("plan")
    create.add_argument("--classes", type=Path, required=True)
    create.add_argument("--baseline", type=Path, required=True)
    create.add_argument("--contracts", type=Path, required=True)
    create.add_argument("--output", type=Path, required=True)
    for command in ("select", "verify", "aggregate"):
        action = commands.add_parser(command)
        action.add_argument("--plan", type=Path, required=True)
        if command != "aggregate":
            action.add_argument("--shard", type=int, choices=range(1, SHARDS + 1), required=True)
        if command == "verify":
            action.add_argument("--audit", type=Path, required=True)
            action.add_argument("--output", type=Path, required=True)
        if command == "aggregate":
            action.add_argument("--reports", type=Path, required=True)
    args = parser.parse_args()
    try:
        if args.command == "record":
            record_baseline(args.reports, args.output)
        elif args.command == "plan":
            plan = make_plan(args.classes, args.baseline, args.contracts)
            args.output.write_text(json.dumps(plan, indent=2) + "\n")
            for index, (group, seconds) in enumerate(zip(plan["shards"], plan["estimated_seconds"]), 1):
                print(f"Shard {index}: {len(group)} classes, historical test time {seconds / 60:.1f} minutes")
        else:
            plan = read_plan(args.plan)
            if args.command == "select":
                print(",".join(plan["shards"][args.shard - 1]))
            elif args.command == "verify":
                result = verify(plan, args.shard, json.loads(args.audit.read_text()))
                args.output.write_text(json.dumps(result, indent=2) + "\n")
                print(f"Shard {args.shard}: verified {sum(result['cases'].values())} test cases")
            else:
                print(f"All four runtime shards accounted for: {aggregate(plan, args.reports)} test cases")
    except (ValueError, KeyError, OSError) as error:
        parser.exit(1, f"Runtime coverage failed: {error}\n")


if __name__ == "__main__":
    main()
