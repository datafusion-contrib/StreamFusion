#!/usr/bin/env python3
"""Report Actions execution cost separately from dependency and runner start delays."""

import argparse
from collections import Counter, defaultdict
from datetime import datetime
import hashlib
import json
from pathlib import Path
import re
import subprocess
import xml.etree.ElementTree as ET


def timestamp(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def seconds(start, end):
    return max(0, (timestamp(end) - timestamp(start)).total_seconds())


def summarize(run, jobs):
    groups = defaultdict(lambda: {"jobs": 0, "runner_seconds": 0, "max_job_seconds": 0})
    steps = defaultdict(lambda: {"executions": 0, "seconds": 0})
    rows = []
    for job in jobs:
        # GitHub may preallocate started_at for queued jobs. It is not execution evidence.
        executed = any(step.get("started_at") and step.get("conclusion") != "skipped"
                       and (step.get("conclusion") or step.get("status") == "in_progress")
                       for step in job.get("steps", []))
        started = job.get("started_at") if executed and job["status"] in ("in_progress", "completed") else None
        elapsed = seconds(started, job["completed_at"]) if started and job.get("completed_at") else None
        name = job["name"]
        group = ("canonical" if name.startswith("Canonical savepoint jobs")
                 else "canonical preparation" if name.startswith("Prepare canonical Java")
                 else "java" if name.startswith("Runtime, connectors and formats")
                 else "upstream preparation" if "Prepare upstream build" in name
                 else "upstream tests" if " suite (Flink " in name else "other")
        groups[group]["jobs"] += 1
        if elapsed is not None:
            groups[group]["runner_seconds"] += elapsed
            groups[group]["max_job_seconds"] = max(groups[group]["max_job_seconds"], elapsed)
        for step in job.get("steps", []):
            if step.get("started_at") and step.get("completed_at") and step.get("conclusion") != "skipped":
                cost = seconds(step["started_at"], step["completed_at"])
                steps[step["name"]]["executions"] += 1
                steps[step["name"]]["seconds"] += cost
        rows.append({"name": name, "url": job["html_url"], "status": job["status"],
                     "conclusion": job.get("conclusion"), "group": group,
                     "execution_seconds": elapsed,
                     "start_delay_seconds": seconds(run["created_at"], started) if started else None,
                     "failed_steps": [step["name"] for step in job.get("steps", [])
                                      if step.get("conclusion") == "failure"]})
    completed = [j["completed_at"] for j in jobs if j.get("completed_at")]
    complete = run["status"] == "completed" and all(j["status"] == "completed" for j in jobs)
    return {"run": run["html_url"], "sha": run["head_sha"], "status": run["status"],
            "conclusion": run.get("conclusion"), "complete": complete,
            "elapsed_seconds": seconds(run["created_at"], max(completed)) if complete and completed else None,
            "runner_seconds": sum(g["runner_seconds"] for g in groups.values()),
            "groups": dict(groups),
            "steps": dict(sorted(steps.items(), key=lambda item: -item[1]["seconds"])),
            "jobs": rows,
            "limits": "Start delay includes prerequisites, concurrency limits and runner queues. "
                      "Runner seconds exclude jobs without a completed execution interval; "
                      "they are not billed minutes. Step times overlap between parallel jobs."}


def api(endpoint):
    return json.loads(subprocess.check_output(["gh", "api", endpoint], text=True, timeout=30))


def canonical_coverage(reports, baseline):
    expected = baseline["canonical_baseline"]
    if not expected:
        raise ValueError("The baseline has no canonical cohorts")
    cohorts = defaultdict(lambda: {"shards": {}, "classes": {}, "selected": set(),
                                   "cases": Counter(), "inventories": set()})
    for directory in sorted(reports.glob("canonical-job-results-*")):
        match = re.fullmatch(r"canonical-job-results-(flink[\d.]+-jdk\d+)-shard(\d+)", directory.name)
        if not directory.is_dir() or not match or match[1] not in expected:
            raise ValueError(f"Unexpected canonical artifact: {directory.name}")
        key, shard = match[1], int(match[2])
        cohort = cohorts[key]
        manifests = list(directory.rglob("canonical-job-includes.json"))
        if len(manifests) != 1:
            raise ValueError(f"Expected one selection manifest: {directory.name}")
        manifest = json.loads(manifests[0].read_text())
        count = manifest["count"]
        if (not isinstance(count, int) or isinstance(count, bool) or count < 1
                or shard not in range(1, count + 1) or manifest["shard"] != shard
                or manifest["suite"] != "canonical"
                or manifest["flink_line"] != key.split("-jdk")[0].removeprefix("flink")
                or shard in cohort["shards"]):
            raise ValueError(f"Invalid canonical selection: {directory.name}")
        cohort["shards"][shard] = count
        cohort["inventories"].add(manifest["inventory_sha256"])
        reported = set()
        for path in sorted(directory.rglob("TEST-*.xml")):
            suite = ET.parse(path).getroot()
            name = suite.get("name")
            if name not in expected[key]["classes"] or name in cohort["classes"]:
                raise ValueError(f"Unexpected or duplicate canonical class: {key}/{name}")
            cases = list(suite.iter("testcase"))
            actual = {"tests": len(cases), **{
                field: sum(case.find(element) is not None for case in cases)
                for field, element in (("errors", "error"), ("failures", "failure"), ("skipped", "skipped"))}}
            if (actual != {field: int(suite.get(field, 0)) for field in actual}
                    or actual != expected[key]["classes"][name]):
                raise ValueError(f"Changed canonical outcomes: {key}/{name}")
            cohort["classes"][name] = actual
            reported.add(name.split("$", 1)[0].replace(".", "/"))
            for case in cases:
                if not case.get("classname") or not case.get("name"):
                    raise ValueError(f"Missing canonical invocation identity: {key}/{name}")
                cohort["cases"][(case.get("classname"), case.get("name"), case.find("skipped") is not None)] += 1
        selected = manifest["classes"]
        expected_reports = {name for name in selected if name.replace("/", ".") in expected[key]["classes"]}
        if (len(selected) != len(set(selected)) or cohort["selected"].intersection(selected)
                or reported != expected_reports):
            raise ValueError(f"Canonical selection differs from reports: {directory.name}")
        cohort["selected"].update(selected)
    if set(cohorts) != set(expected):
        raise ValueError("Missing canonical cohorts")
    result = {}
    for key, cohort in sorted(cohorts.items()):
        counts = set(cohort["shards"].values())
        if len(counts) != 1 or set(cohort["shards"]) != set(range(1, next(iter(counts)) + 1)):
            raise ValueError(f"Missing canonical shards: {key}")
        identities = hashlib.sha256(json.dumps(sorted(
            (*identity, count) for identity, count in cohort["cases"].items())).encode()).hexdigest()
        inventory = hashlib.sha256(json.dumps(sorted(cohort["selected"])).encode()).hexdigest()
        if (cohort["classes"] != expected[key]["classes"]
                or cohort["inventories"] != set(expected[key]["inventory_sha256"])
                or cohort["inventories"] != {inventory}
                or identities != expected[key]["case_identities_sha256"]):
            raise ValueError(f"Changed canonical inventory or invocations: {key}")
        outcomes = Counter()
        for values in cohort["classes"].values():
            outcomes.update(values)
        result[key] = {"shards": len(cohort["shards"]), "selected_classes": len(cohort["selected"]),
                       "reported_classes": len(cohort["classes"]),
                       "outcomes": dict(outcomes), "case_identities_sha256": identities,
                       "inventory_sha256": sorted(cohort["inventories"])}
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", required=True)
    parser.add_argument("--run", required=True, type=int)
    parser.add_argument("--json", action="store_true", help="Emit reusable JSON instead of Markdown")
    parser.add_argument("--canonical-reports", type=Path, help="Directory containing downloaded canonical-job-results-* artifacts")
    parser.add_argument("--canonical-baseline", type=Path, help="Prior complete canonical inventory and invocation evidence JSON")
    args = parser.parse_args()
    if bool(args.canonical_reports) != bool(args.canonical_baseline):
        parser.error("--canonical-reports and --canonical-baseline must be supplied together")
    run = api(f"repos/{args.repo}/actions/runs/{args.run}")
    jobs = []
    page = 1
    while True:
        batch = api(f"repos/{args.repo}/actions/runs/{args.run}/attempts/{run['run_attempt']}/jobs?per_page=20&page={page}")
        jobs.extend(batch["jobs"])
        if len(batch["jobs"]) < 20:
            break
        page += 1
    report = summarize(run, jobs)
    if args.canonical_reports:
        report["canonical_coverage"] = canonical_coverage(
            args.canonical_reports, json.loads(args.canonical_baseline.read_text()))
    if args.json:
        print(json.dumps(report, indent=2))
        return
    print(f"[Run {args.run}]({report['run']}) — {report['conclusion'] or report['status']}")
    elapsed = report["elapsed_seconds"]
    print(f"\nElapsed: {elapsed / 60:.1f} min" if elapsed is not None else "\nRun is incomplete.")
    print(f"Completed runner execution: {report['runner_seconds'] / 3600:.2f} hours")
    print("\n| Group | Jobs | Runner minutes | Longest job (min) |")
    print("| --- | ---: | ---: | ---: |")
    for group, costs in report["groups"].items():
        print(f"| {group} | {costs['jobs']} | {costs['runner_seconds'] / 60:.1f} | {costs['max_job_seconds'] / 60:.1f} |")
    print("\nLargest step totals (parallel executions added):")
    for name, costs in list(report["steps"].items())[:8]:
        print(f"- {name}: {costs['seconds'] / 60:.1f} min across {costs['executions']} executions")
    failures = [job for job in report["jobs"] if job["failed_steps"]]
    if failures:
        print("\nFailures:")
        for job in failures:
            print(f"- [{job['name']}]({job['url']}): {', '.join(job['failed_steps'])}")
    print(f"\n{report['limits']}")
    if "canonical_coverage" in report:
        print("\nCanonical inventories, class outcomes and invocation identities match the complete baseline.")


if __name__ == "__main__":
    main()
