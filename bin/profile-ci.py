#!/usr/bin/env python3
"""Report Actions execution cost separately from dependency and runner start delays."""

import argparse
from collections import defaultdict
from datetime import datetime
import json
import subprocess


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
    return json.loads(subprocess.check_output(["gh", "api", endpoint], text=True))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", required=True)
    parser.add_argument("--run", required=True, type=int)
    parser.add_argument("--json", action="store_true", help="Emit reusable JSON instead of Markdown")
    args = parser.parse_args()
    run = api(f"repos/{args.repo}/actions/runs/{args.run}")
    jobs = []
    page = 1
    while True:
        batch = api(f"repos/{args.repo}/actions/runs/{args.run}/attempts/{run['run_attempt']}/jobs?per_page=100&page={page}")
        jobs.extend(batch["jobs"])
        if len(batch["jobs"]) < 100:
            break
        page += 1
    report = summarize(run, jobs)
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


if __name__ == "__main__":
    main()
