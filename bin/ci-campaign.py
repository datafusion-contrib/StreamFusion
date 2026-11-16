#!/usr/bin/env python3
"""Start one ready fork qualification, reuse its runs, and retry only failed jobs."""

import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
import fcntl
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import time
from urllib.parse import quote, urlencode


# Loading the proof helper must not dirty the clean checkout this launcher requires.
sys.dont_write_bytecode = True


CANONICAL = "datafusion-contrib/StreamFusion"
REQUIRED = ("ci.yml", "flink-suite.yml")
OPTIONAL = ("docs.yml", "paimon1.yml")
ACTIVE = {"queued", "in_progress", "waiting", "pending", "requested"}


def command(*args):
    return subprocess.check_output(args, text=True, timeout=30).strip()


def api(path, *, data=None):
    args = ["gh", "api", path]
    if data is not None:
        args += ["--method", "POST", "--input", "-"]
    result = subprocess.run(args, input=None if data is None else json.dumps(data),
                            capture_output=True, text=True, timeout=30)
    if result.returncode:
        raise RuntimeError(result.stderr.strip())
    return json.loads(result.stdout) if result.stdout.strip() else None


def pages(path, field=None, per_page=100):
    page = 1
    while True:
        value = api(path + ("&" if "?" in path else "?") + f"per_page={per_page}&page={page}")
        values = value[field] if field else value
        yield from values
        if len(values) < per_page:
            return
        page += 1


def candidate(repository, branch):
    if command("git", "status", "--porcelain"):
        raise ValueError("Commit the complete locally validated candidate before qualification.")
    sha = command("git", "rev-parse", "HEAD")
    tip = api(f"repos/{repository}/git/ref/heads/{quote(branch, safe='')}")["object"]["sha"]
    if sha != tip:
        raise ValueError(f"Local HEAD {sha} is not the pushed branch tip {tip}.")
    metadata = api(f"repos/{repository}")
    if not metadata.get("fork") or metadata.get("source", metadata.get("parent", {})).get("full_name") != CANONICAL:
        raise ValueError(f"Qualification must use a personal fork of {CANONICAL}.")
    for pr in pages(f"repos/{CANONICAL}/pulls?state=open&head=" + quote(repository.split('/')[0] + ':' + branch), None):
        if pr["head"]["repo"]["full_name"] == repository and pr["head"]["sha"] != sha:
            raise ValueError("The open PR points at a different head; refresh before qualification.")
    return sha


def require_current_base(sha):
    base = api(f"repos/{CANONICAL}/git/ref/heads/main")["object"]["sha"]
    if subprocess.run(["git", "cat-file", "-e", base], capture_output=True).returncode:
        command("git", "fetch", "--no-tags", "--filter=blob:none",
                f"https://github.com/{CANONICAL}.git", base)
    if subprocess.run(["git", "merge-base", "--is-ancestor", base, sha]).returncode:
        raise ValueError("Integrate current canonical main and validate locally before qualification.")


def qualified_docs_candidate(repository, sha, branch):
    spec = importlib.util.spec_from_file_location("ci_plan", Path(__file__).with_name("ci-plan.py"))
    planner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(planner)
    return planner.qualified_branch_docs(repository, sha, branch, fetch=False)


def latest_run(repository, branch, sha, workflow):
    query = urlencode({"branch": branch, "head_sha": sha})
    matches = [r for r in pages(f"repos/{repository}/actions/workflows/{workflow}/runs?{query}", "workflow_runs")
               if r["head_sha"] == sha and r["head_branch"] == branch
               and (r["event"] == "push" or r["event"] == "workflow_dispatch"
                    and (workflow != "flink-suite.yml"
                         or r.get("display_title") == "Full upstream Flink suite (merge gate)"))]
    return max(matches, key=lambda run: run["id"], default=None)


def active_work(repository):
    result = []
    for status in ("in_progress", "queued", "waiting"):
        for run in pages(f"repos/{repository}/actions/runs?status={status}", "workflow_runs"):
            if run["status"] in ACTIVE:
                result.append({**run, "repository_name": repository})
    return result


def competitors(repository, reuse):
    owner = repository.split('/')[0]
    repos = [r["full_name"] for r in pages("user/repos?affiliation=owner&sort=pushed", None)
             if r["owner"]["login"].lower() == owner.lower()]
    if repository not in repos:
        repos.append(repository)
    with ThreadPoolExecutor(max_workers=6) as pool:
        active = [run for group in pool.map(active_work, repos) for run in group]
    # Documentation and the upstream result reporter are tiny bookkeeping jobs.
    return [r for r in active if (r["repository_name"], r["id"]) not in reuse
            and (r["repository_name"] != repository
                 or r.get("path", "").split('@')[0] not in
                 (".github/workflows/docs.yml", ".github/workflows/fork-ci-status.yml"))]


@contextmanager
def campaign_lock():
    # All local worktrees share this kernel lock. Keep the inode stable so two
    # agents cannot both observe an empty queue before dispatches become visible.
    common = Path(command("git", "rev-parse", "--git-common-dir")).resolve()
    with (common / "streamfusion-ci-campaign.lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise ValueError("Another local agent is submitting a qualification; reuse its result.") from None
        try:
            yield
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)


def ensure_capacity(repository, runs, capacity=20, branch=None):
    reuse = {(repository, run["id"]) for run in runs if run and run["status"] in ACTIVE}
    busy = competitors(repository, reuse)
    same_branch = [run for run in busy if run["repository_name"] == repository
                   and branch and run.get("head_branch") == branch]
    if same_branch:
        links = "\n".join("  " + run["html_url"] for run in same_branch)
        raise ValueError("Active work already exists on this branch. Reuse it, or finish/cancel "
                         "this task's superseded work before submitting another campaign:\n" + links)
    queued, running = [], 0
    for run in busy:
        jobs = list(pages(f"repos/{run['repository_name']}/actions/runs/{run['id']}/jobs?filter=latest",
                          "jobs", per_page=20))
        waiting = run["status"] in ("queued", "waiting") or not jobs
        for job in jobs:
            if job["status"] == "completed":
                continue
            executed = job["status"] == "in_progress" and any(
                step.get("started_at") and step.get("conclusion") != "skipped"
                for step in job.get("steps", []))
            running += executed
            waiting |= not executed
        if waiting:
            queued.append(run)
    if queued or running >= capacity:
        links = "\n".join(f"  {r['html_url']} ({r['name']}, {r['head_branch']})" for r in busy)
        raise ValueError(f"Other work has queued jobs or fills the {capacity}-job scheduling budget "
                         f"({running} observed executing jobs). Finish local work and wait; "
                         "do not cancel another task's runs:\n" + links)
    if busy:
        print(f"Other campaigns have only executing tail jobs: {running}/{capacity} scheduling slots. "
              "No queued work will be displaced.")


def wait_for_dispatch(repository, branch, sha, workflow, previous=0):
    for _ in range(10):
        run = latest_run(repository, branch, sha, workflow)
        if run and run["id"] > previous:
            return run
        time.sleep(3)
    raise RuntimeError("Dispatch was accepted but its run is not visible yet. Query the same "
                       "head again; do not dispatch another copy.")


def qualify(repository, branch, workflows, *, start=False, validation="", capacity=20):
    sha = candidate(repository, branch)
    runs = [(workflow, latest_run(repository, branch, sha, workflow)) for workflow in workflows]
    missing = []
    for workflow, run in runs:
        if run:
            print(f"{workflow}: {run['conclusion'] or run['status']} {run['html_url']}")
            if run["status"] in ACTIVE or run.get("conclusion") == "success":
                continue
            raise ValueError(f"{workflow} already has an unsuccessful run for this head. "
                             "Diagnose it locally or with a focused probe, then use retry --run "
                             f"{run['id']}; do not restart the matrix.")
        missing.append(workflow)
        print(f"{workflow}: missing for {sha}")
    if not missing:
        print("All requested workflows are already successful or active; nothing dispatched.")
        return
    if not any(run and (run["status"] in ACTIVE or run.get("conclusion") == "success")
               for _, run in runs):
        if not qualified_docs_candidate(repository, sha, branch):
            require_current_base(sha)
    ensure_capacity(repository, [run for _, run in runs], capacity, branch=branch)
    if not start:
        print("Ready to submit after local validation: " + ", ".join(missing))
        return
    if not validation.strip():
        raise ValueError("Record the focused local checks with --validation before starting CI.")
    print("Local validation: " + validation)
    for workflow in missing:
        if candidate(repository, branch) != sha:
            raise ValueError("The head changed during submission; stop and prepare the new candidate.")
        api(f"repos/{repository}/actions/workflows/{workflow}/dispatches", data={"ref": branch})
        run = wait_for_dispatch(repository, branch, sha, workflow)
        print(f"Started {workflow}: {run['html_url']}", flush=True)


def retry(repository, branch, run_id, validation, capacity=20):
    sha = candidate(repository, branch)
    run = api(f"repos/{repository}/actions/runs/{run_id}")
    if run["head_sha"] != sha or run["head_branch"] != branch:
        raise ValueError("Refusing to retry a superseded commit or another branch.")
    workflow = run["path"].split('@')[0].split('/')[-1]
    if workflow not in (*REQUIRED, *OPTIONAL):
        raise ValueError("Retry is restricted to qualification workflows.")
    current = latest_run(repository, branch, sha, workflow)
    if not current or current["id"] != run_id:
        raise ValueError("A newer run exists; inspect and reuse it instead.")
    if run["status"] != "completed" or run.get("conclusion") not in ("failure", "cancelled", "timed_out", "action_required"):
        raise ValueError("Only a completed unsuccessful run needs a retry.")
    if not validation.strip():
        raise ValueError("Record the failure diagnosis and focused validation with --validation.")
    same_head = [latest_run(repository, branch, sha, w) for w in (*REQUIRED, *OPTIONAL)]
    ensure_capacity(repository, same_head, capacity, branch=branch)
    if candidate(repository, branch) != sha:
        raise ValueError("The branch head changed before retry.")
    print("Failure diagnosis and focused validation: " + validation)
    api(f"repos/{repository}/actions/runs/{run_id}/rerun-failed-jobs", data={})
    print("Retrying failed jobs only: " + run["html_url"])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("status", "start", "retry"))
    parser.add_argument("--repo", required=True, help="Personal fork OWNER/REPO")
    parser.add_argument("--branch", help="Defaults to the checked-out branch")
    parser.add_argument("--include", choices=OPTIONAL, action="append", default=[])
    parser.add_argument("--validation", default="", help="Actual focused local checks, or retry diagnosis")
    parser.add_argument("--run", type=int, help="Unsuccessful current-head run for retry")
    parser.add_argument("--capacity", type=int, default=20,
                        help="Conservative observed runner budget, not a configured account limit (default: 20)")
    args = parser.parse_args()
    branch = args.branch or command("git", "branch", "--show-current")
    if not branch:
        parser.error("Check out the candidate branch or supply --branch.")
    if args.capacity < 1:
        parser.error("--capacity must be positive")
    try:
        with campaign_lock():
            if args.action == "retry":
                if not args.run:
                    parser.error("retry requires --run")
                retry(args.repo, branch, args.run, args.validation, args.capacity)
            else:
                qualify(args.repo, branch, tuple(dict.fromkeys((*REQUIRED, *args.include))),
                        start=args.action == "start", validation=args.validation, capacity=args.capacity)
    except (ValueError, RuntimeError, subprocess.SubprocessError) as error:
        parser.exit(1, f"{error}\n")


if __name__ == "__main__":
    main()
