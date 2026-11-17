#!/usr/bin/env python3
"""Reuse proven coverage for identical merged trees or documentation-only pushes."""

import json
import os
from pathlib import PurePosixPath
import re
import subprocess
import sys
from urllib.parse import urlencode


CANONICAL = "datafusion-contrib/StreamFusion"


def api(endpoint):
    return json.loads(subprocess.check_output(["gh", "api", endpoint], text=True))


def changed_paths(base, head, cwd=None):
    # Disable rename detection: moving Java into docs must retain the deleted source path.
    return subprocess.check_output(
        ["git", "diff", "--no-renames", "--name-only", "-z", base, head], cwd=cwd).decode().split("\0")[:-1]


def documentation_only(paths):
    def allowed(name):
        path = PurePosixPath(name)
        return (name in ("readme.md", "CLAUDE.md", "AGENTS.md")
                or name.startswith("docs/") and path.suffix == ".md"
                or path.parent == PurePosixPath("docs/benchmarks")
                and re.fullmatch(r"ci-profile-\d{8}\.json", path.name) is not None)
    return bool(paths) and all(allowed(name) for name in paths)


def proven_push(runs, sha, branch):
    matches = [run for run in runs if run.get("head_sha") == sha
               and run.get("head_branch") == branch and run.get("event") == "push"]
    if not matches:
        return False
    latest = max(matches, key=lambda run: run["id"])
    return latest.get("status") == "completed" and latest.get("conclusion") == "success"


def proven_qualification(runs, sha, branch, workflow):
    matches = [run for run in runs if run.get("head_sha") == sha
               and run.get("head_branch") == branch
               and (run.get("event") == "push"
                    or run.get("event") == "workflow_dispatch"
                    and (workflow != "flink-suite.yml"
                         or run.get("display_title") == "Full upstream Flink suite (merge gate)"))]
    if not matches:
        return None
    latest = max(matches, key=lambda run: run["id"])
    return latest if latest.get("status") == "completed" and latest.get("conclusion") == "success" else None


def qualified_merge(repository, head, branch):
    if repository != CANONICAL or branch != "main":
        return None
    pulls = api(f"repos/{repository}/commits/{head}/pulls?per_page=100")
    for pr in pulls:
        if (not pr.get("merged_at") or pr.get("merge_commit_sha") != head
                or pr.get("base", {}).get("ref") != "main"):
            continue
        source = pr["head"]
        fork = source.get("repo")
        if not fork or not fork.get("fork") or fork["full_name"] == CANONICAL:
            continue
        sha = source["sha"]
        if not re.fullmatch(r"[0-9a-f]{40}", sha):
            continue
        subprocess.run(["git", "fetch", "--no-tags", "--filter=blob:none", "--depth=1",
                        f"https://github.com/{fork['full_name']}.git", sha], check=True,
                       stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        # Compare the complete committed tree, including workflows and fixtures.
        # Merging additional upstream changes must retain its full integration run.
        if changed_paths(sha, head):
            continue
        evidence = []
        for workflow in ("ci.yml", "flink-suite.yml"):
            query = urlencode({"head_sha": sha, "branch": source["ref"]})
            path = f"repos/{fork['full_name']}/actions/workflows/{workflow}/runs?{query}"
            runs = []
            page = 1
            while True:
                batch = api(path + f"&per_page=100&page={page}")["workflow_runs"]
                runs.extend(batch)
                if len(batch) < 100:
                    break
                page += 1
            proven = proven_qualification(runs, sha, source["ref"], workflow)
            if not proven:
                break
            evidence.append(proven)
        if len(evidence) == 2:
            print(f"Verified identical merged tree from PR #{pr['number']}: {fork['full_name']}@{sha}",
                  file=sys.stderr)
            for run in evidence:
                print(f"Verified complete qualification: {run['html_url']}", file=sys.stderr)
            return sha
    return None


def qualified_branch_docs(repository, head, branch, *, fetch=True):
    query = urlencode({"branch": branch})
    ci = api(f"repos/{repository}/actions/workflows/ci.yml/runs?{query}&per_page=100")["workflow_runs"]
    previous = [run for run in ci if run.get("head_branch") == branch
                and run.get("head_sha") != head and run.get("event") in ("push", "workflow_dispatch")]
    if not previous:
        return None
    # Use the immediately preceding submitted head, never an older green head
    # behind a failed or unfinished qualification on this branch.
    base = max(previous, key=lambda run: run["id"])["head_sha"]
    if not re.fullmatch(r"[0-9a-f]{40}", base or ""):
        return None
    proven_ci = proven_qualification(ci, base, branch, "ci.yml")
    if not proven_ci:
        return None
    query = urlencode({"branch": branch, "head_sha": base})
    upstream = api(f"repos/{repository}/actions/workflows/flink-suite.yml/runs?{query}&per_page=100")["workflow_runs"]
    proven_upstream = proven_qualification(upstream, base, branch, "flink-suite.yml")
    if not proven_upstream:
        return None
    if fetch:
        subprocess.run(["git", "fetch", "--no-tags", "--filter=blob:none", "--depth=1",
                        f"https://github.com/{repository}.git", base], check=True,
                       stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    paths = changed_paths(base, head)
    if paths and not documentation_only(paths):
        return None
    for run in (proven_ci, proven_upstream):
        print(f"Verified unchanged branch source: {run['html_url']}", file=sys.stderr)
    return base


def plan(repository, head, before, branch):
    qualified = qualified_merge(repository, head, branch)
    if qualified:
        return False, qualified, "identical-merged-tree-with-passing-fork-qualification"
    if re.fullmatch(r"[0-9a-f]{40}", before or "") and before != "0" * 40:
        base_repository, base, base_branch = repository, before, branch
    else:
        base_repository, base_branch = CANONICAL, "main"
        base = api(f"repos/{CANONICAL}/commits/main")["sha"]
    if not re.fullmatch(r"[0-9a-f]{40}", base):
        raise ValueError("Invalid baseline commit")
    subprocess.run(["git", "fetch", "--no-tags", "--filter=blob:none", "--depth=1",
                    f"https://github.com/{base_repository}.git", base], check=True,
                   stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    paths = changed_paths(base, head)
    if not documentation_only(paths) and (paths or base == head):
        return True, base, "source-or-configuration-changed"
    for workflow in ("ci.yml", "flink-suite.yml"):
        runs = api(f"repos/{base_repository}/actions/workflows/{workflow}/runs?head_sha={base}&event=push&per_page=100")
        if not proven_push(runs["workflow_runs"], base, base_branch):
            return True, base, "baseline-tests-not-proven"
        print(f"Verified {workflow} push coverage: {base_repository}@{base} on {base_branch}", file=sys.stderr)
    return False, base, "documentation-only-with-passing-base"


def main():
    tests, baseline, reason = True, "", "non-push-event"
    event_name = os.environ.get("GITHUB_EVENT_NAME")
    exhaustive = False
    if event_name == "workflow_dispatch":
        try:
            with open(os.environ["GITHUB_EVENT_PATH"]) as stream:
                exhaustive = json.load(stream).get("inputs", {}).get("canonical_coverage") == "full"
        except (OSError, ValueError, KeyError):
            pass
    if exhaustive:
        reason = "explicit-exhaustive-canonical-qualification"
    manual_docs = (event_name == "workflow_dispatch"
                   and os.environ.get("GITHUB_REF_NAME") != "main" and not exhaustive)
    if event_name == "push" or manual_docs:
        try:
            with open(os.environ["GITHUB_EVENT_PATH"]) as stream:
                event = json.load(stream)
            if manual_docs:
                baseline = qualified_branch_docs(os.environ["GITHUB_REPOSITORY"],
                                                 os.environ["GITHUB_SHA"], os.environ["GITHUB_REF_NAME"]) or ""
            if baseline:
                tests, reason = False, "documentation-only-with-passing-branch-qualification"
            else:
                tests, baseline, reason = plan(os.environ["GITHUB_REPOSITORY"], os.environ["GITHUB_SHA"],
                                               event.get("before"), os.environ["GITHUB_REF_NAME"])
        except (OSError, ValueError, KeyError, subprocess.SubprocessError):
            # Missing API access, history or evidence must never turn untested source green.
            reason = "unable-to-prove-previous-coverage"
            print("Could not prove unchanged qualified source; running the complete suite.", file=sys.stderr)
    print(f"tests={str(tests).lower()}\nbaseline={baseline}\nreason={reason}")
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as stream:
            stream.write(f"Tests required: **{str(tests).lower()}**\n\n"
                         f"Baseline: `{baseline or 'none'}`\n\nReason: `{reason}`\n")


if __name__ == "__main__":
    main()
