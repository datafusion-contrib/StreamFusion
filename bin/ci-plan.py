#!/usr/bin/env python3
"""Reuse proven source coverage only when a push changes documentation alone."""

import json
import os
from pathlib import PurePosixPath
import re
import subprocess
import sys


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
        return (name == "readme.md"
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


def plan(repository, head, before, branch):
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
    if not documentation_only(changed_paths(base, head)):
        return True, base, "source-or-configuration-changed"
    for workflow in ("ci.yml", "flink-suite.yml"):
        runs = api(f"repos/{base_repository}/actions/workflows/{workflow}/runs?head_sha={base}&event=push&per_page=100")
        if not proven_push(runs["workflow_runs"], base, base_branch):
            return True, base, "baseline-tests-not-proven"
        print(f"Verified {workflow} push coverage: {base_repository}@{base} on {base_branch}", file=sys.stderr)
    return False, base, "documentation-only-with-passing-base"


def main():
    tests, baseline, reason = True, "", "non-push-event"
    if os.environ.get("GITHUB_EVENT_NAME") == "push":
        try:
            with open(os.environ["GITHUB_EVENT_PATH"]) as stream:
                event = json.load(stream)
            tests, baseline, reason = plan(os.environ["GITHUB_REPOSITORY"], os.environ["GITHUB_SHA"],
                                           event.get("before"), os.environ["GITHUB_REF_NAME"])
        except (OSError, ValueError, KeyError, subprocess.SubprocessError):
            # Missing API access, history or evidence must never turn untested source green.
            reason = "unable-to-prove-documentation-only-coverage"
            print("Could not prove passing baseline coverage; running the complete suite.", file=sys.stderr)
    print(f"tests={str(tests).lower()}\nbaseline={baseline}\nreason={reason}")
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as stream:
            stream.write(f"Tests required: **{str(tests).lower()}**\n\n"
                         f"Baseline: `{baseline or 'none'}`\n\nReason: `{reason}`\n")


if __name__ == "__main__":
    main()
