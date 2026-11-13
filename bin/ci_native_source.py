"""Reuse a completed native producer only when its build inputs still match."""

import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import time


INPUTS = ("native", "rust-toolchain.toml", ".cargo", ".github/actions/setup-rust")


def api(path):
    for attempt in range(3):
        try:
            result = subprocess.run(["gh", "api", path], capture_output=True, text=True, timeout=15)
        except subprocess.TimeoutExpired:
            if attempt == 2:
                raise
        else:
            if result.returncode == 0:
                return json.loads(result.stdout)
            if attempt == 2 or not re.search(r"HTTP (429|500|502|503|504)", result.stderr):
                raise RuntimeError(result.stderr.strip())
        time.sleep(2 ** attempt)


def verify(repository, run_id):
    run = api(f"repos/{repository}/actions/runs/{run_id}")
    page = 1
    while True:
        jobs = api(f"repos/{repository}/actions/runs/{run_id}/jobs?per_page=20&page={page}")["jobs"]
        producer = next((job for job in jobs if job["name"] == "Shared debug native libraries"), None)
        if producer is not None:
            if producer["conclusion"] != "success":
                raise ValueError("Native producer has not succeeded")
            break
        if len(jobs) < 20:
            raise ValueError("Native producer is missing")
        page += 1
    sha = run["head_sha"]
    if not re.fullmatch(r"[0-9a-f]{40}", sha):
        raise ValueError("Invalid native source commit")
    subprocess.run(["git", "fetch", "--no-tags", "--filter=blob:none", "--depth=1", "origin", sha], check=True)
    subprocess.run(["git", "diff", "--exit-code", sha, "HEAD", "--", *INPUTS], check=True)
    return f"debug-native-{sha}"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository", default=os.environ.get("GITHUB_REPOSITORY"), required=not os.environ.get("GITHUB_REPOSITORY"))
    parser.add_argument("--run", type=int, required=True)
    args = parser.parse_args()
    if args.run < 1:
        parser.error("Expected a positive native workflow run ID")
    artifact = verify(args.repository, args.run)
    if os.environ.get("GITHUB_OUTPUT"):
        with Path(os.environ["GITHUB_OUTPUT"]).open("a") as output:
            output.write(f"artifact={artifact}\n")
    print(f"Reusing native libraries from successful producer: {artifact}")


if __name__ == "__main__":
    main()
