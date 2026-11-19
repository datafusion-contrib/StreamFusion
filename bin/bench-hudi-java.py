#!/usr/bin/env python3
"""Alternate Java-only Hudi changes against an identical native build on one host."""

import argparse
from collections import Counter
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import shutil
import signal
import subprocess


ROOT = Path(__file__).resolve().parents[1]
JAVA = (
    "streamfusion-core/src/main/java/tech/streamfusion/operator/NativeCalcOperator.java",
    "streamfusion-core/src/main/java/tech/streamfusion/operator/TaskOffHeapMemory.java",
    "streamfusion-hudi/src/main/java/tech/streamfusion/planner/stream/HudiSinkMatcher.java",
)
FIXED = (
    "native", "rust-toolchain.toml", ".cargo", ".github/actions/setup-rust", "pom.xml",
    "streamfusion-core/pom.xml", "streamfusion-hudi/pom.xml", "streamfusion-parquet/pom.xml",
    "streamfusion-hudi/src/nexmark", "streamfusion-runtime/src/test/java/tech/streamfusion/NexmarkMatrixBenchmark.java",
    "streamfusion-runtime/src/test/java/tech/streamfusion/NexmarkKafkaBenchmark.java",
    "streamfusion-test-utils/src/main/java/tech/streamfusion/SharedFlinkCluster.java",
)


def git(*args):
    return subprocess.check_output(["git", *args], cwd=ROOT, text=True).strip()


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def validate(report, rows, queries):
    expected = Counter((query, engine, trial) for query in queries
                       for engine in ("stock", "previous", "native") for trial in range(3))
    actual = Counter((item["query"], item["engine"], item["trial"])
                     for item in report["observations"])
    if report["status"] != "passed" or actual != expected:
        raise ValueError("Incomplete Hudi comparison or changed trial identities")
    for key, value in {"rows": rows, "parallelism": 4, "source_partitions": 4,
                       "warmups": 1, "runs": 3, "checkpoint_ms": 1000,
                       "object_reuse": True}.items():
        if report[key] != value:
            raise ValueError(f"Changed Hudi benchmark configuration: {key}")
    for item in report["observations"]:
        validation = ("processing-time bounded counts" if item["query"] == "q12"
                      else "full-value multiset")
        if item["validation"] != validation:
            raise ValueError("Changed Hudi output validation")


def compare(args):
    if not re.fullmatch(r"[0-9a-f]{40}", args.baseline):
        raise ValueError("Use a full baseline commit SHA")
    if set(args.queries.split(",")) - {"q8", "q10", "q12"}:
        raise ValueError("This focused comparison supports q8, q10 and q12")
    queries = args.queries.split(",")
    if len(set(queries)) != len(queries):
        raise ValueError("Duplicate query")
    if args.rows < 1 or args.rows > 2_000_000 or args.pairs < 1:
        raise ValueError("Use 1..2000000 events and at least one paired pass")
    try:
        git("cat-file", "-e", f"{args.baseline}^{{commit}}")
    except subprocess.CalledProcessError:
        git("fetch", "--no-tags", "--depth=1", "origin", args.baseline)
    head = git("rev-parse", "HEAD")
    if git("diff", "--name-only", args.baseline, head, "--", *FIXED):
        raise ValueError("Baseline must use identical native inputs and benchmark harness")
    changed = set(git("diff", "--name-only", args.baseline, head, "--",
                      "streamfusion-core/src/main", "streamfusion-hudi/src/main").splitlines())
    if changed - set(JAVA):
        raise ValueError(f"Unverified Java changes in baseline: {sorted(changed - set(JAVA))}")
    if not changed:
        raise ValueError("Baseline and candidate have no Java changes to compare")
    if git("diff", "HEAD", "--", *FIXED, *JAVA):
        raise ValueError("Commit production and benchmark inputs before comparing")
    if getattr(args, "verify_only", False):
        print("Verified identical native inputs and benchmark harness", flush=True)
        return
    libraries = args.native_library_dir.resolve()
    extension = "dylib" if platform.system() == "Darwin" else "so"
    hashes = {name: digest(libraries / name) for name in
              (f"libstreamfusion.{extension}", f"libstreamfusion_parquet.{extension}")}
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    temporary = output / "runtime"
    temporary.mkdir()
    original = {name: (ROOT / name).read_bytes() for name in JAVA}
    result = {"status": "running", "baseline": args.baseline, "candidate": head,
              "native_tree": git("rev-parse", "HEAD:native"), "libraries_sha256": hashes,
              "platform": platform.platform(), "processors": os.cpu_count(),
              "scope": "Focused q8/q10/q12 complete jobs; not the full Nexmark matrix",
              "trials": []}
    summary = output / "summary.json"

    def save():
        summary.write_text(json.dumps(result, indent=2) + "\n")

    save()
    try:
        for pair in range(args.pairs):
            for variant in (("baseline", "candidate") if pair % 2 == 0
                            else ("candidate", "baseline")):
                for name, contents in original.items():
                    (ROOT / name).write_bytes(
                        subprocess.check_output(["git", "show", f"{args.baseline}:{name}"], cwd=ROOT)
                        if variant == "baseline" else contents)
                label = f"{pair}-{variant}"
                report = output / f"{label}.json"
                environment = {**os.environ, "SF_HUDI_BENCH": "true", "SF_ROWS": str(args.rows),
                               "SF_WARMUP": "1", "SF_RUNS": "3", "SF_MATRIX_QUERIES": args.queries,
                               "SF_HUDI_BACKENDS": "memory", "SF_HUDI_MINIBATCHES": "false",
                               "TMPDIR": str(temporary)}
                environment.pop("SF_HUDI_PROFILE", None)
                command = ["mvn", "-B", "-ntp", "-Phudi,flink-1.18,bench,benchmarks,hudi-nexmark",
                           "-pl", "streamfusion-hudi", "-am", "test", "-Dnative.build.skip=true",
                           "-Dnative.test.resource.skip=true", "-Dmaven.javadoc.skip=true",
                           "-Dsf.testForks=1", "-Dsurefire.failIfNoSpecifiedTests=false",
                           "-Dtest=NexmarkHudiBenchmark", f"-Dsf.hudi.benchmark.output={report}",
                           f"-Dsf.extraJvmArgs=-Djava.library.path={libraries}"
                           f" -Djava.io.tmpdir={temporary} -Duser.timezone=UTC -Dfile.encoding=UTF-8"]
                trial = {"pair": pair, "variant": variant,
                         "java_sha256": {name: digest(ROOT / name) for name in JAVA}}
                result["trials"].append(trial)
                save()
                with (output / f"{label}.log").open("w") as log:
                    process = subprocess.Popen(command, cwd=ROOT, env=environment,
                                               stdout=log, stderr=subprocess.STDOUT,
                                               start_new_session=True)
                    try:
                        status = process.wait()
                    except BaseException:
                        os.killpg(process.pid, signal.SIGTERM)
                        try:
                            process.wait(timeout=10)
                        except subprocess.TimeoutExpired:
                            os.killpg(process.pid, signal.SIGKILL)
                            process.wait()
                        raise
                trial["exit_code"] = status
                if report.exists():
                    trial["report"] = json.loads(report.read_text())
                save()
                if status:
                    raise RuntimeError(f"Hudi comparison failed: {label}")
                validate(trial["report"], args.rows, queries)
                if hashes != {name: digest(libraries / name) for name in hashes}:
                    raise ValueError("Native libraries changed during comparison")
                print(f"Passed {label}", flush=True)
        result["status"] = "passed"
    except BaseException:
        result["status"] = "failed"
        raise
    finally:
        for name, contents in original.items():
            (ROOT / name).write_bytes(contents)
        shutil.rmtree(temporary)
        save()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--native-library-dir", type=Path, default=ROOT / "native/target/release")
    parser.add_argument("--rows", type=int, default=200_000)
    parser.add_argument("--queries", default="q8,q12")
    parser.add_argument("--pairs", type=int, default=2)
    parser.add_argument("--verify-only", action="store_true")
    compare(parser.parse_args())


if __name__ == "__main__":
    main()
