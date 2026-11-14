#!/usr/bin/env python3
"""Run one unchanged two-million-event Hudi Nexmark matrix configuration."""

import argparse
from collections import Counter
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import signal
import subprocess


ROOT = Path(__file__).resolve().parents[1]
QUERIES = {f"q{number}" for number in range(24)} - {"q6", "q7"}
ENGINES = {"stock", "previous", "native"}
REJECTION = "Incomparable types: TIMESTAMP_LTZ(3) and TIMESTAMP(3) NOT NULL"


def configurations(selection):
    pairs = [{"backend": backend, "mini_batch": mini}
             for backend in ["memory", "rocksdb"] for mini in ["false", "true"]]
    if selection == "all":
        return pairs
    selected = [pair for pair in pairs
                if selection == f'{pair["backend"]}-{pair["mini_batch"]}'
                or selection == "off" and pair["mini_batch"] == "false"]
    if not selected:
        raise ValueError("Unknown Hudi matrix configuration selection")
    return selected


def run_logged(command, environment, log):
    process = subprocess.Popen(command, cwd=ROOT, env=environment, stdout=subprocess.PIPE,
                               stderr=subprocess.STDOUT, start_new_session=True)
    try:
        with process.stdout:
            for line in process.stdout:
                log.write(line)
                if line.startswith(b"[hudi-nexmark]"):
                    print(line.decode("utf-8", errors="replace").rstrip("\r\n"), flush=True)
        return process.wait()
    except BaseException:
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait()
        raise


def validate(report, backend, mini_batch):
    expected = Counter((query, engine, trial) for query in QUERIES
                       for engine in ENGINES for trial in range(3))
    timed = [item for item in report["observations"] if "milliseconds" in item]
    actual = Counter((item["query"], item["engine"], item["trial"]) for item in timed)
    rejected = [item for item in report["observations"] if "milliseconds" not in item]
    if report["status"] != "passed" or actual != expected:
        raise ValueError("Incomplete Hudi matrix or changed trial identities")
    if (len(rejected) != 1 or rejected[0].get("query") != "q7"
            or rejected[0].get("status") != "stock-planner-rejected"
            or rejected[0].get("reason") != REJECTION):
        raise ValueError("Changed q7 stock planner exclusion")
    for key, value in {"rows": 2_000_000, "input_bids": 1_840_000, "parallelism": 4,
                       "source_partitions": 4, "sink_partition_modulus": 4,
                       "checkpoint_ms": 1000, "warmups": 1, "runs": 3,
                       "object_reuse": True, "heap_max_bytes": 8 * 1024**3,
                       "native_task_off_heap_capacity_bytes": 48 * 1024**3}.items():
        if report[key] != value:
            raise ValueError(f"Changed Hudi matrix configuration: {key}")
    for item in report["observations"]:
        if item["backend"] != backend or item["mini_batch"] != mini_batch:
            raise ValueError("Changed Hudi matrix backend/mini-batch configuration")
    for item in timed:
        expected_validation = ("processing-time bounded counts" if item["query"] == "q12"
                               else "full-value multiset")
        if item["validation"] != expected_validation:
            raise ValueError("Changed Hudi matrix output validation")
    checks = report["state_backend_checks"]
    if backend == "rocksdb":
        actual_checks = Counter((item["engine"], item["query"], item["mini_batch"],
                                 item["rocksdb_opened"]) for item in checks)
        if actual_checks != Counter((engine, "q4", mini_batch, True) for engine in ENGINES):
            raise ValueError("Missing actual RocksDB backend engagement")
    elif checks:
        raise ValueError("Unexpected RocksDB checks in a heap configuration")


def run(args):
    if args.native_producer_run < 1:
        raise ValueError("Provide a verified native producer run ID")
    subprocess.run(["git", "diff", "--exit-code", "HEAD"], cwd=ROOT, check=True,
                   stdout=subprocess.DEVNULL)
    output = args.output.resolve()
    libraries = args.native_library_dir.resolve()
    extension = "dylib" if platform.system() == "Darwin" else "so"
    files = [libraries / f"libstreamfusion.{extension}",
             libraries / f"libstreamfusion_parquet.{extension}"]

    def hashes():
        return {path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in files}

    def git(value):
        return subprocess.check_output(["git", "rev-parse", value], cwd=ROOT, text=True).strip()

    result = {"status": "running", "commit": git("HEAD"), "native_tree": git("HEAD:native"),
              "native_producer_run": args.native_producer_run, "libraries_sha256": hashes(),
              "platform": platform.platform(), "processors": os.cpu_count(),
              "backend": args.backend, "mini_batch": args.mini_batch == "true"}
    if Path("/proc/cpuinfo").exists():
        result["cpu_model"] = next((line.partition(":")[2].strip()
                                    for line in Path("/proc/cpuinfo").read_text().splitlines()
                                    if line.startswith("model name")), platform.processor())
    else:
        result["cpu_model"] = platform.processor()
    try:
        result["physical_memory_bytes"] = os.sysconf("SC_PHYS_PAGES") * os.sysconf("SC_PAGE_SIZE")
    except (OSError, ValueError):
        result["physical_memory_bytes"] = None
    output.mkdir(parents=True, exist_ok=False)
    temporary = output / "runtime"
    temporary.mkdir()
    summary = output / "summary.json"

    def save():
        summary.write_text(json.dumps(result, indent=2) + "\n")

    report = output / "result.json"
    save()
    try:
        environment = {**os.environ, "SF_HUDI_BENCH": "true", "SF_ROWS": "2000000",
                       "SF_WARMUP": "1", "SF_RUNS": "3", "SF_HUDI_BACKENDS": args.backend,
                       "SF_HUDI_MINIBATCHES": args.mini_batch, "TMPDIR": str(temporary)}
        for name in ["SF_HUDI_PROFILE", "SF_MATRIX_QUERIES"]:
            environment.pop(name, None)
        command = ["mvn", "-B", "-ntp", "-Phudi,flink-1.18,bench,hudi-nexmark",
                   "-pl", "streamfusion-hudi", "-am", "test", "-Dnative.build.skip=true",
                   "-Dnative.test.resource.skip=true", "-Dmaven.javadoc.skip=true", "-Dsf.testForks=1",
                   "-Dsurefire.failIfNoSpecifiedTests=false", "-Dtest=NexmarkHudiBenchmark",
                   f"-Dsf.hudi.benchmark.output={report}",
                   f'-Dsf.extraJvmArgs=-Djava.library.path="{libraries}"'
                   f' -Djava.io.tmpdir="{temporary}" -Duser.timezone=UTC -Dfile.encoding=UTF-8']
        with (output / "maven.log").open("wb") as log:
            code = run_logged(command, environment, log)
        result["exit_code"] = code
        if report.exists():
            result["report"] = json.loads(report.read_text())
        if code:
            raise RuntimeError("Hudi matrix Maven invocation failed")
        validate(result["report"], args.backend, args.mini_batch == "true")
        if result["libraries_sha256"] != hashes():
            raise ValueError("Native libraries changed during Hudi matrix timing")
        result["status"] = "passed"
    except BaseException:
        result["status"] = "failed"
        raise
    finally:
        shutil.rmtree(temporary)
        save()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backend", choices=["memory", "rocksdb"], required=True)
    parser.add_argument("--mini-batch", choices=["false", "true"], required=True)
    parser.add_argument("--native-producer-run", type=int, required=True)
    parser.add_argument("--native-library-dir", type=Path, default=ROOT / "native/target/release")
    parser.add_argument("--output", type=Path, required=True)
    run(parser.parse_args())


if __name__ == "__main__":
    main()
