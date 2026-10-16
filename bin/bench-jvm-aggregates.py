#!/usr/bin/env python3
"""Run complete JVM aggregate jobs after a separate release build has finished."""
import argparse
import hashlib
import json
import os
import platform
from pathlib import Path
import re
import statistics
import shlex
import subprocess
import time

ROOT = Path(__file__).resolve().parents[1]
RESULT = re.compile(
    r"\[jvm-aggregate\] shape=(\w+) key_type=(\w+) key_prefix_chars=(\d+) "
    r"rows=(\d+) keys=(\d+) stock=([\d.]+) sf=([\d.]+) "
    r"stock_trials=(\[.*?\]) sf_trials=(\[.*?\])"
)


def summaries(log, expected_runs):
    records = []
    for match in RESULT.finditer(log):
        shape, key_type, prefix, rows, keys, stock, native, raw_stock, raw_native = (
            match.groups()
        )
        trials = [json.loads(raw_stock), json.loads(raw_native)]
        if any(
            len(values) != expected_runs or any(value <= 0 for value in values)
            for values in trials
        ):
            raise ValueError("incomplete or nonpositive benchmark trials")
        medians = [statistics.median(values) for values in trials]
        if any(
            abs(actual - float(reported)) > 0.000001
            for actual, reported in zip(medians, [stock, native])
        ):
            raise ValueError("reported median differs from retained trials")
        records.append(
            {
                "shape": shape,
                "key_type": key_type,
                "key_prefix_chars": int(prefix),
                "rows": int(rows),
                "keys": int(keys),
                "stock_seconds": trials[0],
                "native_seconds": trials[1],
                "stock_median_seconds": medians[0],
                "native_median_seconds": medians[1],
                "stock_range_seconds": [min(trials[0]), max(trials[0])],
                "native_range_seconds": [min(trials[1]), max(trials[1])],
                "stock_over_native": medians[0] / medians[1],
                "admitted": False,
            }
        )
    if not records:
        raise ValueError(
            "no typed benchmark result; rebuild test classes before measuring"
        )
    return records


def command(args, label, key_type, keys, profile=False):
    native = ROOT / "native/target/release"
    out = args.output
    flags = [
        f"-Djava.library.path={native}",
        "-Dstreamfusion.experimental.jvmGroupAggregates=true",
        f"-Dstreamfusion.experimental.jvmAccumulatorCacheEntries={args.capacity}",
        f"-Xlog:gc*,safepoint:file={out / (label + '-gc.log')}:time,uptime,level,tags",
    ]
    if args.borrowed_string_exit:
        flags.append("-Dstreamfusion.experimental.borrowedStringExit=true")
    if profile:
        flags.append(
            f"-agentpath:{args.profiler_agent}=start,event=cpu-clock,interval=2ms,cstack=dwarf,alloc=1m,file={out / (label + '.jfr')}"
        )
    return [
        "mvn",
        "-B",
        "-ntp",
        "surefire:test@default-test",
        "-P" + args.profile,
        "-pl",
        "streamfusion-runtime",
        "-Dtest=JvmAggregateBenchmark",
        "-Dnative.build.skip=true",
        f"-Dnative.resource.directory={native}",
        "-Dsf.testForks=1",
        "-Dsf.benchmarkJvmArgs=-Xmx2g -Dstreamfusion.exchange.zeroCopyLocal=false",
        "-Dsf.extraJvmArgs=" + " ".join(shlex.quote(flag) for flag in flags),
        "-Djvmagg.expectNative=true",
        "-Djvmagg.cases=" + ("udaf,builtin" if profile else "udaf"),
        f"-Djvmagg.keyType={key_type}",
        f"-Djvmagg.keyPrefixChars={args.prefix_chars}",
        "-Djvmagg.expectCache=true",
        f"-Djvmagg.rows={min(args.rows, 1000000) if profile else args.rows}",
        f"-Djvmagg.keys={keys}",
        f"-Djvmagg.warmup={args.warmup}",
        f"-Djvmagg.runs={3 if profile else args.runs}",
    ]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--flink-line", choices=["2.2", "1.18"], required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--rows", type=int, default=2000000)
    parser.add_argument("--capacity", type=int, default=65536)
    parser.add_argument("--warmup", type=int, default=2)
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument("--prefix-chars", type=int, default=16)
    parser.add_argument("--profiler-agent", type=Path)
    parser.add_argument("--borrowed-string-exit", action="store_true")
    args = parser.parse_args()
    if (
        min(args.rows, args.capacity, args.warmup, args.runs) <= 0
        or args.prefix_chars < 0
    ):
        parser.error(
            "sizes, warmups and trials must be positive; prefix length must be nonnegative"
        )
    args.profile = "bench" + (",flink-1.18" if args.flink_line == "1.18" else "")
    args.output = args.output.resolve()
    args.output.mkdir(parents=True, exist_ok=True)
    library = (
        ROOT
        / "native/target/release"
        / (
            "libstreamfusion.dylib"
            if platform.system() == "Darwin"
            else "libstreamfusion.so"
        )
    )
    if not library.is_file():
        parser.error(
            "release native library is absent; finish the bench-profile build first"
        )
    if args.profiler_agent:
        args.profiler_agent = args.profiler_agent.resolve()
        if not args.profiler_agent.is_file():
            parser.error("profiler agent is absent")
    java = (
        str(Path(os.environ["JAVA_HOME"]) / "bin/java")
        if os.environ.get("JAVA_HOME")
        else "java"
    )
    report = {
        "source_commit": subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
        ).strip(),
        "flink_line": args.flink_line,
        "platform": platform.platform(),
        "native_sha256": hashlib.sha256(library.read_bytes()).hexdigest(),
        "native_source_fingerprint": os.environ.get("SF_NATIVE_SOURCE_FINGERPRINT"),
        "native_payload_cache_hit": os.environ.get("SF_NATIVE_PAYLOAD_CACHE_HIT"),
        "cpu_count": os.cpu_count(),
        "processor": platform.processor(),
        "java": subprocess.run(
            [java, "-version"], capture_output=True, text=True, check=True
        ).stderr,
        "configuration": {
            "rows": args.rows,
            "capacity": args.capacity,
            "warmup_pairs": args.warmup,
            "pairs": args.runs,
            "heap": "2 GiB",
            "parallelism": 1,
            "production_ipc": True,
            "both_transposes": True,
            "cluster_lifetime": "per job",
            "preflight_explain": "both engines outside the clock",
            "fixture_method": "symmetric-preflight-v2",
            "borrowed_string_exit": args.borrowed_string_exit,
            "boundary": "complete executeSql(insert).await; cluster boot/config outside clock",
        },
        "measurements": [],
        "profiles": [],
        "admitted": False,
    }
    (args.output / "results.json").write_text(json.dumps(report, indent=2) + "\n")
    env = dict(os.environ, SF_BENCHMARK="true")
    matrix = [
        (f"{kind}-{label}", kind, keys, False)
        for kind in ["int", "bigint", "string"]
        for label, keys in [("low", 16), ("medium", 65536), ("distinct", args.rows)]
    ]
    if args.profiler_agent:
        matrix += [
            (f"profile-{kind}-{label}", kind, keys, True)
            for kind in ["int", "string"]
            for label, keys in [
                ("medium", 32768),
                ("distinct", min(args.rows, 1000000)),
            ]
        ]
    for label, kind, keys, profile in matrix:
        cmd = command(args, label, kind, keys, profile)
        (args.output / (label + "-command.json")).write_text(
            json.dumps(cmd, indent=2) + "\n"
        )
        print("Starting " + label, flush=True)
        started = time.time()
        with (args.output / (label + ".log")).open("w") as log:
            result = subprocess.run(
                cmd, cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT
            )
        entry = {
            "label": label,
            "exit_code": result.returncode,
            "elapsed_seconds": time.time() - started,
            "profile_timing_excluded": profile,
        }
        text = (args.output / (label + ".log")).read_text()
        if result.returncode == 0:
            entry["results"] = summaries(text, 3 if profile else args.runs)
            if any(item["key_type"] != kind for item in entry["results"]):
                raise ValueError("fixture executed the wrong key type")
            for item in entry["results"]:
                print(json.dumps(item), flush=True)
        (report["profiles"] if profile else report["measurements"]).append(entry)
        (args.output / "results.json").write_text(json.dumps(report, indent=2) + "\n")
        if result.returncode:
            raise SystemExit(result.returncode)
        if profile:
            if not (args.output / (label + ".jfr")).is_file():
                raise ValueError("profiler produced no JFR")
    print(
        "Complete; results record measurements, not acceleration admission.", flush=True
    )


if __name__ == "__main__":
    main()
