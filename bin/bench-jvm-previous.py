#!/usr/bin/env python3
"""Measure an immutable previous runtime with the current benchmark fixtures."""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import platform
import re
import shutil
import subprocess
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
PREVIOUS = "b965b8bf0c97450f6971f20db0fe6abc6bbac8ae"
FIXTURES = [
    "JvmAggregateBenchmark.java",
    "TableFunctionBenchmark.java",
    "RecoveryExecutionMetrics.java",
]


def git(*args):
    return subprocess.check_output(["git", *args], cwd=ROOT, text=True).strip()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--flink-line", choices=["1.18", "2.2"], required=True)
    parser.add_argument("--worktree", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--smoke", action="store_true")
    parser.add_argument(
        "--native-table-baseline",
        help="Measure only native table functions at this full commit SHA",
    )
    parser.add_argument("--borrowed-string-exit", action="store_true")
    args = parser.parse_args()
    previous = args.native_table_baseline or PREVIOUS
    if not re.fullmatch(r"[0-9a-f]{40}", previous):
        parser.error("previous runtime must be an immutable full commit SHA")
    java = (
        str(Path(os.environ["JAVA_HOME"]) / "bin/java")
        if os.environ.get("JAVA_HOME")
        else "java"
    )
    version = subprocess.run(
        [java, "-version"], check=True, capture_output=True, text=True
    ).stderr
    if not re.search(r'version "17\.', version):
        parser.error("JDK 17 is required; set JAVA_HOME before building or measuring")
    args.worktree = args.worktree.resolve()
    args.output = args.output.resolve()
    args.output.mkdir(parents=True, exist_ok=True)
    if args.worktree.exists():
        parser.error(
            "worktree must be a new path; existing checkouts are never replaced"
        )
    if subprocess.run(
        ["git", "cat-file", "-e", previous], cwd=ROOT, capture_output=True
    ).returncode:
        subprocess.run(
            [
                "git",
                "fetch",
                "https://github.com/datafusion-contrib/StreamFusion.git",
                previous,
            ],
            cwd=ROOT,
            check=True,
        )
    subprocess.run(
        ["git", "worktree", "add", "--detach", str(args.worktree), previous],
        cwd=ROOT,
        check=True,
    )
    inputs = [
        "native",
        "pom.xml",
        "streamfusion-core/pom.xml",
        "streamfusion-runtime/pom.xml",
        ".cargo",
        ":(glob)rust-toolchain*",
    ]
    same_native = not git("diff", previous, "HEAD", "--", *inputs)
    native = args.worktree / "native/target/release"
    library_name = (
        "libstreamfusion.dylib"
        if platform.system() == "Darwin"
        else "libstreamfusion.so"
    )
    if same_native:
        native.mkdir(parents=True, exist_ok=True)
        shutil.copy2(ROOT / "native/target/release" / library_name, native)
    fixture_hashes = {}
    for name in FIXTURES:
        relative = Path("src/test/java/tech/streamfusion") / name
        shutil.copy2(ROOT / relative, args.worktree / relative)
        fixture_hashes[str(relative)] = hashlib.sha256(
            (ROOT / relative).read_bytes()
        ).hexdigest()
    profile = "bench" + (",flink-1.18" if args.flink_line == "1.18" else "")
    base = ["mvn", "-B", "-ntp", "-P" + profile, "-pl", "streamfusion-runtime"]
    report = dict(
        runtime_revision=previous,
        fixture_revision=git("rev-parse", "HEAD"),
        flink_line=args.flink_line,
        java=version,
        fixture_sha256=fixture_hashes,
        fixture_worktree_dirty=bool(
            git("status", "--porcelain", "--", *fixture_hashes)
        ),
        reused_identical_native_inputs=same_native,
        smoke=args.smoke,
        configuration=dict(
            fixture_method="symmetric-preflight-v2",
            cluster_lifetime="per job",
            heap="2 GiB",
            parallelism=1,
            production_ipc=True,
            previous_runtime=(
                "native table functions"
                if args.native_table_baseline
                else "whole-query fallback, zero native operators"
            ),
            borrowed_string_exit=args.borrowed_string_exit,
        ),
        measurements=[],
        admitted=False,
    )
    output = args.output / "results.json"

    def run(label, command):
        (args.output / (label + "-command.json")).write_text(
            json.dumps(command, indent=2) + "\n"
        )
        print("Starting previous runtime: " + label, flush=True)
        with (args.output / (label + ".log")).open("w") as log:
            result = subprocess.run(
                command,
                cwd=args.worktree,
                env=dict(os.environ, SF_BENCHMARK="true"),
                stdout=log,
                stderr=subprocess.STDOUT,
            )
        report["measurements"].append(dict(label=label, exit_code=result.returncode))
        output.write_text(json.dumps(report, indent=2) + "\n")
        result.check_returncode()

    run(
        "build",
        base
        + [
            "-am",
            "-DskipTests",
            "-Dnative.build.skip=" + str(same_native).lower(),
            "-Dnative.resource.directory=" + str(native),
            "test-compile",
        ],
    )
    report["native_sha256"] = hashlib.sha256(
        (native / library_name).read_bytes()
    ).hexdigest()
    subprocess.run(
        ["git", "diff", "--exit-code", "--", "src/main", *inputs],
        cwd=args.worktree,
        check=True,
    )
    report["runtime_source_unchanged"] = True
    spec = importlib.util.spec_from_file_location(
        "aggregate_diagnostics", ROOT / "bin/bench-jvm-aggregates.py"
    )
    aggregate = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(aggregate)
    aggregate.ROOT = args.worktree
    settings = SimpleNamespace(
        output=args.output,
        profile=profile,
        capacity=0,
        borrowed_string_exit=args.borrowed_string_exit,
        prefix_chars=16,
        rows=2000 if args.smoke else 2000000,
        warmup=1 if args.smoke else 2,
        runs=1 if args.smoke else 5,
    )
    report["configuration"].update(
        rows=settings.rows, warmup_pairs=settings.warmup, pairs=settings.runs
    )
    for kind in [] if args.native_table_baseline else ["int", "bigint", "string"]:
        for keys in [16] if args.smoke else [16, 65536, settings.rows]:
            label = f"{kind}-{keys}"
            command = aggregate.command(settings, label, kind, keys)
            command = [
                arg.replace(
                    "-Djvmagg.expectNative=true", "-Djvmagg.expectNative=false"
                ).replace("-Djvmagg.expectCache=true", "-Djvmagg.expectCache=false")
                for arg in command
            ]
            run(label, command)
            summaries = aggregate.summaries(
                (args.output / (label + ".log")).read_text(), settings.runs
            )
            assert len(summaries) == 1 and summaries[0]["key_type"] == kind
            report["measurements"][-1]["results"] = [
                {
                    key.replace("native", "previous"): value
                    for key, value in item.items()
                }
                for item in summaries
            ]
            output.write_text(json.dumps(report, indent=2) + "\n")
    table_cases = [
        ("basic", "split,numeric", settings.rows, 16, 1),
        ("wide", "split", settings.rows, 256, 1),
        ("grouped", "group", min(settings.rows, 500000), 16, 1),
    ]
    if args.native_table_baseline:
        table_cases += [
            ("distinct", "split", settings.rows, 16, settings.rows),
            ("wide-distinct", "split", settings.rows, 256, settings.rows),
        ]
    for label, shape, rows, width, text_cardinality in table_cases:
        run(
            "table-" + label,
            base
            + [
                "-Dnative.build.skip=true",
                "-Dtest=TableFunctionBenchmark",
                "-Dsf.testForks=1",
                "-Dnative.resource.directory=" + str(native),
                "-Dsf.benchmarkJvmArgs=-Xmx2g -Dstreamfusion.exchange.zeroCopyLocal=false",
                f"-Dsf.extraJvmArgs=-Djava.library.path={native} -Dstreamfusion.experimental.borrowedStringExit={str(args.borrowed_string_exit).lower()} -Xlog:gc*,safepoint:file={args.output}/table-{label}-gc.log:time,uptime,level,tags",
                "-Dtablefunc.expectNative="
                + str(bool(args.native_table_baseline)).lower(),
                "-Dtablefunc.cases=" + shape,
                f"-Dtablefunc.rows={rows}",
                f"-Dtablefunc.width={width}",
                f"-Dtablefunc.textCardinality={text_cardinality}",
                "-Dtablefunc.keys=65536",
                f"-Dtablefunc.warmup={settings.warmup}",
                f"-Dtablefunc.runs={settings.runs}",
                "surefire:test@default-test",
            ],
        )
        text = (args.output / ("table-" + label + ".log")).read_text()
        matches = re.findall(
            r"\[table-function\] shape=(\w+).*?stock_trials=(\[[^\]]+\]) native_trials=(\[[^\]]+\])",
            text,
        )
        assert [match[0] for match in matches] == shape.split(
            ","
        ), "missing table-function results"
        report["measurements"][-1]["results"] = []
        for name, stock, previous in matches:
            stock, previous = json.loads(stock), json.loads(previous)
            assert len(stock) == len(previous) == settings.runs
            assert min(stock + previous) > 0
            report["measurements"][-1]["results"].append(
                dict(
                    shape=name,
                    rows=rows,
                    width=width,
                    text_cardinality=text_cardinality,
                    keys=65536,
                    stock_seconds=stock,
                    previous_seconds=previous,
                )
            )
        output.write_text(json.dumps(report, indent=2) + "\n")
    print(
        "Previous runtime complete; raw native_seconds fields describe the recorded previous runtime.",
        flush=True,
    )


if __name__ == "__main__":
    main()
