#!/usr/bin/env python3
"""Compare nested scalar jobs with stock Flink and an immutable previous runtime."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import shlex
import shutil
import statistics
import subprocess

ROOT = Path(__file__).resolve().parents[1]
PREVIOUS = "b170a474d5658330ed7e92e9b8bcc54117c8d287"
FIXTURES = [
    "NestedScalarBenchmark.java",
    "FlinkNestedScalarSqlHarnessTest.java",
    "RecoveryExecutionMetrics.java",
]
RESULT = re.compile(
    r"\[nested-scalar\] shape=(\w+) rows=(\d+) width=(\d+) "
    r"stock=([\d.]+) native=([\d.]+) stock_trials=(\[[^\]]+\])"
    r" native_trials=(\[[^\]]+\])"
)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--flink-line", choices=["2.2", "1.18"], required=True)
    parser.add_argument("--previous-worktree", type=Path, required=True)
    parser.add_argument("--native-directory", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--profiler-agent", type=Path)
    parser.add_argument(
        "--cargo-target-directory", type=Path, default=ROOT / "native/target"
    )
    parser.add_argument("--smoke", action="store_true")
    args = parser.parse_args()
    args.output = args.output.resolve()
    args.native_directory = args.native_directory.resolve()
    previous = args.previous_worktree.resolve()
    if previous.exists():
        parser.error("previous worktree must be a new path")
    java = (
        str(Path(os.environ["JAVA_HOME"]) / "bin/java")
        if os.environ.get("JAVA_HOME")
        else "java"
    )
    version = subprocess.run(
        [java, "-version"], check=True, capture_output=True, text=True
    ).stderr
    if not re.search(r'version "17\.', version):
        parser.error("JDK 17 is required")
    library = (
        "libstreamfusion.dylib"
        if platform.system() == "Darwin"
        else "libstreamfusion.so"
    )
    if not (args.native_directory / library).is_file():
        parser.error("build the current release library before running")
    if args.profiler_agent:
        args.profiler_agent = args.profiler_agent.resolve()
        if not args.profiler_agent.is_file():
            parser.error("profiler agent is absent")
    args.output.mkdir(parents=True, exist_ok=True)
    profile = "bench" + (",flink-1.18" if args.flink_line == "1.18" else "")
    base = ["mvn", "-B", "-ntp", "-P" + profile, "-pl", "streamfusion-runtime"]
    rows, warmups, runs = (2000, 1, 1) if args.smoke else (2000000, 2, 5)
    report = dict(
        candidate_revision=subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
        ).strip(),
        previous_revision=PREVIOUS,
        flink_line=args.flink_line,
        java=version,
        smoke=args.smoke,
        admitted=False,
        configuration=dict(
            rows=rows,
            warmup_pairs=warmups,
            measured_pairs=runs,
            heap="2 GiB",
            parallelism=1,
            production_ipc=True,
            both_transposes=True,
        ),
        fixtures={},
        runtimes={},
        commands=[],
        limitations=[
            "Profile durations and smoke durations are not performance evidence.",
            "Each runtime has its own stock controls; cross-JVM drift prevents assigning all changes to the implementation.",
        ],
    )
    output = args.output / "results.json"

    def save():
        output.write_text(json.dumps(report, indent=2) + "\n")

    def run(label, command, worktree, env=None):
        record = dict(label=label, command=command, working_directory=str(worktree))
        report["commands"].append(record)
        save()
        print("Starting " + label, flush=True)
        with (args.output / (label + ".log")).open("w") as log:
            result = subprocess.run(
                command, cwd=worktree, env=env, stdout=log, stderr=subprocess.STDOUT
            )
        record["exit_code"] = result.returncode
        save()
        result.check_returncode()
        return record

    if subprocess.run(
        ["git", "cat-file", "-e", PREVIOUS], cwd=ROOT, capture_output=True
    ).returncode:
        subprocess.run(
            [
                "git",
                "fetch",
                "https://github.com/datafusion-contrib/StreamFusion.git",
                PREVIOUS,
            ],
            cwd=ROOT,
            check=True,
        )
    subprocess.run(
        ["git", "worktree", "add", "--detach", str(previous), PREVIOUS],
        cwd=ROOT,
        check=True,
    )
    for name in FIXTURES:
        relative = Path("src/test/java/tech/streamfusion") / name
        shutil.copy2(ROOT / relative, previous / relative)
        report["fixtures"][str(relative)] = hashlib.sha256(
            (ROOT / relative).read_bytes()
        ).hexdigest()
    report["fixture_worktree_dirty"] = bool(
        subprocess.check_output(
            ["git", "status", "--porcelain", "--", *report["fixtures"]],
            cwd=ROOT,
            text=True,
        ).strip()
    )
    # A shared Cargo target reuses dependency builds. Pin the candidate before replacing its output.
    candidate_native = args.output / "candidate-native"
    candidate_native.mkdir()
    shutil.copy2(args.native_directory / library, candidate_native / library)
    cargo_target = args.cargo_target_directory.resolve()
    run(
        "previous-native-build",
        [
            "cargo",
            "build",
            "--manifest-path",
            "native/Cargo.toml",
            "--locked",
            "-p",
            "streamfusion",
            "--release",
            "--features",
            "mimalloc",
        ],
        previous,
        dict(os.environ, CARGO_TARGET_DIR=str(cargo_target)),
    )
    previous_native = previous / "native/target/release"
    previous_native.mkdir(parents=True, exist_ok=True)
    shutil.copy2(cargo_target / "release" / library, previous_native / library)
    run(
        "previous-java-build",
        base
        + [
            "-am",
            "-DskipTests",
            "-Dnative.build.skip=true",
            "-Dnative.resource.directory=" + str(previous_native),
            "test-compile",
        ],
        previous,
    )
    subprocess.run(
        [
            "git",
            "diff",
            "--exit-code",
            "--",
            "src/main",
            "native",
            "pom.xml",
            "streamfusion-core/pom.xml",
            "streamfusion-runtime/pom.xml",
        ],
        cwd=previous,
        check=True,
    )
    for name, worktree, native in [
        ("candidate", ROOT, candidate_native),
        ("previous", previous, previous_native),
    ]:
        resource = (
            worktree
            / "streamfusion-runtime/target/classes/tech/streamfusion/native"
            / library
        )
        digest = hashlib.sha256((native / library).read_bytes()).hexdigest()
        assert (
            hashlib.sha256(resource.read_bytes()).hexdigest() == digest
        ), "resource and java.library.path libraries differ"
        report["runtimes"][name] = dict(
            native_sha256=digest,
            production_clean=not subprocess.check_output(
                [
                    "git",
                    "status",
                    "--porcelain",
                    "--",
                    "src/main",
                    "native",
                    "pom.xml",
                    "streamfusion-core/pom.xml",
                    "streamfusion-runtime/pom.xml",
                ],
                cwd=worktree,
                text=True,
            ).strip(),
        )
        assert report["runtimes"][name][
            "production_clean"
        ], "runtime production sources are dirty"
    save()

    def measure(runtime, shape, width, profiled=False):
        worktree, native = (
            (ROOT, candidate_native)
            if runtime == "candidate"
            else (previous, previous_native)
        )
        label = f"{runtime}-{shape}-{width}" + ("-profile" if profiled else "")
        flags = [
            f"-Djava.library.path={native}",
            "-Dstreamfusion.experimental.nestedScalarUdfs=true",
            f"-Xlog:gc*,safepoint:file={args.output / (label + '-gc.log')}:time,uptime,level,tags",
        ]
        if profiled:
            flags.append(
                f"-agentpath:{args.profiler_agent}=start,event=cpu-clock,interval=2ms,cstack=dwarf,alloc=1m,file={args.output / (label + '.jfr')}"
            )
        count = 3 if profiled and not args.smoke else runs
        record = run(
            label,
            base
            + [
                "-Dnative.build.skip=true",
                "-Dtest=NestedScalarBenchmark",
                "-Dsf.testForks=1",
                f"-Dnative.resource.directory={native}",
                "-Dsf.benchmarkJvmArgs=-Xmx2g -Dstreamfusion.exchange.zeroCopyLocal=false",
                "-Dsf.extraJvmArgs=" + " ".join(shlex.quote(flag) for flag in flags),
                "-Dnestedbench.expectNative=" + str(runtime == "candidate").lower(),
                f"-Dnestedbench.cases={shape}",
                f"-Dnestedbench.rows={min(rows, 1000000) if profiled else rows}",
                f"-Dnestedbench.width={width}",
                f"-Dnestedbench.warmup={warmups}",
                f"-Dnestedbench.runs={count}",
                "surefire:test@default-test",
            ],
            worktree,
            dict(os.environ, SF_BENCHMARK="true"),
        )
        matches = list(RESULT.finditer((args.output / (label + ".log")).read_text()))
        assert len(matches) == 1, "missing or duplicate benchmark summary"
        name, actual_rows, actual_width, stock, measured, raw_stock, raw_measured = (
            matches[0].groups()
        )
        assert (name, int(actual_rows), int(actual_width)) == (
            shape,
            min(rows, 1000000) if profiled else rows,
            width,
        )
        trials = [json.loads(raw_stock), json.loads(raw_measured)]
        assert all(len(values) == count and min(values) > 0 for values in trials)
        assert all(
            abs(statistics.median(values) - float(median)) < 0.000001
            for values, median in zip(trials, [stock, measured])
        )
        record.update(
            runtime=runtime,
            shape=shape,
            width=width,
            rows=int(actual_rows),
            stock_seconds=trials[0],
            runtime_seconds=trials[1],
            timing_excluded=profiled or args.smoke,
        )
        if profiled:
            assert (args.output / (label + ".jfr")).stat().st_size > 0
            record["stacks"] = {}
            converter = args.profiler_agent.parent.parent / "bin/jfrconv"
            for kind in ["cpu", "alloc"]:
                stacks = args.output / (label + "-" + kind + ".collapsed")
                subprocess.run(
                    [str(converter), "--" + kind]
                    + (["--total"] if kind == "alloc" else [])
                    + [
                        "-o",
                        "collapsed",
                        str(args.output / (label + ".jfr")),
                        str(stacks),
                    ],
                    check=True,
                )
                content = stacks.read_text()
                assert content.strip(), "profile has no stacks"
                if shape == "rust" and kind == "cpu":
                    assert (
                        "streamfusion::" in content
                    ), "Rust control lacks symbolized native stacks"
                record["stacks"][kind] = dict(
                    file=stacks.name,
                    sha256=hashlib.sha256(stacks.read_bytes()).hexdigest(),
                )
        save()

    # Every build finishes before any measurement. Keep unfavorable trials and separate stock controls.
    for runtime in ["candidate", "previous"]:
        for width in [16, 256]:
            for shape in ["identity", "transform"]:
                measure(runtime, shape, width)
        measure(runtime, "rust", 16)
    if args.profiler_agent:
        for runtime in ["candidate", "previous"]:
            for shape in ["identity", "transform", "rust"]:
                measure(runtime, shape, 256, True)
    print(
        "Nested scalar comparisons retained; admission requires reviewing all trials.",
        flush=True,
    )


if __name__ == "__main__":
    main()
