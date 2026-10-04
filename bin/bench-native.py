#!/usr/bin/env python3
"""Discover and run release-mode Criterion suites with allocation diagnostics."""
import argparse
import csv
import datetime
import json
import os
from pathlib import Path
import platform
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "native" / "Cargo.toml"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--list", action="store_true", help="List suites without compiling")
    parser.add_argument("--smoke", action="store_true", help="Run fixtures once using Criterion --test")
    parser.add_argument("--package", action="append", default=[], help="Select a workspace package (repeatable)")
    parser.add_argument("--bench", action="append", default=[], help="Select a suite (repeatable)")
    parser.add_argument("--filter", help="Criterion case filter")
    parser.add_argument("--output", type=Path, help="Artifact directory; defaults under native/target")
    baseline = parser.add_mutually_exclusive_group()
    baseline.add_argument("--save-baseline")
    baseline.add_argument("--baseline")
    args = parser.parse_args()
    if args.smoke and (args.save_baseline or args.baseline):
        parser.error("Smoke checks do not produce timing baselines")
    metadata = json.loads(subprocess.check_output([
        "cargo", "metadata", "--manifest-path", str(MANIFEST), "--locked", "--no-deps", "--format-version", "1"
    ], cwd=ROOT, text=True))
    suites = sorted((package["name"], target["name"]) for package in metadata["packages"]
                    if package["id"] in metadata["workspace_members"]
                    for target in package["targets"] if "bench" in target["kind"])
    packages = {package for package, _ in suites}
    benches = {bench for _, bench in suites}
    for unknown in set(args.package) - packages:
        parser.error(f"No benchmark package named {unknown}")
    for unknown in set(args.bench) - benches:
        parser.error(f"No benchmark suite named {unknown}")
    selected = [(package, bench) for package, bench in suites
                if (not args.package or package in args.package) and (not args.bench or bench in args.bench)]
    if not selected:
        parser.error("No suites match the selection")
    if args.list:
        for package, bench in selected:
            print(f"{package}\t{bench}")
        return 0
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
    output = args.output or Path(metadata["target_directory"]) / "native-benchmarks" / stamp
    output.mkdir(parents=True, exist_ok=True)
    details = {
        "timestamp_utc": stamp, "commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
        "dirty": bool(subprocess.check_output(["git", "status", "--porcelain"], cwd=ROOT, text=True)),
        "rustc": subprocess.check_output(["rustc", "-Vv"], text=True), "platform": platform.platform(),
        "cpu_count": os.cpu_count(), "mode": "smoke" if args.smoke else "timing", "suites": selected,
        "filter": args.filter, "save_baseline": args.save_baseline, "baseline": args.baseline,
        "commands": [], "environment": {key: os.environ[key] for key in ("RUSTFLAGS", "CARGO_BUILD_JOBS", "JAVA_HOME") if key in os.environ},
    }
    environment = os.environ.copy()
    if any(bench in {"scalar_registry", "jvm_truncate", "parquet_host_reader"} for _, bench in selected):
        classpath = environment.get("SF_NATIVE_BENCH_CLASSPATH")
        if not classpath:
            classpath_file = (output / "java-classpath.txt").resolve()
            java_phase = "test-compile" if any(bench in {"jvm_truncate", "parquet_host_reader"} for _, bench in selected) else "compile"
            command = ["mvn", "-B", "-ntp", "-pl", "streamfusion-runtime", "-am", java_phase,
                       "org.apache.maven.plugins:maven-dependency-plugin:3.7.0:build-classpath",
                       "-Dnative.build.skip=true", f"-Dmdep.outputFile={classpath_file}"]
            details["java_setup_command"] = command
            print("Preparing JVM benchmark classes; see java-build.log", flush=True)
            with (output / "java-build.log").open("w") as log:
                subprocess.run(command, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT, check=True)
            classpath = os.pathsep.join([str(ROOT / "streamfusion-runtime" / "target" / "classes"),
                                       str(ROOT / "streamfusion-runtime" / "target" / "test-classes"),
                                       classpath_file.read_text().strip()])
            environment["SF_NATIVE_BENCH_CLASSPATH"] = classpath
        details["java_classpath"] = classpath
        details["java_version"] = subprocess.check_output(["java", "-version"], stderr=subprocess.STDOUT, text=True)
    with (output / "allocations.csv").open("w", newline="") as allocation_file:
        writer = csv.writer(allocation_file, lineterminator="\n")
        writer.writerow(["package", "suite", "case", "allocation_calls", "requested_bytes", "shared_output_buffer_bytes", "new_output_buffer_bytes"])
        for package, bench in selected:
            command = ["cargo", "bench", "--manifest-path", str(MANIFEST), "--locked", "-p", package, "--bench", bench, "--"]
            if args.filter:
                command.append(args.filter)
            if args.smoke:
                command.append("--test")
            for option in ("save_baseline", "baseline"):
                if getattr(args, option):
                    command.extend(["--" + option.replace("_", "-"), getattr(args, option)])
            details["commands"].append(command)
            (output / "metadata.json").write_text(json.dumps(details, indent=2) + "\n")
            print(f"Running {package}/{bench}; log: {output / (package + '-' + bench + '.log')}", flush=True)
            with (output / f"{package}-{bench}.log").open("w") as log:
                process = subprocess.Popen(command, cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, env=environment)
                for line in process.stdout:
                    log.write(line)
                    log.flush()
                    if line.startswith("NATIVE_ALLOCATION,"):
                        record = next(csv.reader([line.rstrip()]))[1:]
                        if len(record) != 5:
                            process.terminate()
                            raise ValueError(f"Malformed allocation record: {line}")
                        writer.writerow([package, bench, *record])
                        allocation_file.flush()
                result = process.wait()
            if result:
                print(f"Suite failed ({result}); see {log.name}", file=sys.stderr)
                return result
    print(f"Artifacts: {output}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
