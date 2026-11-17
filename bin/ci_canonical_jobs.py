"""Run a canonical shard in measured, sequential lanes over one prepared reactor."""

import argparse
from contextlib import ExitStack
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess

from ci_runtime_test_shards import load_durations, partition_workers
from ci_canonical_coverage import audit, costs as coverage_costs, load_inventory


def selections(includes, workers, line, inventory=None, coverage="full"):
    manifest = json.loads(includes.with_suffix(".json").read_text())
    names = [row.removesuffix(".java") for row in includes.read_text().splitlines() if row]
    if manifest["suite"] != "canonical" or manifest["flink_line"] != line:
        raise ValueError("Canonical selection does not match the requested Flink line")
    if names != manifest["classes"]:
        raise ValueError("Canonical includes and inventory manifest differ")
    costs = load_durations(line)
    missing = [name for name in names
               if name.startswith("tech/streamfusion/Canonical") and name not in costs]
    if missing:
        raise ValueError("Canonical classes need measured or estimated scheduling costs: "
                         + ", ".join(missing))
    if inventory:
        data = load_inventory(inventory)
        if (data["line"] != line or manifest.get("coverage") != coverage
                or manifest.get("coverage_inventory_sha256") != hashlib.sha256(inventory.read_bytes()).hexdigest()):
            raise ValueError("Canonical worker coverage differs from its inventory")
        costs = coverage_costs(costs, data, coverage)
    lanes = partition_workers(names, workers, costs)
    directory = includes.with_suffix(".workers")
    directory.mkdir(exist_ok=False)
    entries = []
    for index, classes in enumerate(lanes, 1):
        path = directory / f"worker-{index}.includes"
        path.write_text("".join(name + ".java\n" for name in classes))
        entries.append({"worker": index, "classes": classes,
                        "estimated_seconds": sum(costs.get(name, 1.0) for name in classes)})
    (directory / "selection.json").write_text(json.dumps({
        "shard": manifest["shard"], "flink_line": line,
        "inventory_sha256": manifest["inventory_sha256"], "workers": entries,
    }, indent=2) + "\n")
    return manifest["shard"], directory, entries


def prepare_command(profiles, shard, workers, jvm_args, extra):
    return ["mvn", "-B", "-ntp", "-P" + profiles, "-pl", "streamfusion-runtime", "-am",
            "install", "-Dnative.build.skip=true", "-Dsf.protobuf.checkStaleness=true",
            "-Dsf.ci.precompiledJava=true", "-Dmaven.javadoc.skip=true",
            "-DskipTests=" + str(shard != 1).lower(), "-Dsf.runtime.tests.skip=true",
            "-Dsf.testForks=" + str(workers), "-Dsf.testReuseForks=false",
            "-Dsf.extraJvmArgs=" + jvm_args, *extra]


def worker_command(profiles, includes, jvm_args, extra):
    return ["mvn", "-B", "-ntp", "-P" + profiles, "-pl", "streamfusion-runtime",
            "surefire:test@default-test", "surefire:test@orc-tests",
            "-Dsf.runtime.tests.skip=false", "-Dsf.testForks=1", "-Dsf.testReuseForks=false",
            "-DtempDir=surefire-" + includes.stem,
            "-Dsf.runtime.testIncludesFile=" + str(includes),
            "-Dsf.extraJvmArgs=" + jvm_args, *extra]


def run(root, includes, profiles, line, workers, jvm_args, extra=(), inventory=None, coverage="full"):
    shard, directory, entries = selections(includes, workers, line, inventory, coverage)
    if inventory:
        extra = [*extra, "-Dsf.canonical.coverage=" + coverage]
    subprocess.run(prepare_command(profiles, shard, workers, jvm_args, extra),
                   cwd=root, check=True)
    processes = []
    with ExitStack() as stack:
        try:
            for entry in entries:
                index = entry["worker"]
                log = directory / f"worker-{index}.log"
                output = stack.enter_context(log.open("w"))
                command = worker_command(profiles, directory / f"worker-{index}.includes",
                                         jvm_args, extra)
                process = subprocess.Popen(command, cwd=root, stdout=output,
                                           stderr=subprocess.STDOUT, start_new_session=True)
                processes.append((index, process, log))
                print(f"Worker {index}: {len(entry['classes'])} classes, "
                      f"{entry['estimated_seconds']:.1f} estimated seconds", flush=True)
            outcomes = [(index, process.wait(), log) for index, process, log in processes]
        finally:
            for _, process, _ in processes:
                if process.poll() is None:
                    try:
                        os.killpg(process.pid, signal.SIGTERM)
                    except ProcessLookupError:
                        pass
            for _, process, _ in processes:
                if process.poll() is None:
                    try:
                        process.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        try:
                            os.killpg(process.pid, signal.SIGKILL)
                        except ProcessLookupError:
                            pass
                        process.wait()
    failed = False
    for index, code, log in outcomes:
        print(f"Worker {index}: exit {code}; log {log}", flush=True)
        if code:
            failed = True
            print("\n".join(log.read_text(errors="replace").splitlines()[-80:]), flush=True)
    if not failed and inventory:
        result = audit(json.loads(includes.with_suffix(".json").read_text()), inventory,
                       root / "streamfusion-runtime/target/surefire-reports", coverage)
        (directory / "coverage-audit.json").write_text(json.dumps(result, indent=2) + "\n")
    return 1 if failed else 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--includes", type=Path, required=True)
    parser.add_argument("--profiles", required=True)
    parser.add_argument("--line", choices=("1.18", "2.2"), required=True)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--jvm-args", default="-Xmx1g")
    parser.add_argument("--maven-arg", action="append", default=[])
    parser.add_argument("--inventory", type=Path, required=True)
    parser.add_argument("--coverage", choices=("full", "representative"), default="full")
    args = parser.parse_args()
    try:
        return run(args.root.resolve(), args.includes.resolve(), args.profiles, args.line,
                   args.workers, args.jvm_args, args.maven_arg, args.inventory, args.coverage)
    except (OSError, ValueError, subprocess.CalledProcessError) as error:
        parser.exit(1, f"Canonical shard failed: {error}\n")


if __name__ == "__main__":
    raise SystemExit(main())
