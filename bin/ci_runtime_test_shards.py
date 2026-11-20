"""Partition the compiled default Surefire selection without splitting test classes."""

import argparse
import csv
import fnmatch
import hashlib
import json
import math
from pathlib import Path

from ci_canonical_coverage import costs as coverage_costs, load_inventory


DEFAULT_INCLUDES = ("Test*", "*Test", "*Tests", "*TestCase")
AUDIT_CLASSES = {
    "tech/streamfusion/FlinkPortableSqlAuditTest",
    "tech/streamfusion/StatefulRecoveryMatrixTest",
    "tech/streamfusion/StatefulSqlRescaleTest",
}
FIRST_SHARD_CLASSES = AUDIT_CLASSES | {
    "tech/streamfusion/FlinkRecursiveScalarSqlHarnessTest",
    "tech/streamfusion/FlinkMixedRecursiveScalarSqlHarnessTest",
    "tech/streamfusion/FlinkRecursiveTableFunctionSqlHarnessTest",
    "tech/streamfusion/FlinkNestedBridgeTableFunctionSqlHarnessTest",
}
DURATIONS = Path(__file__).resolve().parents[1] / "dev/ci/runtime-durations.json"
RELOCATIONS = Path(__file__).resolve().parents[1] / "dev/runtime-layout/test-relocations.tsv"


def relocated_costs(values):
    with RELOCATIONS.open() as stream:
        aliases = {row["previous_class"].replace(".", "/"): row["current_class"].replace(".", "/")
                   for row in csv.DictReader(stream, delimiter="\t")}
    result = {name: duration for name, duration in values.items() if name not in aliases}
    for name, duration in values.items():
        if name in aliases:
            result.setdefault(aliases[name], duration)
    return result


def _validate_durations(durations):
    if any(not isinstance(value, (int, float)) or isinstance(value, bool)
           or not math.isfinite(value) or value < 0 for value in durations.values()):
        raise ValueError("Class duration estimates must be finite nonnegative numbers")


def load_durations(line=None):
    recorded = json.loads(DURATIONS.read_text())
    durations = {**relocated_costs(recorded.get("estimated_seconds", {})),
                 **relocated_costs(recorded.get("estimated_seconds_by_line", {}).get(line, {})),
                 **relocated_costs(recorded["seconds"]),
                 **relocated_costs(recorded.get("seconds_by_line", {}).get(line, {}))}
    _validate_durations(durations)
    provisional = relocated_costs(recorded.get("provisional_seconds_by_line", {}).get(line, {}))
    _validate_durations(provisional)
    for name, floor in provisional.items():
        durations[name] = max(durations.get(name, 0), floor)
    return durations


def partition_workers(candidates, count, durations):
    """Balance fixed worker lanes so class launch order cannot extend their total work."""
    if not isinstance(count, int) or isinstance(count, bool) or count < 1:
        raise ValueError("Worker count must be a positive integer")
    candidates = list(candidates)
    if (not candidates or any(not isinstance(name, str) or not name for name in candidates)
            or len(set(candidates)) != len(candidates)):
        raise ValueError("Worker inventory must contain nonempty unique class names")
    _validate_durations(durations)
    workers = [[] for _ in range(min(count, len(candidates)))]
    loads = [0.0] * len(workers)
    def cost(name):
        return max(0.001, durations.get(name, 1.0))
    for name in sorted(candidates, key=lambda name: (-cost(name), name)):
        index = min(range(len(workers)), key=lambda index: (loads[index], len(workers[index]), index))
        workers[index].append(name)
        loads[index] += cost(name)
    return [sorted(worker) for worker in workers]


def candidates_for_suite(classes, suite):
    if suite not in ("all", "runtime", "canonical"):
        raise ValueError("Unknown test suite")
    candidates = sorted(
        path.relative_to(classes).with_suffix("").as_posix()
        for path in classes.rglob("*.class")
        if "$" not in path.name
        and any(fnmatch.fnmatchcase(path.stem, pattern) for pattern in DEFAULT_INCLUDES)
    )
    if suite == "all":
        return candidates
    return [name for name in candidates
            if (name.startswith("tech/streamfusion/Canonical") and name.endswith("JobTest"))
            == (suite == "canonical")]


def partition(classes, count, suite="all", durations=None, line=None):
    if count < 1:
        raise ValueError("Shard count must be positive")
    candidates = candidates_for_suite(classes, suite)
    if not candidates:
        raise ValueError("No compiled Surefire test candidates found")
    if durations is None:
        durations = load_durations(line)
    _validate_durations(durations)
    def cost(name):
        return max(0.001, durations.get(name, 1.0))
    shards = [[] for _ in range(count)]
    loads = [0.0] * count
    ordered = sorted(
        candidates, key=lambda name: (name not in FIRST_SHARD_CLASSES, -cost(name), name))
    for name in ordered:
        index = 0 if name in FIRST_SHARD_CLASSES else min(
            range(count), key=lambda index: (loads[index], len(shards[index]), index))
        shards[index].append(name)
        loads[index] += cost(name)
    if any(not shard for shard in shards):
        raise ValueError("An empty includes file would restore Surefire's default selection")
    return [sorted(shard) for shard in shards]


def write_selection(classes, count, index, output, suite="all", line=None,
                    inventory=None, coverage="full"):
    if index not in range(1, count + 1):
        raise ValueError("Shard index must be between 1 and the shard count")
    durations = load_durations(line)
    if inventory:
        data = load_inventory(inventory)
        if suite != "canonical" or data["line"] != line:
            raise ValueError("Coverage inventory does not match the selected canonical line")
        if set(data["classes"]) != {name.replace("/", ".") for name in candidates_for_suite(classes, suite)}:
            raise ValueError("Coverage inventory differs from the compiled canonical classes")
        durations = coverage_costs(durations, data, coverage)
    shards = partition(classes, count, suite, durations=durations, line=line)
    output.write_text("".join(name + ".java\n" for name in shards[index - 1]))
    manifest = {"shard": index, "count": count, "suite": suite, "flink_line": line,
                "classes": shards[index - 1],
                "complete_inventory_sha256": hashlib.sha256(
                    json.dumps(candidates_for_suite(classes, "all")).encode()).hexdigest(),
                "inventory_sha256": hashlib.sha256(
                    json.dumps(sorted(name for shard in shards for name in shard)).encode()).hexdigest(),
                "shard_sizes": list(map(len, shards))}
    if inventory:
        manifest.update(coverage=coverage,
                        coverage_inventory_sha256=hashlib.sha256(inventory.read_bytes()).hexdigest())
    output.with_suffix(".json").write_text(json.dumps(manifest, indent=2) + "\n")
    print(f"Runtime shard {index}/{count}: {len(shards[index - 1])} classes; "
          f"complete partition sizes: {manifest['shard_sizes']}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--classes", type=Path, required=True)
    parser.add_argument("--count", type=int, required=True)
    parser.add_argument("--index", type=int, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--suite", choices=("all", "runtime", "canonical"), default="all")
    parser.add_argument("--line", choices=("1.18", "2.2"), help="Use this Flink line's recorded costs")
    parser.add_argument("--inventory", type=Path)
    parser.add_argument("--coverage", choices=("full", "representative"), default="full")
    args = parser.parse_args()
    try:
        write_selection(args.classes, args.count, args.index, args.output, args.suite, args.line,
                        args.inventory, args.coverage)
    except (OSError, ValueError) as error:
        parser.exit(1, f"Runtime test partition failed: {error}\n")


if __name__ == "__main__":
    main()
