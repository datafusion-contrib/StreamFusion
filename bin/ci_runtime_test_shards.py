"""Partition the compiled default Surefire selection without splitting test classes."""

import argparse
import fnmatch
import hashlib
import json
import math
from pathlib import Path


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
        recorded = json.loads(DURATIONS.read_text())
        durations = {**recorded["seconds"],
                     **recorded.get("seconds_by_line", {}).get(line, {})}
    if any(not isinstance(value, (int, float)) or isinstance(value, bool)
           or not math.isfinite(value) or value < 0 for value in durations.values()):
        raise ValueError("Class duration estimates must be finite nonnegative numbers")
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


def write_selection(classes, count, index, output, suite="all", line=None):
    if index not in range(1, count + 1):
        raise ValueError("Shard index must be between 1 and the shard count")
    shards = partition(classes, count, suite, line=line)
    output.write_text("".join(name + ".java\n" for name in shards[index - 1]))
    manifest = {"shard": index, "count": count, "suite": suite, "flink_line": line,
                "classes": shards[index - 1],
                "complete_inventory_sha256": hashlib.sha256(
                    json.dumps(candidates_for_suite(classes, "all")).encode()).hexdigest(),
                "inventory_sha256": hashlib.sha256(
                    json.dumps(sorted(name for shard in shards for name in shard)).encode()).hexdigest(),
                "shard_sizes": list(map(len, shards))}
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
    args = parser.parse_args()
    try:
        write_selection(args.classes, args.count, args.index, args.output, args.suite, args.line)
    except (OSError, ValueError) as error:
        parser.exit(1, f"Runtime test partition failed: {error}\n")


if __name__ == "__main__":
    main()
