"""Partition the compiled default Surefire selection without splitting test classes."""

import argparse
import fnmatch
import hashlib
import json
from pathlib import Path


DEFAULT_INCLUDES = ("Test*", "*Test", "*Tests", "*TestCase")
AUDIT_CLASSES = {
    "tech/streamfusion/FlinkPortableSqlAuditTest",
    "tech/streamfusion/StatefulRecoveryMatrixTest",
    "tech/streamfusion/StatefulSqlRescaleTest",
}


def partition(classes, count):
    if count < 1:
        raise ValueError("Shard count must be positive")
    candidates = sorted(
        path.relative_to(classes).with_suffix("").as_posix()
        for path in classes.rglob("*.class")
        if "$" not in path.name
        and any(fnmatch.fnmatchcase(path.stem, pattern) for pattern in DEFAULT_INCLUDES)
    )
    if not candidates:
        raise ValueError("No compiled Surefire test candidates found")
    shards = [[] for _ in range(count)]
    for name in candidates:
        # Keep the three required SQL audit artifacts together in the first shard.
        index = 0 if name in AUDIT_CLASSES else int.from_bytes(
            hashlib.sha256(name.encode("utf-8")).digest(), "big") % count
        shards[index].append(name)
    if any(not shard for shard in shards):
        raise ValueError("An empty includes file would restore Surefire's default selection")
    return shards


def write_selection(classes, count, index, output):
    if index not in range(1, count + 1):
        raise ValueError("Shard index must be between 1 and the shard count")
    shards = partition(classes, count)
    output.write_text("".join(name + ".java\n" for name in shards[index - 1]))
    manifest = {"shard": index, "count": count, "classes": shards[index - 1],
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
    args = parser.parse_args()
    try:
        write_selection(args.classes, args.count, args.index, args.output)
    except (OSError, ValueError) as error:
        parser.exit(1, f"Runtime test partition failed: {error}\n")


if __name__ == "__main__":
    main()
