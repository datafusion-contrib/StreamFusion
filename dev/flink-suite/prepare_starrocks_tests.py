#!/usr/bin/env python3
"""Copy the released StarRocks connector's sink ITs, unchanged, for its published connector jar."""

import argparse
import hashlib
import json
from pathlib import Path


TESTS = (
    "it/StarRocksITTestBase.java",
    "it/env/StarRocksTestEnvironment.java",
    "it/sink/StarRocksTableUtils.java",
    "it/sink/StarRocksSinkITTest.java",
    "it/sink/kafka/KafkaTableTestBase.java",
    "it/sink/kafka/KafkaTableTestUtils.java",
    "it/sink/kafka/KafkaToStarRocksITTest.java",
)

# The Debezium change logs the Kafka-to-StarRocks IT replays.
RESOURCES = (
    "data/sink/debezium-upsert-and-delete.txt",
    "data/sink/debezium-update-with-pk-changed.txt",
)


def copy(original_path, output):
    original = original_path.read_bytes()
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_bytes(original)
    return hashlib.sha256(original).hexdigest()


def prepare(source_root, output_root, resource_root):
    manifest = []
    for test in TESTS:
        relative = Path("com/starrocks/connector/flink") / test
        digest = copy(source_root / "src/test/java" / relative, output_root / relative)
        manifest.append({
            "test": test,
            "source_sha256": digest,
            "compiled_sha256": digest,
            "adaptation": "none",
        })
    for resource in RESOURCES:
        digest = copy(source_root / "src/test/resources" / resource, resource_root / resource)
        manifest.append({
            "resource": resource,
            "source_sha256": digest,
            "copied_sha256": digest,
            "adaptation": "none",
        })
    (output_root / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--resource-root", type=Path, required=True)
    args = parser.parse_args()
    prepare(args.source_root, args.output_root, args.resource_root)
