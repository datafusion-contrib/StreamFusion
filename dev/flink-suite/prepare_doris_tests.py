#!/usr/bin/env python3
"""Compile released upstream tests against Doris's published, shaded connector API."""

import argparse
import hashlib
import json
from pathlib import Path


TESTS = (
    "source/DorisSourceITCase.java",
    "source/AbstractDorisIncrementalITCase.java",
    "source/DorisIncrementalSourceITCase.java",
    "source/DorisIncrementalSourceFailoverITCase.java",
    "sink/DorisSinkITCase.java",
    "catalog/DorisCatalogITCase.java",
    "lookup/DorisLookupTableITCase.java",
    "table/DorisRowDataJdbcLookupFunctionITCase.java",
)


def prepare(source_root, output_root):
    upstream = source_root / "flink-doris-connector/flink-doris-connector-it/src/test/java"
    manifest = []
    for test in TESTS:
        relative = Path("org/apache/doris/flink") / test
        source = upstream / relative
        original = source.read_text()
        adapted = original
        if test == "table/DorisRowDataJdbcLookupFunctionITCase.java":
            old = "import com.google.common.cache.Cache;"
            if original.count(old) != 1:
                raise ValueError("Expected the pinned upstream cache import exactly once")
            adapted = original.replace(old, "import org.apache.doris.shaded.com.google.common.cache.Cache;")
        output = output_root / relative
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(adapted)
        manifest.append({
            "test": test,
            "source_sha256": hashlib.sha256(original.encode()).hexdigest(),
            "compiled_sha256": hashlib.sha256(adapted.encode()).hexdigest(),
            "adaptation": "published Guava Cache import" if adapted != original else "none",
        })
    (output_root / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    args = parser.parse_args()
    prepare(args.source_root, args.output_root)
