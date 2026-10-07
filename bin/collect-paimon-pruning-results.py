#!/usr/bin/env python3
"""Archive Paimon pruning fixture observations and inclusive quartile summaries."""

import argparse
import json
import re
import statistics
from pathlib import Path


def parse_log(path, prefix):
    observations = []
    plans = []
    for line in path.read_text().splitlines():
        if line.startswith("PAIMON_PARTITION_SQL_PLAN partitions="):
            plans.append(line.split("partitions=", 1)[1])
        if not line.startswith(prefix + " "):
            continue
        record = {}
        for key, value in re.findall(r"(\w+)=([^ ]+)", line):
            if value in ("true", "false"):
                record[key] = value == "true"
            else:
                record[key] = int(value) if value.isdigit() else value
        if prefix == "PAIMON_PARTITION_SQL" and "partition" not in record:
            record["partition"] = "p00" if record["selective"] else "all"
        observations.append(record)
    if not observations:
        raise ValueError(f"No {prefix} observations in {path}")
    return {"source_log": path.name, "planned_partition_orders": plans, "observations": observations}


def describe(values):
    quartiles = statistics.quantiles(values, n=4, method="inclusive") if len(values) > 1 else values * 3
    return {"count": len(values), "median": statistics.median(values), "min": min(values),
            "max": max(values), "q1": quartiles[0], "q3": quartiles[2],
            "iqr": quartiles[2] - quartiles[0]}


def summarize(records, keys, metrics):
    groups = {}
    for record in records:
        key = tuple(record[name] for name in keys)
        groups.setdefault(key, []).append(record)
    return [dict(zip(keys, key), **{
        metric: describe([record[metric] for record in group]) for metric in metrics})
        for key, group in sorted(groups.items())]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--micro-log", type=Path, required=True)
    parser.add_argument("--sql-before-log", type=Path)
    parser.add_argument("--sql-after-log", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    micro = parse_log(args.micro_log, "PAIMON_PARTITION_PRUNING")
    micro["summary"] = summarize(micro["observations"], ["rows", "payload_bytes", "mode"],
                                 ["read_ns", "splits", "planned_files", "planned_file_bytes",
                                  "decoded_rows", "selected_rows", "checksum", "native_files"])
    result = {
        "schema_version": 1, "baseline_revision": "96f98a32",
        "candidate": "Retained partition equality handoff in audit-performance worktree",
        "environment": {"jdk": "OpenJDK 17", "flink": "2.2.1", "paimon": "2.0.0",
                        "native_build": "release"},
        "methodology": {
            "statistics": "Median, min/max, inclusive interpolated quartiles and IQR; all observations retained.",
            "micro_boundary": "Released scan planning, full consumption of every planned split, file decoding and residual checksum; table generation/commit outside timer.",
            "micro_warmups": 1, "micro_observations_per_shape": 3,
            "production_handoff": "PaimonSourceMatcher translates retained partition equalities through ability schemas; physical/exec nodes pass the predicate to NativePaimonSource, which calls ReadBuilder.withFilter before creating ContinuousFileStoreSource. NATIVE_PRUNED uses that same released ReadBuilder API and production NativePaimonSplitReader, but applies the predicate directly rather than through SQL planning.",
            "sql_warmups_per_shape": 1,
            "sql_command": "SF_PAIMON_PARTITION_SQL_BENCHMARK=true SF_PAIMON_PARTITION_ROWS=262144 SF_PAIMON_PARTITION_REPEATS=5 mvn -Pbench,paimon -pl streamfusion-paimon -am -Dnative.build.skip=true -Dtest=PaimonPartitionSqlBenchmark -Dsurefire.failIfNoSpecifiedTests=false test; add SF_PAIMON_PARTITION_REQUIRE_PRUNING=true for the candidate",
            "sql_boundary": "Job startup, live result-prefix collection, Arrow-to-row output and cancellation; planning/setup/oracle outside timer. Only the all-partitions case collects every initial snapshot row.",
            "limitations": [
                "Micro NATIVE_PRUNED is a direct read-builder comparison, not a before/after SQL planner run.",
                "Planned file bytes are metadata totals, not measured I/O bytes or copied bytes.",
                "Per-reader native_files counts actual native opens in the full-drain fixture; it is not a published aggregate SQL job metric.",
                "Selective SQL cancellation can avoid irrelevant decoding; p00/p31 names do not guarantee scheduling order.",
                "Separate JVM runs, startup and short warmup can dominate SQL ratios.",
                "SQL source transformation and optional explain witness verify admission/handoff, not decoded-row counts."]},
        "micro": micro, "sql": {}}
    for side in ("before", "after"):
        path = getattr(args, f"sql_{side}_log")
        if path is None:
            continue
        run = parse_log(path, "PAIMON_PARTITION_SQL")
        run["summary"] = summarize(run["observations"], ["input_rows", "partition", "native"],
                                   ["job_ns", "selected_rows", "checksum"])
        result["sql"][side] = run
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + "\n")


if __name__ == "__main__":
    main()
