#!/usr/bin/env python3
"""Export reproducible Criterion before/after estimates and original samples."""

import argparse
import hashlib
import json
from pathlib import Path


def measurement(directory):
    estimates = json.loads((directory / "estimates.json").read_text())
    sample = json.loads((directory / "sample.json").read_text())
    if len(sample["iters"]) != len(sample["times"]) or not sample["iters"]:
        raise ValueError(f"Invalid sample arrays: {directory}")
    return {
        "mean_ns": estimates["mean"],
        "median_ns": estimates["median"],
        "sampling_mode": sample["sampling_mode"],
        "iterations": sample["iters"],
        "total_times_ns": sample["times"],
        "per_iteration_ns": [t / n for t, n in zip(sample["times"], sample["iters"])],
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--criterion", type=Path, default=Path("native/target/criterion"))
    parser.add_argument("--before", default="audit-before")
    parser.add_argument("--after", default="new")
    parser.add_argument("--baseline-revision", help="Revision used to collect the before baseline")
    parser.add_argument("--fixture-revision", help="Revision containing the benchmark fixtures")
    parser.add_argument("--candidate", help="Candidate revision or working-tree label")
    parser.add_argument("--source", nargs="*", type=Path, default=[], help="Repository-relative sources to fingerprint")
    parser.add_argument("--groups", nargs="+", default=["dedup_pending", "interval_selective_probe"])
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    results = []
    for group in args.groups:
        root = args.criterion / group
        baselines = sorted(root.rglob(f"{args.before}/estimates.json"))
        if not baselines:
            raise ValueError(f"No baseline measurements for {group}")
        for estimates in baselines:
            case = estimates.parent.parent
            before = measurement(case / args.before)
            after = measurement(case / args.after)
            results.append({
                "case": case.relative_to(args.criterion).as_posix(),
                "before": before,
                "after": after,
                "mean_speedup": before["mean_ns"]["point_estimate"] / after["mean_ns"]["point_estimate"],
            })
    artifact = {
        "format_version": 1,
        "units": "nanoseconds",
        "before_baseline": args.before,
        "after_baseline": args.after,
        "baseline_revision": args.baseline_revision,
        "fixture_revision": args.fixture_revision,
        "candidate": args.candidate,
        "source_sha256": {source.as_posix(): hashlib.sha256(source.read_bytes()).hexdigest() for source in args.source},
        "speedup_definition": "before mean / after mean; descriptive ratio, not a confidence interval",
        "results": results,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(artifact, indent=2) + "\n")
    print("| Case | Before mean µs (95% CI) | After mean µs (95% CI) | Speedup |")
    print("| --- | ---: | ---: | ---: |")
    for result in results:
        cells = []
        for phase in ("before", "after"):
            mean = result[phase]["mean_ns"]
            ci = mean["confidence_interval"]
            cells.append(f'{mean["point_estimate"] / 1000:.3f} ({ci["lower_bound"] / 1000:.3f}–{ci["upper_bound"] / 1000:.3f})')
        print(f'| {result["case"]} | {cells[0]} | {cells[1]} | {result["mean_speedup"]:.2f}× |')


if __name__ == "__main__":
    main()
