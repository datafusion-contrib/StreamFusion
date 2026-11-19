# Historical benchmark report schema

Maintained reports keep measurements, trial identities, source/library witnesses, SQL and
reproduction settings in Git. Large historical plan bodies are references to immutable original
reports in the [artifact manifest](artifacts.json). This changes evidence storage, not the
measurement boundary or the outcome of any recorded trial.

## Versions and consumers

Version 1 is the original JSON shape: `plan`, `stockPlan` and `nativePlan` contain strings.
The existing Java TPC-H and recovery harnesses still produce this format during local runs.
They compute route checks from those strings before publishing their report. They do not read
archived reports. New run outputs can be reviewed and archived separately; this migration does
not silently change a harness's output or execute any benchmark.

Version 2 adds `_evidence_schema: {"version": 2, "original": "docs/benchmarks/...json"}`
at the report root and replaces selected full-plan strings with typed references:

```json
{
  "$plan": {
    "path": "docs/benchmarks/embedded-plans-20261119/report.json",
    "pointer": "/queries/0/nativePlan",
    "sha256": "SHA-256 of the complete immutable original report"
  }
}
```

The [machine-readable schema](https://github.com/datafusion-contrib/StreamFusion/blob/main/dev/schemas/benchmark-report-v2.schema.json) describes the
maintained shape. The reader also enforces the semantic contract: each reference must identify
the current field's exact JSON pointer, the report named in the version header and that report's
manifest hash. All original plan fields must be present. Restoring the plans must reproduce the
entire original JSON value, including every structured observation. Unknown versions, missing
references, altered measurements or changed hashes fail; they are never accepted as empty plans.

Use the compatible reader to restore the exact original bytes:

```sh
python3 dev/benchmark_reports.py docs/benchmarks/results/tpch-native-parquet-final-20261110.json \
  --output /tmp/tpch-original.json
```

Python consumers can use `benchmark_reports.load_report(path, destination, cache)`; it accepts
both versions and returns the original string-valued fields. The CLI and docs publisher use
`original_report(...)`, which preserves the original encoding, whitespace and final newline too.
The documentation hook replaces version-2 source downloads with those verified original bytes.
Existing public JSON URLs therefore keep their previous version-1 shape and byte content.
The archived originals are also published directly as evidence, with stable URLs and hashes.

## Migration provenance

The 2026-11-19 bundle preserves 12 complete original reports from `099d27102`, with 466 plan
fields (10 TPC-H reports and two aggregate recovery reports). The 522-byte `explanation` in the
floating-window diagnostic remains ordinary structured prose; matching a substring of a key
is not sufficient to classify it as a plan.

The archive was published, downloaded independently and checked against every per-file SHA-256
and the repository copies before replacing the embedded fields. Every migrated report roundtrips
to byte-identical original JSON. The [evidence index](evidence.md#maintaining-the-evidence) records
the release and retrieval procedure. Publication requires these checks and the reader regression
tests, rather than trusting a local archive or a successful upload alone.
