# Upstream SQL inventory

[Open the searchable inventory](report.html). Download the per-line CSV files below.

This is an observed snapshot, not a performance benchmark. Native labels require an admitted execution plan and passing upstream assertions. Tests with multiple query plans retain remaining fallback gaps. All in-scope streaming SQL gaps are targets, including functionality outside current documented support.

| Flink | Passing cases | Accelerated | Should be accelerated | Not accelerated |
|---|---:|---:|---:|---:|
| [1.18](flink-1.18.csv) | 5,661 | 1,342 | 1,834 | 2,485 |
| [2.2](flink-2.2.csv) | 8,571 | 3,090 | 2,822 | 2,659 |

## Streaming coverage targets

Counts are passing test invocations, including parameter variants; the same missing feature can affect many cases. Categories overlap when a case has several blockers.

| Category | Flink 1.18 | Flink 2.2 |
|---|---:|---:|
| scalar-expression | 442 | 1,694 |
| aggregation | 336 | 514 |
| state-backend | 805 | 0 |
| window | 338 | 440 |
| join | 100 | 91 |
| table-function | 60 | 75 |
| async-and-model-functions | 0 | 71 |
| pattern-matching | 24 | 24 |
| watermark | 21 | 21 |
| rank-and-deduplication | 17 | 23 |
| data-type | 9 | 10 |
| ordering | 8 | 10 |
| changelog | 0 | 4 |

## Outside acceleration coverage

| Reason | Flink 1.18 | Flink 2.2 |
|---|---:|---:|
| batch | 1,620 | 1,694 |
| validation-or-host-error | 368 | 456 |
| api-validation | 203 | 252 |
| source-or-constant-only | 124 | 124 |
| stock-plan-contract | 91 | 23 |
| catalog-or-metadata | 35 | 59 |
| upstream-early-return | 33 | 35 |
| schema-only | 4 | 6 |
| plan-only | 3 | 3 |
| procedure-call | 1 | 4 |
| datastream-bypass | 2 | 2 |
| configuration-only | 1 | 1 |

## Provenance

Both corpora run unchanged release tests with StreamFusion installed. Every passing CSV row joins to one recorded JUnit invocation; skips and failures remain visible in the report but are excluded from the passing-case totals. These are the complete planner runtime corpora, including SQL, Table API, function, batch, catalog and compiled-plan fixtures; connector-module suites are separate.

[Original CI run](https://github.com/datafusion-contrib/StreamFusion/actions/runs/36181880502).

Both upstream runtime test steps passed, as did all 65 Flink 1.18 and 77 Flink 2.2 native/fallback execution contracts. The Flink 2.2 job is red because the initial CSV exporter rejected an unpaired Unicode surrogate in an upstream fixture; the corrected exporter and classifier were rerun against all original XML and JSON observations. Every passing invocation joins exactly once, with no missing or stale evidence. These tool-only commits do not change the engine from b913ce8947582f884356120922ab17f541913d58.

- Flink 1.18: StreamFusion `8fec58b7195acefa277fd293fbf49ea979706826`; 5,686 reported cases; outcomes `{'passed': 5661, 'skipped': 25}`.
- Flink 2.2: StreamFusion `8fec58b7195acefa277fd293fbf49ea979706826`; 8,619 reported cases; outcomes `{'passed': 8571, 'skipped': 48}`.

CSV exports escape unpaired Unicode surrogates as `\uXXXX`; the compressed JSON preserves the exact strings. The JSON also contains observed SQL, original/final plan operators, fallback reasons, planner modes and translation errors. Native/host plan counts describe planning attempts; a passing negative fixture whose translations all fail receives no execution credit.

See [the upstream suite](../upstream-flink-suite.md#complete-sql-invocation-inventory) for reproduction, evidence semantics and the separate native-work contracts. Follow-up coverage accounting is tracked in [#168](https://github.com/datafusion-contrib/StreamFusion/issues/168).
