# Upstream SQL inventory

[Open the searchable inventory](report.html). Download the per-line CSV files below.

This is an observed snapshot, not a performance benchmark. Native labels require an admitted execution plan and passing upstream assertions. Tests with multiple query plans retain remaining fallback gaps. All in-scope streaming SQL gaps are targets, including functionality outside current documented support.

| Flink | Passing cases | Accelerated | Should be accelerated | Not accelerated |
|---|---:|---:|---:|---:|
| [1.18](flink-1.18.csv) | 6,014 | 1,358 | 1,952 | 2,704 |
| [2.2](flink-2.2.csv) | 9,155 | 3,162 | 3,002 | 2,991 |

## Suites

Use the suite filter to inspect the original runtime corpus separately from the selected connector/format corpora. Connector labels include retained streaming source/sink gaps; `sql_label` preserves the SQL-only result and `native_components` records what did accelerate. Direct Java format tests and DataStream/legacy DataSet programs bypass SQL admission and are classified separately.

| Suite | Flink | Passed | Accelerated | Should be accelerated | Not accelerated | Skipped | Failed |
|---|---|---:|---:|---:|---:|---:|---:|
| delta | 2.2 | 4 | 2 | 1 | 1 | 0 | 0 |
| formats | 1.18 | 158 | 0 | 40 | 118 | 10 | 0 |
| formats | 2.2 | 175 | 0 | 40 | 135 | 10 | 0 |
| kafka | 1.18 | 72 | 12 | 60 | 0 | 0 | 0 |
| kafka | 2.2 | 86 | 17 | 69 | 0 | 0 | 0 |
| orc | 1.18 | 44 | 0 | 3 | 41 | 0 | 0 |
| orc | 2.2 | 46 | 0 | 3 | 43 | 0 | 0 |
| paimon | 1.18 | 71 | 4 | 9 | 58 | 4 | 0 |
| paimon | 2.2 | 265 | 53 | 61 | 151 | 0 | 0 |
| parquet | 1.18 | 8 | 0 | 6 | 2 | 0 | 0 |
| parquet | 2.2 | 8 | 0 | 6 | 2 | 0 | 0 |
| runtime | 1.18 | 5,661 | 1,342 | 1,834 | 2,485 | 25 | 0 |
| runtime | 2.2 | 8,571 | 3,090 | 2,822 | 2,659 | 48 | 0 |

## Streaming coverage targets

Counts are passing test invocations, including parameter variants; the same missing feature can affect many cases. Categories overlap when a case has several blockers.

| Category | Flink 1.18 | Flink 2.2 |
|---|---:|---:|
| scalar-expression | 451 | 1,704 |
| aggregation | 336 | 514 |
| state-backend | 805 | 0 |
| window | 341 | 443 |
| connector-boundary | 116 | 178 |
| join | 100 | 91 |
| table-function | 60 | 75 |
| async-and-model-functions | 0 | 71 |
| watermark | 24 | 24 |
| pattern-matching | 24 | 24 |
| rank-and-deduplication | 17 | 23 |
| data-type | 12 | 19 |
| ordering | 8 | 10 |
| changelog | 0 | 9 |

## Outside acceleration coverage

| Reason | Flink 1.18 | Flink 2.2 |
|---|---:|---:|
| batch | 1,780 | 1,968 |
| validation-or-host-error | 378 | 470 |
| api-validation | 203 | 252 |
| source-or-constant-only | 126 | 128 |
| catalog-or-metadata | 53 | 76 |
| stock-plan-contract | 91 | 23 |
| upstream-early-return | 33 | 35 |
| non-sql-program | 17 | 17 |
| procedure-call | 12 | 4 |
| schema-only | 4 | 6 |
| plan-only | 3 | 4 |
| format-api | 1 | 5 |
| datastream-bypass | 2 | 2 |
| configuration-only | 1 | 1 |

## Provenance

These corpora run unchanged release tests with StreamFusion installed. Every passing CSV row joins to one recorded JUnit invocation; skips and failures remain visible in the report but are excluded from passing-case totals. The suite column identifies the selected corpus; runtime includes SQL, Table API, function, batch, catalog and compiled-plan fixtures.

[Original CI run](https://github.com/datafusion-contrib/StreamFusion/actions/runs/36181880502).

[Original CI run](https://github.com/datafusion-contrib/StreamFusion/actions/runs/36193571368).

Both upstream runtime test steps passed, as did all 65 Flink 1.18 and 77 Flink 2.2 native/fallback execution contracts. The Flink 2.2 runtime job is red because the initial CSV exporter rejected an unpaired Unicode surrogate in an upstream fixture; the corrected exporter and classifier were rerun against all original XML and JSON observations. All 11 connector/format jobs passed. Every passing invocation joins exactly once, with no missing or stale evidence. Runtime observations were collected at 8fec58b7195acefa277fd293fbf49ea979706826 and connector observations at baed8451b6fbb7c358b7cd068ab387a4e198d9cb; both use the unchanged engine from b913ce8947582f884356120922ab17f541913d58. The published labels apply the final classifier in this snapshot. The separate state-backend suite and legacy Delta 1.18 host audit are outside this inventory.

- Flink 1.18: StreamFusion revisions `8fec58b7195acefa277fd293fbf49ea979706826, baed8451b6fbb7c358b7cd068ab387a4e198d9cb`; 6,053 reported cases; outcomes `{'passed': 6014, 'skipped': 39}`.
- Flink 2.2: StreamFusion revisions `8fec58b7195acefa277fd293fbf49ea979706826, baed8451b6fbb7c358b7cd068ab387a4e198d9cb`; 9,213 reported cases; outcomes `{'passed': 9155, 'skipped': 58}`.

CSV exports escape NUL and unpaired Unicode surrogates as `\uXXXX`; the compressed JSON preserves the exact strings. The JSON also contains observed SQL, original/final plan operators, fallback reasons, planner modes and translation errors. Native/host plan counts describe planning attempts; a passing negative fixture whose translations all fail receives no execution credit.

Native SQL admission does not imply that every source, sink or format accelerated. Inspect admitted native components and final plan operators for connector boundaries. Existing native-work contracts remain separate evidence.

See [the upstream suite](../upstream-flink-suite.md#complete-sql-invocation-inventory) for reproduction, evidence semantics and the separate native-work contracts. Follow-up coverage accounting is tracked in [#168](https://github.com/datafusion-contrib/StreamFusion/issues/168).
