# Upstream SQL inventory

[Open the searchable inventory](report.html). Download the per-line CSV files below.

The primary label answers whether the actual query passes StreamFusion's all-or-nothing gate. Every interior operator must run natively; rowwise source/sink boundaries and their transposes are allowed. One unsupported interior operator sends the entire query to Flink. Labels require passing upstream assertions and describe admission, not speedup or per-query runtime work. All in-scope streaming SQL gaps are targets.

| Flink | Passing cases | Accelerated | Should be accelerated | Not accelerated |
|---|---:|---:|---:|---:|
| [1.18](flink-1.18.csv) | 6,014 | 1,430 | 1,861 | 2,723 |
| [2.2](flink-2.2.csv) | 9,155 | 3,280 | 2,864 | 3,011 |

Each row remains one test invocation. Expand **Query plans** in the report or download the [Flink 1.18 query-plan CSV](flink-1.18-queries.csv) and [Flink 2.2 query-plan CSV](flink-2.2-queries.csv) for individual root verdicts. A test with both admitted and fallback queries remains a coverage target. Source/constant-only roots have no interior SQL computation. Observed roots can repeat during planning and are not counts of distinct SQL strings or completed executions; the existing observations do not associate SQL strings or individual translation failures with roots. Expected translation-only failures receive no admission credit.

## Suites

Use the suite filter to inspect runtime and connector corpora separately. The primary label always concerns whole-query admission. `connector_label`, `connector_category` and `connector_note` retain the separate connector I/O targets; `sql_label` is an alias of the primary label. A host source or sink at an allowed perimeter does not make a fully admitted SQL query partial. Direct Java format tests and DataStream/legacy DataSet programs bypass SQL admission.

| Suite | Flink | Passed | Accelerated | Should be accelerated | Not accelerated | Skipped | Failed |
|---|---|---:|---:|---:|---:|---:|---:|
| delta | 2.2 | 4 | 2 | 1 | 1 | 0 | 0 |
| formats | 1.18 | 158 | 19 | 2 | 137 | 10 | 0 |
| formats | 2.2 | 175 | 19 | 2 | 154 | 10 | 0 |
| kafka | 1.18 | 72 | 51 | 21 | 0 | 0 | 0 |
| kafka | 2.2 | 86 | 65 | 21 | 0 | 0 | 0 |
| orc | 1.18 | 44 | 3 | 0 | 41 | 0 | 0 |
| orc | 2.2 | 46 | 3 | 0 | 43 | 0 | 0 |
| paimon | 1.18 | 71 | 12 | 1 | 58 | 4 | 0 |
| paimon | 2.2 | 265 | 95 | 18 | 152 | 0 | 0 |
| parquet | 1.18 | 8 | 3 | 3 | 2 | 0 | 0 |
| parquet | 2.2 | 8 | 6 | 0 | 2 | 0 | 0 |
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
| join | 100 | 91 |
| table-function | 60 | 75 |
| async-and-model-functions | 0 | 71 |
| pattern-matching | 24 | 24 |
| watermark | 24 | 24 |
| connector-boundary | 19 | 27 |
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
| source-or-constant-only | 145 | 148 |
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

CSV exports escape NUL and unpaired Unicode surrogates as `\uXXXX`; the compressed JSON preserves the exact strings. The JSON also contains observed SQL, original/final plan operators, fallback reasons, planner modes and translation errors. `query_verdicts` checks each final root for native computation with no remaining host interior operators. Fallback reasons are recorded per optimizer call and can cover several roots; they are not attributed to a specific SQL string.

Native source/sink/format implementation is an additional connector optimization, separate from whole-query SQL admission. Existing native-work contracts remain separate evidence.

See [the upstream suite](../upstream-flink-suite.md#complete-sql-invocation-inventory) for reproduction, evidence semantics and the separate native-work contracts. Follow-up coverage accounting is tracked in [#168](https://github.com/datafusion-contrib/StreamFusion/issues/168).
