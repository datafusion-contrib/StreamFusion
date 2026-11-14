# TPC-DS with Paimon input and output

The opt-in TPC-DS harness imports all 103 SQL variants for the 99 templates from
Apache Flink release 2.2.1. It runs the SQL unchanged against 24 Paimon input tables
and commits each query's result to a typed Paimon output table. The source manifest
records the immutable upstream commit and SHA-256 of every query and schema.

The released `io.trino.tpcds:tpcds:1.4` generator prepares the inputs through Paimon's
Java SDK outside timing. Its `p_response_targe` column spelling is matched by ordinal
to Flink's `p_response_target`; all 425 column names and types are checked. SF1 is the
comparison scale. Smaller data is useful for auditing but can produce division by
zero in otherwise valid upstream queries; the harness does not rewrite those queries.

Both engines receive identical data, schemas, parallelism, compression and memory
settings. Like Flink's own TPC-DS runner, both enable join reordering and a 10 MiB
broadcast threshold. Catalog statistics use observed row/null counts, integral/date
bounds and string lengths, plus released Flink NDV estimates capped by non-NULL rows.
Decimal bounds are omitted to avoid rounding a decimal interval into an unsafe double
bound. Prepared fixtures are owned, checksummed and reused only when their manifest matches.

The `tpcds` profile uses production JVM assertion settings. In released Flink, an
`IN` predicate over a string column can estimate selectivity above one when the list
has more values than the column's NDV. The planner's Java assertion rejects that
estimate in Q73 under `-ea`; a normal production JVM proceeds. This profile affects
the benchmark module and retains every JUnit schema, result and native-work assertion.
The unchanged upstream qualification suites run with their existing settings.

## Run the suite

Use a release native build and an owned scratch directory:

```sh
SF_TPCDS=true mvn test -Ppaimon,bench,tpcds -pl streamfusion-paimon -am \
  -Dtest=PaimonTpcdsBenchmark -Dsurefire.failIfNoSpecifiedTests=false \
  -Dsf.tpcds.directory=/path/to/owned/tpcds-run \
  -Dsf.tpcds.scale=1 -Dsf.tpcds.mode=compare
```

Modes are `prepare`, `plan`, `audit`, `verify` and `compare`. `audit` records fallbacks;
`verify` and `compare` require native source and compute work in addition to exact
results. `compare` defaults to three warmups and seven measured trials per engine,
alternating engine order. `sf.tpcds.queries` accepts a comma-separated diagnostic
selection; only an empty selection denotes the complete suite. `sf.tpcds.parallelism`
defaults to two and `sf.tpcds.nativeBytes` to 536870912. The legacy adapter shares the
harness through `-Ppaimon1,bench,tpcds` with its matching released Flink line.

For queries that previously fell back for computation, `sf.tpcds.compute=stock`
keeps native Paimon input/output and uses stock computation. Its report labels the
engine `native_io_stock_compute` and asserts that no native compute ran. This is
a controlled ablation on the current engine, including its corrected shared-buffer
ownership, rather than a rebuilt historical release. It includes the row/Arrow
conversions needed between stock computation and native IO. Fully native runs
require zero entry transposes and zero source fallback rows.

Timing starts before SQL submission and ends after the final Paimon commit. Input
preparation, output DDL and stock readback are outside timing. The verifier compares
exact typed result multisets, preserving duplicates, NULLs, decimal scales and floating
bit patterns. It checks committed row counts, native Parquet footers, source counters,
compute counters and stock's absence of native work. Each report records physical
input/output files, splits, partitions and buckets, along with plans and callback lists.
Inputs and outputs are unpartitioned tables; query parallelism and hash exchanges are
recorded separately from table partitioning. Diagnostic `sf.tpcds.retainGauges=true`
retains deployed/planned share counts; regular repeated comparisons retain counters.

The report is rewritten after each query. Output tables are removed after validation;
remove the owned run directory after retaining concise benchmark evidence. This is
an engine comparison using TPC-DS-derived queries, not an audited TPC benchmark result.

## Qualification

CI plans every unchanged variant against generated SF0.01 Paimon fixtures on both
supported Flink lines and the legacy Paimon adapter. These checks require native
source and compute planning; they do not execute the full suite. SQL operator tests
cover runtime edge cases separately. SF1 verification and repeated comparisons
add exact committed results, actual compute counters and output encoder checks.

Full SF1 qualification and repeated comparisons are in progress. The maintained
results will distinguish native computation from generated JVM scalar callbacks and
retain unfavorable measurements alongside gains.

The preliminary comparison was stopped when main adopted compatibility-based,
all-or-nothing batch computation in `918716ab8`. Its partial observations are historical
diagnostics; the final comparison uses a rebuilt runtime based on that policy.
