# Batch scalar comparisons

This local investigation compares complete Flink **batch** jobs with the initial
DataFusion-backed scalar candidate. Current coverage and fallback policy live in
[Batch SQL](../operators/batch.md). The candidate's cheap standalone paths are not
enabled by default after these measurements.

The benchmark uses Flink 2.2.1, JDK 17.0.17, DataFusion 54, Arrow 58.4.1 and an
Apple M4 with ten CPUs and 16 GiB RAM. Rust uses the workspace release profile
(one codegen unit) with mimalloc. Only the engine library was built, without the
optional RocksDB state backend, because these stateless jobs never use it. The
same native library and resource limits apply to all native trials.

Each job generates the same deterministic BIGINT sequence with Flink's rowwise
datagen source and writes to a rowwise blackhole sink at parallelism one. The
native plan must contain both transposes and actual native input work. Timing
includes SQL planning, submission, startup, source generation, JNI, conversions,
transport and sink completion. Table setup and EXPLAIN witnesses are outside
timing. Two warmups precede alternating engine order; every measured trial is
retained. The previous StreamFusion batch behavior is stock Flink, so the stock
control is also the previous-behavior control.

[All trial sets and source fingerprints](results/batch-scalars-20261109.json)
retain unrestricted candidates, the batch-size experiment, profiling diagnostic,
final admission controls and accepted UDF costs separately.

## Initial candidate

At two million rows, five measured trials with 1,024-row Arrow batches produced:

| Query | Stock median ms (min–max) | Candidate median ms (min–max) | Stock / candidate |
|---|---:|---:|---:|
| Arithmetic and MOD filter | 228.7 (211.2–304.2) | 313.0 (280.1–330.0) | 0.73× |
| SHA2(CONCAT(CAST(id AS STRING), payload), 256) | 1,278.0 (1,273.4–1,304.9) | 901.5 (860.2–936.5) | 1.42× |
| REGEXP_REPLACE over a constructed string | 1,006.7 (985.2–1,417.1) | 1,276.4 (1,210.8–1,320.4) | 0.79× |

At 262,144 rows, three trials gave corresponding ratios of 0.94×, 1.34× and
0.84×. These smaller jobs include substantial startup cost.

A separate 8,192-row-batch experiment did not remove the arithmetic or regex
deficit. Its stock controls slowed substantially as well, so it cannot establish
a before/after performance effect for changing batch size. Within that run,
SHA-256 still improved and the other two workloads still lost.

After removing streaming flush-timer registration from bounded transposes, a
ten-million-row arithmetic control still lost: stock median **892.0 ms** versus
candidate **1,040.1 ms**, with three trials after two warmups. Stock ranges were
834.1–919.9 ms and candidate ranges 999.2–1,155.9 ms. Median Flink-reported job
runtime was 878 versus 1,026 ms, so planning/startup alone does not explain the
regression. These results motivated stock fallback for unqualified scalar-only
paths rather than a default acceleration claim.

A paired arithmetic JFR run found substantial datagen, boxed/row allocations and
Flink transport activity around the native work. Java sampling does not resolve
Rust internals, and these samples do not establish that conversion alone causes
the deficit. Timer removal was not isolated in a controlled speedup experiment.

## Reproduction

With the final admission policy, the two-million-row SHA-256 repeat measured stock
median **1,334.4 ms** (1,287.3–1,438.3) against native **896.3 ms**
(873.0–928.9), **1.49×** stock/native. Both cheap controls used stock fallback and
recorded zero native work. This repeat checks the final admitted route; it is not
a measurement of the batch-size or timer changes in isolation.

A later final-source SHA-256 repeat retained wider variation: stock median
1,663.1 ms (1,364.4–2,121.7), enabled median 1,034.4 ms (850.0–1,059.6),
1.61× stock/enabled. Both repeats show a gain; their different stock controls do
not establish an improvement between native revisions.

The scalar-UDF exception was measured separately with a nullable BIGINT increment
function at 262,144 rows: stock median **120.7 ms** (105.9–140.1) and native-island
median **149.2 ms** (133.8–178.0), five trials after two warmups. The admitted UDF
path is about 24% slower here and is supported under the explicit UDF exception;
it is not an acceleration claim.

Build the release native library with the Maven `bench` profile, then run
`BatchSqlBenchmark` in `streamfusion-runtime` with the same profile. It accepts
`sf.batch.bench.rows`, `sf.batch.bench.warmups`, `sf.batch.bench.trials` and
`sf.batch.bench.query` (`arithmetic`, `hash`, `strings`, `udf`, or `all`) as JVM properties.
Pass them through `sf.extraJvmArgs`. Each printed `BATCH_BENCH` line retains all
trials, actual admission route and Flink-reported job runtimes. Current rejected
queries compare stock against the enabled planner's stock fallback; they do not
recreate the historical unrestricted candidate.

Optional `sf.batch.bench.profile` names a local output prefix for an operation-only
JFR recording. Profiling is separate from authoritative unprofiled measurements.
