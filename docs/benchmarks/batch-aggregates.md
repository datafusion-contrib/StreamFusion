# Batch global aggregate comparisons

Global signed integer aggregates now compose with an otherwise qualified
[batch SQL island](../operators/batch.md). Standalone aggregate queries retain
stock Flink. These are complete-job measurements, including the row/Arrow
perimeter; no isolated aggregate-kernel speedup is claimed.

## Admitted compositions

`BatchAggregateJobBenchmark` runs the same SQL and bounded datagen source in both
engines, ending at a stock rowwise blackhole sink:

```sql
INSERT INTO output
SELECT SUM(v), COUNT(v), COUNT(*), MIN(v), MAX(v), AVG(v)
FROM (
  SELECT CASE WHEN MOD(id,17)=0 THEN CAST(NULL AS <type>)
              ELSE CAST(MOD(id*48271,<cardinality>) AS <type>) END AS v
  FROM input WHERE SHA2(CAST(id AS STRING),256) <> ''
) AS values_input
```

The timer starts before `executeSql` and ends after both `await` and the completed
`JobExecutionResult`. It retains planning, submission, scanning, SHA2 evaluation,
JNI, both transposes, local aggregation, blocking Arrow IPC partial-state shuffle,
final aggregation and sink/job completion. Every native trial asserts exactly
`rows + 2` consumed aggregate input rows, including the two partial states; stock
asserts zero. The native plan must contain both transposes, aggregate and exchange.
Previous StreamFusion behavior for these aggregate queries was whole-query stock
fallback, so the stock column is also the previous-behavior baseline.

Release measurements use Flink 2.2.1, DataFusion 54, Arrow Rust 58.4.1/Java 18.3.0,
Temurin 17, mimalloc without optional RocksDB and an 8 GiB Java heap. The shared
local MiniCluster has one TaskManager/eight slots, parallelism-two producers,
a singleton final aggregate, 128 MiB native off-heap authority, 1 GiB managed
memory and 1,024-row batches. Both engines receive the same configuration.
Three warmups and seven retained trials alternate engine order. Times are median
milliseconds [observed range].

| Rows / type / cardinality | Stock and previous | Native | Stock/native |
|---|---:|---:|---:|
| 1,000,000 / BIGINT / 2,147,483,647 | 248.43 [236.20–261.52] | 142.39 [135.21–147.63] | 1.74× |
| 2,000,000 / INT / 64 | 448.72 [420.91–518.33] | 210.65 [197.09–223.51] | 2.13× |

These gains apply to the complete measured SHA2/aggregate pipelines. They do not
establish isolated aggregation throughput, remote-network performance or a general
gain for cheap standalone aggregates. The local host was shared with intermittent
independent test activity; repeated alternating trials reduce but cannot eliminate
contention. Raw trials, net runtimes and the measured library hash remain in the
[benchmark evidence](results/batch-aggregates-20261109.json). The separate
[validation witnesses](results/batch-aggregate-validation-20261109.json) retain
both-line upstream routes, positive admitted SQL work and cleanup/recovery controls.

## Fixed-width extrema compositions

Global MIN/MAX now also admit BOOLEAN, DECIMAL (precision 1–38), DATE and TIME
inside otherwise qualifying pipelines. The query below uses all four types:

```sql
SELECT MIN(b), MAX(b), MIN(d), MAX(d), MIN(dt), MAX(dt), MIN(tm), MAX(tm), COUNT(*)
FROM extrema WHERE SHA2(CAST(id AS STRING),256) <> ''
```

Both engines use the same bounded, rowwise Flink sequence source and deterministic
map. Every seventeenth row has NULL values; other rows include alternating Boolean
and decimal signs, full-width DECIMAL(38,18), dates across years 1–9999 and
millisecond times. Cardinality controls the derived value sequence, not grouping.
The source/map, both perimeter transposes, blocking Arrow IPC partial states and
completed job result remain inside the timer. Resources and 3/7 alternating
warmup/trial method match the integer cohorts above. Prior StreamFusion kept these
queries entirely stock, so stock is also the previous-behavior baseline.

| Rows / value cardinality | Stock and previous | Native | Stock/native |
|---|---:|---:|---:|
| 1,000,000 / 2,147,483,647 | 305.66 [286.49–324.85] | 188.72 [183.51–218.02] | 1.62× |
| 2,000,000 / 64 | 451.21 [445.28–470.56] | 280.41 [277.57–288.07] | 1.61× |

These are complete SHA2/extrema pipeline gains. They establish neither an isolated
extrema-kernel ratio nor remote-network throughput. Standalone extrema retain the
existing stock default. [Raw trials and configuration](results/batch-extrema-20261109.json)
and [both-line native/replay witnesses](results/batch-extrema-validation-20261109.json)
retain the evidence.

## Rejected standalone candidates

The same six-function query without its SHA2 predicate was measured before
restricting admission. Initial one-million-row BIGINT medians were 117.14/107.41 ms
stock/native, but eight-million-row jobs lost: 331.28/387.64 ms (0.85×).

The first optimization removes the sort driver's unnecessary input copy for these
primitive global reductions. DataFusion consumes the imported batch while the
existing Arrow allocation remains charged, retaining only scalar state. The
larger workload still lost at 329.79/369.66 ms (0.89×); these separate cohorts do
not isolate a precise before/after optimization ratio. A second batching diagnostic
used 4,096 rows and measured 337.57/296.48 ms (1.14×), with wide overlapping ranges.
This did not establish a reliable standalone gain or justify changing the default
batch size. Standalone aggregates therefore do not qualify a native island.

A JFR diagnostic found source/dispatch work dominant, with rowwise Arrow writing,
Calc/JNI work and stock generated aggregation visible in samples. Java stacks do
not establish individual Rust subroutine costs. The remaining work is to reduce
perimeter and per-batch costs and establish a representative standalone gain.
[Keyed reductions](batch-grouped-aggregates.md) have separate hash-exchange,
spill/replay and complete-job evidence; they are not covered by these global results.

Those early borrowed-input diagnostics predate the final explicit expression
workspace reservation for DataFusion casts/literals/FILTER copies. The admitted
measurements include that reservation and typed memory denial under the same
shared authority. Diagnostic timers ended after `await`, before fetching `JobExecutionResult`.
An early SHA2 cohort also had an anomalous net-runtime observation; it is marked
superseded and is not used for the final comparison. The final admitted cohorts
include the result future inside timing. All unfavorable diagnostic trials remain
in the evidence; none is silently discarded as startup overhead.

## Reproduction

Build a release DSO and use `SF_BENCHMARK=true`, Maven's `bench` profile and
`-Dtest=BatchAggregateJobBenchmark`. Supply that same release directory through
`native.resource.directory` and `java.library.path` in `sf.extraJvmArgs`.
Parameters are `sf.batch.aggregate.rows`, `.type`, `.cardinality`, `.warmups`,
`.trials`, `.batchRows`, `.sha2` and `.fixedExtrema` (defaults: 1,000,000, BIGINT,
2,147,483,647, 2, 5, 1,024 and true). The reported cohorts override warmups/trials
to 3/7. Set `.fixedExtrema=true` for the typed source/query above (default false);
`.type` then does not change its four fixed types. Use task-scoped runtime temporary directories and clean them afterwards.

Reproducing rejected candidates requires a temporary source-only change making
`BatchPhysicalGroupAggregateBase` qualify `amortizesRowBoundaries`, followed by
`.sha2=false`. Restore the current gate after the experiment. This is not a
production performance opt-in; the unchanged harness deliberately requires native
plan/work evidence and fails if it silently benchmarks stock twice.
