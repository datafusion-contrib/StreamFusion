# Batch sort comparisons

The SQL planner now admits the [documented signed-integer sort scope](../operators/batch.md).
These measurements separate complete SQL jobs from prepared operation timing. Both
use release builds, stock Flink and equal logical input. Previous StreamFusion
behavior for these queries was whole-query stock Flink fallback.

## Complete SQL jobs

`BatchSortJobBenchmark` runs a stock datagen BIGINT sequence through a Calc,
blocking singleton shuffle, two-key sort and stock rowwise blackhole sink:

```sql
INSERT INTO output
SELECT MOD(id * 48271, <cardinality>) AS sort_key, id AS payload
FROM input ORDER BY sort_key, payload
```

The timer starts before `executeSql` and ends after `await`, retaining planning,
submission, scanning, JNI, both perimeter transposes, blocking Arrow IPC shuffle
and sink completion. Every native trial asserts the full input row count at the
native sort and both transposes/exchange in its plan; stock trials assert zero
native sort work. Net job runtime is retained separately in the evidence.

Each local MiniCluster has parallelism-two producers and a singleton sort, eight
slots and 1 GiB managed memory. Stock sort is configured with 128 MiB; the native
TaskManager off-heap authority is also 128 MiB, shared with its Arrow perimeter.
The same configuration is supplied to both environments. Native batches contain
1,024 rows. Each cohort has two warmups and five retained trials, alternating
engine order. Times below are median milliseconds, with the observed range.

| Rows | Key cardinality | Stock/previous behavior | Native complete job | Stock/native |
|---:|---:|---:|---:|---:|
| 1,000,000 | 2,147,483,647 | 379.51 [370.15–488.53] | 261.24 [252.09–278.67] | 1.45× |
| 2,000,000 | 64 | 921.48 [878.50–961.12] | 574.23 [564.06–681.25] | 1.60× |

These are complete-job gains for the measured BIGINT workloads on a local
MiniCluster. They establish neither remote-network throughput nor speedups for
other types, forced spill or tiny jobs. The high-cardinality fixture uses a large
modulus rather than a random-input source. Planning/startup are retained, so the
ratios should not be extrapolated directly into sustained throughput.

[Complete-job evidence](results/batch-sort-jobs-20261109.json) retains all trials,
configuration, source/library fingerprints and the earlier pre-metrics cohorts
(1.36× and 1.78×). The final measurements include production metric snapshots.
There is no precise before/after optimization claim between cohorts.

## Prepared operation method

`BatchSortAdmissionBenchmark` prepares equal logical data in each engine's own
format: copied Flink `BinaryRowData` and Arrow `RecordBatch` buffers imported into
Rust before timing. All columns are nullable BIGINT; every seventeenth row is NULL,
the other values use a fixed random seed and the stated cardinality. Both engines
sort the first key ascending with NULLs first and consume the same row count/key
order-sensitive hash, including NULL positions. The full row width is retained by their production sort operators.

Stock uses its generated normalized-key comparator and `SortOperator`, with setup
outside the Java timer. Native timing starts and ends inside Rust, after import/setup
and before export. It drives the production bounded DataFusion sorter, including
input/output ownership copies, memory callbacks, I/O yielding and streamed result
consumption. Initial JNI/import/export and perimeter transposes are excluded from
this small-functionality timer; the complete-job results above retain them.

The native authority and stock full-sort managed-memory pool are 32 MiB. Stock
TopK uses its normal JVM heap rather than that managed pool; both use the same
8 GiB Java heap, while native TopK remains subject to the 32 MiB native authority. Each cohort uses three warmups, seven retained
trials, alternating engine order, and 1,024-row native output batches. These are
one-partition operation measurements, not SQL job throughput.

The earlier prepared-operation cohorts are **invalid comparisons**: the Flink
sort-spec flag was interpreted as NULLS FIRST, but it means NULLS LAST. Stock and
native therefore sorted NULLs differently. The former 8.77×/7.18× and prototype
ratios are withdrawn; their raw trials remain explicitly marked discarded in
[operation evidence](results/batch-sort-operation-20261109.json) and
[groundwork evidence](results/batch-sort-groundwork-20261109.json). Four still earlier
cohorts also had unequal memory budgets. None establishes an acceleration claim.
The complete SQL jobs above use identical SQL and no NULL keys and are unaffected.

The corrected harness now passes the inverse `nullsFirst` flag to Flink and
validates an order-sensitive key hash, including NULL positions, against a sorted
reference. Corrected full-sort and TopK measurements are recorded separately.

Corrected full-sort results, median milliseconds [range], with matching ASC NULLS FIRST:

| Rows | BIGINT columns | Cardinality | Stock | Native | Stock/native |
|---:|---:|---:|---:|---:|---:|
| 131,072 | 4 | 131,072 | 25.00 [23.09–41.97] | 3.50 [3.31–3.64] | 7.14× |
| 65,536 | 8 | 127 | 11.06 [9.99–13.57] | 2.05 [1.97–2.13] | 5.39× |

[Corrected evidence](results/batch-sort-corrected-operation-20261109.json) retains the
trial arrays and library fingerprint. These are the current valid prepared full-sort
comparisons; no before/after speedup is inferred from discarded cohorts.

## Sorted LIMIT/FETCH

The driver also runs DataFusion TopK and applies the offset after sorting. It
preserves Flink's local/global stages and blocking Arrow shuffle. Prepared tests
use matching ASC NULLS LAST and validate an order-sensitive key hash. Stock uses
its normal heap-based sort-limit operator; native retains its 32 MiB authority.
Times are median milliseconds with the observed range in brackets.

| Prepared rows × BIGINT columns | Cardinality | Offset / fetch | Stock median ms | Native median ms | Stock/native |
|---:|---:|---:|---:|---:|---:|
| 131,072 × 4 | 131,072 | 0 / 64 | 2.20 [2.17–2.25] | 0.89 [0.86–0.90] | 2.48× |
| 65,536 × 8 | 127 | 1,023 / 16,384 | 12.08 [11.01–14.00] | 6.83 [6.67–6.94] | 1.77× |

The complete jobs use the two-key BIGINT query above, followed by OFFSET/FETCH.
The SHA2 composition additionally filters with
`WHERE SHA2(CAST(id AS STRING), 256) <> ''` before sorting, retaining evaluation
of the predicate in both engines. Two warmups/five alternating trials and the
same complete-job boundaries/resources apply.

| Rows | Offset / fetch | SHA2 filter | Stock median ms | Native median ms | Stock/native |
|---:|---:|:---:|---:|---:|---:|
| 1,000,000 | 0 / 64 | no | 104.92 [103.55–138.82] | 107.29 [102.57–116.63] | 0.98× |
| 2,000,000 | 1,023 / 16,384 | no | 166.43 [150.26–211.48] | 220.58 [193.03–244.98] | 0.75× |
| 1,000,000 | 0 / 64 | yes | 245.23 [225.10–257.72] | 149.05 [139.04–151.71] | 1.65× |
| 2,000,000 | 1,023 / 16,384 | yes | 463.09 [449.46–524.65] | 293.84 [285.84–297.01] | 1.58× |

**Standalone sorted limits remain stock.** Their prepared single-key operations
are faster, but the complete two-key jobs did not establish a gain. The larger
candidate was slower, and a 4,096-row batching diagnostic still lost (175/204 ms
stock/native). A sorted limit therefore does not qualify an island by itself;
it can compose with the existing SHA2, unrestricted-sort or accepted UDF rules.
This preserves the scalar admission design without a performance opt-in.

[Sorted-limit evidence](results/batch-sorted-limit-20261109.json) retains every
trial, ranges, the rejected candidates and the batching diagnostic. Previous
StreamFusion behavior was stock fallback for all these sorted-limit queries,
including the now-admitted compositions. These local results do not establish
remote-network throughput or performance for other types, key counts or limits.

## Environment and reproduction

Measurements used Flink 2.2.1, DataFusion 54.0.0, Rust Arrow 58.4.1, Arrow Java 18.3.0,
Temurin 17.0.17, an 8 GiB JVM heap and an Apple M4 with 10 CPUs/16 GiB RAM. The release
native engine uses mimalloc and omits optional RocksDB, which sort does not use.
Correctness, blocking-shuffle replay and packaged-loader controls also pass on
Flink 1.18.1; these performance measurements are from 2.2.1.

Use the repository's JDK 17 setup and release benchmark profile:

```bash
SF_BENCHMARK=true mvn -pl streamfusion-runtime -am test -Pbench \
  -Dtest=BatchSortJobBenchmark -Dsurefire.failIfNoSpecifiedTests=false \
  -Dsf.batch.sort.job.rows=1000000 -Dsf.batch.sort.job.cardinality=2147483647

SF_BENCHMARK=true mvn -pl streamfusion-runtime -am test -Pbench \
  -Dtest=BatchSortAdmissionBenchmark -Dsurefire.failIfNoSpecifiedTests=false \
  -Dsf.batch.sort.rows=131072 -Dsf.batch.sort.width=4 \
  -Dsf.batch.sort.cardinality=131072
```

The low-cardinality complete job uses 2,000,000 rows/cardinality 64. The wide
operation cohort uses 65,536 rows/width 8/cardinality 127. These harnesses preserve
the production boundaries described above and do not change Nexmark.

For the admitted small-limit composition, add
`-Dsf.batch.sort.job.fetch=64 -Dsf.batch.sort.job.sha2=true` to the complete-job
command. The large composition uses rows 2,000,000, cardinality 64, offset 1,023
and fetch 16,384. Prepared TopK uses `sf.batch.sort.offset`/`sf.batch.sort.fetch`;
its NULL order defaults to last and is independently configurable with
`sf.batch.sort.nullsFirst`. The full-sort default remains NULLS FIRST.
