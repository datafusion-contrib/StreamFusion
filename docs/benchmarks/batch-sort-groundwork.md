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
checksum. The full row width is retained by their production sort operators.

Stock uses its generated normalized-key comparator and `SortOperator`, with setup
outside the Java timer. Native timing starts and ends inside Rust, after import/setup
and before export. It drives the production bounded DataFusion sorter, including
input/output ownership copies, memory callbacks, I/O yielding and streamed result
consumption. Initial JNI/import/export and perimeter transposes are excluded from
this small-functionality timer; the complete-job results above retain them.

Both operators receive 32 MiB pools. Each cohort uses three warmups, seven retained
trials, alternating engine order, and 1,024-row native output batches. These are
one-partition operation measurements, not SQL job throughput.

Current copy-inclusive results, in median milliseconds [range]:

| Rows | BIGINT columns | Key cardinality | Stock operation | Native operation | Stock/native |
|---:|---:|---:|---:|---:|---:|
| 131,072 | 4 | 131,072 | 29.70 [28.89–38.16] | 3.39 [3.28–3.55] | 8.77× |
| 65,536 | 8 | 127 | 14.93 [12.26–16.00] | 2.08 [2.04–2.35] | 7.18× |

[Current operation evidence](results/batch-sort-operation-20261109.json) retains all
trials and the copy-inclusive pre-metrics control. These gains include the copies
required to transfer retained ownership safely.

The earlier prototype, before the compact ownership copies, measured 34.17/2.92 ms
(stock/native) for 131,072 rows × four BIGINTs, and 9.66/1.35 ms for 65,536 rows × eight
BIGINTs with cardinality 127. Those historical cohorts are retained in
[groundwork evidence](results/batch-sort-groundwork-20261109.json). Their source
fingerprints differ from the current implementation; do not interpret cross-cohort
changes as a measured optimization.

Four still earlier exploratory cohorts used Flink's test-harness default of 3 MiB
against native's 32 MiB. They are invalid comparisons, excluded from all conclusions,
and explicitly marked discarded in that evidence.

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
