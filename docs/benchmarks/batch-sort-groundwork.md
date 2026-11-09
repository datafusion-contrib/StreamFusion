# Batch sort operation groundwork

The bounded sort prototype is **not selected by the SQL planner**. These are small
functionality measurements, not complete-job results. They do not include SQL
planning, scans, network exchange, JNI entry/import, perimeter row/Arrow conversion,
output export or sink completion. The [remaining runtime work](../batch-execution-runtime.md)
must be completed before admitting sort SQL.

## Method

`BatchSortAdmissionBenchmark` prepares equal logical data in each engine's own
format: copied Flink `BinaryRowData` and Arrow `RecordBatch` buffers imported into
Rust before timing. All columns are nullable BIGINT; every seventeenth row is NULL,
the other values use a fixed random seed and the stated cardinality. Both engines
sort the first key ascending with NULLs first and consume the same row count/key
checksum. The full row width is retained by their production sort operators.

Stock Flink uses its generated normalized-key comparator and `SortOperator`, with
operator setup/open outside the Java timer. The timer covers processing the
prepared rows, `endInput` and checksum consumption. Native timing starts and ends
inside Rust, after import/setup and before export. It drives the production bounded
DataFusion sorter, including memory callbacks, I/O yielding and streamed result
consumption. It does not time a Java stopwatch around JNI.

The harness explicitly supplies and asserts **32 MiB** of Flink managed sort
memory and **32 MiB** for the native task off-heap authority. Input and setup
preparation are outside timing. Each cohort uses three warmups, seven retained
trials, alternating engine order, and 1,024-row native output batches. These are
one-partition sort operation measurements; they include engine startup effects
inside the operation and should not be extrapolated into sustained SQL throughput.

The release engine uses mimalloc and omits the optional RocksDB state backend,
which this operator does not use. Measurements used Flink 2.2.1, DataFusion 54.0.0,
Arrow 58.4.1, Temurin 17.0.17, an 8 GiB JVM heap and an Apple M4 with 10 CPUs/16 GiB
RAM. Stock Flink is also the previous and current StreamFusion SQL route for these
sort queries; no previous native batch sorter exists.

## Results

Times are median milliseconds with the full observed range in brackets.

| Rows | BIGINT columns | Key cardinality | Stock Flink | Native operation | Stock/native |
|---:|---:|---:|---:|---:|---:|
| 131,072 | 4 | 131,072 | 34.17 [27.30–44.68] | 2.92 [2.80–6.78] | 11.70× |
| 65,536 | 8 | 127 | 9.66 [8.49–10.45] | 1.35 [1.31–1.45] | 7.16× |

These establish a promising operation-level result for the tested BIGINT sort;
they do not establish performance for other key types, multi-key sorting, forced
spill, tiny inputs, distributed exchange, or complete jobs. In particular, native
borrowed-buffer accounting remains conservative and can spill early.

Four earlier exploratory cohorts used Flink's test-harness default of **3 MiB**
against the native **32 MiB** pool. They are invalid comparisons and excluded from
the conclusions above. The retained evidence labels them as discarded, with their
actual budgets and trial arrays, so their larger ratios cannot be mistaken for
valid acceleration evidence. There is no measured before/after optimization claim
between the exploratory and corrected cohorts.

[Machine-readable evidence](results/batch-sort-groundwork-20261109.json) retains
all valid trials, discarded controls, timer boundaries and source/DSO fingerprints.

## Reproduce

Use the repository's JDK 17 setup and release benchmark profile:

```bash
SF_BENCHMARK=true mvn -pl streamfusion-runtime -am test -Pbench \
  -Dtest=BatchSortAdmissionBenchmark -Dsurefire.failIfNoSpecifiedTests=false \
  -Dsf.batch.sort.rows=131072 -Dsf.batch.sort.width=4 \
  -Dsf.batch.sort.cardinality=131072
```

The wide cohort uses 65,536 rows, width 8 and cardinality 127. This benchmark does
not enable native SQL sort, change the Nexmark harness or bypass the full-job
performance requirements for eventual planner admission.
