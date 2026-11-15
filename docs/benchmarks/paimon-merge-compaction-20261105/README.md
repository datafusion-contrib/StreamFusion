# Paimon merge-engine compaction worker comparison

Release/mimalloc libraries on Apple M4 (10 CPUs, 16 GiB), JDK 17, 8 GiB heap. Each workload
contains 400,000 input changes in four stock-written runs with 100,000 overlapping keys.
Narrow values are BIGINT; wide values add a nullable string and nested integer array. The
aggregation workload sums BIGINT and uses default last-non-null for other values. Partial-update
inputs exercise overlapping versions and nullable wide fields. Each engine gets the same table,
settings and resources, including a 1 GiB sort-spill budget. One warmup and three measured runs
rotate stock Java, previous StreamFusion (stock compaction with existing format shim), and native.

The worker timer includes writer creation, Java selection, Java FileIO/JNI, released paimon-rust
read/merge, format encoding, metadata and close. Stock Paimon reopens every output outside the
timer; row count and a SHA-256 over the complete physical KV rows, stored kinds and sequences
must match. Native trials require positive rewrite groups, zero fallback and native encoder
footers. These are file-worker timings, not running-job or startup-inclusive job speedups.
All 648 observations across the original and additional host qualifications, including warmups and output fingerprints, are in [worker-trials.csv](worker-trials.csv).

| Paimon | Format | Workload | Stock median ms | Previous median ms | Native median [min, max] ms | Stock/native |
|---|---|---|---:|---:|---:|---:|
| 2.1.0 | parquet | first-row | 64.90 | 63.31 | 45.93 [45.21, 51.39] | 1.41× |
| 2.1.0 | parquet | first-row-wide | 125.57 | 125.82 | 70.54 [70.13, 71.82] | 1.78× |
| 2.1.0 | parquet | partial-update | 72.08 | 70.44 | 66.76 [65.20, 67.09] | 1.08× |
| 2.1.0 | parquet | partial-update-wide | 138.24 | 138.07 | 108.57 [107.44, 108.78] | 1.27× |
| 2.1.0 | parquet | aggregation | 75.59 | 76.60 | 68.44 [67.97, 74.06] | 1.10× |
| 2.1.0 | parquet | aggregation-wide | 156.06 | 156.84 | 124.35 [123.84, 124.76] | 1.25× |
| 2.1.0 | orc | first-row | 48.40 | 46.74 | 32.72 [30.16, 42.25] | 1.48× |
| 2.1.0 | orc | first-row-wide | 76.02 | 76.66 | 61.04 [60.68, 67.11] | 1.25× |
| 2.1.0 | orc | partial-update | 54.51 | 54.33 | 53.17 [52.93, 53.46] | 1.03× |
| 2.1.0 | orc | partial-update-wide | 92.57 | 93.20 | 96.97 [96.09, 97.01] | 0.95× |
| 2.1.0 | orc | aggregation | 63.89 | 64.94 | 53.82 [53.66, 53.94] | 1.19× |
| 2.1.0 | orc | aggregation-wide | 113.63 | 114.00 | 111.32 [111.08, 112.80] | 1.02× |
| 1.0.0 | parquet | first-row | 112.39 | 102.26 | 73.98 [59.38, 76.80] | 1.52× |
| 1.0.0 | parquet | first-row-wide | 161.62 | 163.93 | 83.85 [80.36, 90.89] | 1.93× |
| 1.0.0 | parquet | partial-update | 82.19 | 79.79 | 75.06 [75.04, 76.13] | 1.09× |
| 1.0.0 | parquet | partial-update-wide | 161.84 | 176.37 | 121.41 [120.21, 122.23] | 1.33× |
| 1.0.0 | parquet | aggregation | 90.98 | 90.93 | 77.44 [76.86, 79.17] | 1.17× |
| 1.0.0 | parquet | aggregation-wide | 183.72 | 191.62 | 139.00 [137.35, 140.77] | 1.32× |
| 1.0.0 | orc | first-row | 80.92 | 75.06 | 47.98 [40.91, 59.82] | 1.69× |
| 1.0.0 | orc | first-row-wide | 108.39 | 108.37 | 71.91 [69.77, 85.20] | 1.51× |
| 1.0.0 | orc | partial-update | 67.41 | 69.49 | 66.31 [65.02, 67.51] | 1.02× |
| 1.0.0 | orc | partial-update-wide | 105.93 | 105.07 | 112.54 [111.81, 116.51] | 0.94× |
| 1.0.0 | orc | aggregation | 79.31 | 80.32 | 69.11 [68.60, 70.80] | 1.15× |
| 1.0.0 | orc | aggregation-wide | 134.33 | 134.85 | 137.30 [135.08, 137.81] | 0.98× |

Wide ORC partial-update regresses on both releases, and wide ORC aggregation is marginally slower
on 1.0. Those candidate measurements led to retaining stock for nested ORC partial-update and aggregation;
they are not enabled acceleration claims. A separate 12-repeat JFR diagnostic reproduced the
partial-update regression. Its main sampled native boundary was `NativePaimon.readerNext`;
the released reader materializes field-merge results per key. There is no additional local reducer
to optimize without changing the reuse boundary. The concise [profile summary](profile-summary.json)
retains sampled frames; its instrumented timings are excluded from this table. First-row timings measure full data compaction to the highest
level. Lower-level L0 rewrites requiring historical lookup remain stock. No custom Rust reducer
was added for any engine.

Reproduce with `PaimonCompactionBenchmark` under `-Pbench,paimon` or
`-Pflink-1.18,bench,paimon1`, the suite agent and a per-run inventory directory. Set
`SF_PAIMON_COMPACTION_BENCHMARK=true`, `SF_PAIMON_COMPACTION_ROWS=400000`,
`SF_PAIMON_COMPACTION_REPEATS=3`, `SF_PAIMON_COMPACTION_SORT_BUFFER="1 gb"`,
`SF_PAIMON_COMPACTION_SHAPES=first-row,first-row-wide,partial-update,partial-update-wide,aggregation,aggregation-wide`
and `SF_PAIMON_FILE_FORMAT` to `parquet` or `orc`. Use UTC for ORC. Exclude the two wide field-merge
ORC shapes when requiring positive native work on the final admitted implementation; their table
rows retain the candidate measurements taken before that performance guard.

## Additional modern Flink 1.18 / Paimon 2.0 qualification

Paimon 2.0 always checks sum overflow, so sums retain Java; these aggregate workloads explicitly
use BIGINT `min`. All engines receive the same settings. The initial one-warmup/three-measurement
matrix is retained as `run=initial`, including the narrow ORC partial-update result of 0.99× stock.
A separate 12-repeat [JFR diagnostic](profile-2.0-summary.json) showed substantial warmup drift.
Qualification then used three warmups and five uninstrumented rotating measurements for every
engine and workload. The extended-warmup measurements below and all earlier observations remain
in the CSV; no engine, workload or unfavorable trial is discarded. These short worker timings
remain sensitive to JIT and host variability, particularly the small scalar partial-update gains.

| Format | Workload | Stock median ms | Previous median ms | Native median [min, max] ms | Stock/native |
|---|---|---:|---:|---:|---:|
| parquet | first-row | 100.41 | 97.92 | 65.49 [61.00, 70.29] | 1.53× |
| parquet | first-row-wide | 171.58 | 169.28 | 98.09 [90.34, 101.07] | 1.75× |
| parquet | partial-update | 112.15 | 102.85 | 96.16 [93.37, 105.02] | 1.17× |
| parquet | partial-update-wide | 189.21 | 188.60 | 155.40 [153.53, 170.09] | 1.22× |
| parquet | aggregation-min | 109.35 | 110.10 | 96.62 [95.66, 102.66] | 1.13× |
| parquet | aggregation-min-wide | 210.24 | 210.56 | 176.42 [173.66, 179.61] | 1.19× |
| orc | first-row | 96.92 | 94.50 | 52.79 [50.82, 62.07] | 1.84× |
| orc | first-row-wide | 128.14 | 120.19 | 87.65 [83.32, 94.95] | 1.46× |
| orc | partial-update | 93.17 | 94.96 | 89.84 [82.18, 94.02] | 1.04× |
| orc | aggregation-min | 93.70 | 95.27 | 84.85 [79.23, 86.26] | 1.10× |

Reproduce this additional matrix with `-Pflink-1.18,paimon,bench`,
`SF_PAIMON_COMPACTION_WARMUPS=3`, and `SF_PAIMON_COMPACTION_REPEATS=5`. Use
`aggregation-min,aggregation-min-wide` for the aggregate Parquet shapes and omit nested field-merge
shapes for ORC. The input report records actual selected file/row counts and input levels.
