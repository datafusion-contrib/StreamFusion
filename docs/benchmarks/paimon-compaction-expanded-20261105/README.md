# Shared Paimon compaction release diagnostic

Measured on 2026-11-05 on an Apple M4 (10 CPUs, 16 GiB RAM), JDK 17, with Flink 1.18.1, released Java Paimon 1.0.0 and the shared crates.io
paimon 0.4.0 backend. These are file-worker measurements, including Java selection, writer
construction, JNI, decoding/merging/encoding, metadata, commit preparation and writer close.
They do not measure complete-job latency or peak RSS. The [120 raw trials](worker-trials.csv)
retain the warmup, rotating engine order, output row counts and complete-row SHA-256 fingerprints.
Each engine uses the same stock-written input files and settings. Every native trial asserts
positive native groups, zero fallback and native encoder footer witnesses. Stock Java readers
verify physical PK rows including stored sequences/kinds, or complete append rows, outside timing.

One warmup and three measured trials per engine; 400k input changes and one bucket. Unique keys
interleave four runs. Versions write each key four times with 20% winning deletes. Wide adds a
nullable string and integer array containing a null. Append shapes retain every insert. Seed,
validation and removal of uncommitted output are outside timing. Stock and previous use Java
compaction; previous retains the existing StreamFusion format shim. Native installs the production
provider. The test agent controls only baseline format selection and records work.

Parquet uses a 256 MiB sort-spill buffer. The ORC run uses 1 GiB for **every engine**, because the
conservative footer preflight declines the four-run nested PK shape at 256 MiB. The declined run
was a failed admission diagnostic, not a native timing, and is excluded from the result table.
The production budget gate is unchanged. All runs use a 256 MiB write buffer, an 8 GiB JVM maximum
heap, release/mimalloc native libraries, default compression and 128 MiB file target.

Seconds: repeated median (range). Ratios compare medians.

| Format | Shape | Stock | Previous | Native | Stock/native | Previous/native |
| --- | --- | --- | --- | --- | --- | --- |
| parquet | unique | 0.201 (0.197–0.221) | 0.200 (0.191–0.246) | 0.158 (0.139–0.177) | 1.27× | 1.26× |
| parquet | versions | 0.097 (0.092–0.098) | 0.097 (0.092–0.101) | 0.067 (0.063–0.067) | 1.46× | 1.45× |
| parquet | wide | 0.213 (0.205–0.232) | 0.214 (0.193–0.279) | 0.113 (0.103–0.129) | 1.89× | 1.89× |
| parquet | append | 0.075 (0.071–0.079) | 0.076 (0.074–0.085) | 0.043 (0.043–0.049) | 1.73× | 1.77× |
| parquet | append-wide | 0.244 (0.240–0.248) | 0.242 (0.242–0.249) | 0.120 (0.120–0.127) | 2.04× | 2.02× |
| orc | unique | 0.463 (0.456–0.466) | 0.465 (0.458–0.489) | 0.323 (0.318–0.386) | 1.43× | 1.44× |
| orc | versions | 0.238 (0.238–0.248) | 0.241 (0.240–0.246) | 0.143 (0.143–0.146) | 1.67× | 1.69× |
| orc | wide | 0.369 (0.367–0.373) | 0.381 (0.372–0.386) | 0.291 (0.276–0.291) | 1.27× | 1.31× |
| orc | append | 0.122 (0.121–0.127) | 0.130 (0.128–0.131) | 0.088 (0.088–0.091) | 1.38× | 1.47× |
| orc | append-wide | 0.457 (0.455–0.464) | 0.454 (0.451–0.467) | 0.348 (0.343–0.354) | 1.31× | 1.30× |

The results establish this released 1.0 worker path only; other host/release combinations need
their own measurements. Startup and individual trials vary; the raw warmups are retained.

Run `PaimonCompactionBenchmark` through Maven's `bench` profile and the test agent as described
on the [compaction optimization page](../../optimizations/paimon-compaction.md#reproduce).
For this adapter use `-Pflink-1.18,paimon1,bench -pl streamfusion-paimon1 -am`.
`SF_PAIMON_FILE_FORMAT` selects parquet/orc, `SF_PAIMON_COMPACTION_SORT_BUFFER` sets the shared
sort-spill budget, and `SF_PAIMON_COMPACTION_SHAPES=unique,versions,wide,append,append-wide`
selects these workloads. Enable `SF_PAIMON_COMPACTION_BENCHMARK=true`, set
`SF_PAIMON_COMPACTION_ROWS=400000` and `SF_PAIMON_COMPACTION_REPEATS=3`.
