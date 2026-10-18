# Streaming Iceberg Nexmark results

The complete comparison passes all **480 stock/native paired validations**: 120
cells, each with one warmup and three measured pairs, covering all 23 headline
queries, four state/mini-batch modes and seven existing expression variants.
Each run uses 2M original deterministic events, parallelism four, four-bucket
Iceberg inputs and fresh four-bucket outputs. Updating outputs use the actual
result identifier fields and Java delete writers.

Speedup is stock elapsed time divided by native elapsed time. The repeated median
ratios are primary; the README sampling convention is retained separately.

| State / mini-batch | Median, README variants | Median, exact SQL | README sampling | Kafka headline |
|---|---:|---:|---:|---:|
| Memory / off | 1.24× | 1.06× | 1.14× | 1.31× |
| Memory / on | 1.44× | 1.38× | 1.30× | 1.44× |
| Disk / off | 2.63× | 2.34× | 2.55× | 2.75× |
| Disk / on | 2.97× | 2.81× | 2.96× | 3.41× |

README variants include its explicitly incompatible decimal/datetime/regex modes;
each query contributes once. Exact SQL aggregates keep those variants separate.
README sampling takes the first memory observation or the best of the first two
disk observations. [Every median, range and sample-selection result](results.md),
[phase/checkpoint/memory metrics](metrics.csv), and [raw observations](observations.jsonl)
retain the slower cases. These are end-to-end measurements, not sustained decoder
throughput. Nominal events/s divides the 2M-event corpus by elapsed time; individual
queries read different normalized tables.

## Configuration and measured boundary

Both engines use Zstd 3, 128 MiB row groups, 1 MiB pages, 2 MiB dictionaries,
512 MiB target files, HASH distribution, one-second checkpoints, UTC and the
headline mini-batch settings (two seconds / 50,000 rows). Flink heap/RocksDB is
compared with native memory/RocksDB. Arrow IPC shuffle is enabled; same-process
handle-only shuffle is disabled. The original event generator, SQL/UDF/lookup
corpus and updating-result keys are reused without rewriting the headline harness.

Input creation is outside timing: 40k people, 120k auctions and 1.84M bids,
written in 50k-event stripes. Each input table occupies all four buckets. Both
engines use the Java source's file-order watermark with the original four-second
delay, 128 MiB split/open-file costs, and timestamp-statistics split ordering.
[Connector documentation](../../connectors/iceberg.md) explains these source settings.

Timing includes planning/submission, discovery, decode, query, shuffle, Java
partition/key decisions, JNI, data/delete encoding, checkpoints and final commit.
After every assigned split is consumed and all tasks run, an ordinary configured
checkpoint settles writes, followed by a draining backend-native savepoint and
job completion. Both are timed, using the shared MiniCluster filesystem storage.
Stock Iceberg reads committed output outside timing. q12 includes its final
processing-time-window wait and validates per-bidder count conservation instead
of inherently variable wall-clock timestamps.

This Linux/WSL2/ext4 host has an Intel i7-12650H, 16 logical CPUs and approximately
9.7 GiB physical RAM. Both job JVMs use **4 GiB maximum heaps**; the Maven launcher
uses 512 MiB. This resource limit differs from the usual 8 GiB test fork because
that setting exhausted this host. The 48 GiB task-off-heap value is an accounting
ceiling, not reserved or measured physical memory. JDK 17.0.20.1, Rust 1.94.0,
Flink 2.2.1, Java Iceberg 1.12.0 and released iceberg-rust 0.10.1 are used, with
release Rust and mimalloc. The [Kafka headline](https://github.com/datafusion-contrib/StreamFusion/blob/main/readme.md) was measured on
an Apple M1 Max with a bounded Kafka completion protocol; these ratios are not
a controlled estimate of changing only the connector on that machine.

The full matrix resumes whole validated cells across three cohorts (34 / 28 / 58).
[Audit metadata](audit.json) records the identical production Java trees and native
library hashes across them; harness-only reporting fixes do not change execution.
Incomplete attempts are excluded rather than substituting individual samples.
Partial [host sampling](host-sampling-summary.json) and compressed raw samples are
retained per cohort; they show substantial I/O pressure and do not establish
exclusive host use. Sampled RSS/task/Arrow peaks are collected during corpus
progress, not throughout every checkpoint/drain wait.

## Columnar and input audit

All 480 native graphs contain no `RowDataToArrow` or `ArrowToRowData`; every native
plan reports no operator fallback. Every native trial uses Rust Parquet tasks and
writers with zero source-task fallback. Runtime assertions distinguish Rust data
footers from Java delete footers; stock trials require Java data footers and zero
native source/writer factories. All paired committed-output validations pass.

Each cohort retains input table metadata and a 480-file inventory with full-file
and data-page SHA-256 hashes in `cohorts/*/inputs.tar.gz`. All data-page bytes match
across cohorts. Cohort two's full-file hashes differ because Parquet writes
unordered encoding lists in a different footer order. The released Java Parquet
decoder confirms every footer is equal after sorting only `encodings` and
`encoding_stats`; [the verification](input-reproducibility.json) covers all 960
cross-cohort file comparisons. [Verifier source](VerifyInputFooters.java) uses the
released Iceberg runtime's shaded Parquet format decoder and SLF4J API. No input
Parquet or checkpoint-state payloads are
stored in Git.

A subsequent [cancellation ownership fix](https://github.com/datafusion-contrib/StreamFusion/commit/2182cb99)
reclaims unclaimed fetched batches when cancelling mid-file. The full matrix
precedes that fix. A separate [post-fix 2M-event q0 check](../iceberg-cleanup-q0-20261017/results.md)
passes all four pairs at 1.53× (stock 1.099 s, native 0.719 s), versus 1.51× in the
matrix. The native libraries and data encoding are unchanged; the original matrix
is retained with its measured revisions.

## Regressions and targeted comparisons

Large-state memory cases can lose heavily: q9 memory/on is 0.16×, q18 memory/off
0.10×, and q20 memory/off/on 0.09× / 0.08×. Their phase timings and
[stock/native profiles](../iceberg-profile-20261017/README.md) identify checkpoint
and storage waits as major costs; source/data-encoder CPU is a small share of the
profiled executions. Native Parquet is not assumed faster merely because it is Rust.

Separate warmed, repeated runs enable Flink snapshot compression for both engines:

| Query / mode | Stock median [range], s | Native median [range], s | Ratio |
|---|---:|---:|---:|
| q9 / memory-on | 3.312 [3.261–3.440] | 2.026 [1.772–2.079] | 1.63× |
| q18 / memory-off | 4.149 [3.937–12.140] | 3.601 [3.600–30.323] | 1.15× |
| q20 / memory-off | 1.906 [1.786–2.290] | 1.406 [1.362–1.439] | 1.36× |
| q20 / memory-on | 11.155 [6.460–12.428] | 5.253 [4.192–9.244] | 2.12× |

Compression reduces checkpoint bytes, but storage variability remains; these are
separate tuning cohorts, not replacements for uncompressed primary observations.

The [comparison against StreamFusion with stock Iceberg I/O](../iceberg-io-baseline-20261017/README.md)
holds native query execution fixed. q0 improves about 1.8×; the four-query geometric
means are 1.90× memory/off and 0.95× disk/off. q9 disk/off regresses to 0.49× in that
cohort. This does not establish an across-the-board gain over the previous I/O path.
[State/checkpoint investigation](https://github.com/datafusion-contrib/StreamFusion/issues/391)
tracks the remaining regressions. The [v2/v3 dynamic writer diagnostics](../../connectors/iceberg.md#current-dynamic-writer-diagnostics)
remain near parity. Supported contracts now accelerate by default when the optional module is installed.

The [final SQL/recovery/native validation](../iceberg-validation-20261017/README.md)
passes 298 tests with 13 batch exclusions, six Rust units and 126 Criterion fixtures.

## Reproduce

Select JDK 17 in `JAVA_HOME`/`PATH`, then run:

```sh
python3 bin/checkpoint-iceberg.py docs/benchmarks/iceberg-local
```

The driver builds/stages release+mimalloc libraries, archives complete validated
cells and resumes them without choosing fast individual repetitions. Use
`--push upstream` only when intending to commit/push those artifacts. The environment
selectors and supported settings are documented on the connector page. Regenerate
the report with `python3 bin/report-iceberg.py <evidence-directory>`.
