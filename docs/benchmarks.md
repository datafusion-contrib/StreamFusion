# Benchmarks

## Methodology

- **Nexmark, end to end.** The headline numbers below are an end-to-end, exactly-once Kafka
  pipeline — not a blackhole sink. Stock Flink and StreamFusion run at **parallelism 4**, read the
  same 2M-event Kafka JSON corpus from a four-partition topic (one split per source subtask), and
  publish each query's result to a fresh Kafka topic with a one-second checkpoint interval.
  Append-only queries use `kafka`; updating queries use `upsert-kafka` with the result's actual
  primary key. Each timed run includes source consumption, query execution, the keyed shuffle,
  serialization, Kafka writes, checkpoints, and the bounded job's final transaction commit. q6 is
  omitted because Flink SQL itself cannot run it. Both engines receive the same
  `properties.max.poll.records=8192` source setting and the same producer settings:
  `batch.size=524288` and `linger.ms=20`.
- **Local shuffle handles are disabled.** The `bench` Maven profile sets
  `streamfusion.exchange.zeroCopyLocal=false`, so StreamFusion serializes columnar shuffle records
  with Arrow IPC just as it would across TaskManagers. Stock Flink likewise serializes records on
  same-JVM network edges; neither side receives a process-local object-handoff advantage.
- **The native Kafka boundaries are asserted.** Stock Flink uses its normal rowwise format decode.
  StreamFusion retains Flink's Kafka enumerator, partition assignment, offsets, checkpointing, and
  client, but its split-aware reader batches Kafka bytes and decodes them directly to Arrow in Rust.
  Sink key/value/tombstone encoding is likewise Rust, feeding Flink's exactly-once KafkaSink. An
  unsupported shape falls back explicitly rather than being credited as native.
- **Every cell asserts the plan shape.** The native plan — including Kafka poll/decode, every
  supported operator, and sink key/value/tombstone serialization — is asserted for every cell, so a
  silent fallback can't masquerade as a native number.
- **Release builds only, always.** Debug Rust is roughly an order of magnitude slower than release;
  every number here comes from the `bench` Maven profile (`mvn test -Pbench ...`), which builds and
  loads the release native library. Reporting a benchmark from a debug build is a standing mistake
  this project checks for explicitly — one early Parquet-copy number silently regressed from 3.19×
  to 0.45× before this was caught.
- **Micro-benchmarks.** Per-operator Criterion benchmarks (`cd native && cargo bench`) measure each
  native operator's steady-state hot loop over an in-memory Arrow batch, isolated from the JVM
  bridge and Flink's scheduling — these are what the entries in [Optimizations](optimizations/index.md)
  cite for a specific technique's speedup.

The format-decode microbenchmark compares the exact Nexmark Kafka value payloads across JSON, bare
Avro, and protobuf. It times only bytes to the engine's destination representation: Flink's format
decoder materializes one `RowData` per message, while the StreamFusion format decoder materializes
one Arrow batch per 8,192 messages. Corpus construction, Kafka polling, SQL operators, checkpoints,
and sinks are outside the timed region. Each decoder is warmed independently, every trial processes
whole batches for a fixed wall-clock interval, and the best trial is reported in rows/s and ns/row.

```sh
TZ=UTC SF_BENCHMARK=true SF_DECODE_BATCH_ROWS=8192 \
  SF_DECODE_WARMUP_SECONDS=1 SF_DECODE_SECONDS=3 SF_DECODE_RUNS=3 \
  mvn -pl :streamfusion-runtime test -Pbench \
  -Dtest=NexmarkFormatDecodeBenchmark
```

For matched Java and native CPU flame graphs of one format, enable
`NexmarkFormatDecodeBenchmark#profileDecode`. The harness warms both decoders, records them
separately for `-Dprofile.seconds=20`, and writes JFR files below `-Dprofile.outputDir`.

```sh
TZ=UTC SF_BENCHMARK=true SF_PROFILE_DECODE=true \
  mvn -pl :streamfusion-runtime test -Pbench \
  -Dtest='NexmarkFormatDecodeBenchmark#profileDecode' \
  -Dprofile.format=protobuf -Dprofile.seconds=20 \
  -Dprofile.outputDir=target/profiles/protobuf-decode
```

The experimental Flink 1.18 build has a separate [row-fed Nexmark compatibility baseline](benchmarks/flink118-nexmark.md). Its generator/blackhole results are not comparable to the Kafka headline workload below.

## Nexmark, parallelism 4

Apple M1 Max, release + `mimalloc`, measured across all four backend/mode combinations. The memory
columns use one measured run per cell (2026-08-25); the disk columns use one warmup and the best of
two measured runs (2026-09-05, after the pinned batched-read and column-state-codec work). The
memory columns compare Flink's default heap state against StreamFusion's memory state; the disk
columns compare stock Flink RocksDB against StreamFusion's native RocksDB backend. The Kafka
harness fixes the table session time zone at UTC so `TIMESTAMP_LTZ` window coverage does not depend
on the benchmark host. Mini-batching ("on") uses the same production-style configuration on both
engines (`allow-latency=2s`, `size=50000`). Each cell is StreamFusion throughput divided by Flink
throughput within the same backend and mode.

| Query | Memory, off | Memory, on | Disk, off | Disk, on |
|---|---:|---:|---:|---:|
| q0 | **1.69×** | **1.39×** | **1.36×** | **1.40×** |
| q1† | **1.40×** | **1.40×** | **1.38×** | **1.41×** |
| q2 | **1.26×** | **1.08×** | **1.11×** | **1.08×** |
| q3 | **1.03×** | **1.16×** | **1.08×** | **1.14×** |
| q4 | **1.83×** | **1.60×** | **8.92×** | **15.11×** |
| q5 | **1.41×** | **1.23×** | **4.38×** | **4.38×** |
| q7 | **1.30×** | **1.47×** | **7.41×** | **11.93×** |
| q8 | **1.27×** | **1.12×** | **2.33×** | **2.43×** |
| q9 | **1.22×** | **1.80×** | **17.83×** | **67.95×** |
| q10† | **1.45×** | **1.94×** | **1.42×** | **1.40×** |
| q11 | **1.47×** | **1.50×** | **9.84×** | **10.03×** |
| q12 | **1.09×** | **1.13×** | **1.45×** | **1.33×** |
| q13 | **1.20×** | **1.14×** | **1.15×** | **1.22×** |
| q14† | **1.47×** | **1.25×** | **1.49×** | **1.53×** |
| q15† | **1.47×** | **1.22×** | **1.61×** | **1.32×** |
| q16† | **1.17×** | **1.41×** | **1.49×** | **1.28×** |
| q17† | **1.20×** | **1.18×** | **1.92×** | **1.65×** |
| q18 | **1.07×** | **1.37×** | **6.01×** | **8.92×** |
| q19 | 0.99× | **2.82×** | **2.44×** | **5.09×** |
| q20 | **1.22×** | **1.56×** | **37.43×** | **68.20×** |
| q21† | **1.23×** | **1.31×** | **1.26×** | **1.27×** |
| q22 | **1.33×** | **1.25×** | **1.30×** | **1.22×** |
| q23 | **1.69×** | **3.32×** | **3.15×** | **12.10×** |
| **geomean** | **1.31×** | **1.44×** | **2.75×** | **3.41×** |

† The benchmark's existing opt-in expression variants are used for these headline cells: q1 uses
approximate decimal arithmetic; q10, q14–q17, and q21 use native datetime or regex/case behavior
that can differ from Flink at documented edge cases.

_Apple M1 Max; numbers are comparable only within a machine._

## Reproducing

```sh
SF_BENCHMARK=true mvn -pl :streamfusion-runtime test -Pbench
```

runs the end-to-end suites (`ThroughputBenchmark`, `NexmarkBenchmark`, `NexmarkKafkaBenchmark`,
`NexmarkMatrixBenchmark`); the `-Pbench` profile is required. The Criterion micro-benchmarks run
independently with `cd native && cargo bench`.

To reproduce the two persistent-backend columns above in one run:

```sh
SF_BENCHMARK=true SF_MATRIX_STATE_BACKENDS=true SF_STATE_BACKENDS_MINI_BATCH=both \
  SF_ROWS=2000000 SF_PARALLELISM=4 SF_KAFKA_PARTITIONS=4 SF_WARMUP=1 SF_RUNS=2 \
  mvn -pl :streamfusion-runtime test -Pbench \
  -Dtest='NexmarkMatrixBenchmark#stateBackendComparison'
```

To capture matched async-profiler CPU recordings for every exactly-once Kafka query with the memory
backend and mini-batching disabled, run `exactlyOnceKafkaSinkProfileAll` with
`SF_PROFILE_ALL_KAFKA_SINK=true`. The harness reuses one broker and input corpus, performs one warmup
per engine/query, and writes `flink-q*.jfr` and `streamfusion-q*.jfr` under
`-Dprofile.outputDir=...`. It invokes `asprof` from `PATH` by default; override that executable with
`-Dprofile.asprof=...`.

## Parquet, Delta, and Paimon sink diagnostics

These diagnostics use the readme-like 2M-event Kafka JSON workload with four input partitions,
parallelism four, memory state, mini-batching disabled, one warmup, and the best of three measured
runs. They cover q0–q5 and q7–q23; q6 is omitted because stock Flink cannot execute it. Unlike the
headline table, these runs measure local data-file output rather than Kafka output.

Apple M1 Max, release + `mimalloc`; Parquet and Delta measured 2026-08-22, Paimon append tables
2026-09-07 and primary-key tables 2026-09-09:

| Sink | Completed | Suite geomean |
|---|---:|---:|
| Parquet physical changelog | 23/23 | **1.535×** |
| Delta (MOR for updating queries) | 23/23 | **1.522×** |
| Combined | 46/46 | **1.529×** |
| Paimon append, bucket-unaware (16 append-only queries) | 16/16 | **1.47×** |
| Paimon append, 4 fixed buckets (16 append-only queries) | 16/16 | **1.47×** |
| Paimon primary key, 4 fixed buckets, in-job compaction (7 updating queries) | 7/7 | **1.64×** |

For the Parquet diagnostic, set `SF_MATRIX_PARQUET_SINK=true` and run
`NexmarkMatrixBenchmark#changelogParquetSinkComparison`. This mode always disables Flink logical
mini-batching. The normal filesystem/Parquet table sink is append-only, so the harness uses a
benchmark-only changelog connector that writes every physical change and prepends `_row_kind`
(`+I`, `-U`, `+U`, or `-D`) to the Parquet schema. The Flink baseline maps each change to a row and
uses parquet-mr; StreamFusion keeps the query output as Arrow, materializes the four-value row-kind
column natively, and feeds the batch directly to parquet-rs. Both paths retain Flink's
checkpoint-aware filesystem writer and each sink subtask produces its own part files. Set
`SF_PARQUET_OUTPUT` to retain them at a chosen path; otherwise the harness prints its temporary
output root. These results are intentionally separate from the headline Kafka table because they
measure local Parquet IO rather than Kafka IO.

```sh
TZ=UTC SF_BENCHMARK=true SF_MATRIX_PARQUET_SINK=true SF_MATRIX_KAFKA=false \
  SF_MATRIX_GENERATOR=false SF_MATRIX_PARQUET=false SF_ROWS=2000000 \
  SF_PARALLELISM=4 SF_KAFKA_PARTITIONS=4 SF_WARMUP=1 SF_RUNS=3 \
  mvn -pl :streamfusion-runtime test -Pbench \
  -Dtest='NexmarkMatrixBenchmark#changelogParquetSinkComparison'
```

Add `SF_MATRIX_QUERIES=q0,q4` for a focused run or set `SF_PARQUET_OUTPUT` to retain the part files
instead of deleting the temporary output root.

Set `SF_PROFILE_PARQUET_SINK=true` and run `changelogParquetSinkProfile` for matched q0 CPU
recordings of the Flink and StreamFusion writers. The harness performs an unprofiled warmup, then
loops each writer for `-Dprofile.seconds=20` by default and writes both JFR files below
`-Dprofile.outputDir=...`.

The Delta diagnostic compares the published Delta 4.4 connector with StreamFusion's data-file
acceleration. Queries with an updating changelog use the result's real primary key and Delta 4.4
merge-on-read upserts; append-only queries use append mode. Delta Kernel owns table metadata,
deletion vectors, statistics, actions, and commits on both sides. The harness pre-creates each table
with deletion vectors enabled because `delta.enableDeletionVectors` is a Delta table property, not
a SQL connector option accepted by the published connector. Set `SF_DELTA_OUTPUT` to retain the
tables.

```sh
TZ=UTC SF_BENCHMARK=true SF_MATRIX_DELTA_SINK=true SF_ROWS=2000000 \
  SF_PARALLELISM=4 SF_KAFKA_PARTITIONS=4 SF_WARMUP=1 SF_RUNS=3 \
  mvn -Pdelta -pl :streamfusion-delta -am test -Pbench \
  -Duser.timezone=UTC -Dnative.build.skip=true \
  -Dtest='NexmarkDeltaSinkBenchmark#mergeOnReadUpsertComparison' \
  -Dsurefire.failIfNoSpecifiedTests=false
```

The explicit UTC setting is required for the timestamp-window queries: the native
`TIMESTAMP_LTZ` window path accepts zones with a fixed offset for the full timestamp range, while a host-local DST zone is an
intentional planner fallback.

The Paimon diagnostic compares the published Paimon 2.0.0 Flink 2.2 connector with StreamFusion's
Paimon sink in three variants. The two append variants cover the 16 queries whose result is
insert-only: every table is a Parquet append table that Paimon creates from the sink DDL; the
bucket-unaware variant keeps Paimon's in-job compaction topology, and the fixed-bucket variant uses
four buckets keyed on the result's first column. Both engines run Paimon's writer, committer, and
manifests unchanged, so the row counts read back through Paimon's snapshots agree on every query
except q12, whose processing-time window Flink never fires at end of input while StreamFusion
flushes it. Per query the speed-up ranges from 1.08× (q3, a join that emits under a thousand rows)
to 2.04× (q23, which writes 5.5 M joined rows); queries that write most of their input land at
1.4–1.8×.

The primary-key variant covers the seven updating queries (q4, q9, q15–q19) with a `deduplicate`
primary-key table on the result's key, four fixed buckets, and Paimon's default in-job compaction;
Flink's upsert materializer is disabled for both engines because Paimon refuses it. Stock Paimon
takes the changelog one row at a time into its sort buffer, while StreamFusion buckets the Arrow
batches natively, merges each bucket's changelog by key in Rust, and writes the level-0 files itself
before Paimon's own writer compacts them. Per query the speed-up ranges from 1.18× (q9, whose
changelog is dominated by updates of the same 120 K keys) to 2.38× (q16, eight keys rewritten a
million times), and the merged row counts read back through Paimon agree on every query. Set
`SF_PAIMON_OUTPUT` to retain the tables, one directory per variant.

```sh
TZ=UTC SF_BENCHMARK=true SF_MATRIX_PAIMON_SINK=true SF_ROWS=2000000 \
  SF_PARALLELISM=4 SF_KAFKA_PARTITIONS=4 SF_WARMUP=1 SF_RUNS=3 \
  mvn -Ppaimon -pl :streamfusion-paimon -am test -Pbench \
  -Duser.timezone=UTC -Dtest='NexmarkPaimonSinkBenchmark' \
  -Dsurefire.failIfNoSpecifiedTests=false
```

`NexmarkPaimonSinkBenchmark` runs all three variants; select `unawareBucketAppendComparison`,
`fixedBucketAppendComparison`, or `primaryKeyComparison` for one of them.

Set `SF_PROFILE_PAIMON_SINK=true` and run `unawareBucketAppendProfile` for matched q0 CPU and
wall-clock recordings of the stock and native paths. A word of caution that this profile taught
us: stock Paimon's parquet-mr wraps its record consumer in per-value debug logging whenever its
logger has DEBUG enabled, so a benchmark JVM with an unconfigured log4j 1.x binding makes the stock
sink look 15–60× slower than it is. The module's test classpath excludes Hadoop's log4j 1.x
bindings for exactly this reason; check the stock profile before trusting a sink number that far
out of line with the rest of this table.
