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

## Nexmark, parallelism 4

Apple M1 Max, release + `mimalloc`, measured 2026-08-25 with one measured run per cell across all
four backend/mode combinations. The memory columns compare Flink's default heap state against
StreamFusion's memory state; the disk columns compare stock Flink RocksDB against StreamFusion's
native RocksDB backend, both after the per-key state rework that put every native operator on
RocksDB's own write path. Mini-batching ("on") uses the same production-style configuration on
both engines (`allow-latency=2s`, `size=50000`). Each cell is StreamFusion throughput divided by
Flink throughput within the same backend and mode.

| Query | Memory, off | Memory, on | Disk, off | Disk, on |
|---|---:|---:|---:|---:|
| q0 | **1.69×** | **1.39×** | **1.83×** | **1.32×** |
| q1 | **1.40×** | **1.40×** | **1.35×** | **1.49×** |
| q2 | **1.26×** | **1.08×** | **1.07×** | **1.08×** |
| q3 | **1.03×** | **1.16×** | **1.03×** | **1.20×** |
| q4 | **1.83×** | **1.60×** | **5.92×** | **9.34×** |
| q5 | **1.41×** | **1.23×** | **3.76×** | **2.89×** |
| q7 | **1.30×** | **1.47×** | **5.17×** | **12.97×** |
| q8 | **1.27×** | **1.12×** | **3.77×** | **2.54×** |
| q9 | **1.22×** | **1.80×** | **15.75×** | **42.68×** |
| q10 | **1.45×** | **1.94×** | **1.63×** | **1.32×** |
| q11 | **1.47×** | **1.50×** | **8.98×** | **11.92×** |
| q12 | **1.09×** | **1.13×** | **1.42×** | **1.45×** |
| q13 | **1.20×** | **1.14×** | **1.14×** | **1.13×** |
| q14 | **1.47×** | **1.25×** | **1.41×** | **1.35×** |
| q15 | **1.47×** | **1.22×** | **5.53×** | **1.84×** |
| q16 | **1.17×** | **1.41×** | **5.61×** | **2.79×** |
| q17 | **1.20×** | **1.18×** | **1.97×** | **1.64×** |
| q18 | **1.07×** | **1.37×** | **5.20×** | **4.41×** |
| q19 | 0.99× | **2.82×** | **2.40×** | **6.06×** |
| q20 | **1.22×** | **1.56×** | **20.87×** | **71.35×** |
| q21 | **1.23×** | **1.31×** | **1.33×** | **1.29×** |
| q22 | **1.33×** | **1.25×** | **1.28×** | **1.23×** |
| q23 | **1.69×** | **3.32×** | **1.60×** | **2.15×** |
| **geomean** | **1.31×** | **1.44×** | **2.84×** | **3.09×** |

_Apple M1 Max; numbers are comparable only within a machine._

## Reproducing

```sh
SF_BENCHMARK=true mvn -pl :streamfusion-runtime test -Pbench
```

runs the end-to-end suites (`ThroughputBenchmark`, `NexmarkBenchmark`, `NexmarkKafkaBenchmark`,
`NexmarkMatrixBenchmark`); the `-Pbench` profile is required. The Criterion micro-benchmarks run
independently with `cd native && cargo bench`.

To capture matched async-profiler CPU recordings for every exactly-once Kafka query with the memory
backend and mini-batching disabled, run `exactlyOnceKafkaSinkProfileAll` with
`SF_PROFILE_ALL_KAFKA_SINK=true`. The harness reuses one broker and input corpus, performs one warmup
per engine/query, and writes `flink-q*.jfr` and `streamfusion-q*.jfr` under
`-Dprofile.outputDir=...`. It invokes `asprof` from `PATH` by default; override that executable with
`-Dprofile.asprof=...`.

## Parquet and Delta sink diagnostics

These diagnostics use the readme-like 2M-event Kafka JSON workload with four input partitions,
parallelism four, memory state, mini-batching disabled, one warmup, and the best of three measured
runs. They cover q0–q5 and q7–q23; q6 is omitted because stock Flink cannot execute it. Unlike the
headline table, these runs measure local data-file output rather than Kafka output.

Apple M1 Max, release + `mimalloc`, measured 2026-08-22:

| Sink | Completed | Suite geomean |
|---|---:|---:|
| Parquet physical changelog | 23/23 | **1.535×** |
| Delta (MOR for updating queries) | 23/23 | **1.522×** |
| Combined | 46/46 | **1.529×** |

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
`TIMESTAMP_LTZ` window path accepts fixed-offset post-1970 zones, while a host-local DST zone is an
intentional planner fallback.
