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

## Nexmark, parallelism 4

Apple M1 Max, release + `mimalloc`, measured 2026-08-11 with one warmup and the best of three
measured runs across all four backend/mode combinations. The memory columns compare Flink's
default heap state against StreamFusion's memory state; the disk columns compare stock Flink
RocksDB against StreamFusion's native RocksDB backend. Mini-batching ("on") uses the same
production-style configuration on both engines (`allow-latency=2s`, `size=50000`). Each cell is
StreamFusion throughput divided by Flink throughput within the same backend and mode.

| Query | Memory, off | Memory, on | Disk, off | Disk, on |
|---|---:|---:|---:|---:|
| q0 | **1.47×** | **1.29×** | **1.27×** | **1.37×** |
| q1 | **1.43×** | **1.28×** | **1.47×** | **1.46×** |
| q2 | **1.04×** | **1.05×** | **1.04×** | **1.04×** |
| q3 | 0.98× | 0.99× | 0.90× | 0.88× |
| q4 | **1.20×** | **1.57×** | **21.69×** | **15.03×** |
| q5 | **1.24×** | **1.20×** | **6.45×** | **6.62×** |
| q7 | **1.53×** | **1.85×** | **16.87×** | **12.06×** |
| q8 | 0.98× | 0.97× | **1.01×** | 0.97× |
| q9 | **1.31×** | **1.73×** | **3.28×** | **2.96×** |
| q10 | **1.35×** | **1.36×** | **1.51×** | **1.49×** |
| q11 | **2.03×** | **2.02×** | **8.65×** | **9.27×** |
| q12 | **1.16×** | **1.16×** | **1.05×** | **1.07×** |
| q13 | **1.33×** | **1.33×** | **1.28×** | **1.23×** |
| q14 | **1.50×** | **1.35×** | **1.54×** | **1.49×** |
| q15 | **1.60×** | **1.35×** | **4.80×** | **1.21×** |
| q16 | **1.24×** | **1.49×** | **5.61×** | **1.94×** |
| q17 | **1.29×** | **1.28×** | 0.32× | **1.03×** |
| q18 | **1.51×** | **1.58×** | **2.66×** | **1.80×** |
| q19 | **1.23×** | **3.24×** | **2.04×** | **4.50×** |
| q20 | **1.29×** | **1.38×** | **2.87×** | **4.36×** |
| q21 | **1.22×** | **1.23×** | **1.23×** | **1.23×** |
| q22 | **1.36×** | **1.38×** | **1.28×** | **1.39×** |
| q23 | **1.85×** | **3.87×** | **7.28×** | **15.15×** |
| **geomean** | **1.33×** | **1.47×** | **2.40×** | **2.36×** |

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

For a disk-output diagnostic using the same Kafka input, memory state, parallelism, and one-second
checkpoints, set `SF_MATRIX_PARQUET_SINK=true` and run
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
SF_BENCHMARK=true SF_MATRIX_PARQUET_SINK=true SF_MATRIX_KAFKA=false \
  SF_MATRIX_GENERATOR=false SF_MATRIX_PARQUET=false SF_MATRIX_QUERIES=q0,q4 \
  SF_PARQUET_OUTPUT=/tmp/streamfusion-nexmark-parquet \
  mvn -pl :streamfusion-runtime test -Pbench \
  -Dtest='NexmarkMatrixBenchmark#changelogParquetSinkComparison'
```

Set `SF_PROFILE_PARQUET_SINK=true` and run `changelogParquetSinkProfile` for matched q0 CPU
recordings of the Flink and StreamFusion writers. The harness performs an unprofiled warmup, then
loops each writer for `-Dprofile.seconds=20` by default and writes both JFR files below
`-Dprofile.outputDir=...`.
