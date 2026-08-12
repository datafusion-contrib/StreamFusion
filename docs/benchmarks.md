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

## Nexmark, parallelism 4

!!! warning
    This table predates the local-handle exclusion and will be refreshed by the next benchmark
    run. Do not compare new results against it as if the methodology were identical.

Apple M1 Max, release + `mimalloc`, best of two measured runs, across all four backend/mode
combinations (memory columns measured 2026-08-02, disk columns 2026-07-28). The memory columns
compare Flink's default heap state against StreamFusion's memory state; the disk columns compare
the production persistent backends — stock Flink on RocksDB against StreamFusion on its
retired columnar state backend. Mini-batching ("on") uses the same production-style
configuration on both engines (`allow-latency=2s`, `size=50000`). Each cell is StreamFusion
throughput divided by Flink throughput within the same backend and mode.

| Query | Memory, off | Memory, on | Disk, off | Disk, on |
|---|---:|---:|---:|---:|
| q0 | **1.71×** | **1.32×** | **1.68×** | **1.68×** |
| q1 | **1.58×** | **1.15×** | **1.69×** | **1.58×** |
| q2 | **1.60×** | **1.24×** | **1.33×** | 0.95× |
| q3 | **1.21×** | **1.40×** | **1.04×** | 0.64× |
| q4 | **1.19×** | **1.83×** | **1.66×** | **1.78×** |
| q5 | **1.44×** | 0.92× | **2.11×** | **2.43×** |
| q7 | **1.43×** | **1.87×** | **1.96×** | **3.19×** |
| q8 | 0.90× | **1.16×** | **1.21×** | **1.90×** |
| q9 | **1.45×** | **1.60×** | **1.75×** | **1.67×** |
| q10 | **1.33×** | **1.34×** | **1.51×** | **1.46×** |
| q11 | **2.73×** | **2.67×** | **9.37×** | **9.61×** |
| q12 | **1.38×** | **1.79×** | **2.22×** | **2.11×** |
| q13 | **1.21×** | **1.32×** | **1.45×** | **1.20×** |
| q14 | **1.45×** | **1.47×** | **1.53×** | **1.59×** |
| q15 | **3.10×** | **1.64×** | **6.62×** | **1.96×** |
| q16 | **1.53×** | **1.35×** | **4.01×** | **2.35×** |
| q17 | **1.69×** | **1.21×** | **1.96×** | **1.66×** |
| q18 | **1.60×** | **1.62×** | 0.99× | **2.67×** |
| q19 | **1.40×** | **2.69×** | **1.27×** | **2.21×** |
| q20 | **1.25×** | **1.46×** | **1.21×** | **1.47×** |
| q21 | **1.06×** | **1.29×** | **1.13×** | **1.16×** |
| q22 | **1.20×** | **1.54×** | **1.45×** | **1.41×** |
| q23 | **1.75×** | **2.05×** | **2.00×** | **2.93×** |
| **geomean** | **1.47×** | **1.51×** | **1.83×** | **1.84×** |

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
