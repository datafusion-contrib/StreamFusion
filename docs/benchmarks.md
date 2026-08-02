# Benchmarks

## Methodology

- **Nexmark, end to end.** The headline numbers below are an end-to-end, exactly-once Kafka
  pipeline — not a blackhole sink. Stock Flink and StreamFusion run at **parallelism 4**, read the
  same 2M-event Kafka JSON corpus from a four-partition topic (one split per source subtask), and
  publish each query's result to a fresh Kafka topic with a one-second checkpoint interval.
  Append-only queries use `kafka`; updating queries use `upsert-kafka` with the result's actual
  primary key. Each timed run includes source consumption, query execution, the keyed shuffle,
  serialization, Kafka writes, checkpoints, and the bounded job's final transaction commit. q6 is
  omitted because Flink SQL itself cannot run it.
- **Both perimeter transposes stay in the measured path.** Nexmark's own source emits Flink
  `RowData` (not a columnar source) and sinks to a rowwise consumer, so a native island pays a
  RowData→Arrow transpose at the source and an Arrow→RowData transpose at the sink — the same cost
  a real rowwise-fed deployment pays. The benchmark harness is never modified to dodge this; an
  operator StreamFusion can't run natively shows up as an honest fallback or a slower number, which
  is the signal to fix the engine, not the harness.
  Between co-located subtasks StreamFusion's shuffle moves Arrow batches by ownership transfer
  (zero serialization); a multi-TaskManager deployment's cross-process edges pay Arrow IPC instead
  — measured at ~11% on the shuffle-heaviest cells and nothing elsewhere.
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

Apple M1 Max, release + `mimalloc`, best of two measured runs, across all four backend/mode
combinations (memory columns measured 2026-08-02, disk columns 2026-07-28). The memory columns
compare Flink's default heap state against StreamFusion's memory state; the disk columns compare
the production persistent backends — stock Flink on RocksDB against StreamFusion on its
[Paimon state backend](backends/paimon.md). Mini-batching ("on") uses the same production-style
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

The disk columns' key enabler is deletion-vector mode: stock Java Paimon maintains the state
tables' deletion vectors synchronously at each barrier, so every committed read is a raw parquet
scan with exact predicate pushdown — no merge reads, no resident index — which is why the
stateful shapes RocksDB pays per-record for show the largest disk-column wins (up to 8.9× on
session windows).

_Apple M1 Max; numbers are comparable only within a machine._

## Reproducing

```sh
SF_BENCHMARK=true mvn -pl :streamfusion-runtime test -Pbench
```

runs the end-to-end suites (`ThroughputBenchmark`, `NexmarkBenchmark`, `NexmarkKafkaBenchmark`,
`NexmarkMatrixBenchmark`); the `-Pbench` profile is required. The Criterion micro-benchmarks run
independently with `cd native && cargo bench`.
