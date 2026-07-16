> [!NOTE]
> This project is not part of Apache Flink or Apache DataFusion.

# StreamFusion

[![CI](https://github.com/datafusion-contrib/StreamFusion/actions/workflows/ci.yml/badge.svg)](https://github.com/datafusion-contrib/StreamFusion/actions/workflows/ci.yml)

Run Apache Flink SQL faster by executing supported operators natively (Rust + Apache
Arrow/DataFusion over JNI) while Flink continues to own planning, coordination, and
everything not yet supported. Substitution is transparent and conservative: a query is
planned by Flink, the operators we can reproduce **exactly** are swapped for native ones, and
anything else falls back to Flink with identical results.

It is DataFusion Comet's idea — a native, columnar accelerator behind an unchanged SQL
front end — applied to streaming instead of batch: stateful windowing, joins, aggregations,
changelog processing, and columnar sources/sinks, not just stateless projection and filter.

## What it accelerates

A query accelerates only when it forms **one fully-columnar island**: every operator except a
rowwise source/sink runs natively, exchanging Arrow batches (the row↔Arrow transpose is paid
once at the host edges, never between native operators). A single unsupported interior operator
drags the whole query back to Flink.

Native coverage is broad — most of the streaming SQL surface:

- **Stateless:** projection/`Calc`, filter, `UNION ALL`, `GROUPING SETS`/`CUBE`/`ROLLUP`, `UNNEST`.
- **Windowed aggregates:** `TUMBLE`/`HOP`/`SESSION`/`CUMULATE` (event-time and proctime, one- and
  two-phase), and `OVER` window functions.
- **Joins:** regular (updating) equi-joins, event-time/proctime interval and window joins,
  event-time temporal-table joins, and processing-time lookup joins (sync and async).
- **Changelog:** non-windowed `GROUP BY`, streaming Top-N / `LIMIT`, deduplication, changelog
  normalization — all consuming and emitting a retract changelog.
- **Connectors:** a Parquet file source (native Arrow scan, local paths) and a Parquet sink that
  writes to any filesystem Flink supports (`s3:`/`gs:`/`abfs:`/`hdfs:`/…, `PARTITIONED BY` and
  partition commit included — native encoding drained into Flink's own recoverable streams); Kafka
  source ingest for JSON/CSV/raw/Avro/protobuf and Debezium/OGG CDC — native rdkafka consumes and
  the independently installed format artifact decodes inside the same poll, invoked through a
  versioned C ABI it hands the connector at runtime (never linked). Watermarked Kafka tables remain
  on Flink for now.
- **UDFs:** a Flink `ScalarFunction` the expression engine can't implement itself is invoked over
  Arrow columns by a native→JVM upcall (Comet's `JvmScalarUdfExpr` pattern), one JNI crossing per
  batch, so the pipeline stays native *through* the UDF and the result is byte-identical.

The exact per-operator terms, and **every** condition that causes a fallback (unsupported
operators, types, expressions, and connector options), live in
**[docs/coverage-and-fallbacks.md](docs/coverage-and-fallbacks.md)** — the single source of truth
for what does and doesn't run natively. The short version of what stays on Flink: lateral table
functions and `MATCH_RECOGNIZE`, PyFlink UDFs, the three-phase distinct aggregate, remote
(`hdfs:`/`s3:`) file paths, a handful of expression/type edges where native execution would
diverge from the JVM (opt-in behind `allowIncompatible`), and connector options we can't yet
reproduce bit-identically (Maxwell/Canal CDC, some protobuf field types).

**Determinism.** Results are byte-identical to stock Flink for everything admitted. The one caveat
is late-data dropping on out-of-order event-time streams, where Flink is itself non-deterministic
(periodic watermarks); we match Flink's deterministic path, which governs in-order data and every
benchmark. Details in [divergences/09](divergences/09-per-batch-watermark-assignment.md).

## Inspiration

StreamFusion is built by porting established engines rather than reinventing operators:

- **[DataFusion Comet](https://github.com/apache/datafusion-comet)** — the model for the whole
  project (native columnar accelerator behind an unchanged SQL planner) and the reference for the
  JNI / Arrow C Data Interface bridge, off-heap memory accounting, the config surface, and
  fallback-reason reporting.
- **[Arroyo](https://github.com/ArroyoSystems/arroyo)** — the streaming-operator implementations
  we port (it already runs on DataFusion); the reference for join/window/changelog logic.
- **[Apache DataFusion](https://github.com/apache/datafusion)** — the native execution and
  expression engine underneath (hash joins, aggregates, Arrow kernels).
- **[RisingWave](https://github.com/risingwavelabs/risingwave)** — the reference for changelog
  semantics and memcomparable arrow-row state encoding.
- **[Apache Flink](https://github.com/apache/flink)** — the **parity target**: every operator is a
  faithful port of Flink's own, verified for identical output by a parity harness.

Divergences from these references are recorded in [`divergences/`](divergences/).

## Nexmark benchmarks

The headline benchmark is an end-to-end, exactly-once Kafka pipeline—not a blackhole sink. Stock
Flink and StreamFusion read the same 500K-event Kafka JSON corpus and publish each query result to a
fresh Kafka topic with a one-second checkpoint interval. Append-only queries use `kafka`; updating
queries use `upsert-kafka` with the result's actual primary key. Each timed run includes source
consumption, query execution, serialization, Kafka writes, checkpoints, and the bounded job's final
transaction commit.

On StreamFusion, Kafka poll/decode, every supported operator, and sink key/value/tombstone
serialization stay native and columnar; Flink's unmodified `KafkaSink` owns producer I/O and the
exactly-once transaction lifecycle. The native plan is asserted for every cell. q6 is omitted because
Flink SQL itself cannot run it ([analysis](.claude/wontdos/39-nexmark-q6-exclusion.md)).

These are the 2026-07-16 Apple M1 Max release+`mimalloc` results, best of two after one warmup.
Mini-batching uses the same production-style configuration on both engines
(`allow-latency=2s`, `size=50000`). Throughput is millions of input events per second. `SF/Flink`
compares engines within one mode; `on/off` measures the effect of enabling mini-batching on that
engine, where a value below 1× is a regression.

| Query | Flink off | StreamFusion off | SF/Flink off | Flink on | StreamFusion on | SF/Flink on | Flink on/off | SF on/off |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| q0 | 0.211 M/s | 0.320 M/s | **1.51×** | 0.233 M/s | 0.281 M/s | **1.20×** | **1.10×** | 0.88× |
| q1 | 0.196 M/s | 0.324 M/s | **1.65×** | 0.190 M/s | 0.270 M/s | **1.42×** | 0.97× | 0.83× |
| q2 | 0.324 M/s | 0.534 M/s | **1.65×** | 0.324 M/s | 0.540 M/s | **1.67×** | 1.00× | **1.01×** |
| q3 | 0.438 M/s | 0.630 M/s | **1.44×** | 0.429 M/s | 0.623 M/s | **1.45×** | 0.98× | 0.99× |
| q4 | 0.285 M/s | 0.491 M/s | **1.72×** | 0.286 M/s | 0.579 M/s | **2.02×** | 1.00× | **1.18×** |
| q5 | 0.403 M/s | 0.592 M/s | **1.47×** | 0.394 M/s | 0.538 M/s | **1.37×** | 0.98× | 0.91× |
| q7 | 0.265 M/s | 0.672 M/s | **2.54×** | 0.236 M/s | 0.565 M/s | **2.40×** | 0.89× | 0.84× |
| q8 | 0.437 M/s | 0.572 M/s | **1.31×** | 0.443 M/s | 0.565 M/s | **1.28×** | **1.01×** | 0.99× |
| q9 | 0.233 M/s | 0.128 M/s | 0.55× | 0.210 M/s | 0.372 M/s | **1.77×** | 0.90× | **2.91×** |
| q10 | 0.210 M/s | 0.290 M/s | **1.38×** | 0.202 M/s | 0.229 M/s | **1.14×** | 0.96× | 0.79× |
| q11 | 0.252 M/s | 0.661 M/s | **2.62×** | 0.259 M/s | 0.675 M/s | **2.61×** | **1.03×** | **1.02×** |
| q12 | 0.361 M/s | 0.557 M/s | **1.54×** | 0.364 M/s | 0.560 M/s | **1.54×** | **1.01×** | 1.00× |
| q13 | 0.268 M/s | 0.394 M/s | **1.47×** | 0.259 M/s | 0.370 M/s | **1.43×** | 0.97× | 0.94× |
| q14 | 0.221 M/s | 0.320 M/s | **1.45×** | 0.225 M/s | 0.323 M/s | **1.43×** | **1.02×** | **1.01×** |
| q15 | 0.157 M/s | 0.190 M/s | **1.21×** | 0.331 M/s | 0.587 M/s | **1.77×** | **2.11×** | **3.08×** |
| q16 | 0.122 M/s | 0.173 M/s | **1.42×** | 0.256 M/s | 0.481 M/s | **1.88×** | **2.09×** | **2.78×** |
| q17 | 0.197 M/s | 0.200 M/s | **1.02×** | 0.314 M/s | 0.478 M/s | **1.52×** | **1.60×** | **2.39×** |
| q18 | 0.204 M/s | 0.201 M/s | 0.99× | 0.167 M/s | 0.193 M/s | **1.15×** | 0.82× | 0.96× |
| q19 | 0.053 M/s | 0.031 M/s | 0.58× | 0.043 M/s | 0.238 M/s | **5.49×** | 0.82× | **7.73×** |
| q20 | 0.301 M/s | 0.467 M/s | **1.55×** | 0.261 M/s | 0.458 M/s | **1.76×** | 0.87× | 0.98× |
| q21 | 0.335 M/s | 0.693 M/s | **2.07×** | 0.342 M/s | 0.767 M/s | **2.24×** | **1.02×** | **1.11×** |
| q22 | 0.249 M/s | 0.326 M/s | **1.31×** | 0.245 M/s | 0.328 M/s | **1.34×** | 0.99× | **1.01×** |
| q23 | 0.144 M/s | 0.120 M/s | 0.83× | 0.097 M/s | 0.127 M/s | **1.31×** | 0.67× | **1.06×** |

With mini-batching disabled, StreamFusion wins 19 of 23 queries. With it enabled, the full matrix
wins all 23, from 1.14× to 5.49×. The largest direct mini-batch gains occur where logical bundles
remove Kafka-visible changelog churn: q9 2.91×, q15 3.08×, q16 2.78×, q17 2.39×, and q19 7.73×
over StreamFusion's own disabled path. A focused repeat reproduced those conclusions
(2.69×/3.09×/2.52×/2.04×/8.16× direct gains; 1.73×/2.30×/1.94×/1.56×/4.80× versus enabled Flink).

q23 is the noisy boundary: it measured 1.31× enabled in the complete run and 0.99× in the focused
repeat, so it should be treated as parity rather than a stable lead. Mini-batching also costs some
append-only queries because they have no changelog churn to amortize. The multi-source/blackhole
ladder, raw timings, focused repeat, reproduction command, and profiling controls remain in
[docs/benchmarks.md](docs/benchmarks.md).

_Apple M1 Max; numbers are comparable only within a machine._

## Running and configuration

### Install

#### Kubernetes or Docker

Build the universal release artifacts, then build and publish a job-neutral Flink base image:

```sh
bin/build-release.sh
bin/build-flink-image.sh --tag registry.example/streamfusion-flink:dev --push
```

Use that image as `spec.image` in a Flink Kubernetes Operator `FlinkDeployment`, or as
`kubernetes.container.image.ref` for Flink's native Kubernetes deployment. It works for either
Session or Application mode:

- **Session:** run the JobManager, TaskManagers, and the SQL/client process from the StreamFusion
  image; submit job JARs through your normal REST, SQL Gateway, or `FlinkSessionJob` path.
- **Application:** derive a job image from the StreamFusion base image, place the job JAR in
  `/opt/flink/usrlib`, and use that image in the Application deployment. Remote job-artifact
  delivery remains supported too.

The pushed tag is a Linux x86_64/ARM64 manifest. The runtime picks the matching native library
inside each pod automatically. StreamFusion itself is in Flink's `lib` directory; do not add it to
the job JAR.

The base image is connector- and format-neutral. Derive a small image and install Flink's connector
and format JARs, the matching StreamFusion connector JAR, and only the StreamFusion format JARs your
jobs use into `/opt/flink/lib`; use that same image for the JobManager, TaskManagers, and submission
client. For example, JSON on Kafka needs four JARs:

```Dockerfile
FROM registry.example/streamfusion-flink:dev
COPY flink-connector-kafka-5.0.0-2.2.jar /opt/flink/lib/
COPY flink-json-2.2.1.jar /opt/flink/lib/
COPY streamfusion-kafka/target/streamfusion-kafka-1.0-SNAPSHOT.jar /opt/flink/lib/
COPY streamfusion-json/target/streamfusion-json-1.0-SNAPSHOT.jar /opt/flink/lib/
```

Replace `streamfusion-json` with `streamfusion-csv`, `streamfusion-raw`, `streamfusion-avro`, or
`streamfusion-protobuf` and add Flink's like-named format JAR. `avro-confluent` uses the standalone
`streamfusion-avro-confluent-registry` JAR with Flink's `flink-avro-confluent-registry`. Use
`fluss-flink-2.2` with `streamfusion-fluss`, or `flink-parquet` with `streamfusion-parquet`, the
same way. The core image does not require any of them.

#### Bare metal

For a local Flink distribution instead:

```sh
bin/build-release.sh
sh bin/install-flink.sh "$FLINK_HOME"
```

Restart Flink after installation, then submit ordinary streaming SQL jobs as usual—no application
dependency or `NativePlanner.install(...)` call is needed.

StreamFusion currently supports **Flink 2.2.x**. The release build enables `mimalloc` by default.

For local development, `mvn compile` is Java-only and does not invoke Cargo. `mvn test` builds the
host debug native library once before executing tests. Build the portable optimized artifacts only
when needed for an image or release with `bin/build-release.sh`.

**Deployment JVM flags** — run the TaskManager JVM with Arrow's safety checks off (as Comet/Spark
do); profiling showed ~1/3 of the transpose CPU was per-accessor bounds/refcount checks:

```
-Darrow.enable_unsafe_memory_access=true -Darrow.enable_null_check_for_get=false
```

**Configuration** (JVM system properties, mirroring Comet's config surface):

- `-Dstreamfusion.native.enabled=false` — master switch; run entirely on Flink.
- `-Dstreamfusion.operator.<name>.enabled=false` — keep one operator on the host (e.g. leave a lone
  cheap `filter` on a row source, which can't earn back the transpose round-trip).
- `-Dstreamfusion.expression.<NAME>.allowIncompatible=true` — opt into the faster pure-Rust path for
  expressions that otherwise use a byte-exact JVM upcall or fall back (`UPPER`/`LOWER`/
  `REGEXP_EXTRACT`, `ROUND` on float, transcendental math). Off by default (parity-first).
- `-Dstreamfusion.memory.accounting.enabled` (default on) — native stateful operators reserve an
  operator-scope share of the slot's managed memory from Flink's `MemoryManager` and bound their
  state by it, failing with a `NativeMemoryLimitException` naming the remedy rather than an
  unattributed OOM ([divergences/16](divergences/16-upfront-managed-memory-reservation.md)).

**Seeing why a query fell back** — substitution is silent by default.
`-Dstreamfusion.logFallbackReasons=true` logs each node that stayed on Flink and why as the plan is
decided. `EXPLAIN` shows native nodes such as `NativeCalc` for an accelerated plan.

**Benchmarks** — the end-to-end suites (`ThroughputBenchmark`, `NexmarkBenchmark`,
`NexmarkKafkaBenchmark`, `NexmarkMatrixBenchmark`) run under
`SF_BENCHMARK=true mvn -pl :streamfusion-runtime test -Pbench`;
the `-Pbench` profile is required (it loads the **release** native library — the debug build is
~10–20× slower and misleading). The Criterion micro-benchmarks run with `cd native && cargo bench`.
See [docs/benchmarks.md](docs/benchmarks.md).

## Related work

Three native Flink accelerators exist, all **closed source**:

- **Flash** (Alibaba Cloud) — a C++ native + SIMD vectorized engine with a custom state backend
  (ForStDB). Stateful, production-deployed at scale; claims 5–10× on streaming Nexmark, 3×+ on batch
  TPC-DS, and ~50% cost reduction across 100k+ compute units. Proprietary, on Alibaba Cloud.
  ([blog](https://www.alibabacloud.com/blog/flash-a-next-gen-vectorized-stream-processing-engine-compatible-with-apache-flink_602088))
- **Vera X** (Ververica, the original Flink creators) — a proprietary native vectorized engine with
  a drop-in compatibility layer and a new state store. Stateful; claims 5–10× on Nexmark SQL and
  ~52% lower resource usage. Implementation undisclosed.
  ([blog](https://www.ververica.com/blog/vera-x-introducing-the-first-native-vectorized-apache-flink-engine))
- **Iron Vector** (Irontools) — the same stack as us (Rust + Arrow + DataFusion over zero-copy JNI,
  Substrait plan serialization, transparent fallback), but **stateless only** today (projections,
  filters, expressions); windows, joins, and exactly-once are described as planned. Claims ~97%
  higher throughput on a stateless ETL pipeline.
  ([blog](https://irontools.dev/blog/introducing-iron-vector/))

Where StreamFusion differs: it is **open source**, and every substitution is gated and verified for
identical results against stock Flink by a parity harness rather than asserted. It is already native
on stateful windowing, joins, and changelog processing — the hard, closed part of the field — where
Iron Vector is stateless-only; it is earlier-stage than Flash and Vera X and doesn't match their
operator breadth or published benchmarks, but its acceleration is auditable and parity-first by
construction.

## License

Licensed under the Apache License, Version 2.0 ([LICENSE](LICENSE) or
<https://www.apache.org/licenses/LICENSE-2.0>).

Unless you explicitly state otherwise, any contribution intentionally submitted for
inclusion in the work by you, as defined in the Apache-2.0 license, shall be licensed
as above, without any additional terms or conditions.
