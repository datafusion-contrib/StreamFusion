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

The steelman: the source is the rowwise `nexmark` datagen (wide event row) and the sink is
`blackhole` (also rowwise) — exactly the published Nexmark plan — so a native island pays a
`RowData → Arrow` transpose at the source **and** an `Arrow → RowData` transpose at the sink. Both
transposes are kept in the measured path on purpose: a real deployment feeds rowwise records and
drains to a rowwise sink, so this is the honest end-to-end number. Object reuse is on for both
engines (standard tuned-prod setting).

StreamFusion runs **every runnable Nexmark query** (q0–q5, q7–q23) natively end-to-end with no
fallback and no flags; only q6 stays out, because Flink SQL itself can't run it
([analysis](.claude/wontdos/39-nexmark-q6-exclusion.md)). These are the current 500K-event release
measurements (2026-07-12), using `mimalloc`, Flink's default configuration with object reuse on, and
the same compressed source bytes for both engines. The Kafka columns compare stock Flink with the
complete native poll-and-decode path, not a selectively faster intermediate rung.

| Query | Shape | From RowData | From Parquet file | From Fluss | From JSON on Kafka | From Avro on Kafka | From Protobuf on Kafka |
|---|---|---|---|---|---|---|---|
| q0 | pass-through projection of `bid` | **1.46×** | **3.65×** | **2.71×** | **2.64×** | **3.48×** | **2.71×** |
| q1 | `0.908 * price` — exact `Decimal128` | **1.22×** | **3.74×** | **2.71×** | **2.70×** | **3.43×** | **2.70×** |
| q2 | filter `WHERE MOD(auction, 123) = 0` | **1.35×** | **2.83×** | **2.94×** | **2.16×** | **2.50×** | **2.03×** |
| q3 | updating join `auction ⋈ person` | 0.98× | **3.46×** | **2.12×** | **2.04×** | **2.10×** | **1.76×** |
| q4 | regular join → `MAX` → `AVG` per category | **1.60×** | **3.66×** | **1.67×** | **2.60×** | **3.06×** | **2.74×** |
| q5 | Hot Items (window re-agg + window join) | **1.31×** | **3.92×** | **1.74×** | **2.94×** | **3.74×** | **2.94×** |
| q7 | tumble `MAX` ⋈ bid | **1.60×** | **4.37×** | **2.86×** | **3.37×** | **3.99×** | **3.68×** |
| q8 | tumble windowed-distinct ⋈ join | 0.86× | **4.17×** | **2.56×** | **2.54×** | **2.70×** | **2.63×** |
| q9 | regular join → `ROW_NUMBER` (≤ 1) | **1.30×** | **1.83×** | **1.60×** | **2.25×** | **2.19×** | **2.20×** |
| q10 | `DATE_FORMAT` projection | **1.46×** | **3.92×** | **3.21×** | **2.79×** | **2.80×** | **2.26×** |
| q11 | session-window `COUNT` per bidder | **2.78×** | **5.19×** | **4.06×** | **4.50×** | **5.15×** | **4.64×** |
| q12 | proctime tumble `COUNT` per bidder | **1.46×** | **3.55×** | — | **2.31×** | **2.60×** | **2.14×** |
| q13 | lookup join (bounded dimension) | **1.11×** | **2.60×** | **2.16×** | **2.37×** | **2.61×** | **2.28×** |
| q14 | `HOUR`/`CASE` + `count_char` UDF + decimal | **1.08×** | **3.40×** | **2.51×** | **2.79×** | **3.29×** | **2.98×** |
| q15 | multi-`DISTINCT` `COUNT`s per day | **1.63×** | **2.26×** | **1.13×** | **2.94×** | **2.87×** | **2.20×** |
| q16 | multi-`DISTINCT` per channel/day | **1.32×** | **1.42×** | 0.99× | **1.83×** | **1.84×** | **1.50×** |
| q17 | group agg + `AVG`/`MIN`/`MAX`/`SUM` per day | **1.43×** | **1.82×** | **1.20×** | **2.73×** | **2.72×** | **2.43×** |
| q18 | `ROW_NUMBER` dedup (≤ 1) | **1.26×** | **2.41×** | **1.68×** | **2.64×** | **3.18×** | **2.99×** |
| q19 | `ROW_NUMBER` topN (≤ 10) | **1.41×** | **1.59×** | **2.71×** | **1.88×** | **1.73×** | **1.77×** |
| q20 | updating join (`category = 10`) | 0.95× | **4.01×** | **2.40×** | **2.81×** | **3.55×** | **2.95×** |
| q21 | `CASE` + `REGEXP_EXTRACT`/`LOWER` — byte-parity | **1.08×** | **2.38×** | **1.78×** | **2.52×** | **2.99×** | **2.69×** |
| q21 † | …opt-in native regex/case | **1.86×** | **5.41×** | **4.32×** | **2.58×** | **3.02×** | **3.00×** |
| q22 | `SPLIT_INDEX(url, '/', n)` projection | **1.46×** | **4.37×** | **3.09×** | **2.38×** | **2.82×** | **2.54×** |
| q23 | three-way join `bid ⋈ person ⋈ auction` | **1.14×** | **4.38×** | **2.30×** | **2.10×** | **2.94×** | **2.26×** |

From `RowData`, 20 of 23 default queries win; only the perimeter-transpose/join-state cluster
trails (q3, q8, and q20 just under parity). The opt-in q21 path is faster still, but deliberately
gives up edge-case compatibility with Flink's regex and case rules
([docs/optimizations.md](docs/optimizations.md) has the hot-path ledger).

The columnar sources remain the clear strength: **all 23 Parquet queries win (1.42–5.19×) and 21
of 22 measurable Fluss queries win (1.13–4.06×)** — q16 sits at 0.99× and q12 has no deterministic
unbounded finish line.

**Every Kafka cell wins — 1.50× to 5.15×.** The Kafka tables declare the canonical Nexmark
watermark, and the native source now regenerates it per split (previous charts silently fell back
to Flink's consume+decode on exactly these cells, compressing them to near parity). The format
decode runs inside the native poll, dispatched through a versioned cross-library ABI; see
[docs/benchmarks.md](docs/benchmarks.md) for the source ladder and every intermediate rung.

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
