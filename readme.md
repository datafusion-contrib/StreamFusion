> [!NOTE]
> This project is not part of Apache Flink or Apache DataFusion.

# StreamFusion

[![CI](https://github.com/datafusion-contrib/StreamFusion/actions/workflows/ci.yml/badge.svg)](https://github.com/datafusion-contrib/StreamFusion/actions/workflows/ci.yml)
[![Maven Central](https://img.shields.io/maven-central/v/tech.streamfusion/streamfusion-loader.svg?label=Maven%20Central)](https://central.sonatype.com/artifact/tech.streamfusion/streamfusion-loader)
[![Discord](https://img.shields.io/badge/Discord-join-5865F2?logo=discord&logoColor=white)](https://discord.gg/BsUVHZpZaP)
[![Slack](https://img.shields.io/badge/Slack-join-4A154B?logo=slack&logoColor=white)](https://join.slack.com/t/streamfusion/shared_invite/zt-49ckmlwmo-6DIjwC1vwJ3qjWmtTHId0A)

**[Read the docs](https://datafusion-contrib.github.io/StreamFusion/)** — connector/format
coverage, per-operator native/fallback status, state backends, deployment, and configuration.

Run Apache Flink SQL faster by executing supported operators natively (Rust + Apache
Arrow/DataFusion over JNI) while Flink continues to own planning, coordination, and
everything not yet supported. Substitution is transparent and conservative: a query is
planned by Flink, the jobs we can reproduce **exactly** are swapped for native ones, and
anything else falls back to Flink with identical results.

## What it accelerates

A query accelerates only when it forms **all**: every operator except a
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
- **Connectors:** Parquet and ORC sinks that write to any filesystem Flink supports
  (`s3:`/`gs:`/`abfs:`/`hdfs:`/…, `PARTITIONED BY` and partition commit included — native encoding
  drained into Flink's own recoverable streams; filesystem reads use Flink's stock source); Delta Lake
  append and merge-on-read sinks, with Delta 4.4 owning table semantics and Rust writing Parquet
  bytes; Apache Paimon streaming sources and sinks using native Parquet/ORC decoding and encoding,
  Arrow bucket routing and key merging while Paimon 2.0 keeps its manifests, commits, and
  in-job compaction; and Kafka source ingest and sink output for JSON/CSV/raw/Avro/protobuf and supported CDC
  formats. Flink's Kafka clients own consumption, production, offsets, and transactions while Rust
  performs the format serialization/deserialization; supported periodic source watermarks retain
  Flink's per-partition watermark semantics.
- **UDFs:** a Flink `ScalarFunction` the expression engine can't implement itself is invoked over
  Arrow columns by a native→JVM upcall (Comet's `JvmScalarUdfExpr` pattern), one JNI crossing per
  batch, so the pipeline stays native *through* the UDF and the result is byte-identical.

The exact per-operator terms, and **every** condition that causes a fallback (unsupported
operators, types, expressions, and connector options), live on the docs site's
**[Operators](https://datafusion-contrib.github.io/StreamFusion/operators/)** and
**[Connectors](https://datafusion-contrib.github.io/StreamFusion/connectors/)** sections — one page
per operator/connector/format, each precisely marked native, partial, or unsupported; together
they're the single source of truth for what does and doesn't run natively. The short version of
what stays on Flink: lateral table functions and `MATCH_RECOGNIZE`, PyFlink UDFs, the three-phase
distinct aggregate, remote (`hdfs:`/`s3:`) file paths, a handful of expression/type edges where
native execution would diverge from the JVM (opt-in behind `allowIncompatible`), and connector
options we can't yet reproduce bit-identically (Maxwell/Canal CDC, some protobuf field types).

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
Flink and StreamFusion run at **parallelism 4**, read the same 2M-event Kafka JSON corpus from a
four-partition topic (one split per source subtask), and publish each query result to a fresh
Kafka topic with a one-second checkpoint interval. Append-only queries use `kafka`; updating
queries use `upsert-kafka` with the result's actual primary key. Each timed run includes source
consumption, query execution, the keyed shuffle, serialization, Kafka writes, checkpoints, and
the bounded job's final transaction commit. Both consumers use `max.poll.records=8192`, and both
producers use `batch.size=524288` with `linger.ms=20`; these are shared workload settings, not
StreamFusion-only tuning. The benchmark profile disables
StreamFusion's optional same-JVM handle-table shuffle, so both engines pay their normal record
serialization costs and StreamFusion uses the same Arrow IPC format it uses across TaskManagers.

On StreamFusion, Flink retains Kafka enumeration, partition assignment, offsets, checkpointing,
client, and exactly-once sink paths; a split-aware reader decodes Kafka bytes directly to Arrow in
Rust. Rust also accelerates every supported operator and sink key/value/tombstone serialization. q6
is omitted because Flink SQL itself cannot run it
([analysis](.claude/wontdos/39-nexmark-q6-exclusion.md)).

These are Apple M1 Max release+`mimalloc` results measured at parallelism 4. The memory columns
use one measured run per cell (2026-08-25); the disk columns use one warmup and the best of two
measured runs (2026-09-05, after the pinned batched-read and column-state-codec work). The memory
columns compare Flink's default heap state against StreamFusion's memory state; the disk columns
compare stock Flink RocksDB against StreamFusion's native RocksDB backend. The Kafka harness sets
the table session time zone to UTC so `TIMESTAMP_LTZ` window coverage is host-independent.
Mini-batching ("on") uses the same production-style configuration on both engines
(`allow-latency=2s`, `size=50000`). Each cell is StreamFusion throughput divided by Flink
throughput within the same backend and mode. Both the source corpus and every exactly-once
output topic carry one partition per subtask — an earlier revision of these tables let the
broker auto-create single-partition output topics, which throttled all four sink writers (on
both engines) behind one partition log. These tables include shared native sources: a query
whose branches scan the same topic reads and decodes it once, as Flink's own sub-plan reuse
already did for the stock plans.

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

Parallelism 4 is a tougher, more honest baseline than the earlier parallelism-1 tables: the keyed
shuffle is real work on both engines, and Flink's heap pipeline scales well with subtasks. The
shuffle-heavy changelog shapes were flat at first — a measured batch-collapse effect (the
exchange fragments every batch p ways, and per-batch fixed cost compounds through changelog
chains) that post-exchange coalescing since removed, worth up to 2× on the compounding shapes
(the A/B and the remaining source-side lever are in
[Benchmarks](https://datafusion-contrib.github.io/StreamFusion/benchmarks/)).
The persistent-backend columns put StreamFusion ahead in all 46 disk query/mode cells. Flink's
RocksDB path pays its per-record costs in every subtask, while StreamFusion batches native state
work; the largest gains are on q9 and q20, especially with mini-batching enabled.
The multi-source/blackhole ladder, raw timings, reproduction commands, and profiling controls
remain on the docs site's [Benchmarks](https://datafusion-contrib.github.io/StreamFusion/benchmarks/)
page.

_Apple M1 Max; numbers are comparable only within a machine._

## Running and configuration

```sh
STREAMFUSION_VERSION=0.1.0-rc4
curl --fail --location \
  "https://repo1.maven.org/maven2/tech/streamfusion/streamfusion-loader/$STREAMFUSION_VERSION/streamfusion-loader-$STREAMFUSION_VERSION.jar" \
  --output "$FLINK_HOME/lib/00-streamfusion-loader.jar"
curl --fail --location \
  "https://repo1.maven.org/maven2/tech/streamfusion/streamfusion-core/$STREAMFUSION_VERSION/streamfusion-core-$STREAMFUSION_VERSION-runtime.jar" \
  --output "$FLINK_HOME/lib/streamfusion-core.jar"
```

StreamFusion currently supports exactly **Flink 2.2.0 and 2.2.1**, installs into Flink's `lib`
directory (never the job JAR), and needs no application-side call to accelerate an ordinary
streaming SQL job. Release JARs come from Maven Central and already contain the runner-built Linux
x86_64 and macOS Apple Silicon native libraries; users do not build Rust or Java from source. The
base install is connector- and format-neutral — layer in only the `streamfusion-*` JARs your job's
connectors and formats actually need, mirroring Flink's own module split. See
**[Deployment](https://datafusion-contrib.github.io/StreamFusion/deployment/)** for the full
Kubernetes/Docker/bare-metal walkthrough and the exact JAR list per connector/format.

Every runtime behavior — the acceleration on/off switches, `allowIncompatible` expression opt-ins,
off-heap memory sizing for the native Kafka buffers, and how to see why a query fell back
(`-Dstreamfusion.logFallbackReasons=true`) — is documented on
**[Configuration](https://datafusion-contrib.github.io/StreamFusion/configuration/)**.

Reproducing the benchmarks above, and running the Criterion micro-benchmarks, is covered on
**[Benchmarks](https://datafusion-contrib.github.io/StreamFusion/benchmarks/)** — the short version
is `SF_BENCHMARK=true mvn -pl :streamfusion-runtime test -Pbench` for the end-to-end suites (the
`-Pbench` profile is required — the debug native library is ~10–20× slower and misleading) and
`cd native && cargo bench` for the operator micro-benchmarks.

## Fluss-to-Fluss Nexmark

The optional connector runs all 23 runnable queries with released Fluss 1.0.0 at both
ends. Append-only outputs use Arrow production; primary-key outputs retain Flink's
stock writer. Profiling removed redundant connection shutdowns, heap receive staging,
frame growth copies and whole-vector LZ4 staging.

Matched Linux release results use 2M events, parallelism 4, RocksDB with fixed
128 MiB state pools per slot, mini-batching off, exact expressions, one warmup and
three measured runs. These timings include startup, flushes and synchronous teardown.
Kafka uses JSON and exactly-once sinks; Fluss uses Arrow and at-least-once appends.

| Query | Stock Kafka, s | Native Kafka, s | Stock Fluss, s | Native Fluss, s | Kafka native / Fluss native |
| --- | ---: | ---: | ---: | ---: | ---: |
| q0 | 2.361 | 1.381 | 4.884 | 0.397 | 3.47× |
| q1 | 2.295 | 1.364 | 4.845 | 0.402 | 3.40× |
| q2 | 1.342 | 1.194 | 4.519 | 0.301 | 3.97× |
| q3 | 1.210 | 1.164 | 4.521 | 0.592 | 1.97× |
| q5 | 3.392 | 4.593 | 54.040 | 4.384 | 1.05× |
| q7 | 6.093 | 3.433 | 11.994 | 3.015 | 1.14× |
| q8 | 1.347 | 1.271 | 11.832 | 0.725 | 1.75× |
| q10 | 2.489 | 1.891 | 5.095 | 0.770 | 2.45× |
| q11 | 5.210 | 1.172 | 11.923 | 0.407 | 2.88× |
| q12 | 1.486 | 1.230 | 4.691 | 0.422 | 2.91× |
| q13 | 2.159 | 1.480 | 4.872 | 0.395 | 3.74× |
| q14 | 2.487 | 1.931 | 5.185 | 0.903 | 2.14× |
| q20 | 15.271 | 4.194 | 10.042 | 3.785 | 1.11× |
| q21 | 1.638 | 1.540 | 5.163 | 0.899 | 1.71× |
| q22 | 2.228 | 1.372 | 5.075 | 0.375 | 3.66× |
| q23 | 25.795 | 7.333 | 18.927 | 9.699 | 0.76× |

The geomean of median speedups over each transport's stock Flink is **8.56× for
Fluss** and **1.57× for Kafka**. Comparing native medians directly, Fluss is
**2.12× faster geometrically**. Removing Netty's two-second shutdown quiet period
is a major bounded-job gain, not a claim about sustained throughput. Stateful
queries retain substantial variance; q23's native Fluss median is slower than Kafka.
All measured trials remain in the results. Primary-key outputs use stock production
and pass separate correctness checks. Deterministic Fluss outputs match stock Flink;
q12 observes processing time. [Configuration, all trials, SQL/CI validation and
profiles](docs/connectors/fluss.md#matched-transport-measurements) are documented.
The connector remains experimental; verified substitutions are enabled by default when installed.
Set `-Dstreamfusion.fluss.enabled=false` to use the stock connector endpoints.

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
