# Native operator chaining (keep columnar between native operators)

**Status:** design — needs architecture sign-off before the build.
**Source:** the end-to-end benchmark ([ticket 20](20-profiling-and-benchmarks.md)):
single-operator substitution is *slower* than Flink (filter 0.58×, tumbling 0.81×)
because every substituted operator converts `RowData → Arrow` on the way in and back
on the way out. The native compute is fast; the per-boundary conversion is the tax.
Chaining removes the conversions *between* adjacent native operators, which is the
only way the project beats Flink.

## The problem with the current model
Each native operator is its own Flink `OneInputStreamOperator<RowData, RowData>`. Two
adjacent native operators therefore convert Arrow→RowData (upstream out) then
RowData→Arrow (downstream in) for nothing — the data was already columnar. The ratio
rises with compute per row (filter → tumbling), confirming the conversion is a fixed
per-row cost that only amortizes when more work rides on each converted batch.

## How Comet does it (and why we can't copy it directly)
Comet replaces operators with columnar (`CometExec`) equivalents, then a transition
pass inserts `ColumnarToRow`/`RowToColumnar` **only** where a columnar operator meets a
row operator. Adjacent Comet operators exchange `ColumnarBatch` and never convert. This
rides on Spark's built-in columnar execution framework (`ColumnarRule`,
`supportsColumnar`, `ColumnarBatch` as a first-class exchange type).

**Flink has no equivalent.** Its runtime is row-based (`RowData`/`StreamRecord`); there
is no `supportsColumnar` contract and no framework that auto-inserts transitions. So we
cannot lean on the engine the way Comet leans on Spark — we have to provide the
columnar boundary ourselves.

## Decision — fuse, don't exchange
Two ways to keep data columnar between native operators:

1. **Columnar stream element + transitions.** Make Arrow a `StreamRecord` payload type
   between native operators and insert conversion operators at native↔host boundaries —
   a hand-built version of Comet's transition pass. Cost: a columnar record type,
   serializers for it (Arrow IPC across any non-chained edge), and transition
   insertion in the planner. Fights Flink's row-based runtime at every turn.
2. **Fuse a native subtree into one Flink operator** (chosen). The planner replaces a
   maximal **shuffle-free connected component** of native-able operators with a *single*
   Flink operator that runs the whole sub-pipeline in Arrow internally, converting
   `RowData → Arrow` once at the component's outer input and `Arrow → RowData` once at
   its outer output. Within the operator there is no boundary, so no inter-op conversion.
   This is Comet's "native subtree as one `CometExec`" mapped onto Flink without needing
   a framework columnar API.

Fusion is chosen because it works *with* Flink's row-based operator model (one operator
in, one operator out, RowData on the wire) and needs no columnar serializer until we
tackle the shuffle. It also yields the maximal win (zero conversion inside the subtree).
This is a deliberate divergence from Comet's framework-assisted transitions — see the
divergences note.

## What anchors the columnar region — the source and sink formats
The conversion is a tax whose *placement* is dictated by the endpoint formats, not by the
operator chain alone. A columnar source (Iceberg/Parquet) hands us Arrow with no
transpose — Flink would instead deserialize it to `RowData`, so reading it columnar both
saves that and starts the native region for free. A columnar sink consumes Arrow with no
transpose. So **run columnar whenever an endpoint is already columnar, and place the
single row↔columnar transpose at each row-based endpoint:**

- **columnar → columnar** (Iceberg → Iceberg): fully columnar, no transpose — the biggest
  win, and where Flink is weakest (it round-trips through `RowData`).
- **row → columnar** (Kafka → Iceberg): transpose row→Arrow once *at the source*, stay
  columnar through to the sink.
- **columnar → row** (Iceberg → Kafka): stay columnar from the source, transpose
  Arrow→row once *before the sink*.
- **row → row** (Kafka → Kafka): both ends are row-based, so going columnar costs two
  transposes; only worth it if the native compute saves more than both. Not the default.
- A **non-native operator** in the middle breaks the region: transpose to row for it, and
  back to columnar after it if a downstream columnar region remains.

This is the same logic as Comet — Comet's columnar region is anchored by its columnar
Parquet scan/write. We generalize it: anchor on whichever endpoints are columnar. It also
explains the benchmark ([ticket 20](20-profiling-and-benchmarks.md)): an in-memory row
source into a discarding sink is the row→row case — the *worst* one, paying conversion
with no columnar endpoint to amortize it (hence 0.58×). The favorable cases need a
columnar endpoint, which means native columnar source/sink support is on the critical
path, not just operator fusion.

## First pipeline: Kafka source → Parquet sink (row → columnar)
The first end-to-end target is a row source feeding a columnar sink, so the transpose
sits once at the Kafka source (record → Arrow) and the region stays columnar through to a
native Arrow → Parquet write. This is a real ingest pipeline and the cleanest place to
show the columnar-sink win, since Flink's filesystem+parquet sink encodes `RowData →
Parquet` at the boundary while we write Arrow batches directly.

Flink baselines exist to compare against: `flink-parquet` (vectorized reader that still
yields `RowData`, and the writer) via the filesystem connector, and `flink-connector-kafka`.
Add those deps for the A/B; the native side adds a Kafka→Arrow source and an Arrow→Parquet
sink.

Build order (each green + benchmarked vs Flink):
1. **Native Parquet sink.** Arrow `RecordBatch` → Parquet via the `parquet` crate (done,
   round-trip tested); a Flink sink operator that batches its input to Arrow and writes
   natively (done); exactly-once two-phase commit — in-progress files committed on
   checkpoint completion, pending files re-committed on recovery (done, harness-tested).
   Remaining: planner sink substitution (new — only operators are substituted today), and
   the benchmark vs Flink's filesystem+parquet sink. **Benchmark blocker:** Flink's Parquet
   writer is built on `parquet-hadoop`, so the baseline needs `hadoop-common` on the
   classpath (declared `provided` in `flink-parquet`); adding Hadoop is a deliberate
   dependency decision, and Flink finalizes Parquet files only on checkpoint, so the
   comparison must run with checkpointing on.
2. **Native Kafka → Arrow source.** Batch Kafka records, deserialize, transpose to Arrow
   once at the source. Deserialization format is the main scoping fork — the cluster uses
   Avro + schema registry (heavy); a first cut may use a simpler/known schema. This is the
   single source-side transpose in the row→columnar case.
3. **Join them.** Kafka → (optional native filter) → Parquet, fully columnar between the
   two transposes; benchmark vs the equivalent Flink job.

## Back burner: a fully native Kafka source (no JNI)
The Kafka→Arrow source above batches and transposes *on the JVM side*, then hands Arrow
across JNI. A more efficient endpoint would subscribe to Kafka *in Rust* and build Arrow
batches natively — no per-record JVM work, no JNI on the hot path — decoding Avro with a
fast Rust Avro→Arrow library (the ecosystem has efficient ones). The hard part is not the
I/O but faithfully reproducing the Flink Kafka source's semantics (offset/partition
assignment, checkpoint-aligned commits, exactly-once, watermark generation); worth doing
only if we can lift those from Arroyo's Rust Kafka connector rather than reimplement them.
On the back burner until the JVM-side source and the sink prove the columnar win.

## Sink substitution (#2) — DONE
A `filesystem`+`parquet` sink to a local path now routes to the native writer: a matcher
reads the sink's resolved-table options and local path, a `StreamPhysicalNativeParquetSink`
replaces the sink at the plan root, and its exec node emits a `LegacySinkTransformation`
wrapping the native sink operator (which preserves the operator's checkpoint-complete
commit). Parity verified end to end: native-written and host-written Parquet read back
identical. Two fixes found along the way — the path option is a URI (`file:///…`), so it
is converted to a local path (and remote schemes fall back); and the whole-row converter
now names Arrow fields by the schema (not positionally), since Parquet readers match by
name. Next: #3 (Kafka→Arrow source), then #1 (the throughput benchmark; baseline ready).

## Sink substitution (#2) — original scoping (kept for reference)
Foundation is in place: the connector/format/Hadoop deps resolve and run together
(test-scoped), and a host Parquet write is verified as the baseline. What remains is the
planner wiring, a deep integration to do carefully:
- **Matcher.** Off `StreamPhysicalSink` (base `…/nodes/calcite/Sink.scala`, which carries
  `contextResolvedTable` and `tableSink`), read the resolved table options; match
  `connector = filesystem` + `format = parquet` and extract `path`. Fall back otherwise.
- **Native sink rel + exec node.** A `StreamPhysicalNativeParquetSink` whose exec node
  produces a `LegacySinkTransformation` wrapping `NativeParquetSinkOperator` via
  `SimpleOperatorFactory.of(operator)` (the operator is `OneInputStreamOperator<RowData,
  Void>`; `LegacySinkTransformation` is the construct that makes an operator a real sink).
  Mind the `Transformation<RowData>` / `StreamOperatorFactory<Object>` typing.
- **Root substitution.** `PhysicalPlanScan` handles the sink at the plan root (it is the
  root for an `INSERT`); swap the matched sink, leave others to the host.
- **Parity test.** `INSERT INTO parquet_sink SELECT …` with substitution on vs off; read
  both outputs back and assert identical rows. The host baseline pattern is in
  `FlinkParquetSinkSmokeTest`.

## #1 benchmark — done, and it reshapes the priorities
The Parquet sink benchmark (`ThroughputBenchmark#parquetSinkThroughput`) lands at **0.52×**
vs the host: a columnar sink fed by a *row* source still pays `RowData → Arrow` at the
sink, while the host writes `RowData → Parquet` directly. With no compute between source
and sink, there is nothing to amortize the transpose, so native loses — same lesson as
filter (0.58×) and tumbling (0.81×).

The data now argues that the **highest-value next build is a columnar *source*** (Parquet
/Iceberg read as Arrow), not the Kafka source: a columnar source means the data never
becomes `RowData`, so a Parquet→Parquet job stays columnar end to end (zero transpose)
while the host round-trips Parquet→RowData→Parquet — the first case expected to cross 1×.
The native Parquet sink (done) is the second half of that pipeline. Suggest reordering #3
toward a columnar Parquet source; the Kafka row source remains valuable but only crosses
1× once chaining lets a transpose-at-source amortize over downstream columnar work.

## Later phases
- **Columnar source (Iceberg/Parquet) as Arrow** — a DataFusion scan instead of Flink
  deserializing to `RowData`; turns the source transpose into zero and unlocks
  columnar→columnar. The other half of the connector story.
- **Stateless prefix fused into a stateful window** — native filter/projection runs on
  each input batch before aggregating, all in Arrow.
- **Operator fusion (mechanism)** — replace a maximal shuffle-free native component with
  one fused operator (per-connected-component substitution); removes inter-operator
  transposes for chains that aren't already merged by the planner (e.g. a Calc feeding a
  window aggregate).
- **Arrow across the shuffle** — two-phase `keyBy` carries Arrow (IPC) so local→global
  stays columnar. Hardest; gated on the rest.

## Acceptance criteria
- The native Parquet sink writes byte-equivalent data (same rows read back) to Flink's
  filesystem+parquet sink, and `ThroughputBenchmark` shows it beating that sink.
- The Kafka→Arrow source produces identical rows to Flink's Kafka source for the same
  topic/offsets; the single source transpose is the only one in a Kafka→Parquet job.
- A columnar source/sink keeps the region columnar end-to-end with the transpose only at
  row endpoints; identical results to Flink across the parity suite.
- A columnar-anchored pipeline crosses 1× vs Flink on `ThroughputBenchmark`.
