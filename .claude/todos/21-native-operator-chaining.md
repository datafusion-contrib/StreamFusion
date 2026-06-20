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

## Decision — columnar flow with transitions (not fusion)
Each native operator is **columnar**: it consumes and produces Arrow batches over a
columnar stream-record type, not `RowData`. Adjacent columnar operators flow Arrow
straight through (object passing within a chained task; Arrow IPC across a network edge),
and the planner inserts a **transpose** operator only at a rowwise↔columnar boundary
(`RowData → Arrow` entering, `Arrow → RowData` leaving). An operator is simply rowwise or
columnar; nothing about its neighbours matters beyond whether the edge crosses the
boundary. This is Comet's model; Flink has no columnar framework (no `supportsColumnar`,
no `ColumnarRule`), so we supply the two missing pieces — the columnar record type and the
transition insertion — ourselves.

**Rejected: fusing a subtree into one operator.** Pattern-matching specific operator
*combinations* (source+sink, filter+window, …) into a bespoke fused node per shape is a
hardcoded replacement rule that does not generalize — a smell. Columnar flow composes any
sequence of columnar operators with no per-combination knowledge. See
[divergences/08](../../divergences/08-columnar-flow-transitions.md).

## Mechanism (validated against Flink 2.2 + Arroyo source)
- **Zero serialization in a chain.** Chained operators pass the `StreamRecord` value by
  reference through `ChainingOutput` when object reuse is on; with it off,
  `CopyingChainingOutput` calls `TypeSerializer.copy()`. The Arrow-batch serializer's real
  serialize/deserialize is invoked *only* across a network edge — so within a native chain
  no bytes are ever serialized. Guarantee the chain with a forward partitioner, equal
  parallelism, `ChainingStrategy.ALWAYS`, and the default slot-sharing group.
- **Serializer surface.** The columnar record needs a `TypeInformation`/`TypeSerializer`.
  `copy()` can be identity — our producers emit a fresh batch per record and never retain
  or mutate it after emit, and the serializer is used only on native↔native edges — and
  `serialize`/`deserialize` use Arrow IPC, exercised only at a shuffle. (Decide object-reuse
  vs identity-copy when wiring; identity-copy is safe here and avoids a global flag.)
- **Two-pass planner.** Pass 1 substitutes and marks native rels `ColumnarRel`. Pass 2
  walks the rewritten tree and inserts a transpose wherever a `ColumnarRel` and a
  non-`ColumnarRel` are adjacent (direction by which side is the producer). Logical
  `RelDataType` is identical across a transpose — only the physical `TypeInformation`
  (set in `translateToPlanInternal` via `InternalTypeInfo`/a new `ArrowBatchTypeInfo`)
  differs, and Flink never re-derives the element type from `RelDataType`, so adjacent exec
  nodes exchanging a non-`RowData` carrier is safe.
- **Single-operator parity.** `transpose(R→A) → op(A→A) → transpose(A→R)` runs the same two
  conversions as today's inline operator; only between adjacent native operators do the
  conversions vanish. So the refactor is behavior-preserving by construction.
- **Shuffle shape (Arroyo, for the later phase).** Inter-operator message is
  `Data(batch) | Signal(watermark|barrier|…)`; network uses Arrow IPC; a keyed shuffle
  splits a batch by key with `sort_to_indices → take → slice` into per-partition batches.

## Build slices (each green)
1. **`ArrowBatch` record type + `ArrowBatchTypeInfo`/serializer.** Holds a
   `VectorSchemaRoot`; serializer does Arrow IPC ser/de and identity `copy()`. Unit-tested
   by an IPC round-trip. Self-contained, no planner changes. ← first
2. **Transpose operators.** `RowData → Arrow` (buffer rows, convert via the whole-row
   converter, emit a batch) and `Arrow → RowData` (read a batch back to rows). Harness-tested.
   **These stay in Java — a native transpose cannot help.** Its row side is JVM `RowData`
   (read per-cell on the JVM) and `GenericRowData` (allocated on the JVM); native code can do
   neither, so going to Rust adds a JNI hop per row/field on top of the irreducible JVM access.
   The columnar half (writing Arrow off-heap from Java) is already cheap. The way to make a
   boundary native is to remove the row side entirely — a native columnar source/sink (slice 5),
   not a native transpose. The only transpose lever is Java-side: the converter is cell-at-a-time;
   a column-vectorized converter would speed it up (Flink's `ArrowUtils` does this but bundles a
   conflicting Arrow version — why we hand-rolled). Track that as a perf item, not a native one.
3. **One native operator to `Arrow → Arrow` + `ColumnarRel` + the transition-inserter pass.**
   Refactor the filter to consume/produce `ArrowBatch`; mark it columnar; second pass inserts
   transposes around it. Parity-test a single-filter query — identical results, plan shows
   `transpose → filter → transpose`.
4. **Remaining native operators to `Arrow → Arrow`** (windows, sink), so multi-native chains
   drop the inter-operator conversions.
5. **Columnar Parquet source**, then **Arrow across the shuffle**.

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

## Build order (columnar flow)
Each green + benchmarked vs Flink.

1. **Columnar stream-record type + transpose operators.** An Arrow-batch stream record,
   plus two operators: `RowData → Arrow` (entering a columnar region) and `Arrow → RowData`
   (leaving it). These are the only places conversion happens.
2. **Native operators present an `Arrow → Arrow` interface.** Refactor the existing native
   operators (filter, window aggregates, sink) so they consume and produce the columnar
   record type instead of converting `RowData` internally at both ends. The sink consumes
   Arrow; a columnar source produces Arrow.
3. **Planner inserts transitions at boundaries.** When the substituted plan has a columnar
   operator adjacent to a rowwise one, insert a transpose; adjacent columnar operators flow
   Arrow with none. No per-combination rules — just "is this edge columnar on both sides?".
4. **Native columnar Parquet source.** A source that reads Parquet → Arrow batches and
   emits them into the columnar stream (no `RowData`). With the native Parquet sink (done),
   a `Parquet → Parquet` job is then two columnar operators flowing end to end — zero
   transpose — while Flink round-trips through `RowData`. **This is the first case expected
   to cross 1×**, and it falls out of the general mechanism, not a fused copy node.
5. **Arrow across the shuffle.** Two-phase aggregation's `keyBy` carries the columnar record
   over the network (Arrow IPC). Hardest; gated on the rest.

Then the row→columnar pipelines (Kafka → Parquet) compose for free: a `RowData → Arrow`
transpose at the row source, columnar flow through to the columnar sink.

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
