# Retracting window buffers

Arroyo's tumbling aggregation uses DataFusion partial/final plans over buffered bins.
Its planner rejects aggregation over updating Debezium input, so it has no equivalent
retracting window implementation to transplant. We retain StreamFusion's existing
columnar window assignment, per-window accumulators and local/global split.

RisingWave's hop-window executor preserves each input change when duplicating rows into
windows, normalizing update-before/after into delete/insert. Its hash aggregator owns
signed group state and watermark cleanup. We use the same separation: assignment does
not discard the change sign; aggregation owns group liveness. Flink's concrete buffers
still determine our representation and results.

Released Flink 2.2.1 uses a nullable sum and signed non-NULL count for retracting SUM,
plus a separate live-row count (or a user COUNT(*)). Zero means empty; a negative count
does not. These fields travel in Flink's local partial order, including across a local
barrier drain, and use the existing raw-keyed and direct RocksDB checkpoint paths.
Integral SUM/COUNT are admitted first; unsupported retracting forms remain explicit
planner fallbacks rather than borrowing append-only accumulator semantics.

FLOAT/DOUBLE SUM extends the same nullable sum/count layout. It follows Flink's declared
sum precision and assigns the first non-NULL value directly to retain negative zero.
Subsequent arithmetic and partial merges stay ordered; a zero-count partial can carry a
nonzero or nonfinite sum that must survive checkpointing. No new window/state architecture
is needed for these types.

DECIMAL SUM also keeps this two-field layout, widening the sum to precision 38 while
preserving the input scale. Its arithmetic reuses the append-only decimal SUM primitive:
overflow produces a NULL sum that the next signed value can reset. The signed count and
zero-count residuals remain independent of that nullable sum, as in Flink's retracting SUM.

DECIMAL AVG reuses its decimal sum/BIGINT count layout and exact division. Its overflow is
sticky, including when the count returns to zero; a NULL sum must therefore remain distinct
from an empty numeric-zero sum in both local partials and checkpoints. The existing live-row
count in the aggregate configuration identifies retracting windows and selects that distinction.
Append-only AVG retains its previous checkpoint interpretation. This extends the existing
per-window accumulator architecture rather than adding a separate state entry or JNI argument.
Flink's released `AvgAggFunction` and Arroyo's `arrow/incremental_aggregator.rs` were consulted
before the extension; Flink's signed counts and decimal overflow determine the buffer semantics.

The Arrow ownership pattern was checked against Comet's ColumnarBatchArrowReader:
producer vectors must not be closed through a second owning root. The window projection
therefore copies the change-kind byte into a vector owned by its exported root, just as
it owns its projected values and keys. The original input keeps its own lifetime.

Filtered COUNT/SUM/AVG use the same buffers. Released Flink 2.2.1 guards both accumulation
and retraction with the aggregate's predicate, and only an unfiltered COUNT(*) can serve as
its live-row count (`AggregateUtil.insertCountStarAggCall`). We mask rejected values to NULL
while preparing the existing canonical Arrow batch, including the synthesized COUNT(*) value.
We retain every input row for assignment and liveness, and never reapply filters to partials.
This avoids a new JNI configuration or checkpoint layout. Arroyo's tumbling partial/final plan
structure remains intact; its updating-input restriction still prevents direct transplantation.
Comet's Arrow writer and FFI ownership guidance were consulted before this extension: masks
are written into the projection's owned vectors, never into producer-owned input buffers.

References consulted before implementation:

- Arroyo `crates/arroyo-worker/src/arrow/tumbling_aggregating_window.rs` and
  `crates/arroyo-planner/src/test/queries/error_no_aggregate_over_debezium.sql`
  at `de9f70f7203ea9e91fe3e1d8d658e82d4662cb9a`.
- RisingWave `src/stream/src/executor/hop_window.rs` and `aggregate/hash_agg.rs`
  at `216a211fc408a2f195615f922781567b3be6adaa`.
- Comet `spark/src/main/scala/org/apache/spark/sql/comet/execution/arrow/ColumnarBatchArrowReader.scala`
  and `native/jni-bridge/src/arrow_array_stream.rs` at
  `ef62b46306e925bc51e7d7f29922c1870eb729e7`.
