# Columnar watermark assignment is per-batch, not per-row

**Kind:** behavioral — when the event-time watermark advances relative to the
rows it governs.
**Diverges from:** Flink's `WatermarkAssignerOperator`, which evaluates and may
advance the watermark per row.
**Follows:** Arroyo, which generates a watermark per `RecordBatch`.

## Flink's decision
Flink's table `WatermarkAssignerOperator` runs the watermark generator **per
row**: each record updates the running max and, when the watermark jumps past the
auto-watermark interval, emits eagerly mid-stream. So within a sequence of rows a
watermark can advance *between* two rows, and a later row whose window the
watermark has already closed is dropped as late.

## What we do
The columnar assigner consumes an Arrow batch, forwards the whole batch, then
emits one watermark computed from the batch's maximum rowtime
(`max(rowtime) - delay`). The watermark therefore advances only at batch
boundaries, never between rows of the same batch — every row in a batch is seen
by the downstream window *before* that batch's watermark, so no row is late
relative to its own batch. Across batches the behavior matches Flink (a later
batch's rows can still be late w.r.t. an earlier batch's watermark).

## Consequence
Identical results to Flink whenever no window closes *within* a batch — i.e. for
in-order / monotonic rowtimes, or any watermark delay large enough that the
running watermark only closes windows at batch boundaries (the common case, and
what real 200 ms periodic emission already approximates). They differ only for
out-of-order rows packed into a single batch where the per-row watermark would
have closed a window mid-batch and dropped a straggler: Flink drops it, we keep
it. The aggregate is then the *more complete* one, but it is not byte-identical to
Flink, so this is a divergence, not a bug.

This is inherent to columnar batch processing of watermarks (the same reason
Arroyo assigns per batch) and is not worth unwinding — reproducing per-row late
drops would mean splitting each batch at every watermark-advance point, which
defeats the columnar model. The parity tests use data/delays that do not close a
window mid-batch; `FlinkWatermarkAssignerSqlHarnessTest` notes the constraint.

## Scope
- Only the **event-time watermark advance granularity** differs; the watermark
  *value* (`max(rowtime) - delay`, floored at 0, MAX at end of input) matches
  Flink exactly, and is unit-tested in `NativeColumnarWatermarkAssignerOperatorTest`.
- Bounded jobs still flush every window at end of input (MAX watermark), so
  completeness is unaffected; only mid-stream late-drop timing can differ.
