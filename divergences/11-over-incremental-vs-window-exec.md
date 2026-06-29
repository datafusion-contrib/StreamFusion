# Event-time OVER: incremental accumulators, not a DataFusion window exec

**Kind:** algorithmic — internal implementation differs; output is identical.
**Diverges from:** Arroyo (unbounded frame); Flink (bounded ROWS/RANGE frame — recompute
vs retraction, see below).
**Forced by parity:** partially — the *unbounded-preceding* frame makes the
incremental model strictly necessary for bounded memory; matching Flink's
streaming OVER operator is the prime directive either way.

## Their decision
Arroyo's `window_fn.rs` implements SQL `OVER` (window functions) by **buffering
rows per distinct event-time instant** and running a **DataFusion window
`ExecutionPlan`** over them — full delegation: the operator is a harness that
stages rows into the plan via channels and drains its output on the watermark.
This generalizes to any window function DataFusion supports (`ROW_NUMBER`, `LAG`,
`RANK`, …), because the plan does the work.

## What we do instead — which is what Flink itself does
For the `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` running aggregate, we
keep, per partition key, only the **incremental accumulators** (the same
DataFusion `Accumulator`s the window aggregates use — `sum`/`min`/`max`/`count`,
plus the int-semantics ones in [01](01-integer-truncating-avg.md)). Each row folds
into its key's accumulators in rowtime order and emits the running value; raw rows
are discarded once folded. This mirrors Flink's own `RowTimeUnboundedOver` operator,
which keeps an accumulator per key, not the rows.

## Why not delegate to a DataFusion window exec here
The frame is **unbounded preceding** — every row's result depends on *all* prior
rows of its key, across every batch and watermark. A DataFusion window
`ExecutionPlan` computes over a materialized input, so delegating would require
**retaining every row ever seen, per key, forever** (or re-reading it from state
each emission). The incremental accumulator keeps a single running state per key
instead — bounded memory, no recomputation, and byte-identical to Flink, which is
built the same way. So here the Arroyo-style delegation is not just unnecessary
but strictly worse, and it would also diverge from Flink's actual operator.

We still use DataFusion's compute: the accumulators *are* DataFusion's. What we do
not adopt is Arroyo's *orchestration* (buffer rows, run a plan), because Flink's
orchestration (incremental per-key state) is both the parity target and the
bounded-memory choice. This is the same reasoning as
[03](03-incremental-window-merge.md), applied to OVER.

## General window functions — also incremental (investigated, not assumed)
`ROW_NUMBER` (shipped), and `RANK`/`DENSE_RANK`/`FIRST_VALUE`/`LAST_VALUE` (to
follow), are the same `OverAggregate` rel with `UNBOUNDED PRECEDING` frames
(`ROW_NUMBER` uses `ROWS`). An earlier version of this note predicted they would
**delegate to a DataFusion window exec like Arroyo**. Investigating both sides
showed that is wrong here:

- **Arroyo's `window_fn` is not a model for this.** It buckets rows **per distinct
  event-time instant** and runs a DataFusion window exec per instant — so its
  window functions are scoped to one instant, *not* cumulative across the
  partition. Flink's `UNBOUNDED PRECEDING` `ROW_NUMBER` counts all prior rows of
  the partition; Arroyo's per-instant model does not produce that.
- **A DataFusion window exec would need unbounded retention.** With an unbounded
  preceding frame, delegating to a batch window plan means keeping every partition
  row forever (the partition never closes in a stream). Flink keeps a counter, not
  the rows.
- **DataFusion's incremental evaluator can't be checkpointed.** `PartitionEvaluator`
  *can* run incrementally (`ROW_NUMBER` is `n_rows += 1`), but unlike `Accumulator`
  it exposes **no serializable state**, so delegating the compute would break
  exactly-once. To checkpoint we must own the state anyway.

So general window functions take the **same incremental, own-the-state path** as the
running aggregates: a small per-key state (a counter for `ROW_NUMBER`; rank+last-value
for `RANK`/`DENSE_RANK`; the first / current value for `FIRST_/LAST_VALUE`),
serialized for checkpointing, matching Flink's `OverAggregate` exactly. For
`ROW_NUMBER` this is identical to DataFusion's own evaluator (`n+1`), so nothing is
re-derived; for the others the logic is small and Flink-defined.

## Bounded frame — recompute over the slice, not incremental retraction
A bounded frame — `ROWS BETWEEN n PRECEDING AND CURRENT ROW` (by row count) or
`RANGE BETWEEN INTERVAL n PRECEDING AND CURRENT ROW` (by rowtime interval) — slides:
rows fall off the trailing edge. The unbounded incremental accumulator above cannot
serve it, because `MIN`/`MAX` are not retractable — once the running extreme leaves
the frame there is no way to recover the new extreme without the underlying values.
Flink's `RowTimeRowsBoundedPrecedingFunction` / `RowTimeRangeBoundedPrecedingFunction`
solve this by keeping the frame's rows in `MapState` and a **retractable**
accumulator, retracting each row as it leaves.

We take the model **RisingWave defaults to** instead: keep a per-key sorted buffer of
the rows still reachable by some future frame and **recompute** each emitted row's
aggregate over its frame slice with a fresh accumulator. The result is identical to
Flink (both aggregate over the same frame extent), and recompute sidesteps the
non-retractable-`MIN`/`MAX` problem entirely — the bounded path reuses the same
`RunningAgg` fold the unbounded path uses, just folded fresh per row over the slice
rather than once cumulatively. The slice is `buffer[i-n ..= i]` for ROWS; for RANGE it
is `[first rt ≥ cur−offset, last rt ≤ cur]`, found by binary search, and rows of equal
rowtime share one frame (ending at the last tied row), matching Flink's RANGE
semantics. Cost is O(frame size) per row, fine for a bounded frame; the trailing edge
is evicted (ROWS: keep the last `n` rows per key; RANGE: drop rows older than
`maxRowtime − offset`) so memory stays bounded. This is an **algorithmic** divergence
from Flink's retraction-based operator with **identical output** — chosen because it is
simpler and correct where incremental retraction would need a MIN/MAX value multiset we
do not keep.

Flink seeds `lastTriggeringTs = 0` in these operators and drops a row whose rowtime is
`<= 0` as late; with real epoch-millis rowtimes that never fires, and we do not
reproduce the epoch-0 quirk (a rowtime-0 row is processed normally).

## Scope
Running aggregates (`SUM`/`MIN`/`MAX`/`COUNT`/`AVG`; `AVG` via Flink's
`$SUM0`+`COUNT` with the divide on the host), `FIRST_VALUE`/`LAST_VALUE`, and the
window functions `ROW_NUMBER`/`RANK`/`DENSE_RANK`, over the unbounded `RANGE` frame, the
bounded `ROWS n PRECEDING` frame, or the bounded `RANGE INTERVAL n PRECEDING` frame.
Both event-time and proctime orders are supported; under proctime the operator folds in
arrival order and emits eagerly (no watermark), assigning a monotonic arrival sequence as
the ordering key so the same fold/frames apply — only a proctime bounded-`RANGE` frame
(a wall-clock interval) falls back as non-deterministic. Decimal bounded frames fall back.

## Verification
Parity harness: running aggregates and `ROW_NUMBER`, partitioned and not, over
DataStream and Parquet sources, including rowtime ties within the unbounded frame.
