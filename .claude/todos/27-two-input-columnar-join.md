# Two-input columnar operator → event-time joins

**Status:** open (next thread). This is the one genuinely new piece of infrastructure
left for the append-only operator family; everything else (keyed shuffle, watermarks,
columnar exchange, per-key state) already exists.

## Goal
A two-input columnar operator, then the append-only joins it unlocks:
- **Interval join** — `a JOIN b ON a.k = b.k AND a.rt BETWEEN b.rt - X AND b.rt + Y`.
  Append-only. **Recommended first slice** (single, well-defined semantics).
- **Window join** — windowing-TVF join (both sides in the same tumbling/hop window).
  Append-only. Builds on the windowing infra we have.

Regular joins, temporal joins, multi-joins are **retracting** → blocked by ticket 06
(insert-only), not by this work.

## Flink plan nodes (verified in flink-table-planner_2.12 2.2.1)
- `org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalIntervalJoin`
  - ctor: `(cluster, traits, left, right, JoinRelType, joinCondition, originalCondition, IntervalJoinSpec$WindowBounds)`
  - `windowBounds` (private; check base/`explainTerms` or `IntervalJoinSpec.WindowBounds` for `leftLowerBound`/`leftUpperBound`/`isEventTime`/the rt column indices), `getJoinType()` (JoinRelType), `originalCondition()`, `requireWatermark()`.
  - extends `...physical.common.CommonPhysicalJoin` — inspect it for `joinSpec()` / equi-join key columns (the `JoinSpec` has left/right key indices + the non-equi condition). **TODO: javap `CommonPhysicalJoin` + `IntervalJoinSpec$WindowBounds` for exact accessors.**
- `StreamPhysicalWindowJoin` — for the window-join variant later.
- `StreamPhysicalJoin` (regular, retracting — do NOT match), `StreamPhysicalTemporalJoin`, `StreamPhysicalLookupJoin` (async, different path — ticket 01).

## What to reuse
- **Columnar exchange** `StreamPhysicalNativeColumnarExchange` + `SplitByKeyGroupOperator` +
  `ColumnarKeyGroupPartitioner`: shuffle EACH join input by its join key. (Both inputs get a
  columnar exchange on their respective key columns; same-key rows co-locate on both sides.)
- **Watermark**: Flink delivers per-input watermarks; a two-input operator gets `processWatermark1`
  / `processWatermark2`, and the operator's effective watermark = **min(wm1, wm2)** — drives state
  cleanup and result emission. (Flink's `AbstractStreamOperator` tracks combined input watermark.)
- `ArrowBatch` / `ArrowBatchSerializer` / `ArrowBatchTypeInformation`, the `ColumnarInput`/`ColumnarOutput`
  markers, and `PhysicalPlanScan`'s transition-insertion (works for two-input rels too — `adapt` runs
  per input edge).
- Native key helpers (`read_key`, `key_arrays`, `GroupKey`), Arrow `filter`/`concat`/`take`, the
  per-key state map pattern from `TumblingAggregator`/`OverWindowAggregator`.

## What's genuinely new
1. **A two-input columnar operator** — Flink `TwoInputStreamOperator<ArrowBatch, ArrowBatch, ArrowBatch>`
   (or `RowData` out). No two-input operator exists in the repo yet (all are `OneInputStreamOperator`).
   Its exec node is a `TwoInputTransformation` (vs `ExecNodeUtil.createOneInputTransformation`); the
   exec node has two input edges (`getInputEdges().get(0)/.get(1)`). The rel is a `BiRel`
   (two inputs), `ColumnarInput` on both edges + `ColumnarOutput`.
2. **Native interval-join state machine** — buffer both sides per join key (rows by rt), held until
   the watermark exceeds their validity window; on each arriving row (or batch), probe the other
   side's buffered rows whose rt is within `[rt - X, rt + Y]` and emit matched pairs (output =
   left cols ++ right cols). Evict per-key rows once `min-watermark` passes their max useful rt.
   This is buffering + a bounded probe + eviction (related to the bounded-OVER row-removal idea,
   deferred in ticket 26 — but here it's inherent).
3. **Planner**: substitute `StreamPhysicalIntervalJoin` (INNER first) when join keys are supported
   types + event-time + the condition is the admitted equi+interval shape; couple a columnar
   exchange on each input (each over a columnar producer), like the window/OVER coupling.

## Recommended first slice
Interval join, **INNER**, equi-key on bigint/int/string + an event-time interval on rt, single
join key, parity-tested vs the host (DataStream sources with SOURCE_WATERMARK, like the OVER tests;
and a Parquet-source variant for the fully-columnar path). Defer LEFT/RIGHT/FULL outer (they emit
nulls on watermark expiry — more state/timer logic) and window join to follow-ups.

## Parity discipline reminders (post-compaction orientation)
- Every substitution is gated + verified by `NativeParity.assertParity` (host vs native, sorted
  result-set equality, asserts substitution happened). Un-admitted shapes must fall back.
- Build bottom-up, commit green increments: native (+ `cargo test`), then JNI + `Native.java`, then
  operator (+ harness test), then matcher/rel/exec-node/planner, then end-to-end parity.
- Record deliberate semantic choices in `divergences/`. Tests: `mvn test` (debug native); benchmarks
  need `-Pbench` (release native).
- Commit style: terse, imperative, no `Co-Authored-By` (matches the StreamFusion history).