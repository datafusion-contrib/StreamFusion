# Join breadth: outer / semi / anti, and a residual non-equi predicate

**Status:** open — planned, not started. All three native joins are INNER-equi only.
**Source:** the operator-coverage gap (ticket 11) — joins are the highest-value remaining breadth.

## Current state (what to extend)
Three native joins, all gated to **INNER** with **no residual non-equi predicate**:
- **Regular (non-windowed) updating join** — `NativeColumnarUpdatingJoinOperator` (+ the Rust updating-join
  in `native/src/lib.rs`), matched by `RegularJoinMatcher`. Emits/consumes a changelog. Per-side keyed
  multiset, probes natively (divergences/14), no degree bookkeeping (INNER needs none).
- **Interval join** (event-time) — `NativeIntervalJoinOperator`, `IntervalJoinMatcher`. Append-only; buffers
  both sides, delegates the match to a DataFusion `HashJoinExec`, evicts on watermark (divergences/12).
- **Window join** (windowing-TVF) — `NativeWindowJoinOperator`, `WindowJoinMatcher`. Same DataFusion-delegated
  shape, joins per window on watermark close (divergences/12).

Each matcher gates: `joinSpec.getJoinType() != FlinkJoinType.INNER → fall back`; `getNonEquiCondition().isPresent() → fall back`;
≥1 null-filtering equi key. To extend, relax those gates per the design below and pass the join type +
non-equi condition through to the operator.

## The key design fact (divergences/14)
**INNER needs no degree table; outer/semi/anti do.** RisingWave keeps a per-side keyed row set and, only for
outer/semi/anti, a per-row **match-degree** (count of associations on the other side) to know when to emit or
retract the null-padded row (outer) or the bare row (semi/anti). Our INNER join already has the per-side
keyed multiset; the extension adds the degree.

## Reference-first (consult before writing)
- **Flink** `~/data/flink/flink-table/flink-table-runtime/.../operators/join/stream/`:
  - `StreamingJoinOperator` — INNER + LEFT/RIGHT/FULL outer; uses the `state/` record-state views plus an
    association count to decide null-row emit/retract. This is the exact state machine to mirror.
  - `StreamingSemiAntiJoinOperator` — SEMI/ANTI.
  - `state/` — `JoinRecordStateView` / `OuterJoinRecordStateView` (the degree/association state).
- **divergences/14** — the degree-table rationale and our guest-state stance (state snapshots into Flink, in
  memory not paged).
- **Arroyo** `~/data/arroyo/crates/arroyo-worker/src/arrow/join_with_expiration.rs` — the interval/window
  (time-bounded) join reference.

## Plan, by family (each its own change + parity test)
1. **Regular updating join — outer/semi/anti** (highest value; the changelog join SQL hits most):
   - Add a per-row **degree** alongside the per-side multiset (count of current matches on the other side).
   - LEFT/RIGHT/FULL OUTER: when a row's degree goes 0→1, retract the null-padded row and emit the matched
     row(s); when it goes 1→0 (other side retracts), emit the null-padded row. Null-pad the absent side's
     columns. Honor input `RowKind` (the join already consumes a changelog).
   - SEMI: emit the left row on degree 0→1 (`+I`), retract on 1→0 (`-D`). ANTI: the inverse.
   - Mirror `StreamingJoinOperator` (outer) and `StreamingSemiAntiJoinOperator` (semi/anti).
   - Parity: `NativeParity.assertChangelogParity` per join type, over append-only and changelog inputs.
2. **Interval / window joins — outer/semi/anti** (more involved; time-based):
   - The matched part can still go through the DataFusion join with the right join type. The new work is
     emitting **unmatched** rows at the time boundary: an outer row with 0 matches when its window/interval
     is evicted by the watermark emits a null-padded row (append-only — emitted once, no retraction);
     semi/anti emit/suppress at eviction. This needs degree tracking against the buffered other side at
     eviction time.
   - Parity: `assertParity` (these stay append-only even for outer — the null row is emitted once at close).
3. **Residual non-equi predicate** (all three; can land alongside or after the types):
   - Updating join: the predicate gates whether a candidate pair is a "match" (so it feeds the degree and
     the emitted pair). Window/interval: pass it as the DataFusion `HashJoinExec` join filter (the interval
     bounds are already a residual filter there — generalize).
   - Parity-test each; an un-admitted predicate shape still falls back.

## Files to touch
- Matchers: `RegularJoinMatcher`, `IntervalJoinMatcher`, `WindowJoinMatcher` — relax the INNER/non-equi gates,
  pass `joinType` + the encoded non-equi condition (reuse `RexExpression`/the expression encoder for the
  predicate) to the physical node / exec node.
- Physical + exec nodes: `StreamPhysicalNativeColumnarUpdatingJoin` / `…IntervalJoin` / `…WindowJoin` and their
  exec nodes — carry the join type + predicate.
- Operators: `NativeColumnarUpdatingJoinOperator`, `NativeIntervalJoinOperator`, `NativeWindowJoinOperator`.
- Rust (`native/src/lib.rs`): the updating-join state (add degree + per-type emit), and the interval/window
  join (join type + boundary unmatched emission + non-equi filter).

## Suggested order
Regular updating join LEFT/RIGHT/FULL → SEMI/ANTI → non-equi predicate (updating) → interval/window
outer/semi/anti → interval/window non-equi. Re-run the full suite (now ~1 min) after each; gate anything not
bit-identical to the host and record divergences.
