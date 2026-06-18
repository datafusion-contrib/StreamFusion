# Session windows (event-time, single-phase)

**Status:** open — design complete, ready to implement
**Source:** ticket 11 (Arroyo coverage) — next aggregating-window operator

## Why this is reachable
Investigation of Flink 2.2 (`~/data/flink` @ release-2.2.0) confirms the
`SESSION(...)` window TVF flows through **the same `StreamPhysicalWindowAggregate`
node** StreamFusion already substitutes for `TUMBLE`/`HOP`. So the planner hook
needs no new wiring — only a new matcher branch and a native operator.

Key facts:
- Window spec is `SessionWindowSpec` (`getGap()`), `isAlignedWindow() == false`.
  Windowing strategy is still `TimeAttributeWindowingStrategy` (rowtime).
- **Single-phase only.** `TwoStageOptimizedWindowAggregateRule` explicitly
  rejects `SessionWindowSpec` ("session window doesn't support two-phase,
  otherwise window assigner results may be different"). So there is no
  local/global split to handle — only the combined `StreamPhysicalWindowAggregate`.
- Merge semantics (`SessionWindowAssigner`): an element at `t` proposes window
  `[t, t + gap)`; it merges with any existing window on the same key that it
  intersects (`start < other.end && other.start < end`); the merged window is
  the cover (union) of bounds. Output bounds are `window_start = min element ts`,
  `window_end = max element ts + gap`. A session fires when the watermark passes
  its `window_end`.

## Semantics & parity argument
The final set of sessions for a key is the set of connected components of the
intervals `[ts_i, ts_i + gap)` — i.e. the classic merge-intervals result, which
is **order-independent**. Two elements share a session iff a chain of overlapping
gap-intervals connects them. So as long as we (a) never fire a session before the
watermark reaches its end and (b) merge transitively, the grouping matches Flink
regardless of arrival order or batch boundaries.

`SUM/MIN/MAX/COUNT` (and integer `AVG`) all have associative, commutative,
mergeable partial state, so merging two sessions' accumulators via `merge_batch`
yields the same result as aggregating the union of their rows. This is where we
diverge from Arroyo (which stores raw batches and re-aggregates per session) —
incremental accumulators give identical Flink results far more cheaply, and the
parity harness is the proof.

## Native design (`SessionAggregator`)
Sessions are **per key** and **dynamic** (no fixed bins), so this is a new
struct rather than an extension of the bin-based `TumblingAggregator`:

```
struct Session { start: i64, end: i64, accs: Vec<Box<dyn Accumulator>> }
// per key, sessions kept sorted by start (BTreeMap<i64 start, Session> or Vec)
sessions: HashMap<i64 /*key*/, BTreeMap<i64 /*start*/, Session>>
gap_millis, aggregates (the WindowAggregate templates), value_type
```

- `update(batch)`: for each row `(ts, key, value)`:
  1. candidate `[ts, ts + gap)`.
  2. find all existing sessions for the key intersecting the candidate; remove
     them, merge their accumulators + bounds into one session, then `update_batch`
     the new row's value into it. (`end` tracked as `max_ts + gap`, so store
     `max_ts` or equivalently `end - gap`; `start = min_ts`.)
  3. Re-merging may chain: after inserting, also merge with neighbors the new
     bounds now intersect. Simplest correct form: collect every intersecting
     session (the new row can bridge at most its immediate lower/upper
     neighbours), union them.
  - Batch the row indices per (key, target session) where possible, but
    correctness only needs per-row update; optimize later if it shows up.
- `flush(watermark)`: for each key, pop sessions with `end <= watermark`, emit
  `[key?, result0..resultN-1, window_start, window_end]` where `window_start =
  start`, `window_end = end` (already `max_ts + gap`). Same output contract and
  session-zone local timestamp conversion as the tumbling flush.
- `snapshot`/`restore`: flatten per-key, per-session bounds + accumulator state
  fields, mirroring the tumbling checkpoint path.

## JVM side
- New matcher branch in `WindowAggregateMatcher` (or a sibling) recognizing
  `SessionWindowSpec`: same rowtime + LTZ + grouping≤1 + single-value-column +
  supported-kind terms; extract `gapMillis = ((SessionWindowSpec) spec).getGap()`.
  Note `windowSize`/`windowSlide` are meaningless for sessions — thread `gap`
  instead via a dedicated field.
- New operator `NativeSessionWindowAggregateOperator` (or a mode flag on the
  combined operator) calling a `createSessionAggregator(gapMillis, valueType,
  keyColumn>=0, kinds)` native entry; `pushBatch`/`emitClosedWindows` reuse the
  existing `updateRaw`/`emitFinal` helpers in `NativeWindowOperatorBase`.
- New rel + exec node mirroring the tumbling pair, carrying `gapMillis` instead
  of size/slide. Only the combined node (no local/global).
- `PhysicalPlanScan`: substitute the combined `StreamPhysicalWindowAggregate`
  when the session matcher fires.

## Acceptance criteria
- `SELECT [k,] window_start, window_end, <aggs> FROM TABLE(SESSION(TABLE src,
  DESCRIPTOR(rt), INTERVAL '..' SECOND[, ...])) GROUP BY [k,] window_start,
  window_end` routes to native with **identical** results to host Flink, via the
  parity harness — covering: single session, two sessions split by a gap, a late
  element that bridges two sessions into one (the merge path), keyed multi-key,
  multiple aggregates, bigint and double values.
- Unsupported shapes (two phase can't occur; multi-key, unsupported kinds) fall
  back cleanly.
- README compatibility chart + ticket 11 updated.

## Open questions / risks
- `SESSION` TVF with `PARTITION BY` — the partition keys become the grouping
  key(s). Confirm the grouping array the matcher sees already reflects this; keep
  to ≤1 key for v1.
- Confirm Flink's exact `window_end` (max_ts + gap, exclusive vs inclusive) by a
  parity test, not by reading alone.
- Intra-batch merge chaining: verify a row that bridges two existing sessions
  merges both (test explicitly).
