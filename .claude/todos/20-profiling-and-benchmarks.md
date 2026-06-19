# Profiling + benchmark layer (measure as we build)

**Status:** in progress — Criterion harness landed (item 1 started); native timing
hook, harness timing, and the remaining benches still open. First numbers in
[docs/benchmarks.md](../../docs/benchmarks.md) already surfaced the per-row key cost
in the tumbling aggregator (~100× slower per element than the filter).
**Source:** every acceleration claim should be measured, not asserted (the README
already contrasts us with closed engines on exactly this point). We need a standing
way to benchmark each operator and catch hot-path regressions as we code.

## Why
We are adding operators quickly and have already found one real hot-path pitfall by
reading (per-batch query re-planning in the stateless filter). Reading does not
scale — we need numbers: per-operator throughput (rows/s) and allocations, native
vs. Flink fallback, tracked over time so a regression is visible.

## What to build
1. **Native micro-benchmarks** (Criterion, in `native/benches/`). ✅ STARTED —
   harness in `native/benches/operators.rs` with `filter/gt_literal`,
   `tumbling/sum_update_flush`, and `tumbling/sum_keyed_update_flush`, run via
   `cargo bench`, documented in [docs/benchmarks.md](../../docs/benchmarks.md) and the
   readme. Remaining: session and two-phase local/global benches, and committing a
   results table from a quiet machine.
2. **A lightweight native timing/counter hook** behind a feature flag — per-operator
   batch count, row count, and wall time, dumpable on close — so we can profile a
   real job without a full tracing dependency. Keep it zero-cost when the flag is off.
3. **End-to-end harness timing.** Extend the parity harness to also record wall time
   for the native vs. fallback run of the same query, so every parity test doubles as
   a (rough) A/B throughput check. Parity stays the gate; timing is informational.
4. **A short `docs/benchmarks.md`** with the method and a results table, kept in step
   as operators land — the auditable counterpart to the README's throughput claims.

## Confirmed findings from the first sweep (seed the backlog)
- **[fixed]** Stateless filter re-planned a full DataFusion query *per batch* — new
  `SessionContext`, logical→physical planning, async stream. Replaced by a
  compile-once predicate handle that evaluates a cached `PhysicalExpr` synchronously
  (see [divergences/07](../../divergences/07-expression-encoding-and-compile-once.md)).
  The legacy `filterBatch`/`filterGreaterThan`/`doubleColumn` still re-plan per batch
  but are superseded (filterBatch) or demos — remove once the planner routes through
  the expression handle.
- **Per-row key allocation.** The aggregator `update` builds a `GroupKey`
  (`Vec<ScalarValue>`, and a `String` per row for string keys) for every row. The
  per-window *clone* is gone (moved into the last window) and the grouping map now uses a
  fast hash (`ahash`) instead of SipHash, but the per-row `Vec` allocation remains. The
  keyed bench still costs ~2.4× the unkeyed one; row-format or dictionary-encoded keys are
  the next target. The same `ahash` swap likely helps the session/partial grouping maps —
  apply once those have benches.
- **[fixed]** `windows_for` allocated a `Vec` per row in the update loop. Reusing one
  buffer across rows cut the tumbling bench ~26% (244 → 181 µs / 17 → 22.6 Melem/s).
- **Session `update` slices one row at a time.** The session bench
  (`session/sum_keyed_update_flush`) is ~10× slower per element than tumbling because
  `update` does a `take` per row instead of once per group; batching that slice is the
  session operator's first target. (Its grouping hash is negligible here, so `ahash` was
  not applied to it.)
- **Not a problem:** the tumbling accumulator update is already vectorized — rows are grouped
  per (window, key), then a single `take` + `update_batch` per group, so accumulators
  see batches, not individual rows. Don't "optimize" this without numbers.

## Acceptance criteria
- `cargo bench` runs the native operator benches and prints rows/s per operator.
- The parity harness emits native-vs-fallback wall time for each query it checks.
- `docs/benchmarks.md` exists with the method and an initial results table.
