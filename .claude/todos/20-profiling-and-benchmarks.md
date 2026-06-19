# Profiling + benchmark layer (measure as we build)

**Status:** open — needed now, before more operators land.
**Source:** every acceleration claim should be measured, not asserted (the README
already contrasts us with closed engines on exactly this point). We need a standing
way to benchmark each operator and catch hot-path regressions as we code.

## Why
We are adding operators quickly and have already found one real hot-path pitfall by
reading (per-batch query re-planning in the stateless filter). Reading does not
scale — we need numbers: per-operator throughput (rows/s) and allocations, native
vs. Flink fallback, tracked over time so a regression is visible.

## What to build
1. **Native micro-benchmarks** (Criterion, in `native/benches/`). One bench per
   native operator over a synthetic batch: filter-expression eval, tumbling/session
   update+flush, two-phase local/global. Report rows/s and ns/row. These are the
   tight loops; Criterion gives stable deltas per commit.
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
  (`Vec<ScalarValue>`, and a `String` per row for string keys) for every row, and
  clones it once per overlapping window. Inherent to keyed aggregation; the standard
  fix (row-format or dictionary-encoded keys) is a later optimization, measure first.
- **`windows_for` allocates a `Vec` per row** in the update loop (1 entry for tumbling,
  several for hop/cumulate). Candidate for an iterator/smallvec once benched.
- **Not a problem:** the accumulator update is already vectorized — rows are grouped
  per (window, key), then a single `take` + `update_batch` per group, so accumulators
  see batches, not individual rows. Don't "optimize" this without numbers.

## Acceptance criteria
- `cargo bench` runs the native operator benches and prints rows/s per operator.
- The parity harness emits native-vs-fallback wall time for each query it checks.
- `docs/benchmarks.md` exists with the method and an initial results table.
