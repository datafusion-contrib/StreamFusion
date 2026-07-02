# Profiling + benchmark layer (measure as we build)

**Status:** mostly done — trimmed to the tail. The Criterion micro-bench harness (item 1) and the
end-to-end native-vs-Flink throughput bench (item 3) both shipped, with results in
[docs/benchmarks.md](../../docs/benchmarks.md) and the readme (and the Nexmark matrix layered on top).
**Remaining:** the feature-flagged native timing/counter hook (item 2, not built), plus two-phase
local/global micro-benches. First numbers already surfaced the per-row key cost in the tumbling
aggregator (~100× slower per element than the filter) — feeds the operator-perf backlog.
**Source:** every acceleration claim should be measured, not asserted (the README
already contrasts us with closed engines on exactly this point). We need a standing
way to benchmark each operator and catch hot-path regressions as we code.

## Why
We are adding operators quickly and have already found one real hot-path pitfall by
reading (per-batch query re-planning in the stateless filter). Reading does not
scale — we need numbers: per-operator throughput (rows/s) and allocations, native
vs. Flink fallback, tracked over time so a regression is visible.

## What to build
1. **Native micro-benchmarks** (Criterion, in `native/benches/`). ✅ DONE —
   `native/benches/operators.rs` covers `filter/gt_literal`, `tumbling/{sum_update_flush,
   sum_keyed_update_flush}`, `session/sum_keyed_update_flush`, `over/{running_sum_keyed,
   row_number_keyed}`, `interval_join/equi_key_push`, and `window_join/equi_key_flush`, run via
   `cargo bench`, with a committed results table in [docs/benchmarks.md](../../docs/benchmarks.md) and
   the readme. Remaining: two-phase local/global benches.
2. **A lightweight native timing/counter hook** behind a feature flag — per-operator
   batch count, row count, and wall time, dumpable on close — so we can profile a
   real job without a full tracing dependency. Keep it zero-cost when the flag is off.
3. **End-to-end native-vs-Flink throughput.** ✅ STARTED — an opt-in benchmark
   (`ThroughputBenchmark`, enable with `SF_BENCHMARK=true`) runs the same query over a
   large generated source into a blackhole sink, native-substituted vs stock Flink, and
   reports rows/s and the speedup. Required by the project: the readme carries a vs-Flink
   number per operator. Remaining: session/cumulative cases, and a parallelism sweep.
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
  fast hash (`ahash`) instead of SipHash, but the per-row `Vec` allocation remains.
  - **[done]** The non-windowed `GROUP BY` no longer clones the key per row to reach its
    group: an existing group (the steady state) is reached by `get_mut`, cloning only when a
    new group is inserted. ~8% on the string-key micro-bench (`group_by/sum_string_key`,
    2.00 → 1.85 ms). The same get-vs-insert pattern could be applied to the other keyed
    operators' update loops.
  - Remaining: the per-row `read_key` `Vec`/`String` allocation itself. Row-format
    (`arrow::row::RowConverter`) or dictionary-encoded keys are the next target — biggest win
    for string/composite keys (a single bigint key barely allocates). The `ahash` swap likely
    helps the session/partial grouping maps too — apply once those have benches.
- **[fixed]** `windows_for` allocated a `Vec` per row in the update loop. Reusing one
  buffer across rows cut the tumbling bench ~26% (244 → 181 µs / 17 → 22.6 Melem/s).
- **[fixed]** Session `update` sliced one row at a time. Now grouped per key and segmented into
  gap-connected runs (the same connected components the row-at-a-time walk built), one `take` +
  accumulator update per run: 2.04 ms → 217 µs (9.4×) on the dense gap-chained shape
  (`session/sum_keyed_dense_update_flush`), tumbling-level throughput. The sparse one-row-session
  shape is bound by per-session costs (accumulator creation, flush materialization), not the
  update loop; its open-session merge scan became a bounded `BTreeMap` range probe.
- **Not a problem:** the tumbling accumulator update is already vectorized — rows are grouped
  per (window, key), then a single `take` + `update_batch` per group, so accumulators
  see batches, not individual rows. Don't "optimize" this without numbers.
- **Whole-row converter (Java).** `RowDataArrowConverter` is the cost of every row↔columnar
  transpose. It was made row-major + pre-sized (~25% faster — ticket 28); a further
  column-vectorized rewrite could speed it more. **Not** a native/JNI candidate — the row side is
  JVM `RowData`, so the work is irreducibly on the JVM (a native decoder was investigated and
  rejected — ticket 28). Java optimization only; bench the transpose before/after.

## Acceptance criteria
- `cargo bench` runs the native operator benches and prints rows/s per operator.
- The parity harness emits native-vs-fallback wall time for each query it checks.
- `docs/benchmarks.md` exists with the method and an initial results table.
