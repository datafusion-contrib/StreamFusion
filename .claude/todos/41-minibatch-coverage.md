# Mini-batch coverage: IncrementalGroupAggregate + the two-phase tails

**Status:** TODO. Prioritized 2026-07-03 (tier 2 of the coverage push — the most common real-world
fallback trigger after the Kafka gate).

**Why now.** Production Flink deployments routinely enable mini-batch
(`table.exec.mini-batch.enabled` + size/latency); it changes the plan shapes we see, and several of
those shapes fall back today, dragging whole queries to the host via the all-or-nothing gate:

- **`IncrementalGroupAggregate`** — a whole operator with no native path. This is what **any
  distinct aggregate** (`COUNT(DISTINCT user)`, …) plans to under two-phase mini-batch, so the
  single most common "why didn't my query accelerate under mini-batch" answer. Flink's shape:
  local `MiniBatchLocalGroupAggFunction` → **incremental** `MiniBatchIncrementalGroupAggFunction`
  (merges the distinct value sets keyed by (group key, distinct key)) → global. Consult Flink's
  `~/data/flink` `table/runtime .../aggregate/MiniBatchIncrementalGroupAggFunction` before design;
  we already have the per-key distinct value set (native `COUNT(DISTINCT)`) and the two-phase
  local/global operators to compose from.
- **Two-phase `AVG`: DONE (2026-07-03).** The local expands an AVG into a widened-sum state plus a
  COUNT over the same column (its two positional partials), the matchers walk partials with
  per-aggregate offsets, and the global folds the pre-summed `(sum, count)` pair into the ordinary
  AVG state via a per-aggregate count-partial column — the divide/truncate/cast-back emit is the
  single-phase code, byte-identical. Parity-tested keyed + global, mixed with single-partial
  aggregates, negative values included. Scope matches the two-phase set (bigint/int/double).
- **Wider two-phase value types** — smallint/tinyint/float value columns decline the local matcher
  today (the single-phase path takes them); extend the running types to match the single-phase set.
  (The previously-listed "widening partials" item was a misdiagnosis: Flink's SUM partial keeps the
  value's own type — `SUM(INT)` two-phase already routes.)
- **Two-phase decimal `SUM`** — the single-phase decimal SUM (i128, `DECIMAL(38, s)`) is native;
  carry the same accumulator through the local/global split.
- **Row-time mini-batch** — the mini-batch assigner over a rowtime (watermark-driven flush) falls
  back; only the proc-time marker is native.

**Acceptance:** each shape parity-verified against the host (the existing two-phase harness tests
are the template), `docs/coverage-and-fallbacks.md` updated in the same commit, and a Nexmark run
with mini-batch enabled to confirm routed fractions improve.
