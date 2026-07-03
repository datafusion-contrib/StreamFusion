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
- **Two-phase `AVG`** — single-phase `AVG` is native; the local/global split (sum + count partials,
  merged then divided at the global) is not modelled. The global merge is a SUM over partials plus
  the existing `AVG` finish; the local needs the two-column partial.
- **Widening partials** — a partial Flink widens past the value type (`SUM(INT)` → bigint partial)
  makes the local matcher decline; today those route single-phase. Model the widened partial type
  in the local/global pair.
- **Two-phase decimal `SUM`** — the single-phase decimal SUM (i128, `DECIMAL(38, s)`) is native;
  carry the same accumulator through the local/global split.
- **Row-time mini-batch** — the mini-batch assigner over a rowtime (watermark-driven flush) falls
  back; only the proc-time marker is native.

**Acceptance:** each shape parity-verified against the host (the existing two-phase harness tests
are the template), `docs/coverage-and-fallbacks.md` updated in the same commit, and a Nexmark run
with mini-batch enabled to confirm routed fractions improve.
