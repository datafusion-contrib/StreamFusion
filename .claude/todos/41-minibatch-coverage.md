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
- **Wider two-phase value types: DONE (2026-07-05).** AVG admits the full AvgAggFunction family
  (smallint/tinyint/float included; the sum partial widens to bigint/double, the emit casts back) —
  the single-phase set, which for SUM/MIN/MAX remains bigint/int/double. Fixing this surfaced and
  fixed a single-phase bug: the group aggregator's value-column downcast lacked the narrow arms, so
  admitted AVG(SMALLINT/TINYINT) folded zeros and AVG(FLOAT) panicked the native library.
- **Two-phase decimal `SUM`/`AVG`: mini-batch pair DONE (2026-07-05).** SUM's partial rides as
  `DECIMAL(38, s)` (bundle overflow → NULL partial, skipped by the SUM merge and latched NULL by
  the AVG merge — verified against the host), MIN/MAX keep `DECIMAL(p, s)` through the extremes
  multiset, AVG merges the `(DECIMAL(38, s), bigint)` pair into the exact-division emit. Remaining:
  the **windowed** local/global split — the native windowed local emits single-field partials
  (matching Flink's one-column accumulators), but our decimal accumulator keeps a two-field
  `(sum, count)` state, so decimal (and int/narrow, whose wrapping accumulators are also two-field)
  stay gated to bigint/double there; carrying them needs split-specific single-field partial
  accumulators with skip-NULL merge (Flink's own merge expression).
- **Row-time mini-batch** — the mini-batch assigner over a rowtime (watermark-driven flush) falls
  back; only the proc-time marker is native.

**Acceptance:** each shape parity-verified against the host (the existing two-phase harness tests
are the template), `docs/coverage-and-fallbacks.md` updated in the same commit, and a Nexmark run
with mini-batch enabled to confirm routed fractions improve.

## Follow-through once coverage lands: the tuned-Flink benchmark column

Decided 2026-07-04: the matrix gains a **"tuned Flink" mode** — `table.exec.mini-batch.*` (+
`table.optimizer.distinct-agg.split.enabled`), the same config on BOTH engines per the steelman
rule, run for the changelog-family queries only (group aggregates, updating joins, dedup, Top-N —
the windowed aggregates have no mini-batch plan variants and would just duplicate rows). Why: the
matrix already runs object reuse as "standard tuned-prod"; mini-batch is the other standard
tuning for exactly the stateful queries where we claim the biggest wins — today's numbers are
honest vs *default* Flink but unpublished vs *tuned* Flink, whose per-record GC churn (the
dominant cost our differential profiles measured on q9/q18/q23) mini-batch largely removes. It is
also the apples-to-apples config for the only public per-query Alibaba table (mini-batch 2s +
distinct split). Do NOT run it before the coverage above lands — with the mini-batch exec nodes
still falling back it measures our fallback, not our engine.

This benchmark mode doubles as ticket 46's harness: if net-diff Top-N emission ships gated on
mini-batch plans, its parity check (collapsed changelog vs mini-batch Flink) and its performance
claim both live here.
