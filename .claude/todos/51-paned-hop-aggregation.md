# Paned (tiered) HOP aggregation: aggregate each slide once, reuse across windows

**Status:** TODO (from the 2026-07-04 reference survey,
`.claude/research/reference-engine-techniques-2026-07.md` — Arroyo's `TieredRecordBatchHolder`,
`sliding_aggregating_window.rs`).

**Why:** a `HOP(size s, slide d)` assigns every row to `s/d` overlapping windows, and our hopping
aggregator folds each row into each window's accumulator independently — q5's 10s/2s hop does 5x
the accumulation work per row. Paned aggregation folds each row **once** into its slide-sized pane
and, when a window closes, merges the `s/d` pane partials — turning per-row work from O(s/d) to
O(1) with an O(s/d) merge per window firing. Arroyo tiers the panes (each width dividing the
next) so even the merge amortizes; for Nexmark-scale s/d a single pane level is enough.

q5 (Hot Items) is the direct beneficiary: 1.32x generator / 2.3–3.5x Kafka today — and notably the
query where Alibaba's own per-query table shows ~1.02x, so this is a place to lead, not chase.
The cumulative (`CUMULATE`) assigner shares the fan-out shape and can reuse the pane machinery.

**Parity note:** pane-merge must reproduce the host's accumulator semantics exactly — SUM/COUNT
merge trivially; AVG merges as (sum, count) partials (already our two-phase shape); MIN/MAX merge
extremes; DISTINCT panes would need set-union (gate them off panes initially, as the two-phase
path already gates). Emission timing/content is unchanged — this is pure state-side amortization,
no changelog encoding question.

Acceptance: hopping-window Criterion bench (add one: multi-window overlap shape) shows the O(1)
fold; q5 re-measured on generator + Kafka; window-aggregate parity suite green incl. session/
cumulate regressions.
