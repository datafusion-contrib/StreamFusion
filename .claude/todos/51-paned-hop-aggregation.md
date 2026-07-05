# HOP aggregation: tiered slice merge in the two-phase global (re-scoped)

**Status:** re-scoped 2026-07-05 after checking the premise against the q5 profile; gated on the
next re-profile.

**The original premise was stale.** The ticket proposed paned aggregation (fold each row once into
a slide-width pane, merge s/d panes per window fire — Arroyo's `TieredRecordBatchHolder`) to cut
the hopping aggregator's per-row O(s/d) fold. But q5's actual plan already computes exactly that:
under the default two-phase strategy the **local** window aggregate pre-aggregates per slice (one
fold per row, `sliceSize = slide`) and the **global** merges each slice partial into the
`size/slide` windows sharing it. The q5-native flame graph confirms both halves
(`update` = the local's per-row slice fold, `updatePartialTumblingAggregator` = the global's
slice merge). Per-row work is already O(1); the O(s/d) survives only per-slice-per-key in the
global's merge. The single-phase hop `update` does fold each row into each window, but the
planner takes that path only when two-phase is unavailable — no Nexmark query is bound by it.

**What might still pay (measure first):** the global's slice merge was ~39% of q5's window-agg
samples (71/181). Two candidate cuts, in order of likely value:
1. **Cheap loop hygiene** — `update_partial` clones the group key once per (window, slice) and
   probes `BTreeMap` + key map per window; the borrowed-byte-probe pattern (ticket 49) and a
   move-into-last-window key handoff (as the single-phase update already does) may take most of
   the bucket without architecture.
2. **Tiered panes** (Arroyo) — retain slice accumulators and merge tiers whose widths divide the
   next, amortizing the per-window merge from s/d to ~log. Only worth it if (1) leaves the merge
   dominating; adds pane-retention state and a snapshot-format change.

Parity note (unchanged from the original ticket): any merge reordering must reproduce the host's
accumulator semantics — and note Flink itself slices hopping windows (`SliceSharedAssigner`), so
slice-merge trees are the host-faithful shape.

Acceptance: post-round q5 profile isolating `update_partial`'s leaves; implement (1) if it shows
key-clone/probe churn; (2) only with a bench showing the merge still dominates after (1).
