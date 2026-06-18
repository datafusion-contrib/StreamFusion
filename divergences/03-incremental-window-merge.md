# Incremental accumulator merge for session (and sliding) windows

**Kind:** algorithmic — internal implementation differs; output is identical.
**Diverges from:** Arroyo.
**Forced by parity:** no — this is a chosen optimization, not a requirement.

## Their decision
Arroyo's session and sliding aggregating windows **store raw input batches** in a
tiered structure (e.g. `batches_by_start_time`) and **re-aggregate** the relevant
rows whenever a window advances or sessions merge. The raw rows are the source of
truth; aggregation is recomputed from them.

## What we do instead — which is what Flink itself does
We keep, per key, only the **incremental accumulators** for each open window, and
when two windows merge we fold one accumulator's partial state into the other via
`merge_batch`. Raw rows are discarded after they are folded in.

This is *not* a freelance optimization: it mirrors Flink's own window operator.
Flink's combiner (`AggCombiner` / `RecordsWindowBuffer`) buffers raw records **in
memory only**, folds them into one accumulator per window, and persists **only the
accumulator** to state — two-phase merges slice accumulators, session merges
window accumulators. So for mergeable aggregates we match the host's strategy
exactly; **only Arroyo differs**, by retaining raw batches and re-aggregating.

## Why
Every aggregate we support (`SUM`/`MIN`/`MAX`/`COUNT`, and integer `AVG`) has
associative, commutative, mergeable partial state, so merging accumulators yields
exactly the same answer as re-aggregating the union of rows — but in bounded
memory (state per open window, not per row) and without recomputation on merge.

The result is identical to Flink (and to Arroyo), verified by the parity harness,
including the case where a late, out-of-order element bridges two open sessions.

## Why diverge at all, given the "copy them" default
Because the divergence is only from Arroyo, and we land on the *host's* approach:
matching Flink is the prime directive, and Flink keeps accumulators, not rows.
Arroyo's raw-retain model exists partly to support aggregates whose state is not
cleanly mergeable — a generality we do not need yet.

**Future (non-mergeable aggregates).** When we add aggregates that are not
associative/invertible, the precedent in *both* engines is **not** a "partial
accumulator + all rows" dual store — that redundancy is what they avoid. Instead
the state is chosen per aggregate: mergeable ones keep the small accumulator
(above); non-mergeable ones keep a **value→count multiset** and recompute on
demand (Flink's `MaxWithRetractAccumulator` is a running scalar plus a
`MapView<value, count>`; Arroyo's `IncrementalState::Batch` is a `HashMap<value,
count>`). We would follow that — per-aggregate state selection, value-multiset for
the non-mergeable set — rather than retaining raw rows alongside partials.
