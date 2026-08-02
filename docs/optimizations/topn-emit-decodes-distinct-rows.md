# Top-N emit decodes distinct rows, not emitted rows

**Applies to:** the append-only and retracting streaming [Top-N](../operators/top-n.md) rankers.
With mini-batch off, each keeps the byte-identical per-input cascade
([divergences/20](https://github.com/datafusion-contrib/StreamFusion/blob/main/divergences/20-minibatch-netdiff-topn.md)).

## The problem: decoding the same row over and over

The with-rank cascade emits the same `Arc`-shared buffered row at many rank positions — in a hot
partition, the same top-N rows appear over and over across a batch's cascades — but emit decoded
arrow-row state bytes per *emitted* row, not per distinct row. In the q19 profile that decode was
**72%** of the operator's CPU.

Emit now decodes each distinct row exactly once and rebuilds the emitted positions with a
vectorized `take`: output stays byte-identical, but decode cost drops from O(emitted) to
O(distinct). q19 gained **+13%** end to end on the generator profile loop, and the decode's CPU
share fell from **72% to 6%**. The operator is then bound by materializing the cascade's output
volume itself — Flink's own changelog contract, not native overhead.

## Under mini-batch: cut the volume itself

Decoding once per distinct row removes the decode cost, but the with-rank cascade still emits every
rank transition on every input row. Mini-batching attacks that volume directly: the ranker carries
each touched partition's preimage across physical Arrow batches and emits the net logical-bundle
rank diff only at count, watermark, checkpoint, or end-of-input boundaries. This preserves the
collapsed changelog exactly — the same first-preimage/final-postimage contract described on
[Logical mini-batches, decoupled from physical Arrow batches](mini-batch-logical-boundary.md) — while
mini-batch off keeps the byte-identical per-input cascade (divergences/20).

Criterion on 4,096 rows, 64 partitions, ascending Top-10 with sustained boundary churn finds the
logical diff **2.70×** faster than a diff after every 256-row physical batch for membership output
(**6.34 vs. 2.35 M rows/s**), and **2.28×** faster with projected rank (**7.25 vs. 3.18 M rows/s**).
It also beats the immediate per-input cascade by **1.40×** and **3.41×** respectively.

A five-second release profile puts most samples in Top-N mutation and `arrow_row::Row::owned`
allocation after coalescing; eliminating that ownership traffic is the next optimization frontier,
not further changelog materialization. The first-touch key/preimage staging is charged to the
operator's managed-memory reservation and released at flush; the shared metrics report its peak
bytes and the actual touched-partition count, not emitted rows as a proxy — see
[Memory accounting designed off the hot path](memory-accounting-off-hot-path.md).

## Retracting Top-N: only the first and final visible window

The same logical-window diff covers retracting Top-N. Its full per-partition buffer still applies
every input insert/retract, so rank `N+1` promotion remains correct — nothing is skipped on the way
in — but only the first visible window and the final visible window are materialized as changelog.

Criterion over 4,096 rows, 64 partitions, and 256-row physical batches measures **3.16 M** input
rows/s for one logical flush versus **1.24 M** for immediate per-row diffs and **2.13 M** for
per-physical-batch diffs — **2.54x** and **1.48x** faster. Rank-projected output uses the same
position-aware final diff. A deterministic SQL parity test covers the real retracting plan produced
by `GROUP BY` into Top-N; mini-batch-off keeps the original per-input behavior.
