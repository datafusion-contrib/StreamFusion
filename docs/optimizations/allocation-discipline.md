# Allocation discipline on the per-row paths

**Applies to:** windowed/session aggregation, [GROUP BY](../operators/group-by.md), [Top-N](../operators/top-n.md),
the updating join, `OVER`, deduplication

Beyond the arrow-row and mini-batch mechanisms covered elsewhere, a series of targeted fixes removed
allocations and redundant per-row work from specific hot loops: reuse instead of realloc, move
instead of clone, batch instead of loop, and — where the access pattern actually fits — a columnar
kernel instead of a row loop at all.

## Per-row allocation cuts

- Reuse the per-row window buffer instead of allocating one per row (**26%** on tumbling, `3833e8d`).
- Move the grouping key into its last window instead of cloning it (**~18%** keyed, `ffec81e`).
- Reach existing groups by `get_mut` and clone the key only on insert (**~8%** on string keys,
  `6802752`).
- Defer owning a Top-N row until it is known to enter the buffer, and share the payload via `Arc` so
  the with-rank cascade's double emits are refcount bumps instead of row deep-clones (q19 0.76x →
  1.13x, q18 0.82x → 1.28x, `22f5c0f`).
- Move the key/row into join state instead of re-cloning it on insert (`c597142`).

## Batch the per-row folds

- The running `OVER` aggregate replaced a DataFusion update-batch-then-evaluate call per row with a
  small typed running state folded directly (**~2.6x**, `945d3da`).
- The INNER updating join gathers all of a batch's candidate pairs, evaluates the residual predicate
  columnar in one pass, and emits by `filter_record_batch` — one convert/eval per batch instead of
  per row (q9 0.39x → ~1.0x, `4429e2f`); associated rows in the residual path bulk-decode in one
  `convert_rows` call (q7 0.33x → 0.74x, `ed74dac`).
- The session aggregator segments each key's rows into gap-connected runs so a run pays one value
  slice and one accumulator update, with the merge scan a bounded O(log n) range probe (**9.4x** on
  dense sessions, `62dffda`).

## Columnar-kernel internal state where it fits

Keep-first dedup holds its per-key candidates as a single Arrow batch — one row per pending key —
reduced per input batch with filter/take/concat kernels, reading only the key and the rowtime per
row, the same minimal per-row read Arrow's own hash aggregate does; it never boxes rows into scalars
(`ebfde70`).

This was deliberately **not** applied to window Top-N: bounded ranking with arrival-order
tie-breaking maps poorly onto columnar kernels, so its buffer stays row-oriented, as it does in
Arroyo and RisingWave.
