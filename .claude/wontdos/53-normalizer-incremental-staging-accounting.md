# Incremental changelog-normalizer staging accounting

**Status:** WONTDO (2026-07-13).

The mini-batch changelog normalizer currently recomputes the retained bytes of its touched-key map
after each physical Arrow batch. A prototype changed `MiniBatchChanges::touch` to report first
insertion and maintained the byte count incrementally, preserving delete/reinsert transition folding.

Criterion rejected the change on the 4,096-row, 64-hot-key replacement storm in
`normalize_logical_bundle`: the 256-row physical-chunk case was statistically unchanged (about
11.78 M rows/s), while the one-flush logical case regressed about 3% (20.31 M rows/s). The touched
set is only 64 entries, so its scan is cheaper than the added per-touch bookkeeping. Reconsider only
with a profile and benchmark dominated by substantially higher touched-key cardinality.
