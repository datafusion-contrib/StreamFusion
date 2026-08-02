# Zero-copy exit transpose

**Applies to:** the Arrow→RowData exit transpose

## What it is

The Arrow→RowData exit transpose used to deep-copy and box every field, because the Arrow batch
was closed immediately after conversion. It now emits a reusable lazy `ColumnarRowData` view over
the Arrow vectors instead — the same columnar→row model Spark/Comet use — keeping the batch open
through the whole emit loop (`5a12f2d`).

## Why it works

A lazy row view reads fields from the still-open Arrow vectors on demand instead of materializing
and boxing every field up front. Combined with Flink object reuse enabled — a standard production
setting, applied to both sides of the benchmark — this removes the per-row allocation and copy that
used to dominate the exit path.

## Measured

Native q0 roughly doubled (`713a0a3`).
