# Row-major, pre-sized entry transpose

**Applies to:** the RowData→Arrow entry transpose

This page opens a line of work on the row↔Arrow perimeter: the transposes at a native island's
edges are the tax every rowwise-fed job pays, and this line of work took Nexmark q0–q2 from ~0.6x
to 1.1–1.6x vs Flink (`fbe714c`). The techniques below it in the nav — the Arrow unsafe-checks
flags, the zero-copy exit transpose, the string-copy reduction, and projection pruning into the
transpose — are further cuts into the same perimeter.

## What it is

The RowData→Arrow entry converter used to fill column-major, growing each Arrow vector with
`setSafe` as rows arrived. It was rewritten row-major into vectors pre-sized to the row count —
the same shape as Comet's `ArrowWriter` (`64528e7`).

## Measured

354 → 265 µs per 4096-row batch.

## Rejected alternative

A native Rust row decoder — parsing Flink's row wire format directly in Rust instead of converting
from a JVM-materialized `RowData` — was investigated and rejected: it only ties the JVM build and
loses once JNI is counted (`f70d078`).
