# DataFusion file scans with framework splits

**Applies to:** the Parquet source scan

## What it is

Parquet is now read through DataFusion's file-scan framework — projection pushed into the decode,
maintained readers, row-group split granularity — rather than through hand-rolled readers
(`ff98896`).

## Measured

Lifted the Parquet copy 4.68x → 4.97x.

## Note

ORC rode the same scan core until its source was removed —
[issue #19](https://github.com/datafusion-contrib/StreamFusion/issues/19).
