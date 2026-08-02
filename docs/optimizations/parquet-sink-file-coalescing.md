# Parquet sink file coalescing

**Applies to:** the Parquet sink

One writer handle is now held open across batches, rolling files at a row target or checkpoint
instead of opening one file per batch — per-batch footer/syscall overhead was a major cost of the
original one-file-per-batch design.

Parquet copy went 2.61x → 4.68x; Parquet sink went 1.06x → 2.24x.
