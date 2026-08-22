# Parquet sink file coalescing

**Applies to:** the Parquet sink

One standard parquet-rs `ArrowWriter` handle is held open across batches for each Flink part file
instead of opening one file per batch. Flink retains ownership of rolling and always finalizes a
bulk-format part on checkpoint; configured size, rollover-time, and inactivity checks can finalize
it earlier. Per-batch footer and stream-open overhead was a major cost of the original design.

The current implementation deliberately uses the public Arrow writer rather than a custom
low-level column writer. It therefore flushes a row group and footer when the part file is finalized,
not individual column chunks on a timer.

Parquet copy went 2.61x → 4.68x; Parquet sink went 1.06x → 2.24x.
