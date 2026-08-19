# Parquet column-chunk streaming

**Applies to:** the native Parquet sink

The native encoder uses parquet-rs' lower-level row-group and column-writer APIs. When a row group
closes, each encoded column chunk is appended immediately through a bounded one-MiB JNI adapter to
Flink's recoverable output stream. The previous high-level `ArrowWriter` path first accumulated the
completed chunks in the output `SharedBuffer`, then drained them after the write call returned. Its
cleared `Vec` retained row-group-sized capacity while the encoder built the next row group.

This keeps Parquet's normal row-group boundary while reducing the additional bridge high-water mark
from a compressed row group to one MiB. It also lets Flink's object-store stream begin forming and
uploading multipart pieces while later column chunks are appended. Flink still decides multipart
size, local staging, rolling, checkpoint recovery, and final publication.

This does not eliminate row-group memory: parquet-rs still retains encoded and not-yet-encoded data
inside the active column writers until that row group closes. It eliminates the second row-group-
sized output copy and releases completed column chunks to Flink one by one.

Raw changelog output stays columnar too. The hidden native `i8` row-kind vector becomes the key
buffer of a four-value Arrow dictionary (`+I`, `-U`, `+U`, `-D`) and parquet-rs consumes that
dictionary directly. The writer therefore does not allocate or copy a two-byte string for every
change; insert-only batches allocate only a compact zero-key vector.

Delta merge-on-read output uses the same encoder without rebuilding a schema for every batch. The
schema is imported once when a data file opens; later C Data Interface calls carry arrays only.
Non-contiguous row selections remain indices over retained Arrow buffers until parquet-rs performs
one native gather per column. File-level Delta statistics are then read from the native Parquet
footer through Delta Kernel's standard typed statistics reader, so the transaction-log values keep
the connector's existing semantics. The retained row view implements Flink's getters directly over
that batch, avoiding a second Java row-view allocation, and each qualified output directory is
initialized only once per handler instead of once per checkpoint file.

On the exact 2M-event Delta q0 comparison, those two Java-side changes reduced StreamFusion from
2.962 to 2.710 seconds while the matched Flink result remained effectively flat (4.233 versus 4.195
seconds), improving speedup from **1.43x** to **1.55x**. Allocation profiles before and after showed
the `ColumnarRowData` allocation disappear; total allocation remained dominated by Delta's shared
stream, string, and hash-table bookkeeping.

In the 2M-event q0 changelog-Parquet benchmark (parallelism 4, memory state, mini-batching off, one
warmup and best of three), Flink's rowwise parquet-mr path took 1.532s and the native Arrow/parquet-rs
path took 1.051s: **1.46x**. A matched 20-second CPU profile completed 19 native iterations versus 12
Flink iterations. Only 9.0% of native CPU samples were below the Parquet writer boundary, and the
JNI output adapter itself accounted for 0.03%; native JSON decode remained the largest leaf cost.
