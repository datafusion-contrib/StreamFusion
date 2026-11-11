# Prepaid Paimon append-compaction working sets

Comet's native memory pools ask the host task manager for incremental reservations through JNI.
StreamFusion follows that ownership pattern and shared host authority, but append compaction
prepays estimated reader and encoder working sets before opening the host rolling writer. Its paimon-rust 0.4.0
`MemoryPool` suballocates the reader's share of that reservation. This allows contention to delegate
the complete Java-selected group before output exists, without introducing a partial-output retry
contract. Ordinary source reads and primary-key merge compaction retain their separate policies.

The rolling file writer cannot accept a paimon-rust resource context: Paimon's Java host still owns
output statistics, sequences, rolling and commits, using StreamFusion's existing columnar codecs.
The operation therefore reserves an encoder allowance and checks the codec's current buffered-byte
estimate after each bundle. Parquet uses parquet-rs memory estimates (including pending INT96
vectors); ORC uses the released Java writer and conversion-vector estimates. Neither estimate is a
strict RSS cap. The reader runs one Parquet row group at a time, and cancellation retains the shared
lease until the last decoder reservation releases. Compact reads also reserve a single shared
staging buffer, bounded by the reader estimate and 4 MiB, and grow it lazily for requested ranges.
Reusing it across sequential input files avoids the previous per-64-KiB stream-read overhead for wide chunks.
Ordinary sources keep 64 KiB transfers. Following StreamFusion's spillable memory-pool
pattern, admission atomically leaves headroom for Arrow imports rather than charging their buffers
twice. That headroom is not exclusive: later competing allocations can still deny an import and
fail the native attempt. Exported Arrow batches keep their independent shared-allocator lifetime
rather than borrowing the reader's local resource context.

See [Paimon coverage](../docs/connectors/paimon.md) for the reservation formula, fallback evidence and
measured limits. This addresses [issue #638](https://github.com/datafusion-contrib/StreamFusion/issues/638)
without changing Java's file selection, writer targets or compaction transaction boundaries.
