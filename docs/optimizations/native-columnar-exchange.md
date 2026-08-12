# Native columnar keyed shuffle

**Applies to:** keyed operators reached through a shuffle

A keyed exchange splits each Arrow batch by the projected key's Flink BinaryRow hash and Murmur
key-group mix, then gathers the rows into at most one order-preserving batch per destination
channel, so a keyed operator's input stays columnar across the shuffle instead of transposing to
rows and back. Matching the host key-group layout now lets native raw
keyed state rescale safely while the hot map stays in Rust.

The fully-columnar windowed pipeline (source → watermark → shuffle → window) measured **1.91x**
vs Flink, where the row-fed window was **1.21x**.

Each serialized Arrow record carries a representative key group from its destination's range. A
random-key 8192-row input therefore produces at most the downstream parallelism in network records,
instead of nearly one IPC stream per row. Rows retain their original order within every destination.

The exchange disables unaligned checkpoints on its edge. An in-flight destination batch can contain
key groups that separate after rescaling, while Flink's recovery API can only keep or discard a
whole record. Alignment drains those topology-specific records before the checkpoint; restored
producers then repartition new batches at the restored parallelism. Native keyed operators honor Flink's
`pipeline.max-parallelism` setting as the stable key-group count; when it is unset they use Flink's
normal parallelism-derived default.

The recovery integration test enables unaligned checkpoints globally, verifies that the columnar
edge retained no channel state, fails the job, and restores the aligned checkpoint from parallelism
2 to 3. Its checkpointed file sink receives every source id exactly once.
