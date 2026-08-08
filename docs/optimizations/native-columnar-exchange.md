# Native columnar keyed shuffle

**Applies to:** keyed operators reached through a shuffle

A keyed exchange splits each Arrow batch by the projected key's Flink BinaryRow hash and Murmur
key-group mix, then maps each key-group batch to its current owner, so a keyed operator's input stays columnar across the shuffle
instead of transposing to rows and back. Matching the host key-group layout now lets native raw
keyed state rescale safely while the hot map stays in Rust.

The fully-columnar windowed pipeline (source → watermark → shuffle → window) measured **1.91x**
vs Flink, where the row-fed window was **1.21x**.

The exchange uses Flink's `RANGE` channel-state mapping, matching a keyed exchange. Each serialized
Arrow record contains exactly one key group and carries that topology-independent group id. During
unaligned-checkpoint recovery, Flink can therefore keep or discard that whole record and rerun the
partitioner to map it to the group's owner at the restored parallelism. This supports rescaling
without row-decoding or custom recovery hooks. Native keyed operators honor Flink's
`pipeline.max-parallelism` setting as the stable key-group count; when it is unset they use Flink's
normal parallelism-derived default.

The recovery integration test creates backpressure, verifies that an unaligned checkpoint actually
contains channel state on the Arrow exchange, fails the job, and restores that checkpoint from
parallelism 2 to 3. Its checkpointed file sink receives every source id exactly once.
