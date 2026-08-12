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

The planner selects the wire shape from Flink's checkpoint configuration. With unaligned
checkpoints disabled (Flink's default), the exchange keeps the fast destination batches above and
forces that edge aligned. With unaligned checkpoints enabled, every parent batch instead emits one
record per non-empty key group. Each fragment carries a parent epoch/sequence, its original row
ordinals, and the parent's non-empty key groups. Flink's ordinary `RANGE` channel-state filter can
therefore reroute every whole fragment after rescaling. During normal execution a checkpointed
reassembler uses that compact manifest to identify its destination-local siblings and restores
their original row order with a k-way merge of the already-sorted ordinal streams.
After recovery it delivers old-attempt fragments independently: some sibling groups may already
have been applied before the checkpoint and now live in downstream operator state. The restored
producer uses a fresh epoch, so newly produced parents immediately resume ordered reassembly.

The recovery-safe representation is used for the whole execution because Flink may start a
checkpoint aligned and switch it to unaligned after its alignment timeout; records already buffered
when that happens must be independently recoverable. Process-local handle-table transfer stays off
in this mode because its handles cannot survive restore. Native keyed operators honor Flink's
`pipeline.max-parallelism` setting as the stable key-group count.

Recovery tests cover both protocols. The aligned test proves that destination batches leave no
Arrow channel state. The unaligned test creates backpressure, proves that the checkpoint captured
Arrow channel state, fails the job, restores from parallelism 2 to 3, and verifies every source id
exactly once. Operator-harness tests separately restore a partially assembled parent and hold a
watermark until all of its key-group fragments arrive.
