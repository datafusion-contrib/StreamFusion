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

For an aligned exchange with one destination, the splitter forwards the existing Arrow root with
representative key group zero. Every key group belongs to that destination, so hashing, gathering,
and the split's JNI export/import round trip are unnecessary. Empty batches are consumed without
emitting a record. The forwarded batch keeps the split operator's handle owner and encoding metrics;
normal IPC serialization and downstream coalescing still apply. Unaligned exchanges retain their
per-key-group fragments even at parallelism one, because recovery may change the destination count.

A release+mimalloc benchmark of two-phase `AVG(DISTINCT)` with DECIMAL(38,2), 20 million runtime
rows, 64 keys, 1,024-row mini-batches, and both transposes measured native elapsed time falling from
18.737 s to 15.938 s (14.9% less time). Five alternating-engine measured trials followed two warmups
on JDK 17 / Flink 2.2.1, with process-local exchange handles disabled. Native trials changed from
18.705–18.853 s to 15.933–16.019 s; the Flink control changed from 13.283 s to 13.683 s. This improves
the existing native path but does not close its gap to Flink. CPU profiling identified split,
serialization, and coalescing costs alongside membership updates; this change removes only the
unnecessary single-destination split, without changing the query, source, sink, or batch settings.

The same setup with two million rows measured:

| Two-phase DISTINCT workload | Previous native (s) | Forwarding native (s) | Flink control (s) |
|---|---:|---:|---:|
| COUNT/SUM, DECIMAL(38,2) | 1.903 | 1.556 | 1.441 |
| AVG, DECIMAL(19,2) | 1.680 | 1.499 | 1.710 |
| AVG, DECIMAL(38,2) | 1.965 | 1.673 | 1.618 |

Native trial ranges were 1.552–1.603 s, 1.357–1.597 s, and 1.665–1.891 s respectively;
Flink ranges were 1.417–1.521 s, 1.677–1.828 s, and 1.538–1.878 s. The previous native
medians are the [typed membership optimization baseline](aggregate-specialization-fast-paths.md).
Narrow AVG now beats its matched Flink control; the wide cases still need further optimization.

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
Arrow channel state, fails the job, restores from parallelism 2 to 3 and 3 to 2, and verifies every source id
exactly once. Operator-harness tests separately restore a partially assembled parent and hold a
watermark until all of its key-group fragments arrive.

Partitioner copies retain their configured channel count. This is required by Flink 1.18's
recovery filter, which sets up a partitioner before copying it and uses that copy directly.
