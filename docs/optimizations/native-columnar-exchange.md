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


## Avoid full-payload heap copies in IPC transport

The IPC serializer writes its serialization buffer directly to Flink's output view,
avoiding `ByteArrayOutputStream.toByteArray()` and its full-payload copy. A serializer
instance reuses that heap buffer after a successful write, starting at 4 KiB and
retaining at most 1 MiB of capacity. Oversized or failed writes discard the buffer.
Only encoded heap storage is reused; the input Arrow root still closes after encoding.
This follows Comet's exposed/reusable shuffle serialization buffer pattern, with a
retention limit because Flink duplicates serializers across edges.

The reader exposes the framed input directly to Arrow instead of first allocating
and filling a complete payload byte array. Reads cannot cross the declared frame
length, and the remaining end-of-stream marker is consumed before returning a batch.
The stream does not own or close Flink's input view. Legacy frames, key-group tags,
ordered recovery metadata and process-local handle frames keep their wire formats.

A matched 20M-row wide-AVG diagnostic isolates removal of payload copies before
buffer reuse: native median 15.199 s (15.097–15.279) becomes 14.886 s
(14.814–15.034), about 2.1% less time. Flink controls are 13.452 s in both runs
(ranges 13.313–13.511 and 13.290–13.764). This uses release+mimalloc, JDK 17,
Flink 2.2.1, Core i7-12650H/Linux, 2 GiB heap, parallelism one, 64 keys,
1024-row mini-batches, two warmups and five alternating engine trials, both
transposes and row source/sink. The improvement leaves native slower than Flink.

With bounded output-buffer reuse as well, the same sustained diagnostic measures
14.524 s native (14.449–14.831) against 13.408 s Flink (13.286–13.463).
That is about 4.4% less native time than the matched 15.199 s baseline, but
still 8.3% slower than Flink. No process-local exchange handles are enabled.

At 2M rows, the final COUNT/SUM diagnostic measures 1.434 s native
(1.407–1.454) versus 1.440 s Flink (1.367–1.469), effectively tied within
variation. The earlier matched-resource native baseline was 1.469 s. This
short-run result does not erase the sustained wide-AVG deficit above.

Validation runs 48 serializer, coalescing, key-group routing/reassembly, recovery
and decimal SQL checks on each released Flink line (2.2.1 and 1.18.1). They
cover consecutive legacy/tagged frames, truncated-frame isolation, growing and
shrinking payloads beyond the reuse limit, and aligned/unaligned checkpoint
recovery with rescaling in both directions. All pass without skips.
