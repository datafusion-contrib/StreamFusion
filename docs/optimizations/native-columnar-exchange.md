# Native columnar keyed shuffle

**Applies to:** keyed operators reached through a shuffle

A keyed exchange splits each Arrow batch by the projected key's Flink BinaryRow hash, Murmur
key-group mix, and channel mapping, so a keyed operator's input stays columnar across the shuffle
instead of transposing to rows and back. Matching the host key-group layout now lets native raw
keyed state rescale safely while the hot map stays in Rust.

The fully-columnar windowed pipeline (source → watermark → shuffle → window) measured **1.91x**
vs Flink, where the row-fed window was **1.21x**.

The exchange uses Flink's `RANGE` channel-state mapping, matching a keyed exchange. It forces
aligned barriers on this edge even when the job enables unaligned checkpoints: an in-flight Arrow
record was batched for the old topology, and Flink's recovery filter can keep or drop a record but
cannot split it across several new key-group ranges after rescaling. Aligned recovery has no
in-flight exchange records; replay passes through the splitter configured for the restored
parallelism.
