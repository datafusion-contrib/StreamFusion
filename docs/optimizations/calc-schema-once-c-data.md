# Export Calc schemas once

**Applies to:** native Calc operators

Arrow C Data transfers the batch buffers without copying them, but an `ArrowSchema` is a separate
tree of field metadata, native strings, and release callbacks. A Calc's input schema cannot change
during one operator instance, yet the original bridge rebuilt and exported that identical tree for
every physical batch.

The first batch still exports both its `ArrowArray` and `ArrowSchema`. Rust caches the resulting
struct type alongside the compiled Calc. Later batches use Arrow Java's array-only export and
Arrow Rust's `from_ffi_and_data_type`, so only buffer ownership and child-array layout cross JNI.
This preserves the standard C Data ownership contract: the Java array's release callback is still
consumed exactly once, and the schema remains owned by the native Calc handle until close.

The Q8 profile that identified the issue measured repeated Calc schema export at 120 samples in a
30-second profile (4.00 samples/second). After the change it fell to 14 samples in 25 seconds (0.56
samples/second), an 86% reduction; the remainder is the required first schema for each new job in
the profile loop. Inclusive Calc CPU fell by roughly 12% after normalizing against completed jobs.

The 2-million-event exactly-once Kafka Q8 timing remained within run-to-run noise: 1.472 seconds
before and 1.499 seconds after, while the adjacent Flink baseline moved from 1.617 to 1.845 seconds.
The optimization is retained because it removes measured allocator and schema-tree work without
changing batch contents, but no end-to-end Q8 gain is claimed from that noisy pair.
