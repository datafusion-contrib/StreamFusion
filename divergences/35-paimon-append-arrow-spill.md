# Columnar append spill behind Paimon's writer lifecycle

Paimon 2.0.0 switches append writers to `InternalRowSerializer` buffers after its writer-count
threshold. Its append writer does not expose a replaceable buffer through the released API. The
native sink therefore wraps Paimon's selected sink write, retaining all of its checkpoint,
coordinator, compaction and commit behavior, while handling the automatic transition before rows
reach that private buffer. A runtime-only table copy disables Paimon's duplicate transition.

At the transition, existing native files are finished and retained. Re-reading and rewriting them
would discard the Arrow representation and assign their sequence numbers again. Retaining them
changes file boundaries and absolute sequence metadata compared with a stock transition, but
preserves rows and the monotonically increasing sequence ranges Paimon assigns to actual writes.
Afterward, Arrow buffers drain one bucket at a time through the original bundle entry. Paimon's
public compaction entry flushes and closes that bucket's encoder before the next bucket drains.

The native temporary-file ownership follows Comet's
`native/shuffle/src/writers/local/spill.rs`: DataFusion's disk manager owns local files, and dropping
the buffer removes them even after failure or cancellation. Arrow batches cross JNI through the
existing Comet-style C Data ownership pattern (`NativeUtil.scala`); they are never serialized as
Java rows for spilling. Compression surrounds Arrow IPC so Paimon's configured Zstandard level
can be honored. LZO follows the block-compression approach used in `paimon-rust`'s
`crates/paimon/src/btree/block.rs`. Compression calls Paimon's released Java codec with reusable
byte buffers; JNI borrows direct buffers synchronously and retains only the compressor object
for the native writer's lifetime. This follows the existing JNI Parquet output adapter's bounded
Java byte-transfer approach. Rows stay columnar. Decoding uses released `lzokay`, whose fixed-size
output slice bounds allocations even on corrupt input. Our local framing uses 64 KiB blocks with
uncompressed/compressed lengths; these disposable spill files are consumed by the same native
writer and do not need Java's spill-file framing.

The buffer uses the table's Arrow memory budget rather than Paimon's Java memory segments.
Managed-memory append sinks consequently keep the stock planner path. The released Java spill
writer dereferences a null compressor with `spill-compression = none`; that setting stays stock
as well rather than silently changing its failure behavior. Primary-key buffering and batch
execution retain their existing paths.
There is no corresponding Arroyo Paimon sink operator to port.

Coverage and configuration are described in [the Paimon connector page](../docs/connectors/paimon.md).
