# 47 — Iceberg keeps its released Java streaming control plane

Comet provides the model for explicit Iceberg file-task metadata and Arrow/JNI ownership.
StreamFusion retains Flink's streaming discovery, split ordinals, row offsets, watermark
emitter, operator/coordinator state, rolling writers, delete encoders and committers.
Released iceberg-rust reads admitted Parquet tasks; parquet-rs encodes one Java-selected
data file at a time. Java FileIO supplies credentials, paths, streams and positional reads.
This extends the [encoding-only sink boundary](22-parquet-sink-encoding-only-native.md).

Flink's range sink adds another host boundary: its statistics operator emits borrowed
row views plus occasional control records into a stateful Java range partitioner.
The adapter delegates the original operator lifecycle/events and coordinator provider,
then applies that same partitioner synchronously to Arrow-backed row views. Control
records update the partitioner at the producer; selected row positions become Arrow
batches through the shared Rust splitter. The normal custom partitioner still disables
unaligned capture on that exchange. Checkpointed statistics, learned-assignment timing,
rescale handling and per-channel row order stay Java-owned; no native sort/routing policy
or separate statistics checkpoint format is introduced.

The cost is Java control traversal, batch gathering when selections are sparse, JNI
and host IO callbacks. These remain inside the separate Iceberg Nexmark timing. The
version-gated topology adapter changes payload factories/types before serialization;
unknown connector layouts retain the normal planning fallback. Encoder retention and
streaming checkpoint coverage are implemented; [coverage and measurements](../docs/connectors/iceberg.md)
record their limits. Remaining state/checkpoint performance work is tracked in
[#391](https://github.com/datafusion-contrib/StreamFusion/issues/391).

The dynamic sink uses the same boundary: wrap its declared `Map` factory cache while
delegating storage/eviction to the original LRU cache. Decorate newly created factories
before the host initializes them, so there is no provisional file creation or writer
reimplementation. The generator/updater still decide metadata and routing over borrowed
views; target-homogeneous selections cross both forward and keyed paths as Arrow batches.
A fixed payload layout avoids the host's materialized-row schema conversion becoming a
transport boundary. Unknown layouts keep the stock path.

The data encoder uses parquet-rs's `AsyncArrowWriter` directly, matching Comet's direct
Arrow/Parquet encoding boundary. Iceberg-rust's released single-file wrapper hides the
writer's retention estimate. Calling the underlying writer permits per-file charges
against Flink's task off-heap pool without replacing any Java rolling or commit policy.
Byte-parity fixtures against that released wrapper cover codecs, page versions, sparse
selections, NaNs, estimated file size and split offsets. The accounting includes reported
writer retention, estimated row-group metadata and the bounded JNI buffer; transient
codec and decoder allocations are observed through sampled RSS, not claimed as bounded.

The legacy committer's incoming write-result edge explicitly disables unaligned
capture, matching Flink's Sink V2 translator. A completed checkpoint must include all
its flushed files; allowing a priority barrier to overtake a write result breaks that
contract. The payload exchange still supports captured Arrow state and rescaling.
Dynamic keyed writes require immediate metadata updates because the released deferred
updater/direct-output union can reorder values for the same key. Deferred append writes
remain admitted. Recovery fixtures separate a completed streaming savepoint from the
terminal drain so they test captured-state recovery independently of end-input flushes.

Source cancellation follows Comet's explicit task-completion ownership pattern.
The cancellation registry owns fetched Arrow batches until the emitter claims them;
it releases records left in Flink's fetch queue when its last fetcher has already
exited. Claimed batches belong to downstream consumers, and the fetcher still owns
native reader destruction. This extends the host-IO cancellation hook without changing
split offsets, queue scheduling or the Java reader lifecycle.
