# Sink candidate state follows Flink's ordered materializer

Arroyo's Kafka sink consumes Arrow batches and retains its transaction/checkpoint boundary in the
connector. Its updating aggregate tracks aggregate preimages, but neither supplies the ordered
candidate multiset required by Flink's sink upsert materialization. This operator therefore follows
the released Flink `SinkUpsertMaterializer` and sequenced-multiset materializer: replacement by
inferred input key, first-match removal and restoration of the preceding visible candidate.

The data plane reuses StreamFusion's native keyed state and Arrow C Data bridge. DataFusion Comet's
export/import ownership pattern remains unchanged: input C structs are borrowed for one call,
retained state owns its payloads, and exported output owns its buffers independently. The native
snapshot wrapper supplies Flink key-group redistribution and backend checkpoint integration.

Flink's LEGACY and VALUE use an ordered vector; MAP uses linked sequence nodes plus a matching-key
index, and ADAPTIVE migrates between them at the configured thresholds. Native state mirrors these
structures because the choice changes observable matching behavior: heap MAP includes generated
hashing, RocksDB MAP compares serialized keys, and VALUE uses equality without a hash lookup.
Snapshots retain the linked cursor and index independently of node payloads, preserving released
Flink behavior after older-slot replacement and subsequent appends. Flink 2.2 disables TTL for
non-legacy strategies, which the native path preserves.

The Rust state remains in the shared native memory budget; the native RocksDB backend checkpoints
it through the existing snapshot store. This differs from Flink's per-entry backend state while
preserving its key matching and sequence semantics. Paimon's merge-on-flush keyed upsert buffer is
not reused: it collapses records for storage and cannot emit the required per-arrival changelog.
