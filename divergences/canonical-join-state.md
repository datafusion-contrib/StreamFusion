# Stock-compatible regular join canonical state

Reference review: Arroyo's `crates/arroyo-worker/src/arrow/join_with_expiration.rs`
keeps separate left/right key-time tables and delegates matching to a DataFusion plan.
It does not define Flink's unbounded updating-join multisets, unique-key state shapes,
or outer association counters. We retain the existing native incremental join state
and project the exact released Flink 2.2.1 `JoinRecordStateViews` and
`OuterJoinRecordStateViews` schemas on each side.

Stock `left-records` and `right-records` independently use a value when the join key
contains a unique key, a map keyed by another unique key, or a full-row occurrence
map when there is no unique key. Outer sides add the stored association degree.
Projection preserves that degree and every original per-row TTL write timestamp;
recomputing associations from the currently visible opposite side would change
continuation after expiry. Keys, row serializers, tuple serializers and TTL envelopes
are released Flink types. No StreamFusion serializer enters the savepoint metadata.

Memory and RocksDB transfer rows directly through the Arrow C Data bridge. The
bridge follows Comet's `native/core/src/execution/utils.rs` ownership transfer:
Java synchronously consumes moved arrays and schemas, and the owner releases the
C structs. There is no intermediate native snapshot. Bucket state is visited in
place; indexed INNER state scans individual records and accepts legacy bucket data.
Indexed imports merge points across bounded transfer chunks without deleting earlier
rows or applying TTL read expiry. Ordinary input keeps its usual expiry semantics.
Imported Arrow batches are released before a validation exception is installed in
the JVM: release callbacks may themselves enter Java and clear a pending exception.
The same cleanup ordering applies to the other canonical import adapters.

Complete input validation now precedes either side's native insertion. Validation uses the same
native row/key decoder in a mode that does not change the stores, then insertion visits the
validated stock state. This extra pass avoids partial import on a malformed later side or batch;
it creates no native snapshot intermediate. Canonical handoff correctness takes priority over
performance for this infrequent operation.

This commit qualifies the direct operator adapter. Automatic planner binding, constructed join
payloads, async state, broader mini-batch/changelog qualification, complete jobs, connector
protocols, versions and rescaling remain required by
[#569](https://github.com/datafusion-contrib/StreamFusion/issues/569). Benchmarking and tuning are
deferred until the required functionality is complete.
