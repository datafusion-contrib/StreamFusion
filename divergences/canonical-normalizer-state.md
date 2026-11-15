# Direct normalizer canonical state

Match the released Flink 2.2.1 `SyncStateDeduplicateFunctionBase` and
`ProcTimeDeduplicateKeepLastRowFunction`: `deduplicate-state` retains the full normalized INSERT
row, optionally wrapped in its original TTL write timestamp. Tombstones carry only keys, so
visible output cannot reconstruct this state. Preserve the existing native upsert state machine;
no new operator algorithm or Arroyo replacement is introduced.

Follow Comet's `native/core/src/execution/utils.rs` moved Arrow C Data ownership pattern. Java
consumes exported batches synchronously, and native import releases borrowed batches before
installing JNI exceptions. The typed RocksDB visitor reads raw values without applying expiry;
normal processing retains its existing expiry behavior. No serialized native snapshot is used.

A fresh target is required for stock initialization. Validate the full stock view before
insertion, including actual-key/payload agreement, row kinds and duplicate keys. Per-batch
validation precedes mutation; raw hydration detects persisted duplicates across chunks.
Receiver failure leaves rows available for retry. Mini-batch staging must be drained by the
existing operator barrier hook before projection; conversion itself does not emit pending rows.

Automatic compiled/ordinary binding captures the selected released synchronous or mini-batch
function before execution, including its actual retention and update mode. It binds the stock
identity/maxP and leaves stock JSON portable. All 52 normalizer/join job scenarios are qualified,
including pending barrier flushes and fixed-max rescaling; three unchanged released native
normalizer fixtures prove real import plus continued input, and two filtered fixtures remain
stock. Wider shared-state, schema/backend/version and connector/failure guarantees remain open
in [#569](https://github.com/datafusion-contrib/StreamFusion/issues/569). Other dedup algorithms
keep separate state contracts. Benchmarking is deferred.
