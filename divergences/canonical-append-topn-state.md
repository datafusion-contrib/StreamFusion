# Stock append Top-N state projection

Reference review: Arroyo's `crates/arroyo-datastream/src/lib.rs` declares windowed `TumblingTopN`
and `SlidingAggregatingTopN`, but supplies no equivalent unbounded Flink append-state interchange
contract. Keep the existing native ranker and project its complete retained state directly into
released Flink 2.2.1 append lists or fast Top-1 winners. Capture the real partition and sort key
selectors and stock serializers; do not rebuild state by replaying visible output. Hidden overflow
rows, tie order, row kinds, original TTL write ages and independent variable bounds can all affect
future output and must survive transfer.

Comet's `native/core/src/execution/utils.rs` establishes moved Arrow C Data ownership. The existing
StreamFusion bridge follows that ownership and preserves a receiver's original Throwable after
dropping native FFI owners. Transfer batches are dropped before installing JNI exceptions. Raw
RocksDB scanning/hydration bypasses expiry only for canonical transfer; normal reads retain TTL.
No intermediate native snapshot, checkpoint artifact or processing replay is produced.

A whole-source validation pass precedes native mutation. Frame validation independently checks
metadata, actual key bytes, key/payload agreement and duplicate winners/bounds, including previous
persisted import chunks. Validation drains read-through absent entries and releases their charges.
A NULL primitive in an invalid declared non-null native payload must be rejected before field
getters can fabricate zero. Physical NULL proctime slots remain eligible for their existing
representation contract; the broader type matrix is still pending. Direct foundation tests do
not qualify normal job submission or the full operator family; see
[#569](https://github.com/datafusion-contrib/StreamFusion/issues/569). Benchmarking is deferred.
