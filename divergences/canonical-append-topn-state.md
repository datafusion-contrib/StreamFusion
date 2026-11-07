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
not qualify the full operator family; see
[#569](https://github.com/datafusion-contrib/StreamFusion/issues/569). Benchmarking is deferred.


Automatic binding uses the actual released AppendFast function rather than the rank label or SQL
text. A fast Top-1 may omit a projected constant rank after Flink optimization. Resolve stock key
serializers, selectors, TTL, identities and maximum parallelism from the real transformation.
Use that maximum on both rank and its columnar exchange. Stock-bound append operators retain
per-record emissions under table mini-batching; native net-diff batching has a different visible
changelog and cannot silently replace this contract. Transposes/coalescers continue to flush
pending batches before the barrier. This is a correctness choice; benchmarking remains deferred.

The released append restore fixture is selected by a thin subclass with original inherited
assertions. Its native audit records only successfully committed import batches, separately from
validation and new input. Typed expiry controls synchronize Guava's ticker with the harness TTL
clock without changing Flink's cache or state logic. Admission remains limited to tested scalar
fields and one sort column while broader rank/state variants are qualified.


Runtime modes must be installed before the shared initializer imports stock state. Calling the
first-bound initializer after the base `open()` had already attempted restore rejected valid bound
headers. Reuse the existing pre-restore hook for both first-bound and non-time ROW_NUMBER modes.
Also propagate the actual update-before flag through append create/restore, as the retracting path
already does; the previous default silently added before-images to negotiated Top-1 upsert streams.
Tests retain stock's raw wire rows and compare them separately from final materialized effects.
