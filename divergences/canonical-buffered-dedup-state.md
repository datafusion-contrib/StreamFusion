# Buffered dedup canonical state

Keep the existing memory/persistent buffered dedup algorithms. Arroyo has no corresponding
released Flink waiting-row/marker/user-timer savepoint contract. Match Flink 2.2.1's actual
watermark-buffered first-row function: pending rows have no TTL, Boolean emitted markers retain
their original write ages, and one event timer belongs to each pending key.

Read live native state directly into stock values and callbacks. Never replay input to hydrate
state, expire raw markers during conversion, or serialize a native snapshot first. Validate the
whole incoming view before importing it into a fresh target. Preserve pending/emitted domains,
key bytes, row-time payloads, timer namespaces and stock serializers; reject unowned callbacks.
Follow Comet's moved C Data ownership pattern and close imported buffers before throwing JNI errors.
Closing a failed memory import releases its reservation.

Transport row kinds and Java string-cache sidecars do not enter buffered state. Reuse the existing
insert-only input normalizer before buffering, including continued input after canonical import.
The compiled-plan complete-job controls reproduced both polluted memory schemas and RocksDB
payload-width failures; preserve those inputs and assertions as regressions.

Canonical keyless emitted markers still represent one key when there are zero key columns.
Specify Arrow row counts explicitly in native partition snapshots and use the same constant key
codec in memory and RocksDB. Neither change alters existing nonempty key encodings.

Nested schemas retain the established RocksDB snapshot-backed native path when the direct typed
codec does not admit them. Instantiate the actual operator/backend during qualification rather
than forcing an unsupported constructor or broadening the codec merely to satisfy a test. The
same canonical contract reads live memory on this path, without a native snapshot intermediate.

Historical upstream ascending-rowtime plans are not necessarily buffered. Capture the actual
released function; do not rewrite fixtures or force a native family to manufacture coverage.
Remaining qualifications are tracked by
[#569](https://github.com/datafusion-contrib/StreamFusion/issues/569). Benchmarking is deferred.
