# Stock canonical window-join state

Arroyo's `crates/arroyo-worker/src/arrow/instant_join.rs` and
`join_with_expiration.rs` retain separate Arrow inputs and delegate matching to a DataFusion
plan. StreamFusion keeps that existing execution structure. Released Flink 2.2.1 instead saves
per-key/window lists and firing timers. The canonical adapter projects retained rows directly
into that stock layout rather than changing normal native state or replaying input through the
join. Original selected keys, row serializers and Long window-end namespaces remain Flink types.
No StreamFusion serializer or saved execution graph is required by the stock state payload.

The JNI boundary follows Comet's `native/jni-bridge/src/arrow_array_stream.rs` transfer/release
pattern. Memory imports follow `native/core/src/execution/operators/copy.rs`: gather sliced arrays
with `MutableArrayData` and own retained child/dictionary buffers before releasing Java's Arrow
batch. Imported C Data is dropped before installing a JNI exception because release callbacks
can enter Java. Persistent export scans bounded chunks of individual records rather than
hydrating a complete key; transient encoding/decoding and import buffers reserve host memory.

The stock operator saves no watermark state. Restored native execution starts with a fresh event
frontier and waits for input watermark replay, matching released recovery semantics. A complete
inventory/key/namespace/timer preflight precedes native mutation. Native processing-time window
joins cannot use this event-time contract: the released stock planner rejects that shape.

Canonical bindings pass the resolved stock maximum parallelism to both columnar exchanges and
the native join. Resolving it again from the new environment can hash imported rows into a
different key-group layout, including a keyless stock operator whose maximum is one. Ordinary
native planning keeps its existing default when no canonical binding supplies a maximum.

See [window-join coverage](../docs/operators/joins/window-join.md#stock-canonical-savepoint-interoperability)
for the tested component scope and remaining full-job/planner work. Complete interoperability
remains tracked by [#569](https://github.com/datafusion-contrib/StreamFusion/issues/569).
Benchmarking is deferred under the maintainer's functionality-first direction.
