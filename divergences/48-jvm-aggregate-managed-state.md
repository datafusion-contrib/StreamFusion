# JVM aggregates retain released Flink managed state

Reference implementations consulted before design: Arroyo's
`crates/arroyo-operator/src/udfs.rs`, Comet's
`native/jni-bridge/src/comet_udf_bridge.rs` and
`native/spark-expr/src/jvm_udf/mod.rs`, and released Flink 2.2.1/1.18.1 grouped aggregate runtime.

Arroyo's UDAF adapter accumulates Arrow array arguments and invokes a vector-taking function.
That structure fits its UDF ABI. A Flink JVM AggregateFunction instead owns an arbitrary
accumulator with incremental accumulate/retract operations, generated external/internal
conversions, managed DataViews and checkpoint serializers. Buffering every input until
getValue would change this public contract and make retracting streaming state unbounded.

The default-off investigation for [#237](https://github.com/datafusion-contrib/StreamFusion/issues/237)
therefore reuses released Flink aggregate generation and the synchronous grouped runtime.
Arrow ownership stays at the operator boundary, following Comet's batch handover; argument
row views are borrowed during synchronous evaluation, and collected output is immediately
written into owned Arrow buffers. No function JNI ABI or native accumulator serializer is added.

The columnar exchange retains Flink BinaryRow key-group ownership. Every logical key establishes
its real managed-state context rather than using a synthetic subtask key. An experimental
primitive-key accumulator cache flushes pending updates to managed state before eviction,
watermarks, completion and snapshots, restoring the previous key context. Checkpoint boundaries
invalidate cached references so subsequent mutations pass through the backend's snapshot copy
protocol. Flink retains persistence, restoration and DataView ownership. TTL, async state and
mini-batch plans are excluded from this initial synchronous experiment.

This is not admitted acceleration. Failed cache designs and dispersed timing trials remain
diagnostic evidence; see [GROUP BY](../docs/operators/group-by.md#jvm-aggregate-investigation).
Merge/reset, OVER and additional function/type contracts remain pending.
