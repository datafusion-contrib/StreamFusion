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

The default-enabled boundary for [#237](https://github.com/datafusion-contrib/StreamFusion/issues/237)
therefore reuses released Flink aggregate generation and the synchronous grouped runtime.
Arrow ownership stays at the operator boundary, following Comet's batch handover; argument
row views are borrowed during synchronous evaluation, and collected output is immediately
written into owned Arrow buffers. No function JNI ABI or native accumulator serializer is added.

The columnar exchange retains Flink BinaryRow key-group ownership. Every logical key establishes
its real managed-state context rather than using a synthetic subtask key. An experimental
primitive-key accumulator cache flushes pending updates to managed state before eviction,
watermarks, completion and snapshots, restoring the previous key context. Checkpoint boundaries
invalidate cached references so subsequent mutations pass through the backend's snapshot copy
protocol. Flink retains persistence, restoration and DataView ownership. Positive TTL uses the
released grouped function's state configuration, including per-operator hint precedence.
Accumulator caching and input coalescing are disabled for expiring state. DISTINCT/FILTER
continue through Flink's generated handlers and managed DataViews. Async state and mini-batch
plans retain fallback.

Wide DECIMAL grouping keys use Flink's RowData serializer for key projection, matching the
native exchange's existing BinaryRow hash contract. The generated Flink projection uses a
different NULL-decimal reservation; mixing it with the native layout can send a key to the
wrong subtask. The serializer-backed selector returns owned INSERT-kind keys and leaves source
changelog kinds unchanged. Same-line checkpoints/savepoints and rescaling are verified;
stock/native or cross-version state migration is not claimed.

This is correctness-verified UDF coverage under the accepted performance exception, without
a speedup requirement. Failed cache designs and dispersed timing trials remain
diagnostic evidence; see [GROUP BY](../docs/operators/group-by.md#jvm-aggregate-investigation).
Merge/reset, OVER and additional function/type contracts remain pending.

The exact released legacy Table API aggregate wrapper is admitted alongside the bridging wrapper.
It reaches the same generated handler, managed POJO/DataView state and grouped runtime; only
planner recognition differs. Wrapper subclasses with custom construction, Python functions and
specialization remain excluded. Unchanged released mixed-DISTINCT WeightedAvg fixtures and
focused POJO accumulator recovery checks verify both supported Flink lines. No legacy state
codec or stock/native migration promise is introduced.


Integer COLLECT reuses this same released handler/managed MapView boundary. Arroyo's updating
aggregate planner and vector-collecting UDAF adapter were consulted before the extension.
Flink's COLLECT exposes incremental multiplicities, retractions and managed DataView state;
retaining its handler avoids introducing a second multiset accumulator or checkpoint codec.
The first admission covers only INT/BIGINT elements with scalar inputs and non-character
output fields. Collection results require a verified preserving Calc or final row sink;
other value types, ARRAY_AGG and unverified state/writer boundaries retain fallback.
This builtin coverage uses the maintainer's bounded collection-performance tradeoff, with
separate stock/previous/native measurements; it does not extend the UDF performance exception.
