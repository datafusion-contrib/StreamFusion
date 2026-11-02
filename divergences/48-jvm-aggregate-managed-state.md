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
continue through Flink's generated handlers and managed DataViews. Async state and multi-phase
JVM plans retain fallback.

Wide DECIMAL grouping keys use Flink's RowData serializer for key projection, matching the
native exchange's existing BinaryRow hash contract. The generated Flink projection uses a
different NULL-decimal reservation; mixing it with the native layout can send a key to the
wrong subtask. The serializer-backed selector returns owned INSERT-kind keys and leaves source
changelog kinds unchanged. Same-line checkpoints/savepoints and rescaling are verified;
stock/native or cross-version state migration is not claimed.

Single-phase scalar JVM mini-batching instead retains released Flink's map-bundle runtime:
owned key copies, `addInput` row copies, count triggers and `finishBundle` evaluation on
watermark, checkpoint and completion boundaries. Arroyo's aggregate planner and window
worker were consulted before design; its vector UDAF contract cannot replace Flink's
incremental JVM handlers, managed DataViews and per-key bundle ordering. Comet's Arrow
reader ownership remains the boundary: buffered state cannot retain borrowed batch views.
The columnar exchange uses the generated key projection's NULL decimal/timestamp slot
layout for this route, since a different key hash changes task-local count-bundle membership.
Existing serializer/native key encoding remains unchanged. Flink owns the managed snapshots;
no cross-plan or cross-version state migration is introduced. Released bundle lifecycle,
including its rich-function-only close helper, is retained rather than substituting the
ordinary grouped function's different close behavior.

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
The verified boundary covers TINYINT/SMALLINT/INT/BIGINT and BOOLEAN elements with scalar inputs and non-character
aggregate result fields. Collection results require a verified preserving Calc or final row sink;
other value types and unverified state/writer boundaries retain fallback.
This builtin coverage uses the maintainer's bounded collection-performance tradeoff, with
separate stock/previous/native measurements; it does not extend the UDF performance exception.

Integer/BOOLEAN ARRAY_AGG on Flink 2.2 follows the same boundary, retaining the released RAW LinkedList
accumulator, NULL treatment, ordering and retraction behavior. Flink 1.18 has no released SQL
ARRAY_AGG operator and remains explicitly N/A. Nullable primitive-array outputs additionally
use the existing verified Top-N/UNNEST carriage. Richer elements and unverified represented
boundaries remain guarded; no new native list state or snapshot format is introduced.

Character grouping fields use the same released BinaryRow key projection as other JVM groups.
Their output is the canonical serialized key, rather than a pre-group Java string cache; the
collection gate therefore checks character aggregate results separately from grouping fields.
Runtime Unicode/cache/collision, parallel and repeated-recovery evidence qualifies that narrow
extension without changing key codecs, managed-state ownership or the grouped operator.

The Flink 2.2 legacy consumer adapter follows the existing 1.18 prepared-sink pattern for
verified direct integer/BOOLEAN collection groups. It captures host-proven keys and retraction
metadata before replacing the input, then retains the released row sink and converters.
The unchanged upstream COLLECT fixture qualifies both backend routes with consumed JVM
group rows and explicit exclusions. This changes the exit admission/metadata boundary,
not the grouped operator, state layout or Arrow/JNI ownership protocol.
