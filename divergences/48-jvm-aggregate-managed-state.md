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
continue through Flink's generated handlers and managed DataViews. Async state retains fallback; scalar local/global mini-batches use the released merge handlers described below.

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
Additional function/type contracts remain in [#237](https://github.com/datafusion-contrib/StreamFusion/issues/237); scalar time-based OVER is described below.

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

Two-phase scalar JVM aggregation keeps Arroyo's local-partial/shuffle/global-merge structure,
but retains Flink's incremental JVM ABI rather than Arroyo's vector-taking UDAF adapter.
References consulted before this extension include Arroyo's `udfs.rs` and updating-aggregate
planner, Comet's `comet_udf_bridge.rs` and `ColumnarBatchArrowReader.scala`, and Flink's
`StreamExecLocalGroupAggregate`, `StreamExecGlobalGroupAggregate` and released map-bundle
functions. The local function accumulates/retracts into owned bundle accumulators; global
handlers first combine partials and then merge into Flink managed state and DataViews.
Internal complex partial fields use Flink's declared serializers inside Arrow binary columns;
scalar partial fields remain typed Arrow columns. This preserves arbitrary declared accumulator
serializers without introducing a Rust JVM-object format or transposes inside the island.
Public inputs/results stay within the verified scalar boundary. Original local/global aggregate-info lists are retained before child substitution, preserving
monotonicity-based retraction elision and partial layouts. Logical count-bundle triggers,
barrier/watermark drains, generated BinaryRow key layout, TTL and released lifecycle remain
Flink-owned. Persistent state and supported migration boundaries are unchanged.

## Scalar time-based OVER

Arroyo's `arroyo-planner/src/plan/window_fn.rs` and
`arroyo-worker/src/arrow/window_fn.rs` finalize timestamp-bucketed Arrow windows on
watermarks, and its UDAF ABI evaluates collected vectors. Those boundaries cannot preserve
Flink's incremental synchronous JVM aggregate ABI, bounded retraction, per-key DataViews,
processing-time frames and released retention behavior. We therefore reuse each released
Flink 2.2.1/1.18.1 OVER frame function and generated handler, with Arrow batch boundaries
and the native columnar exchange. We read the exact released planner/runtime source jars,
not the newer sibling checkout, before choosing this boundary.

The released execution node selects and generates the function. A temporary transformation
provides its unopened function and key selector; only our Arrow transformation enters the
runtime graph. This avoids copying private code generation or version-specific frame
selection. We adapt the ordinary keyed process contexts and internal timer service, keeping
released state descriptors and managed DataViews. There is no Rust encoding of JVM objects.

Comet's JNI UDF bridge and Arrow batch reader establish the ownership precedent: retained
values must survive the borrowed batch. Released heap OVER frames retain entire input rows
and read compact event-time slots with `getLong`. Each input therefore becomes an owned
BinaryRow copy before the Arrow batch closes. A GenericRow copy would violate that physical
contract. Callback output is copied immediately into Arrow, including optional original Java
string-cache columns, before shared or mutable UDF output can change. Checkpoint preparation
flushes output only; it does not force watermarks or empty buffered frames.

This remains covered by the accepted UDF performance exception. Whole-job diagnostics retain
both perimeter conversions and production managed state, and compare stock, previous
production and candidate revisions. See [OVER](../docs/operators/over.md#scalar-jvm-aggregates)
for exact admission, restoration boundaries, exclusions and measurements.
