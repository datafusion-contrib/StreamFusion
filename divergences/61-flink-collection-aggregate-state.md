# Collection aggregate state follows Flink's changelog contracts

COLLECT and ARRAY_AGG reuse the existing Arrow-batch keyed aggregate operator and its
heap/RocksDB checkpoint boundaries. They do not introduce a second operator or JNI ownership
protocol. Arroyo's updating-aggregate planner delegates aggregate work through DataFusion;
StreamFusion's existing keyed aggregate instead preserves the released Flink state contracts.

DataFusion's ordinary collection aggregate state is not a substitute for those contracts.
Flink COLLECT uses signed 32-bit multiplicities, including negative and zero counts, and a
fresh Java HashMap result snapshot. ARRAY_AGG keeps arrival order, removes the first matching
retracted element, and retains unmatched retractions separately. DISTINCT membership has its
own shared-filter TTL/write policy, separate from the retained aggregate result. Rust state
therefore follows the released CollectAggFunction, ArrayAggFunction and StateMapView behavior,
using the project's Java map-order model and shared DISTINCT retention machinery.

The collection snapshot envelope carries a version, aggregate kind and typed element codec.
JVM-managed collection state and this native format are intentionally separate. Restoring a
JVM savepoint requires `streamfusion.experimental.rustCollectionAggregates=false`; automatic
migration is not claimed. The existing JVM route also remains available for verified mixed
aggregate groups whose other functions do not have a Rust implementation. Richer collection
signatures and consumer boundaries retain their existing admission guards.


DATE and TIME extend the existing counted/sequence state with dedicated primitive tags.
Their internal epoch-day/millisecond values use the same boxed-integer hash/equality contract,
while restore and DISTINCT membership preserve the logical type. NOT NULL temporal arrays
retain Flink's generic copy category at aggregate output, through column projections and into
JVM argument batches. The legacy JVM handler uses the same represented output carrier.
Verified scalar-array results can feed the existing generated UNNEST boundary using one
structural producer check shared by native and legacy collection state. Nullable arrays retain
the Arrow take route. Unverified state/order, character/nested array and native-writer consumers
retain their representation guards; updating MAP/MULTISET payloads remain deferred. The generated
expansion is still Java work inside the columnar operator, preserving released unchecked getters
without another native ownership or callback protocol.

Flink source NOT NULL declarations are logical hints, not runtime input validation. Actual
NULL slots can reach a collection aggregate and succeed. Shared Arrow import therefore widens
only inconsistent physical field flags, retaining the logical SQL type, buffers and metadata.
This follows Comet's existing C Data transfer/release pattern; it adds no copy or ownership
protocol. Cached declared schemas stay immutable. Sink NOT NULL enforcement and its failure
remain observable at the released Flink boundary.

Collection aggregates crossing a materializing input Calc over supported NOT NULL scalar
hints retain fallback.
Flink's unchecked primitive getters can reveal a source-specific payload beneath actual NULLs;
preserving an Arrow NULL does not reproduce that value or its TIME conversion failure. The
collection admission guard covers predicates reading those hints and non-identity projections,
while direct input, key-only identity filters and nullable-input COALESCE remain verified.


The state algorithm is generic over a typed element codec. FLOAT/DOUBLE reuse the existing
Java floating key comparison with separate retained payload bits; DECIMAL reuses the decimal
hash model and unscaled i128 values. Accumulation, retraction, DISTINCT/TTL, map-copy order and
checkpoint handling remain one implementation. Primitive version-one snapshot bytes remain
compatible, verified against six prior-implementation fixtures. New codec tags retain width
and decimal precision/scale rather than inferring them from stored bytes.


Timestamp elements reuse the full-range component reader and the same typed collection algorithm,
with i128 membership and the released TimestampData hash model. Interval families reuse the
primitive month/millisecond codecs. The declared output element type drives Arrow construction,
so timestamp metadata survives. Original primitive version-one snapshots remain byte-compatible.

A required exchange copy boundary clears compact timestamp sub-millisecond remainders as Flink's
row serialization does, including before a one-channel fast path. Source buffers remain immutable;
component milliseconds, validity and schema metadata are retained through Arrow references and
the established Comet C Data transfer pattern. Without this boundary, compact COLLECT values/counts
could match while subsequent formatted map iteration differed. This changes no key-group routing,
checkpoint namespace or recovery protocol. Existing nested/state/writer guards and #437 remain.


Owned string ARRAY_AGG reuses the same typed state with Arc<str> instead of copying the numeric
algorithms. Inputs are detached from Arrow batches; outputs and checkpoints own their payloads.
Comet's batch handoff/release pattern remains unchanged. Input and output representation
requirements are separate: a preserved collection output does not force extended scalar string
arguments through a preceding Calc.

Character COLLECT deliberately remains on the generated JVM handler. BinaryStringData implements
StringData, whose Comparable argument is StringData rather than BinaryStringData itself;
HashMap collision-tree tie breaks can therefore use JVM object identity. Lexical Rust ordering
would change an observable result-map contract. This extension admits verified character
collection outputs without admitting unverified character scalar coresults or new state/writer
consumers. The broader native arbitrary-value collection goal remains incomplete.
