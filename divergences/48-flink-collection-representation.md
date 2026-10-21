# Flink collection value representation

Comet's batch-kernel input adapters create fresh nested views over Arrow array, struct and map
slices. StreamFusion follows their lifetime rule: each view belongs to its input batch, and a
synchronous generated evaluator writes its result before that batch closes. No input buffer or
mutable lookup view is retained across evaluations.

Flink adds two deterministic contracts beyond the SQL fields. BinaryStringData can retain a Java
UTF-16 cache, which changes comparison order and preserves isolated surrogates in string consumers.
Nested RowData also has a RowKind. Ordinary UTF-8 bytes and SQL struct fields cannot reconstruct
either detail. The experimental collection entry schema therefore carries nested strings as a
marked struct of UTF-8 plus an optional list of UTF-16 units, and appends a marked kind child to
nested rows. Markers distinguish these children from user fields; row-kind names avoid collisions.
Arrow reversal and range copying retain the complete children and their validity.

Flink's ArrayDataSerializer preserves Java caches when copying GenericArrayData and
ColumnarArrayData. It serializes unfamiliar ArrayData implementations to binary instead. Since
those Flink classes are final, a custom cache-aware view cannot inherit their copy behavior.
Represented arrays undergo the released serializer's snapshot before writing. Their element
carriers record the resulting generic/binary copy category, including when the first slot is NULL.
Readers restore that category: generic arrays share cached strings between consumers, while binary
arrays return fresh values. Row headers also retain their generic/binary category. Ordinary arrays
keep the existing columnar views. SQL checks include generic, binary and independent proxy arrays,
repeated string consumers, NULLs in declared NOT NULL elements and every nested row kind.

Programs containing only nullable primitive collection types keep ordinary layouts and avoid
serializer snapshots that preserve no additional identity. Represented nested-row vectors reuse
their reader for the lifetime of the batch; each returned row is still copied before that reader
can move to another index.

The planner captures the entry and generated-result Arrow schemas in serialized operator/function
specifications. Task JVM properties cannot change those layouts after planning. Connector schemas
retain their ordinary layouts. Representation-sensitive inputs from other columnar producers and
string/ROW/MAP-bearing or NOT NULL-element array results crossing unverified operator boundaries
keep the host route. Represented final results also require a row sink: native writer substitutions
are declined until they verify the extended physical schema. Ordinary nullable primitive array
outputs retain native sinks. These schemas are scoped to generated collection programs; merely enabling
the experimental switch does not change other operators' schemas.

Arrow Java 18.3 requires a non-nullable map-key field declaration. The existing declaration stays,
while Flink's logical key nullability and actual vector validity preserve NULL keys in the verified
generated collection readers/writers. This does not admit Arrow map comparison or state operations
over those extended results. Tests cover nullable keys at fourteen released bridge types/nullability combinations, nested
values, empty/NULL maps and runtime constructors.

The switch defaults on under the maintainer's explicit acceptance on 2026-10-17, despite slower
full row-source jobs. Setting it false restores host execution for the extension.
[Issue #392](https://github.com/datafusion-contrib/StreamFusion/issues/392) tracks performance improvements. Coverage, restrictions and
performance evidence belong in [Calc/filter](../docs/operators/calc-filter.md#collection-expressions)
and the [scalar benchmark page](../docs/benchmarks/scalar-functions.md#extended-function-coverage).

Ordinary OVER COUNT over a newly admitted complex argument needs an IPC-safe retained carrier
when its input schema contains MAP, including MAP columns only passed through as payload.
Arrow Java's MAP key declaration is non-nullable even when Flink supplies NULL keys. Native
MAP_FROM_ARRAYS results therefore reuse the List-of-entry-Struct carrier, with nullable
key/value children and a `streamfusion.map-list` marker on the list's child field. Keeping
the marker in the DataType child preserves it through expression type inference without
unsafe Arrow schema construction. This follows the existing IPC carrier; the Java reader
recognizes either its original outer marker or the expression child marker. Matching
input offsets share both flattened child buffers; other offsets are aligned by row.
NULL keys, duplicate keys and entry order remain visible to subsequent consumers.
Rust IPC
validates it during restore. For these newly admitted queries, a metadata-only schema view aliases
MAP buffers as List-of-entry-Struct with nullable keys before native import. Map and List share
offset/validity/value layout; Comet's C export/load ownership pattern retains the original buffers
before Java closes its input view. No borrowed root closes those vectors independently.
The logical MAP reader accepts this physical carrier after native output and restore, including
nested maps. Existing scalar-count queries merely carrying MAP payloads keep their previous
schema/state representation. MAP-valued native sink output remains gated; numeric counts can
still use a native sink. Released Flink comparisons cover nullable keys, payload emission,
three-way exchange and two memory/RocksDB restores.

Nested MULTISET uses the same physical Map storage and requires the same alias, even if the
input contains no logical MAP. A four-case released-SQL regression checks ARRAY-of-MAP,
ARRAY-of-MULTISET and ROW-of-MULTISET with NULL keys through two restores on each backend.
Before including MULTISET in physical-map detection, the two multiset-only cases failed
with the same IPC non-nullable-key validation error. No primitive or logical MAP benchmark
fixture changes; the additional alias applies only to those previously unverified descendants.


Standalone event-time OVER COUNT over MULTISET uses this same nullable-key entry-list carrier,
with non-null INT occurrence counts. Window aggregates instead consume parent validity and
retain only their BIGINT aggregate state. Neither path sums occurrences. Released Flink
checks return original bags and restore pending/frame state twice on both backends.

## Native basic-type searches

ARRAY_CONTAINS and ARRAY_POSITION follow Comet's typed flat-values/offsets search structure,
with one type dispatch per batch and no per-row subarray allocation. They do not reuse Spark's
or DataFusion's results verbatim: Flink CONTAINS matches NULL needles to NULL elements, and
POSITION returns INT zero when absent and NULL for a NULL needle. Matching BOOLEAN, integer and
DECIMAL types have exact native equality; decimal precision and scale are checked before dispatch.
Scalar needles are not expanded to the batch length. Potentially failing arguments retain the
complete generated Calc to preserve Flink's evaluation schedule. The existing representation
checks keep strings, nested values and NOT NULL-element arrays on their verified generated route.
CONTAINS also retains generated evaluation for non-NULL literals or nonnullable needles: Flink
constructs its array element getter from that needle type. For DECIMAL, a stock binary array's
unchecked read of a NULL cell can throw where the Arrow-backed JVM view returns NULL; that
combination retains the complete stock Calc rather than normalizing the error away.

ARRAY_MIN and ARRAY_MAX instead reuse released DataFusion 54 directly for nullable integer and
DECIMAL arrays. Flink's NULL skipping, nullable empty result, exact ordering and declared result
type agree with those released kernels, including their short-list scan and long-list Arrow
reduction. No duplicated reduction implementation or private dependency is needed. The planner
retains generated evaluation for floating/string/temporal/nested elements, NOT NULL elements,
fallible operands and representation-sensitive consumers; neither Spark floating ordering nor
DataFusion's broader type support establishes parity for these excluded signatures.

MAP projections reuse DataFusion/Comet's shared Arrow view pattern, with the
verified map-list carrier retained for NULL keys. MAP_UNION cannot use ordinary
hash-map overwrite semantics: Flink preserves right duplicates, special-cases
empty right maps and treats left NULL keys differently from other duplicates.
A typed stable gather preserves those released behaviors.
