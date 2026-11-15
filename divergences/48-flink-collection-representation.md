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

Native collection aggregates mark NOT NULL DATE/TIME array outputs with the existing generic
copy category. Direct Calc projections preserve source field metadata under aliases, pruning,
reordering and repeated references; generated JVM argument batches carry that metadata into
their readers. DataFusion's projection schema preserves source metadata by the same rule.
Root schema metadata also survives projection and C Data import. Computed expressions retain
their own output representation contract rather than inheriting an arbitrary input marker.

Programs containing only nullable primitive collection types keep ordinary layouts and avoid
serializer snapshots that preserve no additional identity. Represented nested-row vectors reuse
their reader for the lifetime of the batch. Each returned row owns its fields before that reader
can move to another index: known owning scalar getters need no second serializer copy, while
nested views retain the deep copy.

The planner captures the entry and generated-result Arrow schemas in serialized operator/function
specifications. Task JVM properties cannot change those layouts after planning. Connector schemas
retain their ordinary layouts. Representation-sensitive inputs from other columnar producers and
string/ROW/MAP-bearing or NOT NULL-element array results crossing unverified operator boundaries
keep the host route. Represented final results also require a row sink: native writer substitutions
are declined until they verify the extended physical schema. Ordinary nullable primitive array
outputs retain native sinks. These schemas are scoped to generated collection programs and verified
direct string-keyed map projections; merely enabling
the experimental switch does not change other operators' schemas.

Arrow Java 18.3 requires a non-nullable map-key field declaration. The existing declaration stays,
while Flink's logical key nullability and actual vector validity preserve NULL keys in the verified
generated collection readers/writers. This does not admit Arrow map comparison or state operations
over those extended results. Tests cover nullable keys at fourteen released bridge types/nullability combinations, nested
values, empty/NULL maps and runtime constructors.

The switch defaults on under the maintainer's explicit acceptance on 2026-10-17, despite slower
full row-source jobs. Setting it false restores host execution for the extension.
The [collection representation ledger](../docs/optimizations/collection-representation-work.md)
records measured boundary improvements and remaining costs. Coverage, restrictions and
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
Scalar MAP results retain their complete owned one-row ListArray directly. DataFusion 54's
generic scalar extraction copies the list field's name and nullability but drops its metadata;
using it here loses the MAP marker, including for constant mixed-precision decimal constructors.
The MAP adapters preserve the field through scalar broadcast as well as array evaluation.
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
checks keep unverified nested values and NOT NULL-element arrays on their verified generated route.
Nullable represented STRING searches use the native views described below.
CONTAINS also retains generated evaluation for non-NULL literals or nonnullable needles: Flink
constructs its array element getter from that needle type. For DECIMAL, a stock binary array's
unchecked read of a NULL cell can throw where the Arrow-backed JVM view returns NULL; that
combination retains the complete stock Calc rather than normalizing the error away.

ARRAY_MIN and ARRAY_MAX instead reuse released DataFusion 54 directly for nullable integer and
DECIMAL arrays. Flink's NULL skipping, nullable empty result, exact ordering and declared result
type agree with those released kernels, including their short-list scan and long-list Arrow
reduction. No duplicated reduction implementation or private dependency is needed. The planner
retains generated evaluation for floating/temporal/nested elements, NOT NULL elements,
fallible operands and representation-sensitive consumers; neither Spark floating ordering nor
DataFusion's broader type support establishes parity for these excluded signatures.
Represented STRING extrema use the native comparator described below rather than ordinary
DataFusion UTF-8 ordering.

MAP projections reuse DataFusion/Comet's shared Arrow view pattern, with the
verified map-list carrier retained for NULL keys. MAP_UNION cannot use ordinary
hash-map overwrite semantics: Flink preserves right duplicates, special-cases
empty right maps and treats left NULL keys differently from other duplicates.
A typed stable gather preserves those released behaviors.


Scalar string exits also retain the generic row representation expected by released legacy
`RowData` consumers. Canonical strings may borrow Arrow bytes only during the synchronous
ownership copy; cached or represented strings keep their existing reader. Packing these
outputs into binary rows changes legacy debug rendering, even though typed getters agree,
and is therefore excluded. The [string exit ledger](../docs/optimizations/zero-copy-exit-transpose.md#synchronous-copies-of-cache-free-scalar-strings)
records the retained boundary and its compatibility regression coverage.

Direct fixed-BINARY MULTISET elements reuse the existing map-shaped generated boundary,
with counts carried as INT NOT NULL. Actual binary lengths remain length-delimited rather
than padded by transport. Released evaluation owns explicit and implicit casts: fixed-binary
MULTISET CASE enters the generated program to preserve branch conversions observed on 1.18.
Nullable elements are verified on both lines; NOT NULL elements remain guarded on 1.18,
where unchecked NULL byte getters differ from original-container NULL formatting. Flink 2.2
retains its verified NOT NULL route. No map representation tag, native state or snapshot
serializer is added, and unverified represented consumers keep their existing restrictions.

Flink 2.2 NOT NULL fixed-BINARY MAP keys and values reuse that same release capability and
length-delimited generated transport. Released-Flink parity covers GenericMapData and
BinaryMapData, including real NULL slots beneath NOT NULL hints and retained Java callbacks
across batch release. Flink 1.18 keeps its existing guard; no schema marker or unchecked-getter
normalization is introduced. Representation-sensitive downstream consumers retain their
existing admission gates. The release comparisons and two-pass investigation are recorded in
[the benchmark ledger](../docs/benchmarks/scalar-functions.md#not-null-fixed-binary-map-leaves).


## Native represented collection kernels

The same marked UTF-8/optional UTF-16 columns now feed native collection kernels. Equality
uses Java UTF-16 only when both values have caches; otherwise it uses encoded bytes. This
relation can be nontransitive for isolated surrogates and cold question marks, so STRING
set operations scan retained candidates rather than pretending it is an ordinary Rust Eq key.
Gathering retains the entire original element. Copy-category flags distinguish a new generic
array from an unchanged binary array; empty/singleton reversal and complete slices retain
the original category. Binary ROW/MAP constructors clear caches, while generic OBJECT
construction retains them. Shared mutable OBJECT_UPDATE expressions keep Flink evaluation.

Flink's ARRAY_SORT comparator returns -1 for equal non-NULL ascending values, and +1 descending.
A total Arrow sort cannot preserve its result identities or invalid-comparator exceptions.
The native index sort ports the Apache-licensed Android TimSort from
[Apache Spark 4.0.1](https://github.com/apache/spark/blob/v4.0.1/core/src/main/java/org/apache/spark/util/collection/TimSort.java).
An independent JDK 17 Arrays.sort oracle covers 480 index-order cases, including 107 comparator
failures. Runtime Boolean options and NULL ordering stay inside the same native kernel.

On Flink 1.18, independently compiling `MAP_KEYS(MAP['a',i,'b',j])` produces both `[a,b]` and
`[b,a]`: generated-expression grouping hashes global generated variable names. Represented
native constructors use input order for this inherently nondeterministic construction order.
Flink 2.2's ordered grouping remains deterministic and is preserved. This does not relax
expression deduplication, failure, STRING representation or ownership contracts.

Container casts reuse the existing exact scalar conversions and shared Java HashMap order
implementation. Both released lines construct HashMap() with the default initial capacity of
sixteen; the newer development-source size-based constructor must not be substituted. Numeric
tree-bin order, including NULL keys, has an independent JDK 17 oracle. BinaryStringData is not
Comparable to its own class, so its equal-hash tree tie order depends on JVM identityHashCode;
native STRING tree ties use first-input identity order for that inherently nondeterministic case. They select visible child ranges before conversion, propagate element parsing
failures to the complete outer container for TRY_CAST/legacy CAST, and preserve the original
first key object with the final equal-key value. No JNI callback or borrowed-buffer lifetime
is added. Numeric ARRAY formatting creates the same cached Java UTF-16 result as Flink.


Generated MAP field reads call MapDataSerializer.copy before invoking a collection function or
cast. On GenericMapData this produces binary key/value arrays, clearing nested Java caches.
Native MAP-reference operands mirror that copy category recursively without mutating the input
batch. The resulting nullable-key entry-list carrier shares values/offset buffers and replaces
only representation metadata. Computed MAP_FROM_ARRAYS and other MAP results retain their own
caches: applying the input-field snapshot to computed results would change equality and duplicate
keys. Tests distinguish both cases using cached isolated surrogates, cold strings, NULL keys,
original binary maps, nested RowKinds and downstream key/value/entry projections.


Empty-separator SPLIT can mutate a shared StringData cache. Independently evaluating Arrow
expressions cannot update aliases to that original Java object. Programs combining such input
materialization with cache-sensitive original-input comparisons retain one generated Calc.
A cold GenericArrayData fixture demonstrates the ordering change after two SPLIT calls; binary
and proxy arrays retain their independent copy behavior. Native comparisons of a newly produced
SPLIT array remain admitted because they read its explicit result caches.
