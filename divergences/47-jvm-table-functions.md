# JVM table functions inside columnar islands

Arroyo's UDF host (`crates/arroyo-udf/arroyo-udf-host/src/lib.rs`) passes Arrow arrays through its
native UDF ABI, and its physical planner lowers collection expansion to DataFusion `UnnestExec`.
Its native functions cannot execute a Flink JVM `TableFunction`, whose collector may emit a
variable number of rows and whose conversion, lifecycle and outer-join semantics belong to Flink.

The [table-function boundary](../docs/operators/unnest-correlate.md#jvm-table-functions)
therefore invokes Flink's released correlate generator inside a Java operator with Arrow batch
input/output. This follows Comet's JVM batch boundary and task-scoped function ownership
(`CometUdfBridge.java`, `native/spark-expr/src/jvm_udf/mod.rs`), while retaining Flink's variable
output collector instead of Comet's one-result-per-input scalar contract. Generated evaluation
runs synchronously, borrows input only while the batch is live and writes each output before
returning to the user collector. Output buffers own their values independently of reused user
rows and closed input batches.

The generated correlate evaluator is private to the columnar operator. It is stateless from
Flink's checkpoint perspective; source offsets and downstream operators retain their existing
checkpoint contracts. The implementation verifies repeated recovery with grouped state rather than
introducing an independent function-state codec. Stateful JVM aggregates use the separate
[managed-state boundary](48-jvm-aggregate-managed-state.md); their remaining contracts stay
tracked in [#237](https://github.com/datafusion-contrib/StreamFusion/issues/237).

Mixed Java/Rust CPU profiles identified a redundant projection and writes of discarded input
columns after correlate expansion. Pure column projections are fused into the output writer;
function calls, predicate evaluation and collector order are unchanged. General expressions and
filters remain separate operators. No new Rust kernel or JNI protocol is introduced.

ARRAY/MAP/ROW table-function signatures reuse the representation-preserving boundary already
used by generated UNNEST and scalar UDFs. This keeps NULL map keys, nested string contents,
row kinds and released container-copy behavior within an Arrow island. Both function types
share the nullable typed-array converter adapter, while each generated context supplies its
own released classloader expression. Released type inference selects the verified external Java conversions recursively;
internal container classes retain fallback. ARRAYs may use external Java arrays or Lists,
including nested primitive arrays and result-only List implementations. Scalar primitive
and temporal bridges may occur beneath each verified container. Sensitive outputs may only flow through verified preserving consumers or the final
row boundary. These restrictions avoid inventing a second nested-value or ownership protocol.

A plain type-preserving projection at a row source folds into the existing preserving
entry transpose before table application. Metadata-only AS aliases and reordered unique
columns retain original source names for the owned entry copy; the released correlate reads
ordinals and retains its declared output aliases. The fold is limited to boolean/signed-integer
fields and ROWs recursively containing them; other container copy representations remain
unverified. This avoids passing nested ROW arguments through
an unverified native Calc merely to discard unused source columns. Computed/filtered/duplicated
projections and columnar sources retain their existing representation guards; no duplicate
projection evaluator or conversion ABI is added.


The recursive ARRAY/List extension consulted Arroyo's `UnnestExec` lowering and Comet's
`CometUdfBridge` import/evaluate/export ownership before changing the conversion boundary.
Neither supplies Flink's Java container conversions, so those remain in the exact released
1.18.1/2.2.1 `ArrayListConverter`, `ArrayObjectArrayConverter`, primitive-array converters,
`RowRowConverter` and `MapMapConverter`. Released `ArrayType` validation admits List
subclasses for external-to-internal results but only the List interface for
internal-to-external arguments. The admission check applies this direction at every level.

Flink's nullable columnar copies can place internal array elements in `Object[]`, whereas
its primitive-array converters require typed backing even when nested beneath Lists or
external ROW/MAP conversions. The shared recursive extension of the existing typed-array
repair restores that backing before released table and scalar argument converters. It does
not implement Java container conversion, change JNI or introduce a second Arrow encoding.
A replacement ROW retains its kind; MAP repair retains aligned key/value arrays, including
NULL/duplicate keys, until the released converter applies its own map semantics. No child
buffer or borrowed row is retained past the synchronous callback. Scalar calls reuse the same released conversion directions and retain representation-sensitive
producer/consumer admission.

Released primitive DATE/TIME and interval array results are admitted only beneath an
external ARRAY/List or MAP converter that first serializes them into binary storage. A
result reached only through ROW retains fallback: `ArrayDataSerializer.copy` rejects that
primitive temporal backing in projections/state, even when a plain unprojected sink works.
Arguments using the valid int[]/long[] temporal representations execute; incompatible
TIME/long[] and LTZ/int[]/long[] keep released failure controls on stock fallback.

A second boundary concerns aliases rather than values. Released identity converters may
expose primitive/boxed array backing and binary buffers owned by an upstream user function.
If a later function retains those arguments, stock chaining can observe the producer's next
mutation. Arrow output ownership deliberately copies the produced value immediately, so
that alias relationship cannot survive the batch boundary. Chaining therefore falls back
when an upstream scalar or table function may return mutable aliases and a later argument converter may
expose them. The conservative check includes upstream functions across intervening plans,
without pretending to prove field lineage or stateless user code. ARRAY/List/MAP converters
that binary-serialize non-identity children establish the verified copying boundary; paired
retained-argument tests keep those native routes. Existing flat primitive/boxed-array and binary-leaf chains gain
the same required fallback. Broader alias-preserving producer/consumer contracts remain in
[#237](https://github.com/datafusion-contrib/StreamFusion/issues/237).
