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
