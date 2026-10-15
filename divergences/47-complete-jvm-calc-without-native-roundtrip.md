# Complete generated JVM Calcs can stay on Java Arrow batches

Comet's UDF bridge exports Arrow arguments from its native executor and invokes a generated JVM
batch kernel. StreamFusion follows that pattern for expressions inside Rust Calc execution.

A complete Flink-generated Calc has a narrower opportunity: its owning Flink operator already
runs on the JVM and owns an Arrow batch, while every expression is scheduled by Java. The
experimental temporal path borrows timestamp input vectors directly and returns Arrow columns,
avoiding a native round trip that performs no useful expression work. Other shapes and
changelog-tagged input retain the native bridge. The operator remains columnar on both sides;
row conversion stays at the island edges. Function binding, lifecycle, predicates, exceptions
and call order use the same released Flink evaluator.

The reference reviewed before this change is CometUdfBridge.java and
CometBatchKernelCodegen.scala in datafusion-comet. CPU profiles with symbolized Rust frames show
Arrow FFI/ownership and allocator work in the redundant native stage. Exact timestamp output
comparison can retain Arrow buffers after every call executes; changed columns materialize their
prior prefix. This follows Arrow reference ownership and does not infer function purity.

The path remains explicitly experimental and default off: two standalone workloads beat paired
stock controls, but identity still trails by about 1% and the prior-correct-fallback repeat is
outstanding. See docs/operators/calc-filter.md and docs/optimizations/udf-columnar-upcall.md.
PR #368 remains draft and issue #364 remains open.
