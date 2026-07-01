# Serialize native UDFs into the operator for distributed execution

**Status:** TODO (limitation of the shipped native UDF support). Noted 2026-06-30.

Non-builtin Flink `ScalarFunction`s now run inside the native island via a native→JVM columnar Arrow
upcall (`NativeUdf` + the `JvmUdf` native expression node — datafusion-comet's `JvmScalarUdfExpr`
pattern). At evaluation the native `Calc` exports the argument columns over the Arrow C Data Interface
and the JVM bridge runs the actual `eval` over the batch, so the result is byte-identical to Flink.

**The limitation.** The function is held in a **process-global registry** (`NativeUdf.REGISTRY`),
populated at **plan time** by `RexExpression.emitUdf` (`register(...)` → an int id baked into the encoded
`KIND_UDF` node). `NativeUdf.invokeUdf` looks the id up in that same static map at runtime. This only
works when planning and execution share one JVM — the local/embedded/mini-cluster/benchmark path, which
is what StreamFusion's tests and benchmarks use. On a **distributed** deployment the task managers are
separate JVMs whose registry is empty, so the upcall would fail (`no registered UDF for id N`).

**The fix (to be designed).** Carry the function into the operator instead of a plan-time global:
- Capture the `ScalarFunction` (Flink UDFs are `Serializable`), its resolved `eval` signature, and the
  arg/return marshalling type codes at plan time, and thread them through the `Calc` rel node → exec node
  → `NativeCalcOperator` (the same plumbing the other Calc parameters already take), so Flink serializes
  them to each task with the operator.
- At operator `open()`, register the carried UDFs into the (now per-JVM) registry, obtaining runtime ids,
  and pass a `localIndex → runtimeId` mapping into `createCalcExpression` so the compiled `JvmUdf`
  resolves its `KIND_UDF` local index to the task-local id. (The encoded tree's payload becomes a local
  index rather than a global id.)
- Deregister on operator `close()` so the registry doesn't leak across a task's lifetime (the current
  global map also never clears — fix that here).

**Open questions:** `eval` `Method` isn't serializable — re-resolve it at `open()` from the deserialized
`ScalarFunction` + arg type codes (the same resolution `RexExpression.resolveEval` does). Confirm the
`ScalarFunction` instances Flink hands us via `BridgingSqlFunction.getDefinition()` are serializable in
all registration paths (`createTemporarySystemFunction(class)` vs an instance vs a catalog function).

Relates to: the native UDF support (`NativeUdf`/`JvmUdf`), ticket 19 (expression layer), and the
`streamfusion-jvm-udf-upcall` memory note.
