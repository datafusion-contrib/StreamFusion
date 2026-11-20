# Calc coverage by topic: routing

[Topic index and original links](calc-filter.md). Sections retain their recorded scope, dates and evidence.

## Admission and routing {#admission-and-routing}

Stock compiled-plan lowering keeps a Calc on its released owner if serialized input-reference
types differ from the captured physical input types. This includes interval qualifier metadata
that changes representation through plan serialization. The guarded fallback avoids constructing
an invalid Rex program; neighboring qualified stateful operators retain their own bindings.

`Calc` (Flink's fused projection + filter node) and standalone filters are where per-row expression
evaluation happens, so their admission is expression-level rather than shape-level: the operator
itself always has a native form, but it only runs natively if **every** expression node inside it —
every function call, cast, and operator in the projection and the predicate — is one the native
expression engine admits. One un-admitted node anywhere in the `Calc` declines the whole node,
which (per the [all-or-nothing island rule](index.md#the-all-or-nothing-island)) drags the entire
query back to Flink, not just that node.

`Calc` is also one of the changelog-aware operators (alongside `GROUP BY`, the regular join, a CDC
source, `UNION ALL`, `Expand`, and changelog normalize) exempt from the insert-only guard — a
retracting/updating input doesn't disqualify it by itself. A union may alternate insert-only Arrow
batches and batches carrying a row-kind column; Calc caches each schema separately and preserves
those row kinds through projections and filters. Row-to-Arrow boundaries also retain actual
retractions received from a source that advertised insert-only output.
An empty input or a filter that selects no rows emits no data record; its Arrow
result is released immediately. Watermarks still propagate, and zero-column
batches with a positive row count retain their rows and changelog kinds.
Programs without JVM bindings export only referenced top-level input columns and
the changelog column through C Data. JVM-bound programs keep their original
positional input schema. This changes transport work, not expression admission.

The streaming Calc and filter rules prepare their expression plans once for
one candidate; a Calc also prepares its declared-result-type check. Construction and fallback diagnostics consume that same admission decision;
explaining a declined Calc or filter does not repeat expression generation or optional native probes.
A declined standalone filter passes to general Calc admission without repeating its predicate probe.
Ordinary streaming, bounded batch and stock compiled-plan Calc lowering share an immutable
Calc specification for the native result-type contract. Its expression pools, output names and
representation requirements are fixed after admission. Input pruning creates a separate expression
with remapped references, including whole-Calc JVM input coordinates; it cannot alter a sibling's
prepared program. Whole-Calc representation flags remain attached to every logical output field,
even when one row-valued evaluator root produces several columns. Each constructed operator
receives its own unopened JVM-function binding.
The Calc coordinator delegates numeric value proofs, collection/row representation, exact builtin
routing, temporal formatting and UDF overload selection to semantic owners that do not write
protocol pools. Shared collection rules use their own shape/type facts, without calling back into
the encoder. A dedicated
expression buffer owns node/payload alignment, literal storage, input remapping and exported
pool snapshots. Admission still preserves short-circuiting, shared UDF evaluation and existing
native/JVM choices; this separation changes neither the wire format nor supported SQL.
Batch and stock compiled-plan paths retain their existing, stricter boundary and state admission.
Processing-time projection fusion prepares its changed candidate separately and retains the
original admitted plan if fusion is unsupported. Optional-probe failures still yield to later
rules, while failures constructing an admitted operator propagate.

Planning logs the final function-engine assignments for admitted Calc/filter operators at INFO
by default, including Calcs admitted while lowering a stock compiled plan. Repeated identical
assignments within a pass are combined with their occurrence count. Distinct SQL function, UDF
and cast calls appear under `native` or `jvm`; casts are retained because they can select JVM
evaluation, while arithmetic, comparison and boolean operators are omitted. A mixed node can
list functions in both groups; a whole-Calc
generated evaluator lists every function under `jvm` and retains the reason that selected
row-at-a-time Flink evaluation. Ordinary `EXPLAIN` and submitted Calc/filter names also retain
Flink's original `select` and `where` expressions and show these final assignments as
`nativeFunctions=[...]` and `jvmFunctions=[...]`, even with simplified operator names enabled.
When evaluators are registered, batch EXPLAIN also shows `jvmExpressions` (the
registration count), `jvmEvaluation` (scalar bridges or whole-Calc evaluation),
and `jvmExpressionDetails` for generated scalar expressions. Arithmetic and
comparison bridges can exist even when the named `jvmFunctions` list is empty.
These fields describe available evaluators, not measured callback invocations;
certified native paths may retain a JVM evaluator for exceptional inputs.
No diagnostic flag is required for these plan attributes. The acceleration summary returned by
`PhysicalPlanScan.explainSummary()` includes the function assignments and JVM selection reasons;
the always-on planning log also counts Calc/filter nodes containing JVM functions.
Flink's operator metrics use these detailed names as well; select Calc/filter metric groups by the
`NativeCalc(` or `NativeFilter(` prefix rather than their execution-node class names.

Expression node kinds, primitive cast targets, scalar call and certified-builtin opcodes, and aggregate kinds come from
one versioned protocol manifest. It generates Java and Rust enums for node/type tags and
compile-time constants for operation codes used in dispatch. GROUP/OVER and window aggregate
codes have separate namespaces because some existing values have different meanings in those
contexts. Native decoding is exhaustive over node kinds, and CI checks generation drift,
retired-code reuse and every frozen protocol identity. The compact pools and existing wire values
remain unchanged; this is a planning maintainability change rather than a throughput claim.
Scalar producer selection, certified callback dispatch and shared aggregate call specifications
reference those generated names directly. Argument counts, type codes and accumulator widths
remain separate from operation identities.
Null-safe reference comparisons use these generated operations and append their boolean
literal through the same buffer owner as other expressions.

Pairwise string comparison uses node code 49. Codes 10 and 48 remain reserved
and are rejected by both expression decoders.

Generated expression code is built entirely in the planner. Serialized Calc/filter bindings
carry only the generated function, runtime types/schema and user-function references; workers
do not need Flink's planner or Calcite classes to deserialize or execute them. The runtime
function keeps its existing serialized class name and fields, but its constructors and methods
no longer refer to code generators. Temporal precision helpers also use the runtime compatibility
layer rather than linking execution back to the planner.

`GeneratedExpressionRuntimeIsolationTest` starts fresh worker JVMs with the planner and Calcite
artifacts removed from their classpaths. It covers scalar and complete-row generation, processing
time close/reopen, temporal parsing, and a SQL-planned Calc factory restored through `StreamConfig`.
It also checks strict-cast failures without the planner. The SQL case exercises casts, regex
predicates, user-function lifecycle, empty Arrow batches and preserved changelog row kinds.
This is a deployment-correctness regression, not a throughput optimization.

Filter projections can repeat input columns, including nullable and nested values. Each repeated
reference owns a retained Arrow view until the final reference transfers the source buffers,
so releasing the input cannot erase another projected copy or its changelog row kind.

The rest of this page is the exact admission list: what's unconditionally native, what's native by
default via a JVM upcall (and why that's not a fallback), what's opt-in, and what's a straight
fallback.

- **Unsupported function/operator** outside the admitted set normally declines the whole Calc.
  A [SQL/JSON Calc](calc-filter-strings.md#sqljson-evaluation) or a Calc containing the collection-returning string
  functions below can instead use Flink generation for its complete program,
  subject to the host code generator and the verified batch bridge types.


## Failure order across rows {#failure-order-across-rows}

Calc evaluates independent expressions in column batches. It does not switch the entire
filter and projection list to JVM evaluation merely because several expressions can fail.
When several rows or projections would throw, the first reported exception may differ from
stock Flink. This diagnostic difference is accepted to retain native batch execution.

Successful values, declared types, NULLs and conditional short-circuiting remain unchanged.
The condition still selects rows before projections run, and empty/all-filtered batches do
not evaluate projections. Individual expressions retain their normal native or host-exact
implementation; a necessary JVM expression no longer pulls unrelated native expressions
into a whole-Calc callback. Shared stateful or nondeterministic UDFs still retain the
existing generated schedule because their call order affects values and lifecycle.

Regression tests require failure when invalid expressions are reached, rather than choosing
the same winner among competing failures. They also verify successful filters, NULLs,
untaken branches, nested inputs and independent binary-backed STRING projections.
Selected integer division errors are checked by cause and evaluation phase: the JVM's
`ArithmeticException` and the Rust engine's division-by-zero error both fail the native job.


## Declared-type guard {#declared-type-guard}

Admitting every node in a tree is not the same as producing the type Flink declared for the tree:
the result type of an arithmetic or function call is decided by DataFusion's coercion rules when
the expression is compiled, and those do not always agree with Calcite's. `FLOAT * DECIMAL`, for
example, is `DOUBLE` to Flink but single precision to DataFusion. The columnar boundary builds
Flink's column vectors from the Arrow batch it is handed, so a disagreement used to surface as a
`ClassCastException` on the first row read, after the job had started.

The planner therefore compiles the encoded `Calc` at planning time — types only, no data — against
the Arrow schema of its input and checks that the boundary would read each projection's inferred
Arrow type as its declared column (and that the condition is `BOOLEAN`). "Read as" is the reader's
own rule, not byte-equality of Arrow types: timestamps and times may carry any unit or zone, since
the column vectors convert on read. New temporal expressions nevertheless preserve the engine's
canonical two-component timestamp and millisecond TIME storage for downstream operators. Every other type — width, decimal precision and scale, string
encoding, nested element types — must match exactly. Any disagreement, or a tree DataFusion cannot
coerce at all, is a plain fallback whose recorded reason names the column and both types, e.g.
`projection `EXPR$0` evaluates natively as FloatingPoint(SINGLE) but the plan declares DOUBLE`. Such
a reason is a real gap to close (either the encoder should carry the width Flink uses, as it does
for narrow integer literals, or the tree should be cast to the declared type), not a query to
rewrite. This check requires the native library in the planning JVM, which the standard deployment
already provides (see [Deployment](../deployment.md)).

Typed NULL literals retain their declared Arrow type, including BOOLEAN, integer widths,
DECIMAL precision/scale, fixed-width binary, and nested ARRAY/MAP/ROW/MULTISET fields.
STRING and temporal NULLs retain their existing representations, including component-based
TIMESTAMP/TIMESTAMP_LTZ storage. This also covers NULLs produced by constant folding, such
as a MAP lookup with a literal NULL search key. These projections can share native Calc
with runtime arithmetic or supported collection accesses without disabling the type guard.
Nested field names and nullability are carried in a standard Arrow IPC schema during
expression setup; no per-row schema serialization or host callback is needed.

A release/mimalloc end-to-end diagnostic on Apple M4 Pro, JDK 17 and Flink 2.2.1 used
2,000,000 rows at parallelism 1, two warmups and five interleaved trials per engine.
`SELECT id + 1, CAST(NULL AS DECIMAL(38,18))` took 0.235819 s on Flink and 0.392874 s
natively (0.600x); replacing the NULL type with `MAP<STRING, ARRAY<DECIMAL(38,18)>>`
took 0.251582 s and 0.416719 s (0.604x). Both row/Arrow transposes are included and
asserted. This small projection is slower natively; the change closes a type-coverage gap
so folded NULLs can remain inside larger native islands, and claims no standalone speedup.
Reproduce with `SF_BENCHMARK=true mvn -Pbench -pl :streamfusion-runtime test -Dtest=TypedNullBenchmark`.

