# Calc / filter

**Status:** native, with gaps — the largest admission surface in the project.

Use this page for the current admission rules. For a specific question, start at:

| Topic | Sections |
| --- | --- |
| Routing and failure semantics | [Overview](#admission-and-routing), [failure order](#failure-order-across-rows), [declared-type guard](#declared-type-guard) |
| Basic types | [Casts](#casts), [decimal arithmetic](#decimal-arithmetic), [integer division](#integer-division) |
| Strings and JSON | [String functions](#string-and-calendar-functions), [SQL/JSON](#sqljson-evaluation), [case folding and regex](#case-folding-regex) |
| User functions and collections | [Scalar UDFs](#user-scalar-functions), [collection expressions](#collection-expressions), [subscripts](#collection-subscripts) |
| Time and version compatibility | [Temporal functions](temporal-functions.md), [Flink 1.18](#flink-118-compatibility) |

The [evidence index](../benchmarks/evidence.md) separates recorded performance
comparisons and experiments from these current coverage rules.

## Admission and routing

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

With `streamfusion.logFallbackReasons=true`, planning also logs the final function-engine assignment
for each admitted Calc/filter. Distinct SQL function, UDF and cast calls appear under `native` or
`jvm`; casts are retained because they can select JVM evaluation, while arithmetic, comparison and
boolean operators are omitted. A mixed node can list functions in both groups; a whole-Calc
generated evaluator lists every function under `jvm` and retains the reason that selected
row-at-a-time Flink evaluation. This is the reliable way to distinguish a fully native scalar plan
from JVM work inside a `NativeCalc`. The always-on planning summary separately counts Calc/filter
nodes containing JVM functions.

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
  A [SQL/JSON Calc](#sqljson-evaluation) or a Calc containing the collection-returning string
  functions below can instead use Flink generation for its complete program,
  subject to the host code generator and the verified batch bridge types.

## Nested Java scalar results

`streamfusion.experimental.nestedScalarUdfs` enables synchronous external Java `Row`
arguments and results, using Flink's complete generated Calc evaluator through the existing batch
bridge. It defaults to true under the accepted UDF performance exception; explicit false
retains the preceding transport and whole-query fallback. Computed ROW-valued programs and filters use whole-Calc generation,
including SQL ROW constructors and comparisons, so physical metadata never becomes a logical SQL
field. Input-reference columns carried beside unrelated scalar work keep the existing native path;
whole-Calc generation is required when an expression produces or consumes a ROW value. Recursive
ROW/ARRAY values use the existing verified primitive bridge types; string-to-string MAP fields
are supported. Nested fixed BINARY uses the transport described below. Custom type inference, specialization and internal Java
conversion classes retain whole-query fallback. Only external `Row` signatures enter the new gate.

Flink's released converters own external callback inputs and normalize mutable results before the
next invocation. This retains named-row field mapping, reusable ROW/array results, NULLs, Java
string conversion and callback failures. No generic-array conversion shortcut is enabled: a
profiled experiment reduced allocations but changed Unicode ARRAY_SORT results.

A tagged nullable Int8 Arrow child carries each SQL ROW's `RowKind` outside its logical ordinals.
Missing/NULL metadata reads as INSERT; nested non-INSERT kinds remain visible to Java callbacks.
Key encoding writes the kind into Flink's BinaryRow header without consuming another logical type
descriptor. UNNEST excludes it from SQL columns, and sort keys normalize only its comparison value
while retaining actual kinds in output payloads. The generated Calc's outer projection container
has no extra kind child. File/message schemas omit nested kind sidecars; they are not SQL
fields. Parquet, ORC, Paimon and Iceberg project execution batches onto those logical fields
while retaining value buffers, and file/message decoders request the logical schema. This does
not drop a top-level changelog column. File-loaded nested ROWs have INSERT kind, as in Flink. Configure the flag consistently in planner and task JVMs; this changes
the physical struct schema. See the [transport decision](https://github.com/datafusion-contrib/StreamFusion/blob/fix/237-nested-scalar-results/divergences/nested-row-kind-transport.md).

Under the same default-enabled flag, BINARY values use length-delimited Arrow binary storage.
Released Flink callbacks can return bytes shorter or longer than their declared `BINARY(n)`;
the transport preserves those actual bytes and the resolved SQL type without padding or
truncation. This applies at the input, nested ROW/ARRAY fields and extracted scalar results.
Native fixed-size binary vectors are still readable as bytes. The disabled flag retains the
existing fixed-size mapping and nested admission restrictions. Legacy variable-width casts
still fall back across the previously restricted native boundaries. Binary ELT has its own
default-enabled option and Rust-side functionality gate, described below; complete-job costs
remain separate diagnostics.

[Nested BINARY validation](../benchmarks/results/nested-fixed-binary-coverage-2026-10-16.json)
retains initial 65-case selections on both lines (six compatibility skips on 1.18), followed
by 16 passes per line with expanded NULL-field cases. Tests cover runtime lengths 0/1/4 and
oversized payloads through 4,097 bytes, all nested kinds, mutable ROW/array outputs, extracted
fields, scalar consumers, grouping, exact released BinaryRow bytes/hashes and nullable arrays.
Default selections pass 23 cases with two skips on 2.2 and 18 with seven skips on 1.18.
Thirty selected native Criterion smokes exercise sliced nullable nested binary handoffs and
key hashing. These are correctness checks. The follow-up below adds BINARY recovery and
separate-JVM deployment coverage; representative runtime speed comparisons remain deferred.
Default nested callback admission follows the accepted UDF performance exception. Binary ELT
default admission follows its separate Rust-side functionality gate.

The experiment has no throughput admission. [Java CPU/allocation and symbolized Rust profiles](../benchmarks/results/nested-scalar-rejected-array-conversion-2026-10-16.json)
identify conversion work; their timings include observed external activity and are excluded from
performance claims. Default admission follows the accepted UDF performance exception. These diagnostics and the
correctness/cross-JVM checks below do not establish acceleration.
#237 stays open.

[Release validation evidence](../benchmarks/results/nested-scalar-coverage-2026-10-16.json)
retains 71 opt-in checks on Flink 2.2.1 and 68 passes with three compatibility skips on 1.18.1.
Default-off regressions pass 105 checks with one opt-in-only skip on 2.2.1 and 100 with six skips
on 1.18.1. The native fixtures cover production handoffs, key bytes/hash descriptors, ordering and
inner/left ARRAY-of-ROW UNNEST; their smoke execution does not establish throughput.

Eight additional [checkpoint/recovery checks](../benchmarks/results/nested-scalar-recovery-2026-10-16.json)
pass on the two released lines. Memory and native RocksDB deployments at parallelism one
and three enable object reuse and fail twice, only after source checkpoints at offsets
12 and 32 complete. Recovered native identity and mutable-transform callbacks match the
uninterrupted stock result multiset and resolved schema, including nested RowKinds, NULLs
and long Unicode values. Native consumed-row counters and exact source restore offsets
are asserted. This verifies task recovery with a consistently configured JVM flag.

Four [separate-JVM deployment checks](../benchmarks/results/nested-scalar-remote-2026-10-16.json)
also pass across both released lines at parallelism one and three. The SQL planner submits
stock/native jobs over REST to a child JVM containing a TaskManager. That JVM records all
5,003 native input rows, with a PID distinct from the planner's; complete results and schemas
match stock, including nested kinds and mutable callback outputs across batches. The initial run explicitly enabled the flag in both JVMs. Current default-admission
checks also pass without setting it in either JVM. Child shutdown verifies native-handle, Arrow-allocation and task
reservation cleanup. This checks callback registration and metadata across JVMs on one host;
it does not combine remote process termination with checkpoint recovery or validate differing
planner/task flags. The maintainer initially accepted shipping this implementation with a false default on
October 16 despite its measured whole-job regressions, with optimization deferred.

Reproduce the complete-job benchmark from a clean released build (add `-Pflink-1.18` for that
compatibility line). Every shape uses two warmup pairs and five measured pairs by default;
EXPLAIN and per-job cluster setup precede the clock, while both transposes and production IPC
remain inside execution. The benchmark includes nested identity/transform cases and a primitive
Rust calculation control; it checks consumed native rows and zero native work in stock plans.

The [matched runtime comparison](../benchmarks/nested-scalars.md) additionally builds the
immutable previous StreamFusion runtime, retains its own stock controls and compares narrow
and wide nested values. Separate Java/native CPU and Java allocation profiles cover both
runtimes; profile durations are excluded. The completed release matrix records regressions; it is not a claim of acceleration.

```bash
SF_BENCHMARK=true mvn -Pbench -pl streamfusion-runtime -am \
  -Dtest=NestedScalarBenchmark -Dsurefire.failIfNoSpecifiedTests=false \
  -Dnative.resource.directory="${PWD}/native/target/release" \
  '-Dsf.benchmarkJvmArgs=-Xmx2g -Dstreamfusion.exchange.zeroCopyLocal=false' \
  "-Dsf.extraJvmArgs=-Djava.library.path=${PWD}/native/target/release -Dstreamfusion.experimental.nestedScalarUdfs=true" \
  test
```

[Current default-admission validation](../benchmarks/results/jvm-feature-defaults-2026-10-19.json)
records both released Flink lines, the carried-ROW routing regression and fix, separate-JVM
checks, 794 native engine passes (two ignored), and file-boundary checks. Parquet footer
assertions require exactly the declared nested fields; buffer-address and lifetime assertions
verify the retained file view. These checks establish correctness, not a throughput win.

## Failure order across rows

After the usual expression admission checks, a Calc with multiple potentially failing
evaluations uses Flink's generated evaluator for its complete filter and projection list.
Each JVM expression is conservatively treated as potentially failing, including deterministic
user functions. Native failure sites also count: exact division/remainder with an unproven
nonzero divisor, strict string-to-integer/boolean casts, invalid random bounds, INSTR indices,
extreme decimal rounding scales and throwing SQL/JSON policies.

This keeps an error in a later input row from overtaking an earlier row's projection error.
For example, with rows `[1, 2]`, `SELECT f(id), g(id)` must report `g(1)` if it fails,
even when `f(2)` also fails. The same rule applies between filters and projections. The
generated evaluator preserves predicate-before-projection order, untaken branches, NULLs and
empty filtered results. It still receives and returns Arrow batches, and retains the existing
bridge type and string-representation restrictions; a program that cannot be generated safely
falls back. Unsupported functions and UDF signature/conversion gates are checked before this choice.
Shared non-deterministic scalar calls use complete-row evaluation directly, as described below.
In particular, a complete-row evaluator cannot emit the arbitrary binary-backed STRING from
`FROM_BASE64`. Projecting that string alongside another potentially failing evaluation retains
fallback (`binary-backed STRING requires a final scalar projection`); a single such projection
keeps its existing native byte-preserving path.

This scheduling change prioritizes failure parity, with no throughput improvement claimed.
Regression tests compare error class/message and execution route against released Flink for
INT, BIGINT, DECIMAL and STRING results, native casts, nested expressions, and one-row and
multi-row batches, including errors after earlier complete batches.

## LIKE ESCAPE and SIMILAR TO

LIKE/NOT LIKE with an explicit literal or per-row ESCAPE, and SIMILAR TO/NOT SIMILAR TO,
run Flink's generated SQL-pattern evaluator through the batch JVM bridge. Pattern grammar,
Unicode, NULLs and invalid-escape failures are Flink's own. AND/OR consumers fuse with these
expressions so invalid patterns are not evaluated on rows that Flink short-circuits; filtered-out
batches do not evaluate projections. Existing two-argument LIKE keeps its current native path.

## Generated processing time

Generated scalar expressions and complete-row Calc evaluators bind processing time to the owning
Calc or Filter task's live `ProcessingTimeService` during operator open. Flink's generated
`PROCTIME_MATERIALIZE` access therefore works inside CASE/COALESCE selections and predicates,
including complete-row evaluation selected to preserve filter/projection failure order.
Simple TIMESTAMP_LTZ COALESCE selections over input columns, NULLs and processing-time clocks
with widening precision casts use the existing native CASE path at precisions above three.
No UDF or other nondeterministic/fallible expression enters this path. More complex expressions
retain Flink generation and its call-order safeguards; complete-row generation still applies
when multiple failure sites require it. A lower Calc containing only input references and a lazy
PROCTIME indicator can fuse into this consumer; filters, other computations and remapped input
coordinates keep their existing boundaries.

The clock is read when the generated expression executes; no timestamp or running service is
stored in the serialized plan. Close releases the evaluator's clock reference, and a restored or
reopened binding uses its new task's service. Pure expressions still work in local unit contexts;
evaluating generated processing time without a bound runtime clock reports an error.

Controlled-clock tests cover CASE at TIMESTAMP_LTZ precision 0/3/6/9, preserving non-NULL input
fractions and using Flink's declared casts for materialized clock values. Native Calc and Filter
operator tests cover successive clocks, empty/all-filtered batches and INSERT/UPDATE/DELETE
row kinds. Independent SQL jobs on both released Flink lines compare types and stable input
branches, check clock values against each job's execution interval, and require actual native
consumption across multiple Arrow batches. Precision-6/9 COALESCE and precision-3/6/9 CASE cover native selection and
complete-row generated execution forced by multiple fallible projections in UTC, Asia/Shanghai and America/Los_Angeles.

Released Flink 2.2.1 rejects the direct precision-3 COALESCE form combining a nullable timestamp
and materialized PROCTIME with a generated argument-nullability mismatch. Its valid CASE form
and precision-9 COALESCE are used for the SQL comparisons; this runtime binding does not alter
host SQL validation or repair that separate host code-generation error.

Release comparisons and the remaining 4.1% sustained-filter limitation are recorded in
[the optimization ledger](../optimizations/identity-expression-passthroughs.md#processing-time-indicators).

## COALESCE

`COALESCE` retains the first non-NULL operand without evaluating it again. When an operand
contains a scalar UDF, a volatile expression, or a known potentially failing native expression
(such as a strict STRING-to-INT cast), the complete COALESCE expression uses Flink's
generated code through the existing columnar JVM bridge. This preserves call counts, nullable
results and Flink's evaluation of later operands inside native Calc, including failures from
operands hoisted by host code generation. This includes casts hoisted from later operands
even when an earlier operand is non-NULL. Other pure expressions retain their
existing native CASE lowering. Runtime tests cover INT/STRING stateful UDFs, predicates, nested
expressions, seeded random calls and multiple batches, including NOT NULL output constraints.

## IF

`IF(condition, then_value, else_value)` is admitted by resolved built-in identity. A true
condition selects the first value; false or NULL selects the second. Pure branches whose
types already match Flink's result type use the existing native searched-CASE expression,
so unselected branches are not evaluated. Empty filtered batches skip the projection.

When Flink must normalize branch types, or the expression contains scalar UDFs or volatile
calls, the complete IF uses the existing generated-expression bridge inside native Calc.
This retains Flink's branch casts, call counts and code-generation evaluation order. A
user-defined function named IF retains its own implementation. Unsupported children retain
the existing admission rules. SQL parity tests cover exact numerics, strings, temporal and
binary values, nullable conditions, nested expressions, errors and multiple batches.
The admitted result types are BOOLEAN, numeric, character, DATE, TIME, TIMESTAMP,
TIMESTAMP_LTZ and binary.
BOOLEAN branches preserve TRUE/FALSE/NULL conditions, nullable results, nested predicates and
unselected failing branches across batches. TIMESTAMP_LTZ uses the same selection paths:
matching branch types select native timestamp values, while mixed precisions retain Flink's
generated branch casts. Its regression matrix covers precisions 0/3/6/9 in UTC,
Asia/Shanghai and America/Los_Angeles, pre-epoch fractions, DST transitions, nullable and
nested selections, unselected/selected errors, empty filtered batches and native Calc work
across 5,003 rows. Complex IF result types retain fallback.

The TIMESTAMP_LTZ release benchmark on Linux x86-64 (Intel i7-12650H, JDK 17,
released Flink 2.2.1, mimalloc) uses 2,000,000 runtime rows, NULL every seventh row,
parallelism 1, two warmups and five interleaved measurements per engine. The source
emits rows and the sink is `blackhole`; the harness asserts native Calc and both transposes.

| TIMESTAMP_LTZ(9) workload | Flink median (s) | Native median (s) | Flink/native |
| --- | ---: | ---: | ---: |
| Identity control | 0.277110 | 0.963528 | 0.288x |
| `IF(ts IS NULL, CAST(TO_TIMESTAMP_LTZ(0,3) AS TIMESTAMP_LTZ(9)), ts)` | 0.292844 | 1.008045 | 0.291x |

These standalone row-fed measurements show no speedup. The similar identity-control cost
supports retaining this small admission extension for native pipeline composition, with no
new timestamp evaluator or buffer-ownership machinery. Reproduce with `ScalarFunctionBenchmark`,
`-Pbench`, `-Dscalar.functions=IF_TIMESTAMP_LTZ`, `-Dscalar.rows=2000000`,
`-Dscalar.nullEvery=7`, `-Dscalar.warmup=2`, and `-Dscalar.runs=5`, setting `SF_BENCHMARK=true`.

The release benchmark below measures this coverage change against the previous full Flink
fallback, using `ScalarFunctionBenchmark` on Apple Silicon/JDK 17 with mimalloc,
parallelism 1, 2,000,000 runtime rows, NULL every seventh row, two warmups and five
interleaved measurements per engine. Both row/Arrow transposes are asserted; the string
payload budget is 264 bytes. Medians in seconds:

| Expression | Flink | Native | Flink/native |
| --- | ---: | ---: | ---: |
| String identity control | 0.667389 | 0.947148 | 0.705x |
| BIGINT identity control | 0.272022 | 0.406177 | 0.670x |
| `IF(s IS NULL, 'missing', s)` | 0.731814 | 2.237345 | 0.327x |
| `IF(n > 0, n, CAST(0 AS BIGINT))` | 0.298706 | 0.453794 | 0.658x |

These short row-fed pipelines are slower than Flink. This is expression coverage that
allows IF to remain within a larger native pipeline, not a standalone speed improvement;
no larger-pipeline gain is established by these measurements. Reproduce with:

```bash
SF_BENCHMARK=true mvn -pl streamfusion-runtime -am test -Pbench \
  -Dtest=ScalarFunctionBenchmark -Dsurefire.failIfNoSpecifiedTests=false \
  -Dscalar.functions=IF_STRING,IF_BIGINT -Dscalar.rows=2000000 \
  -Dscalar.nullEvery=7 -Dscalar.warmup=2 -Dscalar.runs=5
```

## IFNULL

`IFNULL(value, replacement)` runs natively with Flink's resolved common operand type and
result nullability, including STRING/VARCHAR, INT/BIGINT and DECIMAL. NULL selects the
replacement; an empty string is a value. Typed NULLs and nested calls retain their resolved types.
Both arguments are evaluated once before selection, matching Flink's scalar function: an error
in the replacement still fails the query when the first argument is non-NULL. This differs from
short-circuiting `COALESCE`/`CASE`, so IFNULL uses a separate eager columnar kernel. Selection
uses Arrow validity and preserves decimal precision/scale; an all-valid input reuses its array.

Eager evaluation applies only to rows that pass the Calc filter. When a batch has no
surviving rows, Calc builds correctly typed empty output columns without evaluating
projections, including scalar replacement expressions such as `1 / 0`. A zero-row input
also skips condition evaluation. Changelog tags retain their empty column and schema.
For example, `SELECT IFNULL(id, 1 / 0) FROM src WHERE id = 99` returns no rows when all
runtime ids are `2`, `0` or NULL; it still fails if a row survives the filter.

Admission checks the resolved built-in definition. A registered user function named IFNULL keeps
its own implementation through the existing scalar-UDF bridge. Unsupported child expressions
still retain their normal fallback rules. SQL tests cover values, schemas, exception behavior,
volatile argument evaluation, all-filtered batches, and an IFNULL projection/filter composed
with native Top-1.

## UUID

The zero-argument built-in `UUID()` runs in native Calc using DataFusion's volatile UUID v4
batch implementation. It returns one lowercase, hyphenated UUID per evaluated row, with Flink's
declared `CHAR(36) NOT NULL` result type. Values are independent across rows, batches and call
sites; independent engine executions are not expected to generate equal identifiers. Empty
inputs and all-filtered batches produce no UUID rows. Conditional expressions retain their
selection and NULL behavior.

Admission checks the resolved builtin identity and its zero-argument CHAR(36) signature.
A registered scalar function named UUID retains its own implementation and existing scalar-UDF
routing, including fallback for unverified signatures.
This supports the string-returning SQL function, not a new UUID logical data type or overload.

Release full-job diagnostic on Apple M4 Pro, JDK 17/Flink 2.2.1: two million rows, parallelism 1,
two warmups and five interleaved trials per engine, bounded rowwise integer source and blackhole
CHAR(36) sink. Native Calc and both transposes are asserted. Median times are 0.737s stock
Flink (0.730-0.742s), 0.726s previous native-enabled fallback (0.721-0.747s), and 0.334s native
UUID (0.331-0.335s): 2.21x versus stock and 2.17x versus fallback. Startup is included;
this is not an isolated sustained-throughput claim. Raw trials, environment and command are in
[`uuid-2026-10-15.json`](../benchmarks/results/uuid-2026-10-15.json). The integer input does not
use the scalar benchmark's text-width setting. Criterion separately covers batch sizes 0/16/1024/16384.

A pure `SELECT UUID()` projection is also checked after every input column is pruned, at
1/127/1,024-row batch boundaries with 5,003 runtime rows. Direct Calc checks retain row counts
for no-column batches and preserve each input changelog tag. Mixed CASE/COALESCE regressions
also verify nullable results and independent values; existing string-identity COALESCE paths
continue through released Flink evaluation inside the island, as shown in function diagnostics.

## RAND and RAND_INTEGER

`RAND()`, `RAND(seed)`, `RAND_INTEGER(bound)` and `RAND_INTEGER(seed, bound)` run in Rust.
Flink's resolved seed and bound arguments must be INT; other input widths use Flink's normal
overload validation. RAND returns DOUBLE in `[0, 1)` and RAND_INTEGER returns INT in `[0, bound)`.
NULL arguments return NULL without advancing the stream. An evaluated non-positive bound fails.

Literal seeds use Java's 48-bit random algorithm and retain a separate stream per call site across
batches. A column-dependent seed initializes a fresh generator per row, matching Flink's generated
code. Unseeded calls produce execution-time streams; their individual values are not expected to
match an independent Flink run. All forms are volatile and produce one result per input row, even
with only literal arguments. CASE evaluates selected branches only. A RAND_INTEGER under AND/OR
requires a positive literal bound; other bounds retain Flink's row short-circuiting through fallback.
As with Flink's generated random fields, stream state belongs to the running operator instance,
not keyed checkpoint state.

Floating-point unary negation also runs natively, including `-RAND(seed)`, and preserves signed
zero, infinities, NaN and NULL. Integer and DECIMAL unary negation use Flink-generated
expressions through the batch JVM bridge inside native Calc.

Release diagnostic on Apple M1 Max, JDK 17/Flink 2.2.1: two million rows, parallelism 1,
two warmups and five interleaved trials, with rowwise source/sink and both transposes asserted:

| Expression | Flink median | Native median | Flink/native |
|---|---:|---:|---:|
| `RAND(42)` | 0.554077s | 0.723574s | 0.766x |
| `RAND(n)` | 0.562129s | 0.728834s | 0.771x |
| `RAND_INTEGER(42, 100)` | 0.546426s | 0.734758s | 0.744x |

These isolated projections are slower than Flink; the same integer-source identity control is
0.546138s versus 0.706594s. This addition keeps random expressions inside larger native islands,
without claiming a standalone speedup. Reproduce with the `bench` profile and
`ScalarFunctionBenchmark#individualFunctions`, selecting `RAND_LITERAL,RAND_DYNAMIC,RAND_INTEGER_LITERAL`.

## Type and database metadata

These functions are enabled by default; set
`streamfusion.experimental.metadataExpressions=false` to disable them. The maintainer accepted
the slower standalone row-source projections on 2026-10-17; the
[release comparison](../benchmarks/scalar-functions.md#extended-function-coverage) includes both
standalone projections and aggregate pipelines.

`TYPEOF(value[, force_serializable])` admits Flink's resolved type strings, including
nullability and nested types. References and literals become a native string constant;
computed arguments remain evaluated before selecting the constant, so errors, volatile calls
and UDF invocations are still evaluated. Summary and serialization spelling come from the running
Flink logical type. A registered user function named TYPEOF keeps its own implementation.
The entry transpose follows the encoded expression's actual dependencies, so a type-only
reference does not materialize the referenced value. Generated JVM expressions still retain
every argument they evaluate.

`CURRENT_DATABASE()` captures Flink's query-level database context as a native string
constant. Changing the current database affects subsequent queries. A bare expression
without that context falls back rather than assuming a default database.

Runtime-source SQL checks compare stock/native values, result schemas, quoted database
names, nested types, registered functions and failing TYPEOF arguments. Native metadata constants
require identifiers that can be represented as valid UTF-8; an unpaired UTF-16 surrogate in a type
field name or database name keeps the complete Calc on Flink. Metadata ordering consumers retain
the existing Java/serialized string-order gate.

## User scalar functions

Table API plans that retain an `AS` wrapper around a scalar expression currently fall back with
`Calc: unsupported function/operator: AS`. This includes upstream constructor-state, inline and
non-static object UDF projection fixtures; ordinary SQL aliases that the planner removes are
unaffected. The execution audit pins both these host routes and the native routes for rich UDF
job parameters, combined functions and code-generation splitting. Alias admission remains part
of the [coverage backlog](https://github.com/datafusion-contrib/StreamFusion/issues/110).

Flink 1.18's deprecated `registerFunction` scalar wrapper is admitted alongside the modern
function definition. Both use the same signature and specialization gates,
and preserve the registered instance's constructor state, job parameters and open/close lifecycle.
Legacy wrapper subclasses that specialize functions during code generation stay on Flink.

Java `ScalarFunction` calls use the existing columnar JVM bridge: Arrow argument columns enter
the function once per batch and an Arrow result column returns to the native island. Supported
external Java types are `String`, boxed/primitive numeric and boolean values, `BigDecimal` for
`DECIMAL(p,s)`, and `byte[]` for `VARBINARY`/`BYTES`. Overload resolution must be unambiguous.
External Java `Row` and `Map` signatures use the default-enabled generated paths described
below. Fixed-size `BINARY`, root ARRAY/MULTISET, DATE/TIME UDF signatures, and alternate Java
conversion classes outside the verified mappings retain fallback. TIMESTAMP/TIMESTAMP_LTZ
signatures have the default-enabled generated path described below.

Non-folded zero-argument scalar UDF calls retain Flink because the generic JVM bridge signature
does not admit them. Flink can constant-fold deterministic calls before native planning. The
separately resolved UUID builtin uses its own niladic signature.

Functions implementing Flink's `SpecializedFunction` also fall back. Their implementation must
be created with the resolved call types and Flink's code-generation context; invoking the
registered, unspecialized instance through the native bridge can produce incorrect results.

`DECIMAL` UDF projections use Flink's `DecimalData.fromBigDecimal`: declared scale, `HALF_UP`,
and NULL on precision overflow, including precision 38. Trailing zeros survive the round trip.
Consumers and predicates also run inside native Calc/filter islands. Their complete expression is
compiled with Flink's expression generator and invoked through the same Arrow batch bridge, so
its external null flag and internal decimal conversion stay together. An overflowing non-null
`BigDecimal` can therefore project NULL while `IS NULL` returns false, exactly as in Flink.

The generated expression preserves CASE/COALESCE decisions, nested UDF reuse of the external
BigDecimal, arithmetic and conversion exceptions, and the declared function signatures. It does
not replace the external null flag with Arrow validity. These expressions execute on the JVM;
the surrounding island stays columnar and the existing UDF type/specialization gates still apply.
Generated and direct calls share function instances and a single task lifecycle, including after
serialization. Code generation and runtime initialization use Flink's user-code classloader.

`VARBINARY` uses raw bytes, preserving empty values, embedded zeros, arbitrary non-text bytes, and
NULL. Results are copied into Arrow before the next row is evaluated. A Calc with multiple
binary UDF calls, including nested calls and calls shared between predicates and projections,
uses Flink's generated code for the **complete row**. This preserves call order and shared mutable
arrays until every result field has been evaluated. For example, if `shared(id)` reuses its own
four-byte buffer, `SELECT shared(id), shared(id + 1)` retains the second call's bytes in both
columns, exactly as Flink does. Copying each call immediately would change that result.

The generated Calc evaluates its predicate before projections and returns one nullable Arrow
struct per input row through the existing batch bridge. A NULL struct drops the row; NULL
fields remain ordinary output values. Native Calc selects the matching changelog tags and
exposes the struct's child columns to the next columnar operator. Zero-column projections,
zero-argument functions, and fully filtered batches preserve their row counts. Function identity,
serialization, lifecycle and exceptions use the same task binding as other scalar calls.

This complete-row path executes on the JVM, including sibling expressions. It admits only
inputs and outputs supported by the scalar bridge: primitive numeric/boolean, character,
BINARY/VARBINARY, DECIMAL, date/time/timestamp and interval values. Fixed BINARY result
projections use this row path to retain their declared Arrow width. Collection and nested ROW inputs
or outputs retain fallback by default; the nested scalar option above extends its verified
ROW/ARRAY boundary. Unsupported UDF signatures and specialized functions still fall back. Temporal
columns and builtin expressions are supported here; temporal **user-function signatures** require
the separate opt-in below. The path preserves columnar operator boundaries and avoids a separate
host Calc between native operators; it does not turn Java UDFs into Rust kernels.

The release/mimalloc diagnostic for `SELECT id, shared(id), shared(id + 1)` measured
**0.342965s Flink / 0.694285s native** (0.494× Flink/native), using one million generated rows,
parallelism one, two warmups and five alternating measured trials per engine. The native plan
includes both RowData→Arrow and Arrow→RowData transposes and the same rowwise blackhole sink.
The isolated projection is slower: this extension preserves correctness and enables composition
with neighboring native operators, with no standalone speedup claimed. Reproduce with
`SF_BENCHMARK=true mvn -Pbench -pl :streamfusion-runtime -am -Dtest=SharedBinaryUdfBenchmark
-Dsurefire.failIfNoSpecifiedTests=false test`.

Registered scalar functions take precedence over builtin names, including in nested expressions
and filters. A function registered as `UPPER`, for example, invokes the registered Java function
through the bridge. Unsupported signatures fall back using the same UDF admission rules.
Within an operator, call sites sharing a function instance share one `open`/`close` lifecycle,
including calls in both projections and predicates. Separate instances retain separate lifecycles.
Serialization preserves shared references; task-local registrations retain each call's signature.
Failed initialization releases earlier registrations and successfully opened functions, and a
failing `close` does not prevent cleanup of the remaining instances.

Runtime parity tests cover mixed projections, repeated decimal calls, nullable precision-38
values, scale normalization, overflow and its pre-conversion nullness, nested external values,
conditional consumers, exception parity, typed NULL arguments, shadowed builtin names, shared
lifecycle-dependent functions, and 5,003-row inputs. C Data tests
cover sliced inputs, output survival after input release, shared-array mutation after export,
and reclamation of Arrow allocations after success or a partially written row result fails.
Shared binary SQL regressions compare ordered raw changelogs against released Flink and require
executed native Calc metrics, including nested calls, conditional evaluation, distinct instances,
NULL/empty buffers, zero-argument stateful calls and multi-batch inputs.

With `streamfusion.calc.sharedStatefulUdf.enabled=true`, non-deterministic scalar function
identifiers used at multiple call sites select Flink's generated evaluator for the complete Calc:
its predicate and projections execute together in row order. This option now defaults to **true**
under the maintainer-approved UDF performance exception.
Explicit `false` retains the shared-call host fallback; slower UDF execution is accepted for
correct coverage without a standalone acceleration claim. Existing complete-row binary and other
already admitted generated expressions keep their existing admission.
This covers INT, BIGINT and STRING results, nested calls, CASE/COALESCE branches and repeated
calls that Flink reuses. The existing column-at-a-time shared-call guard remains in place;
shared functions are admitted through the complete-row path rather than bypassing that guard.
One registered function instance has one open/close lifecycle, and separate instances stay
separate. The same specialization, signature, result conversion and nested-type restrictions
apply before generation; unsupported forms still fall back.

Single calls, independent instances and deterministic scalar functions retain their existing
admission. Binary buffer aliasing remains governed by the complete-row binary rules above.

Already-admitted complete JVM Calcs with primitive numeric/boolean arguments and ordinary
nonempty numeric/boolean/character outputs can evaluate directly over the borrowed Arrow input and write
flat Arrow outputs inside the columnar operator. This avoids sending a batch to Rust only to call
the same complete generated program back on the JVM. It applies only to batches without a
RowKind column; changelog batches and represented, binary, decimal or temporal layouts retain
the existing bridge. Zero-column outputs and string arguments also retain their existing path. Predicates,
projections and shared function calls still execute together in Flink's row order, with one
function lifecycle. Empty results and exceptional partial outputs release their Arrow buffers.
`directPrimitiveCalcRows` counts input rows actually evaluated by this path.

The [release comparison](../optimizations/udf-columnar-upcall.md#complete-generated-calcs-with-primitive-inputs)
reduces the existing opt-in INT/BIGINT/STRING job times by about 28%/32%/10%. These isolated jobs
remain slower than stock Flink. Default admission now follows the accepted UDF exception;
this does not complete [issue #398](https://github.com/datafusion-contrib/StreamFusion/issues/398).

Runtime tests compare ordered values, types, every changelog tag and invocation counts, including
nullable inputs/results, nested and conditional calls, all-filtered and empty inputs, exceptions,
5,003-row sources and balanced task lifecycle. Native operator metrics establish actual consumption.

Release end-to-end measurements retain both transposes and a rowwise blackhole sink. At five
million rows, paired medians for two shared calls were 0.457/0.843 s stock/native for INT,
0.453/0.833 s for BIGINT, and 1.064/1.558 s for STRING. Native throughput is about 0.54× stock
for the integers and 0.68× for strings. One-million-row comparisons also retain the previous
fallback; all completed trials are in the [raw results](../benchmarks/results/shared-stateful-scalar-2026-10-14.json).
A diagnostic CPU profile records 972 JVM-callback, 2,433 entry-transpose and 673 exit-transpose
samples among 6,769 total samples. These inclusive stack counts overlap; they identify boundary
and callback work, not an additive cost breakdown. The row evaluator already reuses its input
row and batches JNI calls; further primitive Arrow conversion work is tracked in
[#248](https://github.com/datafusion-contrib/StreamFusion/issues/248). No standalone speedup is claimed. Default UDF admission supports composition with surrounding native operators,
whose performance must be measured for the complete query.


### Temporal scalar functions

`streamfusion.calc.temporalUdf.enabled=true` admits verified scalar signatures using
`LocalDateTime` for TIMESTAMP and `Instant` for TIMESTAMP_LTZ arguments/results. Precision
0/3/6/9 is preserved through the existing full-range millisecond/nanosecond Arrow representation.
The complete Calc uses Flink's generated code and released external/internal converters, including
its predicate and sibling projections. This retains calendar fields for TIMESTAMP, absolute time
for LTZ, declared result types, NULL callbacks, shared function order and one task lifecycle.
Results remain Arrow columns for downstream native operators, including GROUP BY.

The option now defaults to **true** under the accepted UDF performance exception.
Explicit `false` reports `temporal scalar UDF execution is explicitly disabled`. Builtin temporal
expressions retain their current native coverage. Specialized functions, unsupported conversion
classes (including alternate timestamp result classes), fixed BINARY signatures and nested UDF
types retain their own gates. Other expression contexts keep their existing admission rules.

The runtime suite covers UTC, America/Los_Angeles and Asia/Shanghai; negative epochs, year 1/9999,
DST-adjacent values, all four precisions, NULL/empty/fully filtered input, selected failures,
shared call-site order, lifecycle and grouping consumers. Native Calc input/output counters are
asserted. Direct C Data tests cover sliced nullable timestamp vectors and output ownership after
input release. See the [measured limits](#temporal-scalar-conversion)
and the remaining [performance work](https://github.com/datafusion-contrib/StreamFusion/issues/389).

#### Temporal scalar conversion

Release+mimalloc on Flink 2.2.1/JDK 17, Core i7-12650H, 2 GiB heap, parallelism one,
2M runtime rows and 1,024-row batches, precision 9/UTC, two warmups and five alternating
stock/native pairs. The source cycles through eight precomputed temporal values (one NULL).
Complete jobs retain both transposes, production IPC and the rowwise blackhole sink; plans and
native Calc counters are asserted.

| Projection | Stock median (range), s | Opt-in median (range), s |
| --- | ---: | ---: |
| TIMESTAMP and LTZ identity | 0.623 (0.607–0.641) | 0.875 (0.856–1.001) |
| TIMESTAMP to text | 0.964 (0.960–0.974) | 0.994 (0.987–1.079) |
| LTZ to epoch milliseconds | 0.562 (0.553–0.600) | 0.671 (0.660–0.673) |

The previous correct fallback measured 0.524/0.858/0.478 s, with paired stock controls
0.511/0.864/0.479 s. These are separate JVMs with control drift; they do not isolate an
optimization gain. The original opt-in callback path is slower than stock in all three fixtures.
These historical timings claim no standalone acceleration. Default coverage is now enabled
under the UDF exception. The
[raw results](../benchmarks/results/temporal-udf-2026-10-15.json) retain every warmup and
measured trial, the previous fallback, initial prototype, and rejected experiments.

A 5M-row JFR diagnostic points to general result Arrow writers and perimeter row conversion.
Removing one outer result-row wrapper and reusing the exit StreamRecord both passed correctness
checks but failed to improve complete-job time. Timestamp binary exits, unrolled batch result
writes, inline Calc evaluation inside that loop and bulk timestamp component validity also failed
performance admission. A direct typed-output kernel likewise passed correctness checks but
still lost to stock, including a variant that writes timestamp components directly. None is retained. See the
[rejected experiments](https://github.com/datafusion-contrib/StreamFusion/blob/main/.claude/wontdos/364-temporal-row-output-shortcuts.md).
Further performance work remains tracked in [#389](https://github.com/datafusion-contrib/StreamFusion/issues/389),
including further measured conversion work and complete-job measurements inside surrounding
native operators; correctness-verified UDF admission no longer requires a speedup.

An experimental direct Java Arrow-batch path removes the Rust round trip when the entire Calc
is already generated JVM code and every argument is a timestamp input column. It requires
`-Dstreamfusion.experimental.directTemporalCalc=true` in addition to the normally enabled temporal
UDF option. Changelog-tagged input and other argument expressions retain the existing callback.
The task-local function binding carries the verified input-column mapping; runtime code does
not decode the expression ABI. Predicate evaluation, function lifecycle and row-major call
order use the same released Flink evaluator.

`-Dstreamfusion.experimental.temporalAliases=true` additionally defers timestamp output writes
for unconditional, timestamp-only projections of matching width. Every function still runs.
A byte-equal result column retains the input buffers; the first changed value materializes its
unchanged prefix and all subsequent values. Preinitialized Arrow fields preserve timestamp
component metadata through transfers. Other result shapes use the ordinary flat Arrow writer.
Both experimental switches default off. [PR #368](https://github.com/datafusion-contrib/StreamFusion/pull/368)
merged the temporal UDF semantic support, which now defaults on under the accepted UDF exception;
these two execution experiments remain disabled by default.

In an unprofiled 2M-row repeat with the configuration above, direct batches plus timestamp
aliasing measured identity **0.651 s versus stock 0.645 s**, text **0.830 versus 0.969 s**, and
epoch conversion **0.536 versus 0.570 s**. The identity result still misses performance admission;
these separate JVMs also do not establish an improvement against the previous correct fallback.
All trials and raw CPU/allocation profiles are retained in the results artifact. CPU profiling
covers both Java and symbolized Rust frames with async-profiler 4.0, a 2 ms software CPU timer
and DWARF native unwinding. It identifies Arrow FFI/ownership work in the old Rust path and
extra Java TimestampData allocation at callback input and row exit in the improved path.
Profiled job durations are excluded from acceleration claims. See the
[optimization and profiling details](../optimizations/udf-columnar-upcall.md#experimental-complete-temporal-calc-batches).

Two further profile-guided timestamp reuse experiments were rejected. A bounded value cache
reduced sampled timestamp accessor allocations to about 15.7 MB, and local Arrow-batch value
hints eliminated them in a separate 5M-row profile. Both still lost to paired stock on identity
at cardinalities eight and 16,384; a reusable row-reader variant also lost. These allocation
results cover 25M rows per engine and are sampled Java bytes, not Rust heap or peak memory.
Neither implementation is retained. The raw artifact records all measured arrays, available
warmups, missing-log limitations and compressed CPU/allocation stacks. Three-pair diagnostics
and drifting stock controls do not establish an improvement between variants.

A matching `perimeter` benchmark case projects `t, z` directly from the same source, without UDFs.
Each pair of cases runs in one JVM with the resources and repeated-trial method above:

| Rows / projection | Stock median, s | Unchanged opt-in median, s |
| --- | ---: | ---: |
| 2M / direct temporal columns | 0.569 | 0.628 |
| 2M / temporal identity UDFs | 0.627 | 0.845 |
| 5M / direct temporal columns | 1.292 | 1.496 |
| 5M / temporal identity UDFs | 1.415 | 2.055 |

Both perimeter conversion and callback work remain material. The cases execute sequentially;
subtracting these medians is a diagnostic, not an exact operator-cost measurement. A separate
32-second JFR at 2 ms sampling and stack depth 128 records 3,083 native-task and 2,379 stock-task
execution samples. Timestamp materialization and Arrow writers are prominent on native threads.
Compiled stacks can end before their callers, and native-method samples include blocked I/O;
these are profiling clues rather than an additive CPU breakdown. The raw artifact retains the
sample counts and profiled trials separately from unprofiled performance results.
A separate JVM compilation log confirms that the generated evaluator already inlines the Arrow
timestamp getters, temporal converters and identity functions. Surrounding bridge calls sometimes
remain separate compiled methods. Those decisions alone do not establish an optimization: the
inline batch experiment still loses in complete jobs.

The focused temporal, shared/binary, lifecycle and decimal-consumer suites pass **90 checks on
each of Flink 2.2.1 and 1.18.1**. Reproduce the benchmark with:

```sh
SF_BENCHMARK=true mvn test -Pbench -pl streamfusion-runtime -am \
  -Dtest=TemporalUdfBenchmark -Dsurefire.failIfNoSpecifiedTests=false \
  -Dtemporal.rows=2000000 -Dtemporal.warmup=2 -Dtemporal.runs=5 \
  -Dsf.extraJvmArgs=-Xmx2g
```

`-Dtemporal.cardinality=16384` selects a precomputed runtime corpus with distinct seconds and
precision-aligned fractional nanoseconds, every eighth value NULL. The default cardinality of
eight retains the full-range calendar, negative-epoch and timezone boundary corpus above.

Use `-Dtemporal.cases=perimeter,identity -Dtemporal.rows=5000000` for the matching control.
For JFR, compile first and run `mvn surefire:test@default-test` with the same profile and test
properties: the ordinary `test` lifecycle also launches a separate ORC test JVM, which would
overwrite a shared recording filename after the benchmark completes.


## String ordering

Relational character-string comparisons (`<`, `<=`, `>`, `>=`) fall back, including
inside filters, CASE, and casts. Flink compares retained Java strings by UTF-16 code units,
but serialized strings by UTF-8 bytes. These orders disagree for some supplementary Unicode
characters. Arrow retains the UTF-8 value without the Java-representation information needed
to select Flink's order, so admission cannot safely depend on the column's SQL type alone.
The gate also applies to ASCII/BMP inputs and serialized sources because the planner cannot
prove the representation and character range of every runtime operand. Equality and inequality
(`=`, `<>`) remain native; this gate does not change Top-N's serialized sort order.

## String concatenation and hashes

These functions run entirely in Rust by default, in projections, predicates, and nested expressions:

| Function | Native behavior and admission |
|---|---|
| `CONCAT(s, ...)`, `s \|\| t` | Character-string arguments; any NULL argument makes the result NULL. Empty strings are preserved. |
| `CONCAT_WS(separator, ...)` | A literal or column separator; a NULL separator makes the result NULL. NULL values are skipped, empty strings are preserved, and no values or all-NULL values produce an empty string. |
| `MD5(s)` | Lowercase hexadecimal MD5 of the string's UTF-8 bytes; NULL input produces NULL. |
| `SHA1(s)` | Lowercase hexadecimal SHA-1 of the character string's UTF-8 bytes; NULL input produces NULL. |
| `SHA224(s)`, `SHA256(s)`, `SHA384(s)`, `SHA512(s)` | Lowercase hexadecimal SHA-2 of the UTF-8 bytes; NULL input produces NULL. |
| `SHA2(s, bit_length)` | The two-argument form with a literal bit length of 224, 256, 384, or 512; equivalent to the corresponding fixed-width function. |

A dynamic `SHA2` bit length uses Flink-generated code through the batch JVM bridge, preserving
NULLs and the released host's unsupported-algorithm failure. Literal values are checked exactly,
including `BIGINT`, without truncating to 32 bits; unsupported or NULL literal widths fall back.
For example, `SHA2(s, CAST(4294967520 AS BIGINT))` falls back and retains Flink's unsupported-algorithm
failure instead of being treated as SHA-224. Flink 2.2 does not expose hash
overloads with an explicit character set. Binary/collection concatenation is outside this
character-string admission.

`CONCAT` computes Flink's strict NULL propagation from the input validity bitmaps, without
re-evaluating its arguments. Batches without NULL results use DataFusion's kernel directly; batches
with NULL results append only surviving rows. Neither path revalidates the concatenated UTF-8
payload. `CONCAT_WS` delegates to DataFusion. Hashes use the released RustCrypto libraries used by
DataFusion and Comet, writing lowercase hex directly into presized UTF-8 Arrow buffers without intermediate
binary or string-view columns or per-row heap allocations. See [string copy reduction](../optimizations/string-copy-reduction.md).
SQL parity tests cover NULLs, empty strings, embedded zero bytes, Unicode, long inputs, nested
calls, filters, valid BIGINT widths, and dynamic or oversized bit-length fallback.

The corresponding throughput diagnostics compare stock Flink against native execution with a
rowwise generated source, both row/Arrow transposes, and a blackhole sink. They assert those native
plan boundaries and use one warmup followed by the best of three complete job runs at parallelism 1.
They are function diagnostics, separate from the Nexmark headline benchmark:

```sh
SF_BENCHMARK=true SF_ROWS=2000000 mvn -pl :streamfusion-runtime test -Pbench \
  '-Dnative.cargo.args=build --release --features mimalloc' \
  '-Dtest=ThroughputBenchmark#stringConcatThroughput+stringHashThroughput'

SF_BENCHMARK=true SF_ROWS=2000000 mvn -pl :streamfusion-runtime test -Pbench \
  '-Dnative.cargo.args=build --release --features mimalloc' \
  '-Dtest=ThroughputBenchmark#nullableStringConcatThroughput'
```

Measured on Apple M4 Pro with JDK 17 on 2026-09-08, using the core-only release build above and
2 million rows whose strings cycle through `s0` to `s7`. The nullable diagnostic adds a 1 KiB
literal prefix and a CASE argument that makes 75% of results NULL. The two commands run in
separate JVMs:

| Diagnostic | Flink | StreamFusion | Throughput ratio |
|---|---:|---:|---:|
| `CONCAT` and `CONCAT_WS` projections | 0.566 s | 0.733 s | 0.77x |
| MD5 and SHA-2 projections | 4.141 s | 2.211 s | 1.87x |
| Nullable `CONCAT`, 1 KiB prefix | 0.390 s | 0.613 s | 0.64x |

The short-concatenation ratio changed from 0.94x in the first patchset to 0.77x here,
but native time was effectively unchanged (0.736 s to 0.733 s). Flink's baseline moved
from 0.691 s to 0.566 s, an approximately 18% decrease. The ratio change is within
the observed run-to-run baseline variance, not evidence of a native regression;
these separate runs do not establish a precise change in relative performance.

Both standalone concatenation cases remain slower than Flink. Their admission is useful for keeping
larger queries fully native; these results retain the regressions alongside the hashing gain.
Against the previous native implementation on the same upstream revision, hashing throughput
improved by about 47% and nullable concatenation by about 16%; short concatenation was essentially
unchanged. The [optimization ledger](../optimizations/string-copy-reduction.md#calc-construct-only-the-final-string-buffers)
records the before/after native times.
Both engines are timed through planning, source generation, and job completion; the native path
also includes both transposes. These are end-to-end diagnostics on this machine rather than
isolated kernel timings.

## HASH_CODE

`HASH_CODE(value)` runs as a native scalar for CHAR/VARCHAR, BOOLEAN, integral types,
FLOAT/DOUBLE, DECIMAL, DATE, TIME and plain TIMESTAMP. NULL remains NULL. The result
matches Flink's SQL function: strings use Java UTF-16 hashing and wrapping absolute
value, BIGINT folds its high and low words, DECIMAL includes the declared scale, and
TIMESTAMP retains milliseconds and nanoseconds within the millisecond. This function
does not change the hash used internally by native exchanges or keyed state.

Admission checks Flink's resolved operator identity; a user function named HASH_CODE
keeps its own implementation. Binary, complex types and TIMESTAMP_LTZ are outside this
scalar admission. SQL parity tests cover integer boundaries, Unicode and embedded NUL,
the negative minimum string hash, precision-38 decimals with several scales, NULLs,
filters, and timestamps outside Arrow's nanosecond epoch range.

Supporting this scalar removes the HASH_CODE Calc blocker in split-distinct plans.
The remaining window layouts still fall back and are tracked in
[#166](https://github.com/datafusion-contrib/StreamFusion/issues/166).

Release/mimalloc benchmark on Apple Silicon/JDK 17, parallelism 1, 2,000,000
runtime rows, NULL every seventh row, two warmups and five interleaved measured
runs per engine. Both row/Arrow transposes are asserted; strings use a 264-byte
payload budget. Medians in seconds:

| Expression | Flink | Native | Flink/native |
| --- | ---: | ---: | ---: |
| String identity control | 0.759980 | 1.373877 | 0.553x |
| BIGINT identity control | 0.251243 | 0.424932 | 0.591x |
| DECIMAL identity control | 0.256370 | 0.626968 | 0.409x |
| `HASH_CODE(s)` | 0.700423 | 1.217803 | 0.575x |
| `HASH_CODE(n)` on BIGINT | 0.274135 | 0.421497 | 0.650x |
| `HASH_CODE(n)` on DECIMAL | 0.303444 | 0.451720 | 0.672x |

Standalone row-fed hashing is slower than Flink. This change removes an expression
admission blocker so hashes can compose within native pipelines; these measurements
do not establish a speedup for a larger pipeline or split-distinct aggregation.

```bash
SF_BENCHMARK=true mvn -pl streamfusion-runtime -am test -Pbench \
  -Dtest=ScalarFunctionBenchmark -Dsurefire.failIfNoSpecifiedTests=false \
  -Dscalar.functions=HASH_CODE_STRING,HASH_CODE_BIGINT,HASH_CODE_DECIMAL \
  -Dscalar.rows=2000000 -Dscalar.nullEvery=7 -Dscalar.warmup=2 -Dscalar.runs=5
```

## Declared-type guard

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

## Collection subscripts

ARRAY subscripts accept runtime INT expressions with Flink's one-based indexing. A NULL
container/index, zero, negative or out-of-range runtime index returns NULL. Literal indexes below
one remain on Flink's path so its plan-time validation error is preserved. Elements keep their
declared types, including DECIMAL, TIMESTAMP/LTZ and nested ARRAY/MAP/ROW values.

MAP lookup accepts literal keys and runtime integer, decimal, character, boolean, binary, date,
time and timestamp keys. It returns the first matching value and preserves nested output types.
A NULL lookup key/container or a missing key returns NULL. Dynamic floating-point and collection
keys remain on Flink. Runtime keys are evaluated once per row as an Arrow column; the lookup
compares column entries directly without converting each value to a JVM object.
The runtime search type must match the MAP key type, including decimal precision/scale and
timestamp precision (character widths may differ). Mixed key types fall back so native coercion
cannot narrow an integer or round a decimal search value into an incorrect match.

Flink's BinaryMap lookup reads primitive stored NULL slots without checking their null bit when
the search key is non-NULL: strings/binary read as empty, numbers/date/time as zero, boolean as
false, and compact timestamps as epoch. Native lookup preserves this behavior, including the
order between NULL and explicit empty/zero keys. A NULL search key still returns NULL.

MAP keys declared nullable fall back when they use DECIMAL precision above 18 or timestamp
precision above 3: Flink's non-compact NULL slots have different read/error behavior. A MAP with
these key types declared NOT NULL remains eligible. This restriction applies to literal as well
as runtime searches. Runtime NULL search keys and NULL containers are still supported.

When Flink folds a constant NULL subscript into a typed NULL projection, it retains its native
Arrow type, including nested collection values. Folded NULLs can compose with runtime lookups.

`DynamicCollectionBenchmark` measures this path with a release native build (`-Pbench`), 2 million
rows, parallelism 1, two warmups and five interleaved trials. Both row/Arrow transposes and the
blackhole sink are included. Containers have three entries, every eighth container is NULL, and
runtime indexes/keys include NULLs, misses and invalid array positions.

| Expression | Flink median | Native median | Flink / native |
| --- | ---: | ---: | ---: |
| ARRAY runtime index | 0.304111s | 0.505128s | 0.602x |
| MAP runtime key | 0.813771s | 1.040418s | 0.782x |

Standalone lookup is slower than Flink. This coverage keeps expressions available inside an
existing native pipeline; these measurements do not establish an end-to-end speedup.

## Integer division

Integer `/` truncates toward zero and wraps `MIN_VALUE / -1` back to `MIN_VALUE`,
matching Java. A non-NULL dividend divided by zero fails the job. A NULL operand
produces NULL, including a NULL dividend with a zero divisor.

Integer `/`, `MOD`, and `%` inside `AND` or `OR` stay native when the divisor is a nonzero
integer literal and their operands contain no fallible division. Other divisor shapes fall back
at planning time, including in projections, filters, and nested expressions. DataFusion's batch
evaluation may run division on rows whose guard Flink short-circuits. Constant-divisor filters such
as `event_type = 2 AND MOD(bid.auction, 2) = 0` are safe even when the nested ROW is NULL.
CASE result branches stay native because
CASE selects rows before evaluating them. Floating division remains native under boolean
operators because zero divisors produce IEEE infinity/NaN instead of an exception.

## SIGN

FLOAT/DOUBLE SIGN returns the input for signed zero and NaN, and `-1` or `1` for
nonzero values, including infinities. NULL remains NULL. The result keeps the input's
floating width; dividing by `SIGN(-0.0)` therefore retains negative infinity.

## Floating comparisons

FLOAT/DOUBLE `=`, `<>`, `<`, `<=`, `>`, and `>=` use Java primitive comparisons in
projections and filters. Positive and negative zero compare equal. NaN compares unequal to
every value, including itself; all four relational comparisons involving NaN return FALSE.
NULL propagates. Mixed primitive numeric operands use Java's FLOAT/DOUBLE promotion.
These scalar rules are separate from grouping-key equality and sort ordering.

## Casts

BOOLEAN to STRING, bounded VARCHAR and CHAR runs Flink's cast executor through the batch JVM
bridge inside native Calc. TRUE/FALSE use uppercase text, NULL stays NULL, and bounded targets
retain Flink's truncation and CHAR padding. TRY_CAST follows the same released cast rules.

Native, unconditionally, with no host involvement:

- **Widening numeric** — integer→wider integer, integer→float/double, float→double.
- **Narrowing integer→integer and float/double→integer** — a purpose-built `NarrowingCast` kernel
  reproduces Flink's primitive Java cast semantics exactly: two's-complement wraparound for an
  integer source, and saturation to INT/BIGINT followed by low-bit narrowing for a float source. Arrow's own
  cast kernel can't do this — it errors on overflow instead of wrapping/saturating.
- **DECIMAL → TINYINT/SMALLINT/INT/BIGINT** — truncate toward zero, then keep the destination's
  low bits, matching Flink's BigDecimal `longValue()` followed by the Java integer cast.
  Overflow wraps rather than saturating or producing NULL; NULL input remains NULL.
  This also admits casts above DECIMAL aggregates such as `CAST(AVG(d) AS BIGINT)`.
- **DECIMAL → FLOAT/DOUBLE** and **ARRAY<DECIMAL> → ARRAY<FLOAT/DOUBLE>** — retain Flink's
  intermediate double conversion, including compact-decimal division and final FLOAT narrowing.
  This can differ from rounding a decimal directly to FLOAT. Array casts convert the element
  buffers and preserve offsets, empty arrays, nullable containers/elements and declared types.
  Other changes of collection element type still require a separately supported cast.
- **`CHAR`/`VARCHAR` → `VARCHAR`** when the target length is ≥ the source length — an unpadded
  no-op (e.g. the common `COALESCE(s, 'x')` pattern).
- **Widening timestamp precision** within `TIMESTAMP` or within `TIMESTAMP_LTZ` — Arrow stores both
  at nanosecond precision at the columnar boundary, so widening the Flink declaration is a no-op.
- **`→ DECIMAL` from an exact source** — a `DECIMAL` or integer input, rescaled `HALF_UP`
  before checking the target precision. Overflow produces SQL `NULL`, including a carry caused
  by rounding (`999.995` cast to `DECIMAL(5,2)`), scale increases, and integer inputs. NULLs remain
  visible to surrounding expressions and filters. The result stays an Arrow `Decimal128` column.

Integer narrowing keeps the low bits, matching Java wraparound. FLOAT/DOUBLE to INT or
BIGINT truncates toward zero, saturates at the destination bounds, and maps NaN to zero.
FLOAT/DOUBLE to TINYINT or SMALLINT first performs that INT conversion, then keeps the
low 8 or 16 bits. Thus `128.75` becomes TINYINT `-128`, and positive infinity becomes
TINYINT/SMALLINT `-1`. NULL remains NULL for every target.

### STRING/VARCHAR/CHAR to BOOLEAN

`CAST(s AS BOOLEAN)` uses a native Arrow Boolean builder. It accepts `t`, `true`, `y`,
`yes`, `1` as TRUE and `f`, `false`, `n`, `no`, `0` as FALSE, ignoring ASCII case.
It does not trim whitespace; empty strings, other numeric values and Unicode lookalikes
are invalid. NULL remains NULL. Flink's resolved result type/nullability is retained.

With the default cast behavior an invalid token fails the query with Flink's `TableException`
and parse message preserved through JNI, including for a NOT NULL source.
With `table.exec.legacy-cast-behaviour=ENABLED` it produces NULL. For a NOT NULL
source, Flink retains a NOT NULL result declaration even in legacy mode; the existing
sink enforcer therefore rejects or drops malformed rows according to its ERROR/DROP setting.
Both outcomes are tested against Flink. CASE can skip
an unselected failing cast. Default-mode casts nested under AND/OR still fall back so
that Flink's row short-circuiting suppresses errors on unselected rows; legacy-mode
casts can compose under AND/OR because malformed input returns NULL. A bare expression
encoder without table configuration declines this cast instead of guessing the mode.
`TRY_CAST(s AS BOOLEAN)` uses the same native parser with NULL on invalid input,
independently of legacy mode. It composes under NOT, COALESCE, AND/OR and filters.
When its input expression can fail, the existing generated-expression callback preserves
Flink's input evaluation errors: TRY_CAST only catches conversion failures. The reverse
BOOLEAN-to-character casts use the host-exact path described above.

### Integer/string casts

`CAST` and `TRY_CAST` between `STRING`/`VARCHAR`/`CHAR` and
`TINYINT`/`SMALLINT`/`INT`/`BIGINT` use native Arrow kernels without a JVM callback.
Parsing removes only leading/trailing ASCII spaces, accepts an optional sign and ASCII
digits, and truncates fractional decimal text toward zero after validating every digit.
Like Flink, `.`, `+.`, and `-.9` yield zero. Tabs, newlines, Unicode whitespace/digits,
exponents, internal spaces and additional decimal points are invalid. Values outside the
target integer range fail; this string conversion does not wrap like an integer narrowing cast.

NULL input remains NULL. Ordinary CAST fails on invalid input in default mode; TRY_CAST
and legacy-mode CAST return NULL. CASE suppresses an unselected failing cast. Default CAST
inside AND/OR stays on Flink to preserve row short-circuiting; TRY_CAST and legacy CAST
can compose natively. NOT NULL sink enforcement remains Flink's ERROR/DROP policy.
Native integer-parse failures preserve Flink's `NumberFormatException` and parse message
through JNI. Successful results and NULL-on-error policies match as well.

Integer formatting uses canonical decimal text, including signed minima and zero.
`VARCHAR(n)` truncates to `n` characters; `CHAR(n)` also pads shorter results with spaces.
Legacy mode leaves the formatted text unchanged regardless of the declared length, matching
Flink. Other TRY_CAST pairs, except the BOOLEAN and temporal forms described here and
the binary and DECIMAL forms below, still fall back. Bare encoders without table configuration
decline mode-dependent casts. See the [kernel ledger](../optimizations/scalar-function-kernels.md)
for the release benchmark against the previous host-cast path.

Boolean TRY_CAST regression coverage also verifies native routing in ordinary projections,
filter predicates and CASE consumers, including strings that parse to NULL.

### Temporal TRY_CAST

Standalone DATE, TIME and TIMESTAMP/TIMESTAMP_LTZ TRY_CAST use verified parsing fast paths
inside the existing JVM batch callback. DATE accepts four-digit years and one- or two-digit
month/day fields. TIMESTAMP accepts `yyyy-MM-dd HH:mm:ss` with an optional 1–9 digit fraction,
including Flink's SMART normalization of days 29–31 and zero-fraction `24:00:00`. The result
retains the declared precision and the same Java local-time and zone conversion as Flink.
Other formats and year zero use the released Flink parser. Modern-mode composed character
children execute their generated code exactly once, outside the conversion failure handler.
Legacy mode retains generated DATE conversion and only the direct-input timestamp fast path.
TIME recognizes `HH:mm:ss` with optional 1–3 fractional digits and applies the released
version's logical-type precision rule: Flink 2.2 truncates while 1.18 preserves parsed milliseconds.
Complete-row evaluators retain Flink's generated conversion. See
[temporal parsing measurements](../optimizations/host-exact-builtins-upcall.md#canonical-timestamp-try_cast).

`TRY_CAST` from STRING/VARCHAR/CHAR to DATE, TIME, TIMESTAMP and TIMESTAMP_LTZ
uses the existing columnar callback, with the exact temporal subsets above and Flink's
generated conversion for other cases. Malformed text and rejected conversions return NULL;
errors in the input expression still propagate. Flink 1.18.1 accepts `24:00:00` as an internal TIME but throws
`DateTimeException` during external collection; native execution preserves that failure.
Flink 2.2.1 returns NULL for the same TRY_CAST. CASE and filters suppress unselected failing expressions. Default and legacy
cast modes use their configured Flink rules. TIMESTAMP_LTZ interprets local text in the
configured table time zone, including DST gaps and overlaps.

Runtime-source parity covers DATE, TIME(0/3), TIMESTAMP(0/3/6/9) and
TIMESTAMP_LTZ(0/3/6/9), NULL and NOT NULL character inputs, invalid dates, year/range
boundaries, pre-epoch nanoseconds, UTC, Asia/Shanghai and America/Los_Angeles.
Multi-batch tests verify NativeCalc input/output counters as well as values.
Binary TRY_CAST is described separately below.

The initial, pre-optimization release+mimalloc benchmark on Flink 2.2.1/JDK 17, Linux x86_64
(Core i7-12650H), used 2,000,000 row-fed records, NULL every seventh row, two warmups and five alternating
trials. NativeCalc and both row/Arrow transposes are required by the harness. Median
elapsed seconds (ratio = Flink/native):

| Query | Flink | Native island | Ratio |
|---|---:|---:|---:|
| Boolean-text identity | 0.413 | 0.687 | 0.600x |
| Timestamp-text identity | 0.404 | 0.652 | 0.620x |
| TRY_CAST to BOOLEAN | 0.411 | 0.560 | 0.733x |
| TRY_CAST to TIMESTAMP(9) | 3.073 | 3.982 | 0.772x |
| TRY_CAST to TIMESTAMP_LTZ(9) | 3.123 | 4.003 | 0.780x |

These initial standalone results were slower than stock Flink. Cached timestamp writers and
the canonical parser now improve the timestamp cases; see the [current measurements](../optimizations/host-exact-builtins-upcall.md#canonical-timestamp-try_cast).
Boolean TRY_CAST remains slower in these complete-job measurements; its native route preserves
released Flink evaluation and these results do not establish standalone acceleration.
The parser fast path does not establish a speedup for noncanonical formats or composed expressions.

### Binary casts and fixed-width generated expressions

CAST and TRY_CAST from STRING/VARCHAR/CHAR or BINARY/VARBINARY to BINARY/VARBINARY
use a native byte-copy kernel when their operands do not require Flink's rowwise error handling.
Default-mode BINARY(n) zero-pads or truncates
raw bytes to exactly n bytes; VARBINARY(n) only truncates. Character inputs use their
UTF-8 bytes, including embedded zeros and multibyte sequences cut at a byte boundary.
NULLs and declared result types are preserved. Failures in the operand expression still
propagate; CASE and filters suppress unselected failing expressions.

Generated expressions can read fixed-size Arrow binary vectors through the existing
batch callback. Native casts produce their declared fixed-size Arrow columns directly. Binary ELT
uses the default-enabled route described under [raw payload boundaries](#binary-elt-and-raw-payload-boundaries),
with the older complete-job measurements retained as diagnostics. Other fixed BINARY result
projections use the generated row's declared schema, so default-mode results can feed another
native operator, including grouping. Nested ARRAY/ROW callback arguments and results use the
[verified nested scalar transport](#nested-java-scalar-results); custom Java conversion classes
retain their signature gates.

Legacy casts leave the input byte length unchanged even when the SQL result declares
BINARY(n). Their final projections use the existing variable-binary representation, as
Flink 1.18 ENCODE does. Fused scalar consumers and row sinks preserve all bytes; a legacy
fixed-width result crossing another operator boundary or entering a native columnar sink
falls back before execution. The reported reason starts with `legacy binary variable bytes`.

Runtime-source tests cover widths 1/2/4/16, NULLs, zero padding, truncation, non-text bytes,
typed literals, ELT selection, short-circuiting, and 5,003-row inputs. Bridge tests cover
sliced fixed/variable vectors, output ownership after input closure, and allocator balance.
Binary ELT SQL tests require native execution by default and separately verify the
explicit false override, including nested compositions and empty selections.
Separate binary STARTSWITH/ENDSWITH tests still require native execution.

[Low-cardinality grouped-count measurements](../optimizations/scalar-function-kernels.md#binary-keys-composed-with-grouped-counts)
show 19–35% less native time at 2M rows and 22–26% less at 5M for binary-cast and
ELT keys, retaining both transposes. These historical composed measurements are separate
from the Rust-side functionality gate used for current binary ELT default admission.
The admitted binary-cast composed workloads are faster than Flink;
the [synchronous entry optimization](../optimizations/projection-pruning-transpose.md#borrowing-at-the-synchronous-consumer)
also makes the measured non-null standalone STRING-to-BINARY cast faster than both baselines.
Standalone ELT and the sustained BINARY identity control remain slower. This does not establish
a speedup for arbitrary binary projections or high-cardinality grouping.

The original callback-only release+mimalloc measurements on Flink 2.2.1/JDK 17,
Linux x86_64 (Core i7-12650H), use 2,000,000 row-fed records, NULL every seventh row, two warmups and five alternating
trials. The harness requires NativeCalc and both row/Arrow transposes. Median seconds:

| Query | Flink | Native island | Flink/native |
|---|---:|---:|---:|
| String identity (264-byte payload) | 0.825 | 1.194 | 0.691x |
| BINARY(16) identity | 0.323 | 0.536 | 0.603x |
| TRY_CAST string to BINARY(16) | 0.840 | 1.852 | 0.454x |
| Dynamic ELT over BINARY(16) | 0.335 | 0.682 | 0.491x |

Those callback-only standalone cases are slower than stock Flink. The native kernels remove
the callback for the admitted casts; their current measurements and remaining
performance gap are recorded in the [kernel ledger](../optimizations/scalar-function-kernels.md#binary-casts-and-selection).

```bash
SF_BENCHMARK=true mvn -Pbench -pl streamfusion-runtime -am test \
  -Dtest=ScalarFunctionBenchmark -Dsurefire.failIfNoSpecifiedTests=false \
  -Dscalar.functions=TRY_STRING_TO_BOOLEAN,TRY_STRING_TO_TIMESTAMP,TRY_STRING_TO_TIMESTAMP_LTZ \
  -Dscalar.functions=TRY_STRING_TO_FIXED_BINARY,ELT_FIXED_BINARY \
  -Dscalar.rows=2000000 -Dscalar.nullEvery=7 -Dscalar.warmup=2 -Dscalar.runs=5
```

### DECIMAL TRY_CAST

`TRY_CAST` from STRING/VARCHAR/CHAR to DECIMAL uses Flink's generated conversion through the
existing columnar callback. Malformed text and conversion failures become NULL; failures in
operand expressions still propagate. Default and legacy modes follow the configured host rules.
DECIMAL-to-DECIMAL TRY_CAST reuses the native exact rescaling kernel, including HALF_UP and
NULL on narrowing overflow. Tests cover precision 38, invalid text, signs/exponents, whitespace,
filters, conditional consumers, grouping and aggregation across multiple batches.

Already-evaluated internal Flink decimals cross Arrow without another precision check. Flink's
compact text parser can retain a rounding carry (`999.995` to DECIMAL(5,2) yields `1000.00`);
the bridge preserves that internal unscaled value and its non-NULL status. External BigDecimal
UDF returns still undergo the declared precision/scale conversion. This follows Comet's generated
compact-decimal output pattern; the host-specific contract is recorded in
[exact decimal results](https://github.com/datafusion-contrib/StreamFusion/blob/main/divergences/38-exact-decimal-result-types.md).

### The host-exact JVM upcall

A second group of casts is **native by default, and this is not a fallback** — it's a real JNI call
back into Flink's own cast machinery (`CastExecutor`/`CastRuleProvider`) for the one column being
cast, with the rest of the expression tree still evaluated natively around it:

- **FLOAT/DOUBLE/DECIMAL ↔ string, both directions** — including bounded VARCHAR/CHAR targets.
- **Narrowing a `VARCHAR`** (truncation).
- **String to `CHAR(n)`** (space-padding).
- **`→ DECIMAL` from a `float`/`double`.**

These four are deliberately routed through the host rather than reimplemented, because the host's
output isn't just "a reasonable float-to-string conversion" — it's a specific, JDK-version-dependent
rendering (trailing-zero handling, the scientific-notation threshold, trim semantics), and an
unparsable string input must fail the job exactly the way the host's default cast does. Running the
upcall makes the result byte-identical to Flink by construction instead of by reimplementation, at
the cost of one JNI round-trip per cast column rather than per row family. (The Kafka text sinks
already carry a probed native port of the legacy `Double.toString` spelling; moving the
float-to-string `CAST` onto that port instead of the upcall is a separate, tracked follow-up.)

With `table.exec.legacy-cast-behaviour=ENABLED`, the verified basic pairs remain native:
STRING/CHAR/VARCHAR to DECIMAL; BOOLEAN or DECIMAL to CHAR/VARCHAR; and character to
character. STRING to DECIMAL uses Flink's complete generated expression, including
its catch-on-conversion-failure contract: malformed strings and overflow become NULL.
Setting only the cast executor's legacy flag would still throw for malformed strings.
BOOLEAN uses the host's legacy lowercase spelling; DECIMAL rendering uses the configured
host executor. Legacy character casts preserve the original text without applying default
projection trimming or padding, including empty, overlength and surrogate-containing
strings. Sink length and NOT NULL enforcement remain separate downstream contracts.
Legacy FLOAT/DOUBLE to DECIMAL is admitted by default. A direct unary cast first tries a native
batch kernel: adjacent binary floating values, multiplied by an exact power of ten with outward
rounding, bound every round-tripping decimal representation the running JDK could choose. Only
when both bounds round to the same HALF_UP integer does the kernel use that result, with the
declared precision check. Scale above 18, scaled magnitude at least 2^52, ambiguous rounding,
NaN and infinity retain the complete released Flink expression for the entire batch. Numeric
arguments then borrow Arrow rows and decimal results retain internal DecimalData. The host widens
FLOAT to DOUBLE before decimal conversion, rounds with the target scale, returns NULL on precision
overflow, and raises NumberFormatException for NaN/infinity in both modes. Selection and empty
filtered batches suppress conversions for rows that do not reach the projection. The conversion
uses the JVM inside native Calc whenever the batch cannot use the proven native kernel.
CHAR to DOUBLE is also admitted by default in legacy mode, including NULL on malformed
text. Whitespace, padding, exponents, signed zero, NaN/infinity and over/underflow retain the running
host's parser behavior. The complete mixed DECIMAL ROUND projection remains native, including its
CHAR(8) sort-key conversion. Mixed projections use the existing whole-row JVM schedule when
needed for Flink failure order; a runtime DECIMAL-only control verifies ROUND independently.
VARCHAR-to-floating and CHAR-to-FLOAT legacy casts remain unverified
and fall back. An unknown cast setting also falls back.

Direct legacy CHAR-to-DOUBLE casts parse ordinary ASCII decimal text in native batches using
the same correctly-rounded binary64 conversion as Java. Hexadecimal text, numeric suffixes,
special values, Unicode and malformed input retain the complete released Flink expression for
the batch, including legacy NULL-on-failure behavior. Composed operands are never mistaken for
direct reads. Multiple fallible expressions still use the whole-row JVM schedule to preserve
error order. These paths require no additional setting and preserve
`table.exec.legacy-cast-behaviour`. See the release measurements
for [floating DECIMAL](../benchmarks/basic-type-coverage.md#legacy-floating-decimal-casts-2026-10-16)
and [CHAR to DOUBLE](../benchmarks/basic-type-coverage.md#legacy-char-to-double-casts-2026-10-16).

Generated cast executors initialize with a valid value of the declared input type, including
`CHAR`/`VARCHAR NOT NULL`. Startup does not evaluate an illegal NULL input; actual malformed
runtime values still fail according to Flink's cast rules.

### Still falling back

Unverified legacy host-cast pairs and other pairs not listed above. Temporal casts use Flink-generated
expressions; see [temporal functions](temporal-functions.md).

`FlinkLegacyBasicCastSqlHarnessTest` compares both cast settings against independently
executed stock Flink. It covers runtime NULLs, malformed and overflowing strings,
rounding, CASE/filter consumers, character lengths/Unicode, NOT NULL with DROP, and
two successive recoveries with HashMap and native RocksDB. The complete-job release
measurements and their limits are recorded in
[basic type coverage measurements](../benchmarks/basic-type-coverage.md).

## Decimal arithmetic

### Exact unary and integral functions

Integer and DECIMAL unary minus, ABS and SIGN execute Flink's generated expression code through
the existing batch JVM bridge. Integral FLOOR/CEIL and INT/BIGINT TRUNCATE use the same path,
including per-row TRUNCATE positions. TINYINT/SMALLINT TRUNCATE retains the host compilation
failure described under exact floating math below. This retains resolved widths, decimal precision/scale, NULL handling,
and Java overflow behavior, including ABS of the minimum INT/BIGINT value. Adjacent supported
host expressions fuse before crossing the Arrow boundary. These are host-evaluated functions
inside a columnar native Calc, not pure-Rust kernels.

DECIMAL FLOOR/CEIL/CEILING run inside verified scalar consumers using one fused generated
expression, so the intermediate decimal does not cross Arrow before its consumer executes.
This covers STRING and integral casts, predicates, nested selections, and DECIMAL results whose
precision is normalized by a non-identity cast, exact arithmetic or Flink's IF branch casts.
Precision 12/18/19/20/38, positive/negative/exact/NULL values, large precision-38 values,
selected and unselected failures, empty filtered batches and native work over 5,003 rows
are covered by the runtime regressions.

Direct rounded DECIMAL results and decimal-preserving consumers such as ABS or COALESCE retain
fallback: Flink's FLOOR/CEIL return a decimal with value-dependent precision, which can fail the
released host's binary-writer precision assertion. Normalizing it at an Arrow boundary would hide
that host behavior. The same gate applies to whole-Calc generated execution, including JSON
compositions. Collection and ROW consumers of DECIMAL FLOOR/CEIL remain outside the verified
scalar boundary. Existing floating-point gates and decimal TRUNCATE/ROUND kernels are unchanged.

Release+mimalloc measurements on Linux x86-64 (Intel i7-12650H, JDK 17, Flink 2.2.1),
with two million runtime rows, NULL every seventh row, parallelism 1, two warmups and
five interleaved measurements per engine, include the row source, blackhole sink and
both asserted transposes:

| DECIMAL(38,9) workload | Flink median (s) | Native median (s) | Flink/native |
| --- | ---: | ---: | ---: |
| Identity control | 0.276592 | 0.578956 | 0.478x |
| `CAST(FLOOR(n) AS STRING)` | 0.600918 | 1.028703 | 0.584x |
| `CAST(CEIL(n) AS STRING)` | 0.621295 | 1.035432 | 0.600x |

These standalone projections are slower than Flink. The extension retains columnar pipeline
composition using the existing JVM evaluator; no standalone speedup is claimed. Reproduce with
`ScalarFunctionBenchmark`, `SF_BENCHMARK=true`, `-Pbench`,
`-Dscalar.functions=DECIMAL_FLOOR_STRING,DECIMAL_CEIL_STRING`, `-Dscalar.rows=2000000`,
`-Dscalar.nullEvery=7`, `-Dscalar.warmup=2`, and `-Dscalar.runs=5`.

### Decimal ROUND, TRUNCATE and literals

`ROUND(decimal_column[, literal_integer_scale])` runs with compatibility overrides disabled.
It rounds ties away from zero (HALF_UP), preserves NULLs and Flink's inferred result precision,
scale and nullability, and returns NULL when rounding exceeds the result precision. Negative
positions round the integral part. A position at or above the source scale preserves its value
and scale rather than appending zeroes. A NULL position returns a typed NULL.

Positions from -38 upward use a prepared native decimal kernel. More negative literal positions
use Flink's own decimal rounding through the existing columnar JVM upcall, so extreme BigDecimal
scale/range exceptions match the host instead of being silently clamped to zero. These calls
remain inside native Calc, but fall back when nested under AND/OR to preserve row short-circuiting.
CASE can skip an unselected failing branch. BIGINT scale arguments retain an explicit
planner fallback; float/double ROUND uses exact generated evaluation by default, with the existing approximate Rust opt-in.

Runtime INT scales for ROUND and TRUNCATE run inside native Calc when a verified scalar
consumer encloses the rounding. A standalone STRING cast on direct decimal and INT columns
uses native batch rounding and per-row decimal formatting; a batch with non-NULL positions
below -38 uses the existing Flink evaluator. Other complete expressions use Flink-generated
evaluation, so integer casts, comparisons, predicates, nested rounding, CASE/COALESCE and
explicit decimal normalization see the original runtime precision and scale. Arithmetic,
nonidentity DECIMAL casts and IF can normalize the result before it crosses Arrow.
Negative/NULL scales, scale expansion, precision 38, input errors and AND/OR/CASE
short-circuiting retain released Flink behavior. Positions at or above the input scale
leave the input unchanged; extreme negative positions can throw BigDecimal's scale errors.

Direct runtime-scale DECIMAL outputs, decimal-preserving selections/sign operations, and
complex outputs remain on Flink. Its serializer can reinterpret the internal unscaled
integer using the declared scale: for DECIMAL(20,3) `123.456` and scale 2, direct ROUND and
TRUNCATE collect as `12.346` and `12.345`, while their STRING consumers yield `123.46` and
`123.45`. Scale 1 changes the internal precision and triggers the host binary-writer
assertion with assertions enabled. Tests preserve both behaviors through explicit fallback,
including when a JSON expression would otherwise select complete-row generation.

A release+mimalloc comparison of the direct STRING consumers on Flink 2.2.1/JDK 17,
with two million row-fed records, measured native ROUND at 0.634 s versus Flink's 0.690 s,
and native TRUNCATE at 0.635 s versus 0.694 s. At five million rows, ROUND measured
1.458 s versus 1.605 s and TRUNCATE 1.463 s versus 1.576 s. Both transposes and the row sink
remain in these measurements. These results apply to the direct decimal/INT column shape; composed
expressions retain generated evaluation. See the [method, variability and prior implementation
comparison](../optimizations/udf-columnar-upcall.md#decimal-runtime-scale-consumers).

`TRUNCATE(decimal_column[, literal_integer_scale])` uses the same native fixed-width
kernel with rounding toward zero. Positive, zero and negative positions preserve Flink's
resolved precision/scale and NULL behavior; a position at or above the source scale retains
the input value. Positions below -38 use Flink's generated expression through the columnar
callback, including its extreme-scale errors. They retain the same AND/OR short-circuit
restriction as ROUND. Runtime INT scales use the generated scalar-consumer path above. A standalone STRING cast of ROUND/TRUNCATE on direct decimal and INT columns uses native batch rounding and formatting, with released-function fallback for batches containing extreme negative positions. Other compositions retain generated evaluation. The direct STRING consumer measurements above include the full native island.
Integer TRUNCATE inputs use the generated path described under exact numeric functions;
floating-point inputs use the exact generated path below. SQL tests cover
precision 38, multiple batches, filters, CASE, COALESCE and aggregate consumers, including
projections that combine and nest truncation with DECIMAL-to-FLOAT/DOUBLE casts.

Decimal planner literals are rescaled HALF_UP to their declared scale before encoding their
unscaled integer. A precision overflow becomes a typed decimal NULL. This handles constant-folded
casts whose stored value retains more fractional digits than its resolved DECIMAL type.

### Arithmetic operators

**Native and byte-exact by default, with the boolean short-circuit restriction below.**

- `+`/`-`/`*` whose result type is `DECIMAL` (e.g. Nexmark q1's `0.908 * price`) use fused native
  kernels with Flink's resolved result precision and scale. Operands stay `Decimal128` or signed
  integers; intermediates use checked i128 arithmetic and widen to i256 when needed. The result
  is rounded `HALF_UP` before checking precision and writing one `Decimal128` column. Overflow
  produces SQL `NULL`, including in `IS NULL`, CASE, filters, and group keys. A wide intermediate
  does not discard a result that fits after rounding.
- **Division** (`/`) uses Flink's decimal rounding rather than Arrow's: the quotient is
  computed to 38 *significant* digits with `HALF_UP` rounding (matching `BigDecimal`'s
  `MathContext(38, HALF_UP)`), then rescaled to the declared `DECIMAL(p, s)` with `HALF_UP` again —
  producing `NULL` when the result would exceed `p` digits, and failing the job on division by zero,
  all exactly as the host does.
- **Modulo** (`MOD`, `%`) computes the exact signed remainder with Flink's `MathContext(38)`
  constraint on the integral quotient. A quotient that needs more than 38 significant digits
  after removing trailing zeroes fails the job with `Division impossible`; for example,
  `DECIMAL(38,0)` value `10^37` modulo `DECIMAL(38,38)` value `3 * 10^-38`. A large quotient
  consisting of a power of ten remains valid. The remainder is rescaled `HALF_UP`, with NULL on
  result-precision overflow and job failure on a zero divisor.

Decimal `/`, `MOD` and `%` nested under `AND` or `OR` fall back at planning time.
DataFusion 54 can evaluate an operand for rows Flink skips in a mixed boolean batch,
exposing division-by-zero or `Division impossible` errors on unevaluated rows.
For example, `guard_value OR MOD(a, b) = 0` must skip MOD on rows whose guard is TRUE.
This restriction applies to projections and filters, including nested expressions.
Direct arithmetic and CASE result branches remain native; CASE selects the rows to
evaluate before running the decimal kernel. Evaluated invalid operands still fail the job.

Flink can retain a `NOT NULL` result declaration from non-nullable operands even
when decimal arithmetic overflows. The physical Arrow result of a generated Calc permits
those runtime NULLs while the SQL result type stays unchanged. Flink's downstream constraint
enforcer remains responsible for its configured policy: ERROR fails the job and DROP discards
only the violating row. Arrow validation does not preempt that policy, including in a Calc
whose multiple divisions select complete-row evaluation.

The old `decimalArithmetic.approximate` flag is retired entirely: the float/double→`DECIMAL` cast it
used to gate now runs host-exact through the cast upcall above.

## String and calendar functions

The following list describes admission and fallback. Per-function Flink/native timings and
workload details are on the [scalar function benchmark page](../benchmarks/scalar-functions.md).

Admission requires verified semantics; it does not promise a speedup for every isolated query.
Retained coverage with slower standalone results is a precursor to the concrete optimizations
in this PR: scalar parameter reuse, direct output construction, primitive extrema, and shared
text scans. Cheap expressions can also remain inside a larger native Calc without another
host boundary. This is not a measured whole-query speedup claim. STARTSWITH/ENDSWITH remain
native for this composition benefit; their standalone regressions are documented in the
benchmark results and do not disable otherwise verified expressions.

### STARTSWITH

Binary overloads of STARTSWITH and ENDSWITH use released Flink code through the batch JVM
bridge. They compare bytes directly, retaining empty-prefix/suffix and NULL behavior, and never
decode binary inputs as text. Literal and runtime binary operands are supported within the
generated expression.

Two character arguments, literal or column. Matches a literal prefix, including Unicode and
empty strings; any NULL argument returns NULL. Wildcard characters have no special meaning.

### ENDSWITH

Two character arguments with the same NULL and type rules as STARTSWITH. Matches a literal
suffix; an empty suffix matches every non-NULL string. Wildcards have no special meaning.

### INSTR

`INSTR(string, needle[, start[, occurrence]])` supports two character strings and optional
TINYINT/SMALLINT/INT start and occurrence arguments, including runtime columns. Positions
are 1-based Unicode codepoints; missing matches return zero. A positive start searches
forward, a negative start searches backward from the end, and zero returns zero. Matches
can overlap. Defaults are start 1 and occurrence 1.

Any NULL argument returns NULL before validating start/occurrence. A non-positive occurrence
fails the query, even when start is zero. `INT_MIN` start also fails: its negation overflows
in Flink's recursive reverse search; native reports the failure without recursive stack
exhaustion. For an empty needle and positive occurrence, positive start returns 1 and negative
start returns the string's codepoint length plus 1, even for starts outside the string.
Zero start still returns zero.

Two-argument calls continue using DataFusion's Unicode position kernel. Extended forms use
forward/reverse byte search at codepoint boundaries, preserving overlapping matches without
allocating reversed strings. Built-in admission uses the resolved SQL operator; user functions
named INSTR retain their own behavior. BIGINT start/occurrence arguments remain unsupported.
Extended calls under AND/OR stay native only when a literal start excludes `INT_MIN` and a
literal/default occurrence is positive; otherwise Flink retains row short-circuiting for errors.

### LOCATE

Both LOCATE(needle, s) and LOCATE(needle, s, start) are native. Character inputs and TINYINT/SMALLINT/INTEGER starts are admitted; BIGINT starts fall back without narrowing. Positions count Unicode codepoints. Empty needles return 1 for every non-NULL start. Zero/negative starts search from the beginning, except INTEGER minimum: start - 1 wraps to maximum, matching Flink. Out-of-range starts return zero; any NULL argument returns NULL.

### ASCII

Character input returns its first UTF-8 byte widened as a signed Java byte, not a Unicode
code point. Empty strings return zero and NULL propagates. For example, `ASCII('é') = -61`.
The native kernel preserves these rules in projections, predicates and group keys.

### CHR

Integer inputs use Flink's low-byte rule: negative values return an empty string;
non-negative values produce the character at `value & 255`, including the NUL character
when the low byte is zero. NULL propagates. All four signed integer widths are native;
`CHR(353)` returns `a`, rather than the character at Unicode code point 353.

### BIN

TINYINT, SMALLINT, INTEGER, and BIGINT inputs are admitted. Returns binary digits without leading zeros; zero is `0`. Negative values have 64 two's-complement digits even for narrow input types. NULL returns NULL. Folded string NULL literals retain their declared type.

### HEX

Integer and character inputs are admitted. All four signed integer widths preserve Long.toHexString behavior: no leading zeros, uppercase digits, and 16 digits for negative values. Character strings encode their UTF-8 bytes as uppercase hex. NULL returns NULL.

### TO_BASE64

Character strings and binary columns are encoded as padded RFC 4648 Base64 without line wrapping.
Strings use their UTF-8 bytes; binary inputs preserve every byte, including invalid UTF-8.
Both overloads share the direct-output encoder. Empty input stays empty and NULL propagates.
VARBINARY and fixed-size BINARY literals are native, including typed NULLs.

`FROM_BASE64` accepts character and binary inputs through Flink-generated evaluation. Invalid
encoding raises the same host exception; empty input and NULL retain their host results. Decoded
STRING values may contain arbitrary bytes. Final scalar STRING projections therefore travel as
Arrow Binary and are read as Flink StringData without UTF-8 normalization. Consumers such as
comparison, length, casts and re-encoding fuse in Flink before Arrow export. Sensitive STRING
results crossing another relational operator, feeding a native columnar sink, or contained in
whole-Calc complex outputs retain fallback rather than exposing invalid Arrow Utf8. Row sinks
can consume the final decoded bytes through the Arrow-to-RowData transpose.

`IS_DECIMAL`, `IS_DIGIT` and `IS_ALPHA` use the same generated evaluator, preserving Flink syntax
and Unicode classification, including non-nullable FALSE for NULL and empty input.

### UNHEX

Character inputs produce BYTES. Either hex letter case is accepted; invalid bytes, whitespace, `0x` prefixes, and non-ASCII digits return NULL. Empty input produces empty bytes. Flink validates but discards an odd leading digit, emitting zero: `UNHEX('A') = 00`, `UNHEX('ABC') = 00 BC`. Folded binary constants carry bytes directly as typed binary literals, including fixed BINARY values and typed NULLs.

### GREATEST

Integers, BOOLEAN and matching-precision/scale DECIMAL retain their Rust kernels and strict NULL
propagation. Character results, mixed exact numerics, TIMESTAMP and TIMESTAMP_LTZ use
Flink-generated expressions through the batch JVM bridge, preserving coercions and nanoseconds.
Runtime strings use that bridge when the Calc reads an external DataStream whose conversion
produces Java-backed strings. Unknown or binary-backed input representations retain fallback:
Flink can use UTF-16 ordering before serialization and byte ordering afterward. Floating-point
extrema retain fallback.

### LEAST

Uses the same kernels, generated-expression paths and string-representation gates as GREATEST,
with strict NULL propagation and minimum comparison.

### INITCAP

Character strings only. Only ASCII letters and digits form words; every other character separates words and is preserved. NULL returns NULL. This follows Flink rather than DataFusion word boundaries.

### TRANSLATE

Three character arguments. Mappings use Unicode codepoints, not graphemes. The first duplicate mapping wins, but duplicates consume target positions. Missing target characters delete; a NULL target acts as empty. NULL/empty `from` leaves the source unchanged. A NULL source returns NULL.

### BTRIM

One-argument space trimming and two-argument character-set trimming with a literal set retain
their Rust kernels. Empty sets preserve the input and NULL propagates. Per-row sets use Flink's
generated evaluator when the Calc reads a proven external Java-string conversion. Unknown or
binary-backed representations retain fallback because Flink can change set behavior after an
exchange when the first set character is a space.

### TRIM

The SQL `TRIM([BOTH | LEADING | TRAILING] [characters] FROM s)` forms are native with
a literal trim set, including Unicode, empty and NULL sets. The default set is the ASCII
space, not all whitespace. These forms reuse BTRIM/LTRIM/RTRIM's character-set kernels.
Column trim sets fall back for the same Flink representation-dependent behavior as BTRIM.

### ELT

The binary-result overload uses a native columnar selector. Dynamic INT indices retain
one-based selection, out-of-range NULLs and NULL operands. Multiple binary result columns retain
independent byte arrays across rows and batches. Fixed-length BINARY arguments and results
are admitted alongside BYTES; embedded zero/high bytes remain binary. Expressions requiring
Flink's rowwise exception or shared-function ordering retain the generated callback.

An INTEGER index and character alternatives are admitted. The index is 1-based; out-of-range and NULL indices return NULL. Only the selected alternative's NULL matters. Other index types fall back: Flink casts its boxed index to Integer after its bounds check. Explicit casts to INTEGER follow the existing cast rules.

Binary ELT runs by default, including fixed-declaration inputs whose raw payload lengths
differ from their SQL widths. Explicit false overrides retain tested stock execution; the
unverified BIGINT-index overload keeps its fallback check.

### URL_ENCODE

Character strings use Java form encoding: space becomes `+`, ASCII alphanumerics and `-_. *` are preserved apart from space, and other UTF-8 bytes use uppercase percent escapes. NULL returns NULL.

### OVERLAY

Character strings and integer positions, widened to BIGINT without losing bits. Preserves Java UTF-16 positions, length narrowing/overflow, and substring errors. Non-positive or beyond-end starts return the source; zero/negative lengths omit the suffix. Split surrogate pairs encode as `?`, like Flink. Any NULL argument returns NULL.

### PARSE_URL

The two- and three-argument overloads use Flink's generated evaluator through the batch JVM
bridge, including runtime URL parts and query keys. Java URL component spelling, raw percent
escapes, duplicate query keys, absent components and NULL/invalid-input behavior remain Flink's.
No Rust URL normalization or query decoding is substituted.

### PRINTF

PRINTF uses Flink's generated formatter through the batch JVM bridge for supported scalar
arguments, including STRING, integral and DECIMAL values and per-row formats. Argument indices,
width, precision, locale, NULLs and invalid-format results follow the selected Flink runtime.
Because character formatting can produce isolated UTF-16 surrogates, consumers fuse with PRINTF
before Arrow conversion; a sensitive string crossing another operator retains the existing
representation-protection fallback. Direct final projections are supported.

### URL_DECODE

One character argument is native. Form decoding preserves JDK UTF-8 replacement grouping and returns NULL for malformed escapes. The planner selects the runtime JDK rule: JDK 17/21 (and pre-25 runtimes) use Integer.parseInt, accepting signed one-digit escapes and BMP Unicode hex digits; JDK 25+ uses ASCII-only HexFormat rules.

The JDK rule is selected on the JobManager during planning, so the JobManager and TaskManagers must use the same URL-decoding rule (pre-25 or 25+); mixed JDK groups can produce results that differ from Flink on the TaskManager.

### ENCODE

Character input and a literal UTF-8, US-ASCII, ISO-8859-1, UTF-16, UTF-16BE, or UTF-16LE
charset (including JDK aliases). Returns BYTES, preserves NULL, and replaces unmappable
characters with `?` in ASCII/Latin-1. UTF-16 emits a big-endian BOM for non-empty strings;
UTF-16BE/LE emit no BOM. Empty strings produce empty bytes in all six charsets.
On Flink 2.2, other literal charsets fall back. A runtime character expression for the charset runs through
Flink's generated evaluator in the native Calc's batch JVM bridge. It uses the TaskManager
JDK's charset implementations and aliases, including non-UTF charsets, without a Rust
charset-name approximation. The complete Calc filter and projection list are generated together
to preserve Flink's row and projection evaluation order, including the first charset error when
several expressions or rows would fail. On Flink 2.2, Calcs using only the six verified literal
charsets retain their Rust kernels. NULL arguments return NULL; illegal or unsupported runtime names
raise Flink's `UnsupportedEncodingException` only when the expression is evaluated. Flink 1.18
declares direct ENCODE results as BINARY(1); its generated Calc carries the complete bytes as
Arrow Binary at final projections (see [legacy representation](#legacy-encode-result-representation)).

### DECODE

Binary input and the same six literal charsets as ENCODE. UTF-8 uses the JDK's replacement
grouping for malformed sequences; ASCII replaces each non-ASCII byte; Latin-1 maps all bytes.
UTF-16 detects and consumes an initial BOM, defaulting to big-endian without one. UTF-16BE/LE
use their fixed byte order and retain the BOM as a character. Malformed surrogate pairs and
odd trailing bytes follow JDK UnicodeDecoder grouping, including consuming a high surrogate
and a following non-low code unit together. The planner probes the runtime UTF-16 decoder and
encodes its U+FFFE rule: JDK 11 replaces this code unit with U+FFFD, while JDK 17 preserves it.
This does not change initial BOM detection or fixed-endian U+FEFF preservation. JobManagers
and TaskManagers must use the same decoding rules. NULL stays NULL. Other literal charsets fall back.

Runtime charset expressions use the same generated evaluator as ENCODE, preserving JDK
replacement grouping, aliases and runtime errors. CASE branches, NULL arguments and rows removed
by a filter retain Flink's evaluation behavior. Charsets such as CESU-8 can produce isolated
UTF-16 surrogates: comparisons, LIKE and other consumers remain fused with DECODE before Arrow
conversion. A potentially sensitive string crossing another operator retains the existing
string-identity fallback; direct final projections and consumers within the Calc are supported.

`FlinkDynamicCharsetSqlHarnessTest` checks released Flink 2.2.1 and 1.18.1 with runtime sources:
standard and extended JDK charsets, aliases, NULLs, malformed sequences, CESU-8 surrogates,
filtered and empty batches, untaken CASE branches, and invalid-name exception order.

A local ARM64/JDK 17 diagnostic on Flink 2.2.1 (2026-09-24) used the release native build with
mimalloc, 2,000,000 rows, 264-byte Unicode text, NULLs every seventh row, and per-row UTF-8,
UTF-16LE and windows-1252 names. One warmup precedes three interleaved measured trials; the
runtime source, row sink and both row/Arrow transposes remain in the measured path.

| Runtime expression | Flink median (s) | Native Calc median (s) | Flink/native |
|---|---|---|---|
| ENCODE | 1.438 | 2.821 | 0.510x |
| DECODE | 1.212 | 2.013 | 0.602x |

Both standalone expressions are slower through the batch JVM bridge. This path adds semantic
coverage and keeps an otherwise supported pipeline inside a native island; it is not a charset
conversion speedup. The six literal-charset Rust kernels remain available.

Generated expressions now bypass per-row reflection while invoking the same Flink evaluator.
The [bridge optimization measurements](../optimizations/udf-columnar-upcall.md#direct-generated-expression-dispatch)
show a larger isolated dispatch benefit, a modest dynamic ENCODE whole-job improvement and no
material DECODE improvement. These expressions still execute on the JVM, with unchanged charset,
surrogate, NULL and exception behavior; native Calc admission is not a Rust charset implementation.

```sh
SF_BENCHMARK=true mvn test -Pbench -pl streamfusion-runtime -am \
  '-Dtest=ScalarFunctionBenchmark#individualFunctions' -Dsurefire.failIfNoSpecifiedTests=false \
  -Dscalar.functions=ENCODE_DYNAMIC,DECODE_DYNAMIC -Dscalar.warmup=1 -Dscalar.runs=3 \
  -Dscalar.unicode=true -Dscalar.nullEvery=7
```

### SQL/JSON evaluation

SQL/JSON first tries the verified native expressions below. When a Calc containing JSON
cannot be encoded through that route, StreamFusion generates its complete filter and
projection list with released Flink code through the batch JVM bridge. Native fast paths
remain unchanged; new grammar does not require another Rust parser extension. EXPLAIN
marks the generated route with `jsonEvaluation=[JVM]`. A native Calc marker identifies the
columnar operator, not the language evaluating its JSON expressions.

The generated route covers JSON_QUERY, dynamic JSON_VALUE/JSON_QUERY paths, recursive
descent, filters, slices, multi-selectors, path functions, and invalid paths handled by
Flink's policies. It also admits additional JSON_STRING/JSON_OBJECT scalar types and nested
constructors, dynamic defaults, nullable-boolean consumers, and short-circuited error or
typed-conversion expressions that the native encoder declines. JSON_VALUE with `ERROR ON EMPTY`
or `ERROR ON ERROR` always uses this generated route, including direct projections, so its
exception class, message and policy precedence match Flink. A mixed Calc moves together
to preserve Flink's row order, shared UDF instances, filter-before-projection behavior and
Jackson buffer history. There is one JSON JVM callback per Arrow batch per Calc; row
iteration happens inside that callback. Rejected rows and changelog tags share a filter mask
before Arrow validates nonnullable result fields.

Referenced inputs and projected outputs can carry character, binary, boolean, numeric/decimal,
date/time, interval, supported timestamp types, and recursively nested ARRAY/ROW/MAP/MULTISET
values with those leaves. The broader MAP/MULTISET boundary requires non-null keys/elements,
matching the Arrow map key representation, with the existing character MAP exception below. Nullable map values, outer containers and nested array/row values
remain supported. Nested arguments use Flink internal views over the imported Arrow batch; generated
results are copied into owned Arrow output vectors before that batch closes. The callback still
crosses JNI once per batch, including multi-column results and filtering.
The existing character-key/character-value MAP boundary remains admitted. Other MAP
keys and MULTISET elements must be declared non-null; nullable forms retain fallback,
including inside ARRAY/ROW. Nested fixed BINARY leaves retain their existing restriction.
Programs containing admitted collection functions or runtime constructors can instead use the
broader [generated collection route](#collection-expressions), including `ROW(m)` with a typed MAP.
Its representation and boundary gates apply to the complete program.
Constructing containers internally is allowed when the resulting boundary types are
admitted. Unsupported host code generation
and UDF signatures retain explicit fallback. Flink 2.2.1 rejects dynamic JSON_EXISTS paths;
that host failure is preserved. No configuration opt-in is required.

`JSON_EXISTS(... UNKNOWN ON ERROR) IS TRUE` retains Flink's boxed-null unboxing failure.
Failure-parity tests require the exception class, row-evaluation phase, native route and no output.
They capture JVM-generated NPE messages before the next execution and validate `booleanValue`
when details are present; JDK version alone does not guarantee helpful NPE diagnostics. The same
SQL and stable assertions are exercised with helpful messages disabled, without a fallback or skip.

The JSON string identity gate below still applies, including to JSON_QUERY. Non-Calc
expression contexts, such as residual join predicates, retain their existing narrower
native/fused-expression admission. JSON connector decoding is unaffected. This is general
coverage, not a JSON speed optimization: the [release comparison](../benchmarks/scalar-functions.md#sqljson-jvm-bridge-prototype-2026-09-18)
rejected replacing the measured native fast paths wholesale. The [nested-boundary comparison](../benchmarks/scalar-functions.md#nested-sqljson-batch-boundaries-2026-09-19)
also measured slower row-fed execution (0.85× array serialization and 0.54× a nested result).
The following function sections
describe those retained fast paths and call out when the generated Calc extends them.

### JSON_QUERY

JSON_QUERY uses the generated Calc with literal or dynamic paths, conditional/unconditional
array wrappers and EMPTY/ERROR policies. Its output has the same intermediate STRING
identity restriction as JSON_VALUE.

### JSON_QUOTE

Character input, including NULL. Matches Flink 2.2.1's actual spelling: slash is escaped, non-ASCII values use lowercase Unicode escapes, and supplementary characters emit a full code-point escape followed by a low-surrogate escape. Unlisted ASCII controls are retained.

### JSON_UNQUOTE

One character argument is native. Valid quoted values are unescaped with Flink/Jackson first-token validation; invalid input is preserved and NULL propagates. A truncated Unicode escape after a valid first token fails the job, matching Flink 2.2.1's uncaught bounds exception. A truncated escape inside the first token is invalid JSON and is preserved.

Scalar consumers use the fused JVM expression path, and strings passed to another operator
fall back under the [JSON_VALUE identity restrictions](#json_value).

### JSON_STRING

One character, BOOLEAN, TINYINT, SMALLINT, INTEGER, BIGINT, or DECIMAL scalar is native by default.
SQL NULL returns SQL NULL; other scalars serialize to JSON text. Strings use Jackson's
escaping: quote/backslash and ASCII controls are escaped, other controls use uppercase
`\u00XX`, and slashes and Unicode remain unescaped. Integer widths retain their exact
decimal spelling. Output is written directly into the Arrow string builder.

DECIMAL retains its declared scale and trailing zeros, using Jackson's BigDecimal spelling:
`1.2300` remains `1.2300`, while sufficiently small values use scientific notation
(`0.000000100` becomes `1.00E-7`). Precision and scale through 38 are native, including NULLs.

Additional scalar inputs and direct nested JSON_OBJECT, JSON_ARRAY and JSON(value) calls
use the generated Calc, preserving Flink's raw-JSON handling. Collection input columns still
fall back at the scalar bridge boundary. JSON_STRING applied to an ordinary string column
containing JSON text quotes it normally. No compatibility opt-in is needed.

NULL values must have a supported scalar type, for example `CAST(NULL AS STRING)`.
An operand whose type remains SQL NULL falls back: Flink's JSON node generator cannot
serialize that type. The same typed-NULL rule applies to JSON_OBJECT values.

### JSON_OBJECT

Literal, non-null character keys with character, BOOLEAN, TINYINT, SMALLINT, INTEGER,
BIGINT, or DECIMAL scalar values are native. DECIMAL uses the same scale-preserving
formatting as JSON_STRING, including scientific notation. Keys must contain well-formed
Unicode. The default NULL ON NULL writes JSON null values; ABSENT ON NULL skips them. Duplicate keys retain
the last inserted value, so an absent NULL does not overwrite an earlier non-null value.
Objects with no surviving entries produce `{}`, never SQL NULL.

Keys are sorted in Java UTF-16 order, matching Flink's Jackson serializer even when BMP
and supplementary characters mix. Keys and values use the same escaping as JSON_STRING.
Each batch reuses escaped keys and scalar parameters while writing directly to Arrow.

Dynamic/NULL keys, additional scalar value types, and direct nested JSON_OBJECT, JSON_ARRAY
or JSON(value) expressions use the generated Calc, including the host's failures. An ordinary string containing JSON text is quoted.
No compatibility opt-in is needed.

### IS JSON

`s IS JSON [VALUE | OBJECT | ARRAY | SCALAR]` and their `IS NOT JSON` forms are native
for character input. Omitting the type means VALUE. Results are non-nullable: SQL NULL
and invalid JSON return FALSE (TRUE for the negated form). The JSON literal `null` is
a valid VALUE and SCALAR, but is neither an OBJECT nor an ARRAY.

Parsing matches Flink 2.2.1's Jackson first-document validation, including trailing
content, token boundaries, escaped surrogates, and limits in every nested field.
It shares the streaming/SIMD reader and JDK profiles described below; no compatibility
opt-in is needed. This validates the document directly, without applying JSON path policies.

### JSON_VALUE

The direct Rust kernel is enabled by default for the following verified shapes; no compatibility
opt-in is needed. `ERROR ON EMPTY` and `ERROR ON ERROR` use the generated Calc so failures retain
Flink's exact exception class and message. Outside a Calc, these throwing policies stay on the host.
The fused JVM consumer path below retains Flink's own selector and policy rules.

Character input with a non-null literal path is native for the following selectors.
Definite paths support `$`,
dot members such as `$.user.name`, bracket members such as `$['user name']`, and signed
32-bit array indexes such as `$.users[0].name` and `$.users[-1].name`. Negative indexes count
from the array end: `-1` selects the last element, while `-0` is index zero. Leading zeros
are accepted, and an index beyond either end follows the normal missing-path policies.
Dot names also accept numeric names, punctuation and well-formed Unicode, including literal
controls: `$.order-id.123` and `$.a*b` select literal object keys. A dot name ends at `.` or `[`,
and `(` starts function syntax, which remains on Flink. ASCII spaces are invalid within the name,
and a leading `*` is a wildcard selector rather than a literal member.
Backslashes in dot names are literal: `$.a\u0061` selects the key `a\u0061`, without decoding
the escape. The planner canonically quotes these names for the existing native reader.
Bracket names use single or double quotes and
accept well-formed Unicode, spaces and punctuation, including the other quote character.
Empty bracket names (`$['']` and `$[""]`) select the empty object key, including in nested
member/index paths. They remain distinct from a name containing a space (`$[' ']`).
Member names are case-sensitive. Escaped document keys are compared as UTF-16 code units,
so unpaired surrogates remain distinct from a literal `?` or replacement character, including
when duplicate members occur before or after them. Quoted names also accept escaped quotes,
backslashes, slashes, `\b`, `\f`, `\n`, `\r`, `\t`, and four-hex-digit `\u` escapes.
Unicode escapes may encode controls (including NUL) or paired surrogates; decoding happens
once, so `\\u0061` names the literal six-character key `\u0061`, not `a`. Escaped quotes
and brackets remain part of the member name. The planner uses Flink's released Jayway
unescaper and sends a canonical JSON-escaped name to Rust; both native readers select
against the decoded name without a per-row JVM call.
An escaped character without a special meaning loses only its backslash,
as in released Flink: `\q` names `q`, `\x61` names `x61`, and `\*` names the literal `*`.
This is not hexadecimal decoding or wildcard selection. Both quote styles have executed
parity coverage for every printable ASCII escape, ASCII controls and representative Unicode
characters, including strict/lax and error policies. Backslashes before literal Unicode or
control characters preserve those characters: `\用户` selects `用户`, and a backslash before
a literal newline selects a newline in the member name. The decoded name must still be
well-formed Unicode; the existing canonical encoding carries control characters safely to Rust.
Recursive descent, filters, slices, member-name unions, invalid/incomplete Unicode
escapes, unescaped ASCII controls inside bracket-quoted names,
unpaired surrogates and dynamic paths fall back. Quoted `'*'` is an ordinary member name,
not a wildcard.

**Wildcards** (`$.*`, `$[*]`, `$.a[-1][*]`) compose with native member/index selectors,
including continuations such as `$.a[*].b[-1]` and repeated wildcards such as `$[*][*].b`.
ASCII spaces inside their brackets are accepted (`$[ * ]`). Both spellings select object values
or array elements.
Flink returns a collection even for zero or one matches: JSON_EXISTS is TRUE for that collection,
JSON_VALUE uses ON EMPTY in lax mode, and JSON_VALUE uses ON ERROR in strict mode. A scalar or
JSON null reached by the wildcard yields an empty collection; root JSON null still fails to
construct a path context. Missing properties or wrong-type steps before the first wildcard are errors
in strict mode and empty collections in lax mode. Out-of-range indexes before the wildcard
instead produce an empty collection in both modes. Malformed documents retain the existing
strict/lax parsing policy, including validation of fields outside the selected path. After a
wildcard, missing or wrong-type member/index branches are skipped and the result remains a
collection, even when all branches are skipped.

The native readers track that collection result without allocating its elements, since neither
JSON_VALUE nor JSON_EXISTS exposes them. SQL parity covers empty/single/multiple matches,
scalar/null inputs, nested signed indexes, duplicate ancestors, escaped names, typed defaults,
all error policies, predicates and independent definite/wildcard projections. Retained operator
metrics verify 5,003 rows entering and leaving native Calc. Recursive descent, filters, slices,
member-name unions, path functions and JSON_QUERY use the generated Calc described above.

The wildcard benchmark reads `$.a[*]` from 32-element arrays with 264 bytes of padding.
Release/mimalloc on an Apple M1 Max, one million rowwise inputs, two warmups and five alternating
trials measured **3.869019s / 1.573674s** for JSON_VALUE (Flink/native, **2.459×**) and
**3.164764s / 1.439500s** for JSON_EXISTS (**2.199×**). JSON_VALUE uses lax mode with an
`'empty'` ON EMPTY default. The same-source identity control measured 0.458588s / 0.834914s
(0.549×). Both transposes and the rowwise blackhole sink are included. Reproduce with
`ScalarFunctionBenchmark#individualFunctions`, `-Pbench`,
`-Dscalar.functions=JSON_VALUE_WILDCARD,JSON_EXISTS_WILDCARD`,
`-Dscalar.rows=1000000 -Dscalar.bytes=264 -Dscalar.warmup=2 -Dscalar.runs=5`, and
`SF_BENCHMARK=true`.

**Array-index unions** (`$[0,2,-1]`, `$.a[0,0]`) are native for JSON_VALUE and JSON_EXISTS.
Each index uses ASCII decimal digits and must fit signed INT; leading zeros, negative zero,
duplicates and ASCII spaces around commas are accepted. Trailing U+0000–U+0020 controls after the final index follow the
same rule as a single index. Empty entries, plus signs, controls between entries, mixed member/index
selectors and out-of-range index literals retain fallback.

A union always produces a collection, including duplicate indexes and zero or one matches.
JSON_EXISTS therefore returns TRUE for a successful collection; JSON_VALUE applies ON EMPTY
in lax mode or ON ERROR in strict mode. Unlike a wildcard, the first union requires an array:
a scalar, object or nested null at that step is an error in strict mode and an empty collection
in lax mode. Missing members before the union follow the same policy; an out-of-range preceding
single index skips its branch and returns an empty collection in either mode. Root JSON null
and malformed documents keep their separate context/parse policies.

Unions compose with members, single indexes, wildcards and further unions. Once a wildcard or
union has branched, missing or wrong-type continuation steps skip that branch without changing
the collection result. Both native readers validate the complete document and path, retain
last-duplicate-member behavior, and avoid materializing selected elements. Released-Flink SQL
parity covers these combinations and error/default policies; counters require 5,003 rows to
enter and leave native Calc with independent union, wildcard and definite selections.

The union benchmark selects `$.a[0,16,-1]` from the same 32-element arrays with 264 bytes of
padding. Release/mimalloc on an Apple M1 Max, one million rowwise inputs, two warmups and five
alternating trials measured **2.058325s / 1.551007s** for JSON_VALUE (Flink/native, **1.327
&times;**) and **1.919062s / 1.464042s** for JSON_EXISTS (**1.311 &times;**). The matching
no-expression baseline was **0.431934s / 0.840174s**. All timings include the row source,
row sink and both transposes; the baseline is reported separately, without subtraction.
Reproduce with `mvn -pl :streamfusion-runtime -am -Pbench
-Dtest=ScalarFunctionBenchmark#individualFunctions -Dsurefire.failIfNoSpecifiedTests=false test`,
`-Dscalar.functions=JSON_VALUE_INDEX_UNION,JSON_EXISTS_INDEX_UNION`,
`-Dscalar.rows=1000000 -Dscalar.bytes=264 -Dscalar.warmup=2 -Dscalar.runs=5`, and
`SF_BENCHMARK=true`.

ASCII spaces around a bracket member or index are native, for example `$[ 'user' ][ -01 ]`.
Trailing ASCII spaces after a complete path are also accepted. After an array index's final
digit and before its closing `]`, the planner also removes any sequence of characters U+0000
through U+0020, matching Jayway's `String.trim()` on the index expression. Thus `$[1\t\n ]`
(where `\t` and `\n` stand for literal tab and newline) uses the same native selector as `$[1]`.
This works with negative indexes, leading zeros and nested paths. Before the index, only ASCII
spaces are admitted; controls within digits, before an index, or after a bracket step still
fall back. DEL, non-breaking space and other Unicode whitespace also remain
outside this index suffix grammar.

The planner removes only these verified syntactic characters; spaces inside quoted names
remain significant. An explicit case-insensitive `strict`/`lax` prefix accepts Flink's
mode-separating whitespace. Tabs and newlines within or at the end of a dot member are literal
key characters, including `$.a\t` (where `\t` is a literal tab). Leading whitespace without a mode
and unsupported whitespace after other tokens stay on Flink. General whitespace trimming
would change the selected value. Runtime tests exercise every ASCII control
suffix against released Flink, preserve strict/lax and error policies, and verify 5,003 rows
consumed and emitted by native Calc. Normalized literal paths register no JVM UDF callback.
The dot-member matrix also compares every admitted ASCII character, representative Unicode,
nested paths, duplicate keys, missing/null/scalar/container values and malformed unselected
fields. It verifies error policies, typed conversion failures, and 5,003 rows through native
Calc; literal-path encoding registers no JVM callback.
Remaining path extensions are tracked in
[#91](https://github.com/datafusion-contrib/StreamFusion/issues/91).

The dot-member benchmark selects `$.order-id.123.a\tb` (a literal tab in the final name)
from documents with 264 bytes of padding. Release/mimalloc on an Apple M1 Max, one million
rowwise inputs, two warmups and five alternating trials measured **1.099021s / 0.968465s**
for JSON_VALUE (Flink/native, 1.135×) and **1.051865s / 0.788609s** for JSON_EXISTS (1.334×).
The same-source identity control measured 0.373978s / 0.660353s (0.566×). Both transposes and
the rowwise blackhole sink are included. Reproduce with `ScalarFunctionBenchmark#individualFunctions`,
`-Pbench -Dscalar.functions=JSON_VALUE_DOT_MEMBER,JSON_EXISTS_DOT_MEMBER`,
`-Dscalar.rows=1000000 -Dscalar.bytes=264 -Dscalar.warmup=2 -Dscalar.runs=5`, and
`SF_BENCHMARK=true`.

The trailing-index-control benchmark selects `$.a[31\t\n ]` from 32-element arrays with
264 bytes of padding. Release/mimalloc, one million rowwise inputs, two warmups and five
alternating trials measured **1.705216s / 1.665379s** for JSON_VALUE (Flink/native, 1.024×)
and **1.672195s / 1.480013s** for JSON_EXISTS (1.130×). The same-source identity control
measured 0.465055s / 0.861789s (0.540×). Both transposes and the rowwise blackhole sink are
included. Reproduce with `ScalarFunctionBenchmark#individualFunctions`, the `bench` profile,
`-Dscalar.functions=JSON_VALUE_INDEX_WHITESPACE,JSON_EXISTS_INDEX_WHITESPACE`,
`-Dscalar.rows=1000000 -Dscalar.bytes=264 -Dscalar.warmup=2 -Dscalar.runs=5`, and
`SF_BENCHMARK=true`.

Empty-name SQL regressions execute against released Flink with native Calc assertions,
covering both quote styles, bracket spaces, nested objects/arrays, duplicate ancestors,
missing/null/scalar/container values, strict/lax policies, typed RETURNING, independent
paths and invalid unselected fields, plus explicit fallback for downstream string grouping.
Native reader tests also verify
selection on both the streaming parser and SIMD tape.

Escaped-name regressions execute both quote styles against released Flink, including every
admitted escape, nested negative indexes, duplicate members, missing/null/scalar/container
values, strict/lax policies, typed RETURNING and conversion failures, complete-document
validation, independent selections across batches, and explicit fallback for unverified
escapes. Direct projections are checked to register no JVM expression binding.

The escaped-path scalar benchmark selects nested backslash/newline member names from
1 million rowwise documents with 264 bytes of padding. On an Apple M1 Max, release +
`mimalloc`, two warmups and five interleaved trials, JSON_VALUE took 1.016908 s on Flink
and 0.936989 s natively (1.085×); JSON_EXISTS took 0.969056 s and 0.742654 s (1.305×).
Both native transposes and the rowwise blackhole sink are included. The source-matched
identity control took 0.384798 s / 0.653999 s (Flink/native), so these are full-pipeline
measurements, not isolated parser timings. Reproduce with `ScalarFunctionBenchmark`,
`-Pbench -Dscalar.functions=JSON_VALUE_ESCAPED_PATH,JSON_EXISTS_ESCAPED_PATH`,
`-Dscalar.rows=1000000 -Dscalar.bytes=264 -Dscalar.warmup=2 -Dscalar.runs=5` and
`SF_BENCHMARK=true`.

The printable-ASCII escape extension uses the same native member reader after planning.
With the same release/mimalloc settings, 1 million rows and 264 bytes of padding, selecting
`$["u\ser"]["na\me"]` measured 1.075870 s / 0.776419 s for JSON_VALUE (Flink/native,
1.386×) and 1.070222 s / 0.690900 s for JSON_EXISTS (1.549×). The source-matched identity
control measured 0.378125 s / 0.645387 s (0.586×). Two warmups and five interleaved trials
include both transposes and the rowwise sink, with no competing local builds or tests.
Use `-Dscalar.functions=JSON_VALUE_NONSTANDARD_ESCAPE,JSON_EXISTS_NONSTANDARD_ESCAPE`
with the benchmark options above to reproduce.

The escaped-Unicode variant selects `$["\用户"]["\姓.\名"]` from the same row-fed
Unicode-member fixture. With release/mimalloc, 1 million rows, 264 bytes of padding,
two warmups and five interleaved trials, JSON_VALUE measured 1.346831 s / 1.055570 s
(Flink/native, 1.276×), and JSON_EXISTS measured 1.322327 s / 0.961161 s (1.376×).
The source-matched identity control measured 0.636006 s / 0.939120 s (0.677×).
Both transposes and the row sink remain included. Use
`-Dscalar.functions=JSON_VALUE_UNICODE_ESCAPE,JSON_EXISTS_UNICODE_ESCAPE` to reproduce.

Negative-index regressions cover nested arrays, minimum signed indexes, negative zero, leading
zeros, all strict/lax policies, typed RETURNING and conversion failures, complete-document
validation, and independent selections across multiple batches. The SIMD reader uses its existing
array lengths; the streaming reader counts a negatively indexed array before selecting from it,
without building a JSON object tree or retaining every element. Both are Rust paths with no
generated-expression UDF binding for direct projections.

The default return type and explicit `RETURNING VARCHAR(n)` are native; Flink 2.2.1 does
not truncate this function's result to `n`. `RETURNING BOOLEAN`, `INTEGER` and `DOUBLE`
are also native with the following exact Flink object-type rules:

| RETURNING | Accepted selected scalar | Supported literal DEFAULT |
|---|---|---|
| VARCHAR(n) | String, boolean or number converted to Jackson's text | Non-null character literal |
| BOOLEAN | JSON boolean | Non-null BOOLEAN literal |
| INTEGER | JSON integer token within signed 32-bit range | Non-null INTEGER literal |
| DOUBLE | JSON number with a decimal point or exponent (Jackson BigDecimal) | Not admitted |

`NULL` and `ERROR` behaviors are supported independently for ON EMPTY and ON ERROR.
Other default types, NULL defaults and non-literal defaults use the generated Calc. DOUBLE
defaults retain Flink's generated Double/DecimalData-to-BigDecimal conversion failures.
Selected scalar type mismatches fail the job **outside ON ERROR**, matching Flink: a quoted
`"12"` is not an INTEGER, `1.0` is not an INTEGER, and `1` is not a DOUBLE. Decimal-to-double
conversion preserves rounding, infinity and underflow; a decimal zero has no negative sign.
The [host failure reproducer](../upstream-flink-suite.md#expected-host-failures-in-sql-parity-audits)
checks these conversion errors without native planning or the audit source adapter.

A BOOLEAN form with either NULL policy is admitted only as a direct projection. Flink 2.2.1
can unbox its boxed NULL result without checking the null flag in a bare WHERE condition,
truth predicate or CASE condition, failing the job. Such compositions use the generated Calc and preserve that failure.
BOOLEAN forms with non-null DEFAULT or ERROR for both policies can compose natively.
Typed JSON_VALUE calls nested under AND/OR use the generated Calc: DataFusion may evaluate the
unneeded side on some rows, exposing a scalar conversion failure that Flink short-circuits.
CASE result branches retain native admission and evaluate only selected conversions.
VARCHAR calls and their consumers use the fused JVM path described below, preserving Flink's
AND/OR short-circuiting even with an ERROR policy.

The default path mode is **strict**. Missing members, selected JSON nulls, malformed JSON,
and selected containers invoke ON ERROR in strict mode. In lax mode these invoke ON EMPTY,
except a document containing the JSON literal `null`, which invokes ON ERROR in either mode.
SQL NULL input always returns SQL NULL. ERROR ON EMPTY fails directly, even with a default
ON ERROR. Duplicate members keep the last value, decimal text retains Jackson's BigDecimal
scale/exponent spelling, and unpaired escaped surrogates become `?` in UTF-8 output.
Consumers of a STRING `JSON_VALUE` or `JSON_UNQUOTE` result execute together in one
Flink-generated expression through the batch UDF bridge. Equality, inequality, LIKE, CASE,
filters, nested scalar calls and scalar UDFs therefore observe the original Java UTF-16 value:
an unpaired surrogate remains distinct from a literal `?`. Constant-folded JSON results containing
unpaired surrogates receive the same treatment. These expressions run inside columnar Calc/filter,
but their fused scalar computation runs on the JVM. Only the final result enters Arrow.

Direct JSON string projections retain the Rust kernel. A Calc projecting a JSON-derived STRING
(including nested character fields) into another operator makes the whole query fall back, with
the reason `JSON string identity requires a final projection or a fused scalar consumer`. This
includes grouping, DISTINCT, joins and sorting on those results, and intermediate optimizer blocks
whose output is not final. The gate is conservative even when a particular document contains no
surrogates or a scalar transformation happens to remove them. Final projections are allowed;
non-string consumer results, such as a comparison or integer CASE, can feed native aggregation.
Arrow strings always contain valid UTF-8; final-output replacement is never used to justify
intermediate expression parity.

JSON_VALUE scalar-conversion failures preserve Flink's ClassCastException, naming the source
Java scalar class and the requested target class. This includes integer tokens returned as
BOOLEAN or DOUBLE, and out-of-range integer tokens returned as INTEGER. The exception remains
outside ON ERROR handling, matching released Flink 2.2.1. A typed DataFusion error reaches the
JNI boundary without parsing messages or calling the JVM on successful rows. JSON ERROR policy
failures retain the existing NativeException wrapper; their diagnostic parity is not established.

### JSON_EXISTS

Enabled by default for the following verified shapes; no compatibility opt-in is needed.

Character input and the same literal path grammar as JSON_VALUE are native. Supports FALSE
(the default), TRUE, UNKNOWN and ERROR ON ERROR. A selected scalar or container, including an
empty object/array, returns TRUE. Lax missing paths and selected JSON nulls return FALSE;
strict missing/null paths invoke ON ERROR. Malformed JSON invokes ON ERROR in strict mode
and returns FALSE in lax mode. A document containing the JSON literal `null` invokes ON ERROR
in both modes. SQL NULL input returns SQL NULL.

UNKNOWN ON ERROR uses the native path only as a direct projection; its other contexts use
the generated Calc to preserve the same boxed-null behavior as BOOLEAN JSON_VALUE. ERROR
ON ERROR under AND/OR also uses the generated Calc to preserve row short-circuiting. The default FALSE policy and TRUE ON ERROR
remain native in predicates and nested expressions.

Admitted ERROR ON ERROR calls use Flink's `SqlJsonUtils` through the existing columnar JVM
upcall. This preserves its `TableRuntimeException` and exact parser/path diagnostic, including
the missing member's path, while the surrounding expression stays in the native island.
FALSE, TRUE and UNKNOWN policies continue to use the Rust parser.

These JSON functions use native first-document parsing and validate unselected fields too.
Admission first probes the shaded Jackson runtime once per class loader: Flink 2.2 requires
Jackson 2.18.2 and its default thread-local recycler pool; Flink 1.18 requires Jackson 2.14.2
on JDK 11 or 17 and thread-local buffer recycling. Both require successful buffer acquisition, cross-factory
reuse and release. Missing methods/classes, a different version or pool,
or probe failure decline the native parser for JSON_VALUE, JSON_EXISTS and IS JSON; Calc
can use Flink generation when the host runtime and batch boundary types support it.
JobManagers and TaskManagers must use the same verified shaded Jackson runtime.
The 2.2 profile admits JDK 11, 17, 21, 24 and 25, selecting the corresponding Unicode version for
Jackson's token-termination rules (Unicode 10.0 on JDK 11 and 13.0 on JDK 17); other JDKs use Flink generation in Calc. The profile is selected on the
JobManager, so TaskManagers must use the same JSON parsing rules. Jackson 2.18.2's resource limits
(1000 nesting levels, 1000 number digits, 20 million UTF-16 string units, 50,000 member-name
units) also apply to unselected values. Its numeric boundary has a buffer-dependent exception:
the slow parser can accept an extra digit. Native evaluation uses the task thread's actual
Jackson input-buffer capacity and preserves its growth, including invalid input and SIMD
parsing. A batch exchanges this capacity through JNI; documents and results remain native. The 1.18
profile uses the same reader on JDK 11 and JDK 17, with Double numeric semantics and without the newer
token/depth limits. Deep legacy nesting grows the native stack as needed.
See the [SQL/JSON parser note](https://github.com/datafusion-contrib/StreamFusion/blob/main/divergences/32-sql-json-definite-paths.md)
and [per-function benchmarks](../benchmarks/scalar-functions.md).

### Collection expressions

The following extensions are enabled by default. Set
`streamfusion.experimental.collectionExpressions=false` to disable them. The maintainer accepted
the measured standalone regressions on 2026-10-17;
[issue #392](https://github.com/datafusion-contrib/StreamFusion/issues/392) tracks performance improvements.
Existing admitted collection subscripts and Boolean/integer ARRAY_DISTINCT retain their ordinary paths.

| Functions | Execution inside NativeCalc |
| --- | --- |
| ARRAY_REVERSE, ARRAY_SLICE | Rust kernels for nullable primitive elements; generated Flink program for STRING/ROW/MAP-bearing or NOT NULL-element arrays and consumers requiring Java value semantics |
| CARDINALITY on ARRAY/MAP | Rust outer-offset count, returning INT; NULL container produces NULL |
| ARRAY_CONTAINS, ARRAY_POSITION | Rust scans for matching BOOLEAN, integer and DECIMAL element/needle types with nullable elements; other signatures, representation-sensitive consumers and fallible arguments use the generated Flink program. Non-NULL literal or nonnullable CONTAINS needles retain Flink evaluation; DECIMAL arrays with such needles require whole-Calc fallback |
| ARRAY_MIN, ARRAY_MAX | Released DataFusion Rust kernels for nullable integer and DECIMAL elements; other types, NOT NULL elements, representation-sensitive consumers and fallible arguments retain Flink's generated program |
| ARRAY_APPEND, ARRAY_PREPEND | Rust offset-based copying for nullable BOOLEAN/integer/DECIMAL arrays and a matching element, including a NULL element; sensitive representations and fallible arguments retain Flink evaluation |
| ARRAY_REMOVE | Rust stable selection for matching nullable BOOLEAN/integer/DECIMAL arrays; removes every matching element, including NULL when the needle is NULL; other signatures retain Flink evaluation |
| ARRAY_CONCAT | Rust concatenation for matching nullable BOOLEAN/integer/DECIMAL arrays; any NULL operand returns NULL; other signatures retain Flink evaluation |
| ARRAY_UNION | Released DataFusion Rust kernel for matching nullable BOOLEAN/integer/DECIMAL arrays; first-occurrence order and one NULL element retained; either NULL container returns NULL |
| ARRAY_EXCEPT | Rust stable set difference for matching nullable BOOLEAN/integer/DECIMAL arrays, retaining the first left-side occurrence of each value absent on the right, with NULL treated as a value; either NULL array returns NULL |
| ARRAY_INTERSECT | Rust stable intersection for matching nullable BOOLEAN/integer/DECIMAL arrays; distinct matches follow left-side order even when the left array is shorter; NULL is a value, either NULL container returns NULL |
| ARRAY_SORT | Released DataFusion Rust sort for nullable integer/DECIMAL arrays proven shorter than 32 elements or produced by native ARRAY_DISTINCT, with non-NULL constant Boolean options; unrestricted, runtime-option, sensitive and fallible signatures retain Flink's comparator behavior |
| ARRAY_JOIN | Rust UTF-8 join for total nullable string arrays and string options, including strict explicit NULL replacement; co-projected sensitive values and unverified consumers remain generated |
| ELEMENT | Rust extraction for nullable BOOLEAN/integer/DECIMAL arrays; NULL/empty arrays return NULL, a singleton returns its element, longer arrays raise the selected Flink line's exception with the released message; multiple failure sites and sensitive/fallible operands retain row evaluation |
| MAP_FROM_ARRAYS | Rust construction for nullable basic key/value arrays, preserving duplicate and NULL keys, entry order and strict parent NULLs; mismatched lengths retain Flink's typed exception; other signatures remain generated |
| MAP_KEYS | Rust shared-key view for total maps with nullable BOOLEAN/integer/DECIMAL keys and values; other signatures retain generated evaluation |
| MAP_VALUES | Rust shared-value view under the same verified basic-map admission |
| MAP_ENTRIES | Rust shared entry-row list for verified basic maps; representation-sensitive row consumers remain generated |
| MAP_UNION | Rust stable typed membership/gather for total matching nullable basic maps, preserving right ordering/duplicates and Flink NULL-key behavior |
| Runtime ARRAY[...] | Rust construction for matching nullable BOOLEAN/integer/DECIMAL/DATE/TIME/TIMESTAMP/LTZ elements and total arguments; other projected signatures retain the generated program; UNNEST retains its existing wider scalar-specific admission |
| Runtime MAP[...] | Rust construction for total nullable basic keys/values and distinct literal/input-reference key expressions; repeated/generated keys and fallible arguments remain generated; Flink 1.18 multi-entry constructors retain generated grouping order |
| Runtime ROW(...) | Released DataFusion named-struct construction for total BOOLEAN/integer/DECIMAL fields; other fields and representation-sensitive consumers remain generated |
| OBJECT_OF | Released named-struct construction for validated metadata and total basic fields; other signatures remain generated |
| OBJECT_UPDATE | Native field selection only for one unshared inline basic OBJECT_OF in the complete Calc and atomic literal/input-reference values; shared objects and other signatures remain generated |

The generated route evaluates the complete filter and projection together, preserving failure
order, NULLs, empty filtered batches, function invocation order and released Flink overloads.
It retains Arrow batch inputs/outputs and production JNI costs. A NativeCalc containing these
functions does **not** imply that its scalar work executes in Rust; the function-engine diagnostic
reports the assignment, including ARRAY/MAP/ROW constructors. Programs with user scalar
functions retain their existing admission gates.

Basic-type ARRAY_CONTAINS and ARRAY_POSITION scan the flattened values and offsets without
constructing per-row arrays or invoking the JVM. Admitted scalar needles remain scalar instead of
being broadcast across the batch. NULL arrays produce NULL; CONTAINS can match a NULL needle to a NULL
element, whereas POSITION returns NULL for a NULL needle. POSITION returns the first one-based
position as INT, or zero when absent. DECIMAL equality compares the exact unscaled value at the
resolved precision and scale. Mixed element/needle types and potentially failing arguments retain
Flink evaluation; NOT NULL-element and sensitive-string gates still apply to the complete Calc.
Flink derives CONTAINS' element getter from the needle's type, including nullability. A non-NULL
literal or nonnullable needle can therefore change the behavior of NULL array elements. These
CONTAINS signatures retain Flink evaluation. For DECIMAL, the Arrow-backed JVM view also differs
from the binary array's unchecked getter, so the complete Calc falls back to stock Flink. Tests
compare both successful results and released-version exceptions, including all-filtered inputs.
The [optimization evidence](../optimizations/native-array-search.md) compares these paths with
stock Flink and the prior generated evaluator, including the remaining standalone regressions.

ARRAY_MIN and ARRAY_MAX directly reuse DataFusion 54's released typed kernels for TINYINT,
SMALLINT, INT, BIGINT and DECIMAL arrays with nullable elements. They ignore NULL elements;
NULL arrays, empty arrays and arrays containing only NULL return a nullable result of the
declared element type, preserving DECIMAL precision and scale. The released implementation
dispatches once per batch, scans short lists directly and uses Arrow's integer/decimal reduction
for lists of at least 32 elements. Both paths preserve exact integer and unscaled DECIMAL ordering.
Strings, floating values, temporal elements, nested values, NOT NULL-element arrays and fallible
operands retain their existing Flink evaluation. BOOLEAN extrema remain a Flink validation error.
The existing collection switch and whole-Calc representation/error-order safeguards still apply.
Availability still follows the released Flink catalog: Flink 1.18 has ARRAY_MAX but no ARRAY_MIN;
native registration does not introduce a SQL function missing from the host release.

An ARRAY materialized solely for [UNNEST](unnest-correlate.md) can use released DataFusion's scalar
array constructor because the container's generic/binary identity does not escape that consumer.
The admitted scalar element types preserve raw floating values and the released timestamp writer's
compact/expanded storage rules. BIGINT millisecond epoch conversion and TIMESTAMP_LTZ-to-DATE/TIME
casts also have native kernels in that operand context, with SQL epoch bounds and per-version TIME
truncation preserved. Non-UTC offsets come from JDK ZoneRules through the batch bridge and are
cached between transitions, preserving historical and daylight-saving behavior. This total lookup
does not trigger whole-Calc row evaluation; other JVM calls retain their error-order safeguards.
Ordinary projected arrays of verified nullable basic and temporal elements reuse the same
constructor; floating, NOT NULL-element, character and representation-sensitive projected
arrays retain generated evaluation.

Primitive bridge types and recursively supported ARRAY/MAP/ROW values are admitted, including
nullable typed map keys, decimal and timestamp values. Fixed BINARY leaves inside generated
containers and unpaired UTF-16 field names remain excluded. STRUCTURED results from inline OBJECT_OF/OBJECT_UPDATE are supported;
external STRUCTURED inputs retain fallback. Unsupported host signatures or generated programs
fall back during planning.

Nested strings carry their UTF-8 bytes and optional original Java UTF-16 cache. Nested ROW values
carry a marked change-kind child outside their SQL ordinals. These preserve supplementary/BMP
ordering, isolated surrogates and non-INSERT nested rows. Represented arrays first pass through the released serializer and record its generic/binary copy
category. Readers restore that category, including whether repeated string getters share Java
caches. NOT NULL elements use an explicit carrier so a NULL first slot cannot hide that category.
The extended schemas belong only to generated collection programs and are serialized to tasks.
Other operators and connector schemas keep their ordinary layouts.

The UTF-16 units move with checked bulk copies, preserving malformed surrogates. Optional scalar
cache sidecars appear only in batches with a present Java cache; absent sidecars mean binary-only
strings. A cache first seen later in the batch retains NULL entries for the earlier rows.

String/ROW/MAP-bearing and NOT NULL-element array results currently require a final projection,
a fused consumer or a verified generated [UNNEST](unnest-correlate.md) consumer.
They fall back before stateful, ordering or exchange boundaries that have not verified this
representation. Sensitive inputs from other columnar producers also retain fallback until their
cache/kind contract is verified, including an ordinary native Calc or transpose without the
explicit representation layout. Arrays of nullable primitive elements can continue through ordinary native
operators. Representation-sensitive final results require a row sink: native sink rewrites
with the extended layouts retain whole-query fallback until their writer contract is verified.
Nullable primitive array results keep ordinary native sinks. These are representation gates
even when the experimental option is enabled.

Generated UNNEST outputs can additionally carry an owned binary payload for nested binary ROWs.
The helper is marked Arrow metadata outside SQL field ordinals and survives IPC. Readers retain
BinaryRowData versus NestedRowData copy behavior; generic ROWs retain their string caches and
serializer semantics. An immediate exit transpose can use the payload in Flink's binary projection
only when every non-null ROW in that batch has a verified binary payload. Missing or generic
payloads keep the ordinary reader path.

ARRAY_SLICE uses inclusive, one-based Flink bounds. Negative positions count from the end;
zero normalizes to the first element. NULL containers or bounds produce NULL. Whole slices retain
the original arrays; partial slices copy contiguous child ranges. CARDINALITY counts outer array
elements, including NULL entries, rather than flattening nested arrays; map entries include NULL
values and repeated keys. MULTISET retains released Flink's validation/code-generation behavior.

Flink 1.18.1 lacks ARRAY_APPEND, ARRAY_PREPEND, ARRAY_SORT,
ARRAY_MIN, MAP_UNION, OBJECT_OF and OBJECT_UPDATE. The remaining signatures are checked on
both supported releases; absent functions are explicitly N/A, not counted as native coverage.
Binary-key map projection checks use ordered internal input: external Java byte-array keys have
identity hashes, so independently deserialized external maps can have different iteration orders
before either engine evaluates the function. The checks compare the resulting arrays in order.

Earlier direct extrema and map-projection experiments were slower than Flink and remain
recorded in the [extrema investigation](https://github.com/datafusion-contrib/StreamFusion/blob/main/.claude/wontdos/234-array-extrema.md)
and [map investigation](https://github.com/datafusion-contrib/StreamFusion/blob/main/.claude/wontdos/234-map-array-projections.md).
The earlier extrema prototype's broad standalone admission remains rejected: its historical
Linux short-array regression is not invalidated by newer macOS measurements. Matching nullable
integer/DECIMAL extrema now use released DataFusion inside the already-admitted experimental
extension, with [current before/after evidence](../optimizations/native-array-search.md#integer-and-decimal-array-extrema)
and the remaining standalone limits retained. The map-projection prototype remains unused.
Generated routes remain enabled under the accepted performance tradeoff and do not establish
standalone acceleration.
[Issue #234](https://github.com/datafusion-contrib/StreamFusion/issues/234) also tracks remaining
collection aggregate and cast work outside this extension.

### ARRAY_DISTINCT

Boolean and integer (`TINYINT`, `SMALLINT`, `INT`, `BIGINT`) arrays use typed kernels following
DataFusion's ordered membership and batch-gather structure. Boolean membership uses two bits;
integer and string arrays of at most eight elements use a bounded stack search with a
collision-checked fingerprint, and larger arrays use a reusable hash set. The first occurrence of each value is retained,
including one NULL element. NULL containers remain NULL and empty arrays remain
empty. The result preserves the element width, field metadata, and nullability. Boolean equality
compares values; string equality compares UTF-8 bytes without trimming or Unicode normalization.
VARCHAR/STRING, CHAR, floating, decimal, temporal, binary, and nested element types use the
generated collection route above for verified bridge types; other signatures retain planning fallback.
Runtime SQL parity checks pass against released Flink 2.2.1.
[Whole-job measurements](../benchmarks/scalar-functions.md#integer-array_distinct-2026-09-28)
show large-array gains and small-array sensitivity to run-to-run variation. With
[synchronous entry borrowing](../optimizations/projection-pruning-transpose.md#borrowing-at-the-synchronous-consumer),
the nullable width-eight Boolean diagnostic also beats stock and previous production in two
ten-million-row candidate runs; STRING performance admission remains unresolved, so its tested native kernel is not enabled by the planner.

### SPLIT

Character input and a literal non-empty separator. The separator is literal text, including regex metacharacters. NULL input returns NULL, empty input returns an empty array, and leading/repeated/trailing separators retain empty tokens. Empty or dynamic separators fall back; the empty form splits UTF-16 surrogate units in Flink.

### SUBSTRING

SUBSTRING/SUBSTR accepts dynamic TINYINT, SMALLINT, or INT starts and optional lengths. Positive positions are one-based, zero starts at the first character, negative positions count from the end, and a position before the beginning returns empty. Negative length returns NULL; input NULLs propagate.

### LEFT

Character input with a dynamic TINYINT, SMALLINT, or INT count. Non-positive counts return empty, large counts return the full string, and NULL propagates. Counts measure Unicode code points.

### RIGHT

Character input with a dynamic TINYINT, SMALLINT, or INT count. Non-positive counts return empty, large counts return the full string, and NULL propagates. Counts measure Unicode code points.

### LPAD

STRING, dynamic INT-width length, and literal or dynamic STRING padding. NULL, negative length, or empty padding returns NULL; zero length otherwise returns empty. Flink 2.2.1 counts UTF-16 units, including truncation through surrogate pairs.

### RPAD

Same input and boundary rules as LPAD, with padding appended on the right. Dynamic lengths and padding are admitted, with Flink 2.2.1 UTF-16 counting.

### SPLIT_INDEX

Character separators and TINYINT/SMALLINT/INTEGER indices may be dynamic. Indices are zero-based; negative/out-of-range indices, empty input, or any NULL produce NULL. Whole separators preserve empty tokens. An empty separator uses Java Character.isWhitespace, including tabs and line separators but excluding non-breaking spaces. Numeric separator overloads use Flink-generated code and interpret the integer as a character code. BIGINT indices with character separators fall back.

Generated scalar helpers keep operand computations within the same Flink evaluator, retaining
intermediate StringData representation. For a computed empty trim set, Flink 2.2.1 can throw
an arithmetic exception while the empty value is binary-only, but succeeds after its Java string
has been decoded and cached. The shared empty-string singleton makes this depend on prior use in
the JVM. Native evaluation preserves the host representation and matches either outcome; parity
checks compare the live host outcome rather than assuming that every computed empty set fails.
A separate regression check uses a fresh empty value to verify both representation states.
Regular correctness-test forks use `-XX:-OmitStackTraceInFastThrow` so repeated implicit
exceptions retain the messages and stack frames needed to compare their evaluation phase.
Without it, HotSpot can turn the same arithmetic failure into a stackless exception with no
message after warmup, creating a second test-order dependency. The `bench` profile leaves
this test-only setting empty and retains normal JVM exception optimization.

### Temporal parsing, extraction and rounding

`TO_DATE`, `TO_TIMESTAMP`, all `TO_TIMESTAMP_LTZ` overloads, calendar fields, and temporal
`FLOOR`/`CEIL` now have expression implementations. Formatted parsing and LTZ calendar fields use
Flink's own generated code. Timestamp results retain the complete millisecond/fraction pair and run
by default. See the complete [temporal function inventory](temporal-functions.md).

### LTRIM

One-argument space trimming and literal Unicode sets retain their Rust kernels. Per-row sets
use the same generated evaluator and source-representation gate as BTRIM. Empty sets preserve
the input and NULL propagates.

### RTRIM

Uses the same literal kernels and per-row set admission as LTRIM, trimming from the right.

## Case folding & regex

`REGEXP_EXTRACT_ALL` and `STR_TO_MAP` use Flink-generated evaluation. ARRAY/MAP results use
the whole-Calc batch JVM route with owned Arrow output vectors; scalar consumers can stay in
one generated expression. Capture groups, unmatched optional groups, empty matches, invalid
patterns/indices, NULL containers/elements, regex map delimiters, duplicate keys and missing
values follow released Flink. Filters run before projections, including all-filtered batches.
ARRAY and MAP lookups and cardinality inside the Calc retain the host's internal values.
Character results from these functions crossing another relational operator retain the string
identity fallback, as do whole-Calc character outputs containing arbitrary Base64-decoded bytes.
The operator remains columnar; these functions themselves execute on the JVM.

REGEXP, REGEXP_REPLACE, REGEXP_COUNT, REGEXP_INSTR and REGEXP_SUBSTR use Flink-generated
expressions through the batch JVM bridge for literal and per-row patterns. Java lookaround,
backreferences, UTF-16 positions, empty matches, literal replacement strings, invalid-pattern
behavior and resolved INT/BOOLEAN/STRING types are retained. AND/OR consumers fuse with these
calls to preserve Flink evaluation order. These functions do not use Rust's regex engine.

**Native by default — not a fallback.** `UPPER`/`LOWER` and `REGEXP_EXTRACT` run natively by default
via a columnar JVM upcall to Flink's own string routines — `BinaryStringData` case folding and
`SqlFunctionUtils.regexpExtract` — so the result is byte-identical to the host, and the rest of the
containing expression still evaluates natively around the upcalled function.

Each of these also has a faster **pure-Rust** alternative — Rust's own case folding, and the `regex`
crate — that is **opt-in** under
`-Dstreamfusion.expression.<NAME>.allowIncompatible=true` (or the blanket flag; see
[Configuration](../configuration.md)). It's opt-in rather than default because it can diverge from
the JVM behavior on non-ASCII case folding and on advanced regex features (backreferences,
lookaround, some Unicode character classes) — real correctness differences, not just a performance
trade-off, which is why parity comes first by default.

Neither path falls back to the host for a supported argument type. What *does* fall back: a
non-string argument, or — specifically on the pure-native (opt-in) `REGEXP_EXTRACT` — a non-literal
pattern or index.

## Date/time

The [temporal functions page](temporal-functions.md) lists every supported scalar family, clock,
watermark and window helper, with the exact range and runtime-context gates. Temporal expressions
use Flink's code through the batched JVM upcall unless a verified Rust kernel already exists.
Adjacent temporal calls fuse into one upcall, retaining intermediate TimestampData values inside
Flink rather than converting each one to an Arrow timestamp.

`DATE_FORMAT` and `EXTRACT` retain their existing opt-in Rust LTZ paths. Dynamic patterns, additional
extraction fields and other temporal functions use Flink's implementation. Timestamp-producing
expressions run by default with the full Flink millisecond range and fractional nanos.

## POWER and SQRT

`POWER` (including Flink's lowering of `SQRT`) runs inside native Calc by default through
Flink-generated JVM code in the existing batch UDF bridge. The resolved primitive/DECIMAL
overload uses Flink's own conversion and `Math.pow`, preserving exact deterministic results,
signed zeros, NaN, infinities, overflow/underflow and NULL propagation. Nested powers can fuse
into one generated expression. The surrounding operator remains columnar; the power operation
itself executes on the JVM. The Rust alternative remains available under
`streamfusion.expression.POWER.allowIncompatible=true` or the blanket flag.

## Exact floating math

EXP, LN, LOG10, SIN, COS, TAN, ASIN, ACOS, ATAN, COSH, SINH, TANH, COT, ATAN2,
DEGREES, RADIANS, LOG (one or two arguments) and LOG2 run inside native Calc by default
through Flink-generated evaluation in the existing batch JVM bridge. Resolved numeric
overloads, including integer and DECIMAL operands, use the selected released Flink line's
own conversions and math functions. This preserves deterministic last-bit results, signed
zero, NaN, infinities, domain results and NULL behavior.

Non-DECIMAL ROUND and INT/BIGINT/FLOAT TRUNCATE use the same generated path,
including runtime INT scales. Independently emitted DOUBLE TRUNCATE uses an exact
[bounded shortcut](../optimizations/host-exact-builtins-upcall.md#bounded-double-truncate):
for magnitudes from 1 through 1e9 and scales -6 through 6, outward-rounded bounds must
prove the same truncated decimal integer before arithmetic replaces decimal conversion.
Ambiguous boundaries, other values/scales, and row-fused expressions retain Flink's
implementation. NULLs, failure ordering and declared types are unchanged. FLOAT/DOUBLE
ROUND retains Flink's BigDecimal-based HALF_UP semantics; every TRUNCATE path preserves
Flink's decimal rounding-toward-zero result.
TINYINT/SMALLINT TRUNCATE falls back: released Flink generates an invalid narrowing
assignment for these signatures, and the stock job also fails during initialization.
Non-finite values that fail in Flink fail on the native route with the same root exception.
Supported surrounding expressions fuse into the generated evaluator, preserving CASE and
filter short-circuiting for unselected failing calls. DECIMAL ROUND/TRUNCATE keep their
separate admission rules above.

These are JVM-evaluated functions inside a columnar operator. Existing Rust alternatives for
EXP, LN, SIN, COS, TAN, ASIN, ACOS, ATAN, LOG10 and floating ROUND remain opt-in through
`-Dstreamfusion.expression.<NAME>.allowIncompatible=true` or the blanket flag. Those paths
can differ from Java Math at the last bit, or from Flink's decimal-based floating rounding;
the flags are unnecessary for exact default execution. The new COSH/SINH/TANH/COT/ATAN2,
DEGREES/RADIANS/LOG/LOG2 paths always use generated evaluation.

Release+mimalloc diagnostic on an Intel Core i7-12650H under Linux/WSL, JDK 17,
Flink 2.2.1 (2026-09-27): two million numeric runtime rows, one-seventh NULLs, parallelism 1,
two warmups and five measured runs in alternating engine order. The matched numeric identity
control, both Arrow transposes and a rowwise blackhole sink remain in the measured path.

A [grouped integer-SUM benchmark](../optimizations/host-exact-builtins-upcall.md#composition-with-grouped-integer-sums) shows 16–24% less native elapsed time than Flink at 2M and 5M rows for TAN, COSH and floating TRUNCATE, retaining both transposes. This gain applies to those composed queries; standalone math remains slower. A separate admission ablation confirms that these compositions previously fell back entirely to Flink and were slower than the current native plans. Grouped nullable/extreme-value parity cases pass on Flink 1.18 and 2.2.

The original measurements below are historical. A subsequent [CPU-profile investigation](../optimizations/host-exact-builtins-upcall.md#exact-floating-math-remaining-performance-limits) tested borrowed inputs, primitive batch evaluation and simplified rounding. None established a whole-job improvement; the prototypes were removed. The current default
retains released Flink evaluation inside native Calc, with standalone costs reported separately
from the composed-query improvements above.


| Expression | Flink median (s) | Native median (s) | Flink/native ratio |
|---|---:|---:|---:|
| Numeric identity | 0.293 | 0.429 | 0.685x |
| Exact TAN | 0.367 | 0.643 | 0.571x |
| Exact COSH | 0.337 | 0.534 | 0.631x |
| DOUBLE TRUNCATE with runtime INT scale | 0.782 | 0.939 | 0.833x |

These standalone queries are slower than Flink. The generated path is retained to keep exact
math inside larger native pipelines using the existing bridge; it introduces no new operator
or boundary. It does not establish a floating-math speedup. Reproduce with:

```sh
SF_BENCHMARK=true mvn -pl streamfusion-runtime -am test -Pbench \
  -Dtest=ScalarFunctionBenchmark \
  -Dscalar.functions=TAN_EXACT,COSH_EXACT,FLOAT_TRUNCATE_EXACT \
  -Dscalar.rows=2000000 -Dscalar.nullEvery=7 -Dscalar.warmup=2 -Dscalar.runs=5
```

## Literal/arity guards

A number of otherwise-admitted functions decline when called with an argument shape the native
implementation can't handle, even though the function itself is supported:

- An **unsupported literal type** anywhere in the expression.
- **`TRIM`** — dynamic SQL trim sets; all directions with literal sets are native. Per-row
  BTRIM/LTRIM/RTRIM sets use their documented external-Java-string admission.
- **`POSITION`** — a `FROM` start offset.
- **`CURRENT_WATERMARK`** — requires a Calc watermark context; unsupported in standalone join or UNNEST residuals.
- **Collection subscripts:** non-INT ARRAY indexes and literal indexes below one; runtime MAP keys
  of floating, collection or mismatched types; nullable non-compact decimal/timestamp MAP keys.
  See the [collection contract](#collection-subscripts).
- **Wrong arity** for any otherwise-admitted function.

See [Configuration](../configuration.md) for the full `allowIncompatible` flag surface referenced
throughout this page.

## Generated helper performance

The generated functions above extend the expressions that can stay inside a columnar island.
They are correctness/coverage work, not standalone scalar speedups: Flink still evaluates each
row inside the batch callback. A release-only diagnostic on Apple M4 Pro on 2026-09-21 used 200,000 rows,
64-byte text/binary payloads, parallelism 1, one warmup and three alternating measured runs per
engine. Matched-source identity controls and both row/Arrow transposes remain in the measured
path. Every query verifies native Calc admission before timing.

| Expression workload | Flink seconds | Native seconds | Throughput ratio |
|---|---:|---:|---:|
| BIGINT ABS | 0.094 | 0.117 | 0.806x |
| DECIMAL SIGN | 0.096 | 0.153 | 0.630x |
| Runtime STRING GREATEST | 0.103 | 0.145 | 0.709x |
| BOOLEAN IF | 0.095 | 0.104 | 0.911x |
| BOOLEAN to STRING | 0.093 | 0.110 | 0.844x |
| LIKE ESCAPE | 0.106 | 0.132 | 0.799x |
| REGEXP_COUNT | 0.138 | 0.171 | 0.808x |
| PARSE_URL | 0.134 | 0.174 | 0.771x |
| PRINTF BIGINT | 0.168 | 0.184 | 0.913x |
| Dynamic BTRIM | 0.117 | 0.178 | 0.659x |
| IS_ALPHA | 0.096 | 0.136 | 0.706x |
| Binary STARTSWITH | 0.089 | 0.131 | 0.681x |
| REGEXP_EXTRACT_ALL | 0.200 | 0.279 | 0.718x |

These short local timings measure the cost of the new coverage. They do not establish a gain
for an entire native pipeline, and every standalone workload here is slower than Flink. The
bridge is retained to allow composition with existing native operators; future performance
claims require measuring that complete pipeline. Reproduce with `ScalarFunctionBenchmark`
under `-Pbench`, `SF_BENCHMARK=true`, `-Dscalar.rows=200000 -Dscalar.bytes=64` and
`-Dscalar.warmup=1 -Dscalar.runs=3`, selecting the corresponding `scalar.functions` names.

## Flink 1.18 compatibility

The 1.18 development build uses the shared native SQL/JSON reader with its verified Jackson
2.14.2 profile on JDK 11 or 17. Decimal JSON constructors retain the whole-Calc JVM route, once per
Arrow batch, to preserve that release's decimal spelling. See
[Flink line compatibility](../flink-compatibility.md) for host-only syntax differences.

### Legacy ENCODE result representation

On Flink 1.18, SQL `ENCODE` reports `BINARY(1)` despite returning variable-length bytes.
The shared generated Calc evaluator keeps its original expressions and external schema, but
uses Arrow Binary for affected scalar output columns. This preserves charset resolution,
nulls, byte arrays, casts and row/projection evaluation order without changing ordinary fixed
BINARY columns. Final projections and fused scalar consumers accelerate; an affected fixed-width
result crossing into another operator or a native sink falls back before execution. Casting the
result to BYTES establishes a normal variable-length boundary.

## Flink 1.18 mixed timestamp comparison compatibility

The installed planner supplies the mixed `TIMESTAMP`/`TIMESTAMP_LTZ` comparison coercion
missing from Flink 1.18's code generator. It casts the lower-precision operand to the other
operand's type; equal precision casts the left operand to the right type. Existing casts
introduced by SQL validation are retained, including their precision and timezone semantics.
The cast uses Flink's session timezone handling and preserves NULL behavior, including
`IS DISTINCT FROM` and `IS NOT DISTINCT FROM`. Precision 0, 3, 6, and 9 and DST overlap inputs
are covered by SQL parity tests.

This compatibility normalization also applies when an installed planner has native
substitutions disabled, so rowwise Flink execution can run the same mixed comparison.
The Flink 2.2 line uses its released code generator's existing coercion.

### Exact DOUBLE TRUNCATE

`TRUNCATE` with a DOUBLE operand/result and an optional INT scale uses a generated evaluator
through the existing Arrow-batch JVM scalar bridge. It borrows the imported Arrow rows, avoiding
reflective argument-column materialization. A bounded shortcut brackets the scaled value using outward-rounded adjacent
doubles. Only when both bounds truncate to the same exactly representable integer does it avoid
Flink's decimal conversion. The accepted shortcut domain uses scales -6 through 6 and
`abs(value) <= 1e15 / 10^max(scale,0)`, keeping the coefficient bounded while
allowing larger values at coarser scales. The repeated large-domain whole-job comparisons beat stock and previous StreamFusion;
values below one include a half-unit scale-18 decimal rounding margin in their bounds.
For nonnegative scales, an integral power-of-two-scaled operand additionally proves the value
already has at most the requested fractional decimal digits, so it returns unchanged.
At ambiguous nonnegative scales, an integral decimal-scaled coefficient whose division
round-trips to the input proves a canonical bounded decimal-grid identity. Remaining
ambiguous bounded values use the same canonical decimal text as released Flink, retaining
its scale-18 HALF_UP rounding carry before truncation without constructing decimal objects.
Values outside this bounded signature use released Flink `struncate`. This retains
NULLs, signed-zero results, nonfinite behavior, extreme positions and errors. FLOAT and other
unverified signatures retain their existing gates. An omitted scale is generated as primitive
INT zero without another Arrow argument. No incompatible opt-in is used.

Generated scalar consumers substitute only verified DOUBLE invocations, retaining released
NULL guards and operand evaluation. Whole-row consumers bind verified DOUBLE TRUNCATE calls
to the same helper through Flink's released scalar-function bridge; other overloads and
original expression types remain intact. The released generated Criterion control explicitly
disables this substitution. CASE and COALESCE retain generated row evaluation when this callback can throw.
AND/OR consumers containing verified exact floating calls retain generated row evaluation
inside native Calc. Selected nonfinite operands fail like released Flink; unselected operands
retain its short-circuit behavior. Programs with multiple failing evaluations retain the
existing row-order gate. Nullable COALESCE and nested TRUNCATE samples beat stock and previous production behavior.
Later scale-dependent bounds also improve the measured large-domain consumers. See the [consumer results](../optimizations/host-exact-builtins-upcall.md#generated-consumer-evaluation)
for retained trials and validation limits, and the [scale-dependent bound results](../optimizations/scalar-function-kernels.md#scale-dependent-double-truncate-domain-prototype)
for the later accepted large-domain optimization.

`FollowupExpressionPlanTest` checks runtime-source SQL planning for fixed binary casts,
Boolean ARRAY_DISTINCT, and DOUBLE TRUNCATE including consumer evaluation and exception behavior. It requires native
Calc and both transposes without starting a job. Plan checks supplement, rather than replace,
runtime parity and performance checks. The admitted plans run on each released Flink line.
Binary ELT retains explicit-false runtime fallback witnesses; STRING ARRAY_DISTINCT now has default-enabled
generated-program parity coverage under the accepted collection performance tradeoff. Seven in-process Arrow/JNI ownership tests and three DOUBLE TRUNCATE
helper/admission tests pass on each line.
Two generated-evaluator checks additionally validate omitted and dynamic scales, NULLs, bits
and errors on each released line. Exception-message correctness runs retain
`-XX:-OmitStackTraceInFastThrow`; the `bench` profile normally clears that correctness flag.

Nonfinite DOUBLE TRUNCATE consumer checks compare independent released Flink and native executions, including matching failures when Flink evaluates an otherwise guarded operand. SQL AND/OR optimizations must not be assumed to suppress evaluation.

The recovered release revision passes the complete 54-check selected suite on Flink 1.18.1
with four unavailable-ELT checks skipped, and the corresponding Flink 2.2.1 checks pass.
Eleven DOUBLE TRUNCATE runtime checks and nine Boolean/string ARRAY_DISTINCT checks pass on
both released lines. These are historical correctness results. Binary ELT now uses the
Rust-side functionality gate described below. STRING ARRAY_DISTINCT uses the verified
generated collection route described above; fixed binary casts, Boolean ARRAY_DISTINCT,
and the bounded DOUBLE TRUNCATE helper remain admitted.


The maintainer accepted shipping the verified broader SQL/JSON MAP/MULTISET boundary
on October 16 while deferring performance improvements. Historical MAP whole-job results
remain slower than stock Flink; see the [recorded measurements](../benchmarks/scalar-functions.md#map-sqljson-batch-boundaries-2026-09-19).
This acceptance does not widen nullable-key, fixed-BINARY or non-Calc admission.


### Binary ELT and raw payload boundaries

`streamfusion.experimental.binaryElt` defaults to `true` in planner and task JVMs and admits
verified INTEGER-indexed binary `ELT` arguments/results. Explicit false retains stock execution. Safe selections
use the existing native binary kernel with a length-delimited result. Expressions requiring
released Flink evaluation keep the generated path, including conditional/failing operands.
The unverified BIGINT-index overload remains on Flink. No Rust production kernel changes.

The flag uses Arrow Binary for SQL BINARY(n), because released Flink can carry raw bytes
whose length differs from the declaration. SQL types remain BINARY(n). Existing fixed-width
Arrow results remain readable through this transport. NULL/bounds semantics, selected raw
bytes, literal selection, grouping, object reuse and batch boundaries are covered by
`FlinkVariableBinaryEltSqlHarnessTest` and the existing binary SQL selections. The original
October 16 opt-in evidence is retained below. Default admission now follows the agreed
Rust/Arrow versus Java/RowData functionality gate; historical slower standalone measurements
above remain visible as conversion-inclusive diagnostics.

Nested BINARY checkpoint and separate-JVM checks extend the parent fixtures with raw lengths
0–4097, NULL rows/fields/array elements, all nested RowKinds and mutable callback outputs.
They use memory/nativeRocksDB at parallelism1/3 with repeated completed-checkpoint failures,
and separate planner/worker JVMs with5003 runtime rows. Current validation results are
recorded with this extension; performance is a separate deferred contract.

[The extension evidence](../benchmarks/results/binary-selection-recovery-2026-10-16.json)
records33 focused binary SQL and12 recovery/remote passes on Flink2.2. Its default mode
passes26 checks with4 opt-in method skips. Flink1.18 passes26 with19 expected ELT catalog
availability skips, including all8 recovery and4 remote cases. Across both lines this adds
8 binary recovery and4 binary remote cases alongside the parent STRING cases. All native
worker input counts equal5003, and children exit cleanly. These local correctness checks
use a pinned existing release payload; required CI rebuilds current native code. No duration
is throughput evidence. Combined remote process failure/recovery and inconsistent flags
remain unverified.

### Operator-format performance admission, 2026-10-19

[The admission audit](../benchmarks/feature-admission-2026-10-19.md) excludes perimeter
transposes and compares the same logical BinaryRowData/Arrow inputs. Shared stateful INT,
BIGINT and STRING evaluators still do not establish a repeatable win; temporal identity,
text and epoch UDFs remain slower. Their earlier opt-ins stayed off at that revision.
A mixed temporal direct-evaluation
candidate reduced native time but still lost to Flink and was rejected. Existing diagnostic
execution switches retain their prior contracts. Collection and metadata feature switches
already default on and do not block coverage. The earlier full-job timings remain diagnostic
evidence rather than the small-functionality admission gate.


The maintainer subsequently clarified the gate: small-functionality timing begins inside Rust
with Arrow already imported, while Flink starts in Java with RowData. The earlier Java-wrapper
operator measurements remain diagnostics. Correct UDF execution is explicitly allowed to be
slower, so shared stateful and temporal scalar coverage now defaults on. No faster UDF execution
is claimed. Explicit false overrides, unsupported signatures and semantic safeguards remain.


The default-admission change passes 79 affected checks on each released Flink line (2.2.1 and
1.18.1), covering shared order, balanced lifecycle, failure parity and exact temporal types.
Both explicit false overrides retain stock execution.

Type-preserving planner `AS` labels are compile-time names. The native expression encoder
evaluates their value operand directly when the result type exactly matches it. This also
keeps Table API window-result aliases inside a native island. Labels that change the declared
type do not use this passthrough.

### Binary ELT default admission, 2026-10-20

The verified INTEGER-indexed binary selector now runs without an opt-in. Explicit
`streamfusion.experimental.binaryElt=false` restores stock execution, independently of the
nested-scalar flag. Planner and worker use the same default helper. CI exercises this default
without forcing the flag. The unverified index overloads and released error-order guards
retain their existing routes.

The small-functionality gate uses Flink's production generated Calc on prebuilt BinaryRowData
and StreamFusion's production expression program inside Rust on prebuilt Arrow. Both prewarm
the same 1,024-row prefix outside timing, then process the same logical rows. Timings include
allocation, native execution and equivalent format-native output consumption; input preparation,
initial import/final export and perimeter JNI/transposes are excluded. Full byte values and
INSERT RowKinds are checked separately against native output, including NULL/empty values,
invalid/NULL indices, embedded zero/high bytes and 4,097-byte payloads despite BINARY(4).

Five warmups and nine measured trials produce these medians and measured ranges:

| Rows / nominal byte width | Flink ms (range) | Rust ms (range) | Flink/Rust |
| --- | ---: | ---: | ---: |
| 16,384 / 4 | 1.311 (1.003–2.805) | 0.116 (0.115–0.133) | 11.34× |
| 262,144 / 64 | 12.850 (12.493–31.510) | 2.614 (2.479–2.947) | 4.92× |
| 131,072 / 256 | 15.538 (9.964–34.765) | 2.159 (2.021–2.493) | 7.20× |

All three profiles win by 4.92–11.34×. The previous default used this same stock Calc at the
measured boundary. No kernel optimization was needed to pass the agreed gate. Earlier slower
conversion-inclusive jobs remain above; these operation measurements are not complete-job claims.
[Raw trials, hashes and validation](../benchmarks/results/binary-elt-defaults-2026-10-20.json)
retain every sample and preliminary diagnostics.

Default validation selects 55 unique checks on each released line: all pass on Flink 2.2;
Flink 1.18 passes 36 with 19 expected catalog-availability skips; ELT is absent from that release. Eight recovery and
four separate-worker-JVM cases pass on each line. Explicit false passes 23 checks with four
disabled coverage methods; binary default execution with nested callbacks explicitly disabled
passes seven checks. All 1,104 existing release Criterion fixtures pass. These correctness
checks do not imply faster JVM callbacks.

Reproduce the stock half with `BinaryEltAdmissionBenchmark -Dbinary.admission.export=fixture.arrow`
under `-Pbench` and `SF_BENCHMARK=true`. The compiled release `binary_elt` example accepts
`fixture.arrow 5 9 output.arrow`; `-Dbinary.admission.rustOutput=output.arrow` verifies stock
results against every native output byte before relying on its measurements.


### Map lookup after retained-row operators

Logical MAP `ITEM` retains an explicit map tag when a preceding Top-N, regular join or first-N
operator stores maps as physical Arrow Lists. Literal and dynamic key lookup use the same borrowed
entry offsets/key/value view for physical Maps and tagged Lists; integer map keys cannot be
misinterpreted as one-based array positions. Existing compact timestamp-key and nullable-key
compatibility guards remain in effect. ARRAY subscript behavior is unchanged.

Retained MAP/MULTISET output reaching an unverified native Arrow sink uses the existing row-sink
route. Direct external Java MAP scalar callbacks use the verified generated path below;
retained-row carriage alone does not expand callback conversion coverage.


### Java MAP scalar signatures

The default-enabled `streamfusion.experimental.nestedScalarUdfs` option also admits synchronous
external `java.util.Map` scalar arguments and results. The whole Calc runs as a Flink-generated
program through the existing batch callback, preserving predicate/projection order, shared
function instances, lifecycle, Java failures and declared public conversions. Results are copied
before another invocation can mutate a reused Map; retained external arguments remain owned after
the Arrow batch lease ends. No Rust implementation of the user's Java function is implied.

Verified shapes include nullable string/integer maps, recursively nested ARRAY/ROW/MAP values,
BOOLEAN/DECIMAL, FLOAT/DOUBLE, DATE/TIME, DECIMAL/TIMESTAMP, interval and ROW-key maps. NULL
containers, keys and values, empty maps, signed zero, NaN/infinity, precision-38 decimals,
pre-epoch nanoseconds and nested RowKinds are covered. These are resolved Flink signatures:
nested TIME resolves to TIME(0), and the interval fixture uses INTERVAL MONTH/SECOND(3).
Runtime millisecond TIME values are preserved. This does not admit root DATE/TIME UDF signatures,
fixed BINARY map leaves, arbitrary Java conversion classes or custom type inference.

Scalar results from MAP arguments can feed native aggregation. MAP results can compose with
lookup, CARDINALITY, CASE, predicates and a final row sink, including callbacks lifted above
Top-N. A richer computed MAP cannot enter an unverified state/exchange boundary; this guard
also applies when builtin collection expressions are disabled. Updating retained MAP payloads
still have the independent [correctness restriction](top-n.md#retained-mapmultiset-payload-qualification).
Explicit `nestedScalarUdfs=false` restores MAP-signature fallback. Correct UDF execution is an
accepted performance exception; [job costs](../benchmarks/scalar-functions.md#map-scalar-callback-costs)
include both perimeter transposes and make no standalone acceleration claim.

`TYPEOF` inspects the declared operand type without evaluating its value. A ROW-valued
operand under `TYPEOF` alone does not request whole-Calc ROW materialization or widen another
expression's admission. Composed exact DOUBLE `TRUNCATE` calls register their actual function
definition with Flink's bridging SQL function, preserving runtime implementation discovery.

### Verified native map keys

Within the collection extension, MAP_KEYS evaluates in Rust for total MAP operands
whose keys and values are nullable BOOLEAN, integer or DECIMAL. Parent NULLs,
NULL keys, duplicates and entry order survive as a shared Arrow list view. Other
signatures retain generated Flink evaluation, including representation-sensitive
nested/string values and fallible arguments. This does not change default
collection admission or the standalone conversion limitations tracked by #392.

MAP_VALUES uses the same verified basic-map admission and shared-buffer view, selecting the value child without changing NULLs or entry order.

MAP_ENTRIES uses the same verified basic-map admission and reuses entry children
as a list of nullable key/value rows, preserving order and parent NULLs. Consumers
that inspect row representation or RowKind retain generated evaluation. A direct
projection creates fresh INSERT-kind entry rows, so it does not require the
whole-Calc ROW metadata callback. This exception applies only to the verified
basic-map signature; nested ROW maps and ROW-valued consumers keep that callback.

MAP_UNION folds verified total basic maps left to right, propagating any NULL
parent. Each nonempty right map emits all its entries first; unmatched non-NULL
left keys are deduplicated, while all left NULL-key entries survive when the
right map has no NULL key. An empty right map leaves the left duplicates intact.
Typed hash membership and Arrow gathering avoid per-entry JVM objects. Variadic,
sliced, empty and duplicate/NULL-key cases have production-kernel fixtures.

Runtime MAP constructors use released Arrow array construction plus the nullable
map carrier for total nullable basic keys/values. Keys must be distinct literal
or input-reference expressions. Runtime equal keys remain separate entries;
repeated/generated key expressions retain Flink evaluation because its constructor
deduplicates generated expressions rather than runtime key values. Fallible
operands also remain generated, including discarded value-expression cases.
Flink 1.18 groups generated key expressions through an unordered map rather than
the ordered grouping used by 2.2. Its multi-entry MAP constructors retain complete
Flink generation so later lookups and array-valued consumers do not assume an
unverified construction order. Verified single-entry constructors remain native;
the existing narrow-integer compilation restrictions still apply.

ROW constructors with total BOOLEAN/integer/DECIMAL fields reuse DataFusion's
released named-struct builder, sharing field arrays. Fields retain their SQL
names and nullable values; the constructed row itself is present with INSERT
row kind. These fresh basic rows and their scalar field projections bypass the
whole-Calc ROW metadata callback. Other fields and representation-dependent row
consumers remain generated; nested ROW arguments still trigger metadata handling.

OBJECT_OF with validated class/field metadata and total basic fields reuses the
released named-struct builder. Runtime values and field order match Flink's fresh
GenericRowData construction; metadata validation still belongs to Flink planning.
String/nested/temporal fields and fallible operands retain generated evaluation.

OBJECT_UPDATE admits only one unshared inline basic OBJECT_OF construction in the
complete Calc, with literal/input-reference field values and literal field names.
It folds the field choices into the released named-struct builder. Flink still
validates field names and rejects repeated update names during planning. Shared objects, runtime structured inputs,
nested updates and non-atomic values retain generated evaluation to preserve
Flink's in-place mutation, aliases, evaluation order and random/UDF effects.

ARRAY_JOIN evaluates total nullable string arrays in Rust, skipping NULL elements
in its two-argument form and propagating an explicit NULL replacement in its
three-argument form. Every input is materialized separately to UTF-8 bytes before
joining, as in Flink concatWs. Empty/all-skipped arrays yield an empty string.
Runtime separators/replacements and isolated UTF-16 source surrogates have parity
fixtures. A Calc that also projects original sensitive values or uses an unverified
string/collection consumer retains generated evaluation, preserving their Java
representation. Other array element types/nullability and fallible operands stay
generated. DataFusion's explicit NULL replacement skips elements, so it is not a
direct substitute for this Flink kernel.

ELEMENT preserves the selected released line's exception: Flink 1.18 throws
java.lang.RuntimeException for multiple elements, whereas Flink 2.2 throws
TableRuntimeException. The compatibility profile selects that native variant;
empty, NULL and singleton arrays keep identical values on both lines.

Flink 1.18 ELEMENT over TINYINT/SMALLINT retains stock fallback: the released
host's narrow-result conditional does not compile in Janino. The parity tests
retain this host compilation failure rather than asserting a successful value.

Flink 1.18 ARRAY constructors with TINYINT/SMALLINT elements keep stock fallback:
the released generated array initializer calls narrow writers with int literals
and fails Janino compilation. Tests compare that released initialization failure;
Flink 2.2 retains native construction for these verified element types.

Flink 1.18 MAP constructors with TINYINT/SMALLINT keys or values keep stock
fallback because the released binary-array initializer does not compile. Direct
map inputs and map projection kernels retain their independently verified narrow
type support. Flink 2.2 retains native basic MAP construction.

Flink 1.18 ROW constructors containing TINYINT/SMALLINT fields keep stock fallback
for the released narrow-writer initialization failure. The SQL tests assert that
failure for the constructed row and field-access forms; Flink 2.2 still evaluates
verified basic rows in Rust.

Native map-producing collection expressions retain the row-sink requirement. Their
nullable-key map-list carrier is an internal batch representation, not a verified
Parquet MAP encoding; a native sink must not consume it as an ordinary Arrow MAP.

Native ROW-bearing collection results retain the existing row-sink requirement as
well. Arrow field sharing inside Calc does not widen connector admission for Java
row representation, RowKind, or nested copy semantics.

Unary MAP_UNION accepts an ordinary map or the nullable-key map-list carrier and
returns the verified carrier by sharing entry children, offsets and validity.
This preserves the declared native type as well as Flink's unchanged contents.
