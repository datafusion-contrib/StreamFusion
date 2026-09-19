# Calc / filter

**Status:** native, with gaps — the largest admission surface in the project.

`Calc` (Flink's fused projection + filter node) and standalone filters are where per-row expression
evaluation happens, so their admission is expression-level rather than shape-level: the operator
itself always has a native form, but it only runs natively if **every** expression node inside it —
every function call, cast, and operator in the projection and the predicate — is one the native
expression engine admits. One un-admitted node anywhere in the `Calc` declines the whole node,
which (per the [all-or-nothing island rule](index.md#the-all-or-nothing-island)) drags the entire
query back to Flink, not just that node.

`Calc` is also one of the changelog-aware operators (alongside `GROUP BY`, the regular join, a CDC
source, `UNION ALL`, `Expand`, and changelog normalize) exempt from the insert-only guard — a
retracting/updating input doesn't disqualify it by itself.

The rest of this page is the exact admission list: what's unconditionally native, what's native by
default via a JVM upcall (and why that's not a fallback), what's opt-in, and what's a straight
fallback.

- **Unsupported function/operator** outside the admitted set normally declines the whole Calc.
  A [SQL/JSON Calc](#sqljson-evaluation) can instead use Flink generation for its complete program,
  subject to the host code generator and the verified batch bridge types.

## COALESCE

`COALESCE` retains the first non-NULL operand without evaluating it again. When an operand
contains a scalar UDF or a volatile expression, the complete COALESCE expression uses Flink's
generated code through the existing columnar JVM bridge. This preserves call counts, nullable
results and Flink's evaluation of later operands inside native Calc, including failures from
operands hoisted by host code generation. Pure expressions retain their
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
The admitted result types are numeric, character, DATE, TIME, plain TIMESTAMP and binary.
BOOLEAN, TIMESTAMP_LTZ and complex result types retain fallback because their IF overloads
are not registered by the released Flink code generator.

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
zero, infinities, NaN and NULL. Integer and DECIMAL unary negation retain their existing fallback.

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

## User scalar functions

Table API plans that retain an `AS` wrapper around a scalar expression currently fall back with
`Calc: unsupported function/operator: AS`. This includes upstream constructor-state, inline and
non-static object UDF projection fixtures; ordinary SQL aliases that the planner removes are
unaffected. The execution audit pins both these host routes and the native routes for rich UDF
job parameters, combined functions and code-generation splitting. Alias admission remains part
of the [coverage backlog](https://github.com/datafusion-contrib/StreamFusion/issues/110).

Java `ScalarFunction` calls use the existing columnar JVM bridge: Arrow argument columns enter
the function once per batch and an Arrow result column returns to the native island. Supported
external Java types are `String`, boxed/primitive numeric and boolean values, `BigDecimal` for
`DECIMAL(p,s)`, and `byte[]` for `VARBINARY`/`BYTES`. Overload resolution must be unambiguous.
Fixed-size `BINARY`, temporal UDF signatures, collections, and alternate Java conversion classes
outside these mappings retain their existing fallback.

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
VARBINARY, DECIMAL, date/time/timestamp and interval values. Collection and nested ROW inputs
or outputs still fall back, as do unsupported UDF signatures and specialized functions. Temporal
columns and builtin expressions are supported here; temporal **user-function signatures** retain
the restriction above. The path preserves columnar operator boundaries and avoids a separate
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

Non-deterministic scalar UDF instances shared by multiple independently evaluated expression
nodes fall back with a per-row invocation-order diagnostic. This covers separate projections,
predicate/projection sharing, nested calls and CASE branches: column-at-a-time evaluation can
otherwise change a function's state before another call observes it. The identity is Flink's
function identifier, as used by generated expressions. Repeated calls contained in one complete
generated expression, including a complete Calc fused for multiple binary calls, remain native
because that callback preserves row order. Single calls,
independent instances and deterministic scalar functions retain native admission. Builtin random
and clock functions are unaffected. Tests compare values and filtered rows across native batches
and retain lifecycle checks on both native and fallback paths.

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

A non-literal or NULL `SHA2` bit length, and other bit lengths, are not admitted. A dynamic bit length
falls back; literal values are checked exactly, including `BIGINT`, without truncating to 32 bits.
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
BOOLEAN-to-string and BOOLEAN TRY_CAST remain unsupported.

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
Flink. Other TRY_CAST pairs, except the DECIMAL forms below, still fall back. Bare encoders without table configuration
decline mode-dependent casts. See the [kernel ledger](../optimizations/scalar-function-kernels.md)
for the release benchmark against the previous host-cast path.

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

The upcall casts **decline** — i.e. fall back to Flink entirely — when the deprecated
`table.exec.legacy-cast-behaviour` is enabled, since its null-on-failure semantics differ from the
default cast the upcall reproduces.

Generated cast executors initialize with a valid value of the declared input type, including
`CHAR`/`VARCHAR NOT NULL`. Startup does not evaluate an illegal NULL input; actual malformed
runtime values still fail according to Flink's cast rules.

### Still falling back

Boolean-to-string casts and other pairs not listed above. Temporal casts now use Flink-generated
expressions; see [temporal functions](temporal-functions.md).

## Decimal arithmetic

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
CASE can skip an unselected failing branch. Runtime scale columns and BIGINT scale arguments
retain an explicit planner fallback; float/double ROUND keeps its existing compatibility gate.
Flink 2.2.1 itself can fail when a runtime scale changes the returned DecimalData precision;
the regression suite preserves the resulting binary-writer assertion failure through fallback.

`TRUNCATE(decimal_column[, literal_integer_scale])` uses the same native fixed-width
kernel with rounding toward zero. Positive, zero and negative positions preserve Flink's
resolved precision/scale and NULL behavior; a position at or above the source scale retains
the input value. Positions below -38 use Flink's generated expression through the columnar
callback, including its extreme-scale errors. They retain the same AND/OR short-circuit
restriction as ROUND. Integer and floating-point inputs are not admitted for TRUNCATE;
Flink 2.2.1 rejects nonliteral TRUNCATE positions during validation. SQL tests cover
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
when decimal arithmetic overflows. Its downstream constraint enforcer then fails
the job on that NULL; the native path preserves this failure as well.

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

Two character arguments, literal or column. Matches a literal prefix, including Unicode and
empty strings; any NULL argument returns NULL. Wildcard characters have no special meaning.
Binary operands fall back.

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
VARBINARY literals are native; fixed-size BINARY literals retain the literal encoder's fallback.
FROM_BASE64 falls back.

### UNHEX

Character inputs produce BYTES. Either hex letter case is accepted; invalid bytes, whitespace, `0x` prefixes, and non-ASCII digits return NULL. Empty input produces empty bytes. Flink validates but discards an odd leading digit, emitting zero: `UNHEX('A') = 00`, `UNHEX('ABC') = 00 BC`. Folded VARBINARY constants carry bytes directly as typed binary literals, including empty values and NULLs; fixed-size BINARY literals retain the existing fallback.

### GREATEST

Integers, BOOLEAN and matching-precision/scale DECIMAL are native, with strict NULL propagation. Strings require ASCII literals or CASE results composed entirely of ASCII literals. Unrestricted string columns fall back: Flink uses UTF-16 order for Java-backed strings and byte order after binary materialization. Floating point and mixed decimal scales fall back.

### LEAST

Uses the same type and ASCII-proof gates as GREATEST, with strict NULL propagation and minimum comparison.

### INITCAP

Character strings only. Only ASCII letters and digits form words; every other character separates words and is preserved. NULL returns NULL. This follows Flink rather than DataFusion word boundaries.

### TRANSLATE

Three character arguments. Mappings use Unicode codepoints, not graphemes. The first duplicate mapping wins, but duplicates consume target positions. Missing target characters delete; a NULL target acts as empty. NULL/empty `from` leaves the source unchanged. A NULL source returns NULL.

### BTRIM

One-argument space trimming and two-argument character-set trimming with a literal set are native. Empty sets preserve the input and NULL propagates. Column trim sets fall back because Flink can change their meaning after an exchange when the first set character is a space.

### TRIM

The SQL `TRIM([BOTH | LEADING | TRAILING] [characters] FROM s)` forms are native with
a literal trim set, including Unicode, empty and NULL sets. The default set is the ASCII
space, not all whitespace. These forms reuse BTRIM/LTRIM/RTRIM's character-set kernels.
Column trim sets fall back for the same Flink representation-dependent behavior as BTRIM.

### ELT

An INTEGER index and character alternatives are admitted. The index is 1-based; out-of-range and NULL indices return NULL. Only the selected alternative's NULL matters. Other index types and binary alternatives fall back: Flink casts its boxed index to Integer after its bounds check. Explicit casts to INTEGER follow the existing cast rules.

### URL_ENCODE

Character strings use Java form encoding: space becomes `+`, ASCII alphanumerics and `-_. *` are preserved apart from space, and other UTF-8 bytes use uppercase percent escapes. NULL returns NULL.

### OVERLAY

Character strings and integer positions, widened to BIGINT without losing bits. Preserves Java UTF-16 positions, length narrowing/overflow, and substring errors. Non-positive or beyond-end starts return the source; zero/negative lengths omit the suffix. Split surrogate pairs encode as `?`, like Flink. Any NULL argument returns NULL.

### URL_DECODE

One character argument is native. Form decoding preserves JDK UTF-8 replacement grouping and returns NULL for malformed escapes. The planner selects the runtime JDK rule: JDK 17/21 (and pre-25 runtimes) use Integer.parseInt, accepting signed one-digit escapes and BMP Unicode hex digits; JDK 25+ uses ASCII-only HexFormat rules.

The JDK rule is selected on the JobManager during planning, so the JobManager and TaskManagers must use the same URL-decoding rule (pre-25 or 25+); mixed JDK groups can produce results that differ from Flink on the TaskManager.

### ENCODE

Character input and a literal UTF-8, US-ASCII, ISO-8859-1, UTF-16, UTF-16BE, or UTF-16LE
charset (including JDK aliases). Returns BYTES, preserves NULL, and replaces unmappable
characters with `?` in ASCII/Latin-1. UTF-16 emits a big-endian BOM for non-empty strings;
UTF-16BE/LE emit no BOM. Empty strings produce empty bytes in all six charsets.
Other or dynamic charsets fall back.

### DECODE

Binary input and the same six literal charsets as ENCODE. UTF-8 uses the JDK's replacement
grouping for malformed sequences; ASCII replaces each non-ASCII byte; Latin-1 maps all bytes.
UTF-16 detects and consumes an initial BOM, defaulting to big-endian without one. UTF-16BE/LE
use their fixed byte order and retain the BOM as a character. Malformed surrogate pairs and
odd trailing bytes follow JDK UnicodeDecoder grouping, including consuming a high surrogate
and a following non-low code unit together. NULL stays NULL. Other or dynamic charsets fall back.

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
date/time, interval, supported timestamp types, and recursively nested ARRAY/ROW values with
those leaves. Nested arguments use Flink internal views over the imported Arrow batch; generated
results are copied into owned Arrow output vectors before that batch closes. The callback still
crosses JNI once per batch, including multi-column results and filtering.
MAP/MULTISET boundary values, including maps nested inside an array or row, remain explicit
fallback. Constructing containers internally is allowed when the resulting boundary types are
admitted. Unsupported host code generation
and UDF signatures retain explicit fallback. Flink 2.2.1 rejects dynamic JSON_EXISTS paths;
that host failure is preserved. No configuration opt-in is required.

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
Admission first probes the shaded Jackson runtime once per class loader: version 2.18.2,
the default thread-local recycler pool, and successful buffer acquisition, cross-factory
reuse and release are required. Missing methods/classes, a different version or pool,
or probe failure decline the native parser for JSON_VALUE, JSON_EXISTS and IS JSON; Calc
can use Flink generation when the host runtime and batch boundary types support it.
JobManagers and TaskManagers must use the same verified shaded Jackson runtime.
They currently admit JDK 17, 21, 24 and 25, selecting the corresponding Unicode version for
Jackson's token-termination rules; other JDKs use Flink generation in Calc. The profile is selected on the
JobManager, so TaskManagers must use the same JSON parsing rules. Jackson's resource limits
(1000 nesting levels, 1000 number digits, 20 million UTF-16 string units, 50,000 member-name
units) also apply to unselected values. Its numeric boundary has a buffer-dependent exception:
the slow parser can accept an extra digit. Native evaluation uses the task thread's actual
Jackson input-buffer capacity and preserves its growth, including invalid input and SIMD
parsing. A batch exchanges this capacity through JNI; documents and results remain native.
See the [SQL/JSON parser note](https://github.com/datafusion-contrib/StreamFusion/blob/main/divergences/32-sql-json-definite-paths.md)
and [per-function benchmarks](../benchmarks/scalar-functions.md).

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

Character separators and TINYINT/SMALLINT/INTEGER indices may be dynamic. Indices are zero-based; negative/out-of-range indices, empty input, or any NULL produce NULL. Whole separators preserve empty tokens. An empty separator uses Java Character.isWhitespace, including tabs and line separators but excluding non-breaking spaces. Numeric separators and BIGINT indices fall back.

### Temporal parsing, extraction and rounding

`TO_DATE`, `TO_TIMESTAMP`, all `TO_TIMESTAMP_LTZ` overloads, calendar fields, and temporal
`FLOOR`/`CEIL` now have expression implementations. Formatted parsing and LTZ calendar fields use
Flink's own generated code. Timestamp results retain the complete millisecond/fraction pair and run
by default. See the complete [temporal function inventory](temporal-functions.md).

### LTRIM

One-argument space trimming and two-argument trimming with a literal Unicode character set are native. Empty sets preserve the input; NULL propagates. Dynamic trim sets fall back because Flink semantics depend on whether strings are Java-backed or binary-backed.

### RTRIM

Uses the same literal-set gate as LTRIM, trimming from the right. Dynamic trim sets fall back; one-argument space trimming is native.

## Case folding & regex

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

## Opt-in math

**Off by default, native only under `-Dstreamfusion.expression.<NAME>.allowIncompatible=true`** (or
the blanket flag): `EXP`, `LN`, `SIN`, `COS`, `TAN`, `ASIN`, `ACOS`, `ATAN`, `LOG10`
(last-ULP libm divergence from Java's `Math`), and float/double `ROUND` (`BigDecimal`-based
rounding in Flink vs. binary-float rounding natively).

These remaining functions fall back to Flink by default and only run natively once you've opted
in and accepted the (typically last-bit) divergence. POWER's generated JVM path does not widen
their admission.

## Literal/arity guards

A number of otherwise-admitted functions decline when called with an argument shape the native
implementation can't handle, even though the function itself is supported:

- An **unsupported literal type** anywhere in the expression.
- **`TRIM`** — dynamic trim sets; all directions with literal sets are native.
- **`POSITION`** — a `FROM` start offset.
- **`SPLIT_INDEX`** — the numeric separator overload.
- **`CURRENT_WATERMARK`** — requires a Calc watermark context; unsupported in standalone join or UNNEST residuals.
- **Collection subscripts:** non-INT ARRAY indexes and literal indexes below one; runtime MAP keys
  of floating, collection or mismatched types; nullable non-compact decimal/timestamp MAP keys.
  See the [collection contract](#collection-subscripts).
- **Wrong arity** for any otherwise-admitted function.

See [Configuration](../configuration.md) for the full `allowIncompatible` flag surface referenced
throughout this page.

## Flink 1.18 compatibility

The 1.18 development build disables unverified Jackson buffer emulation and runs SQL/JSON through
the whole-Calc JVM route, once per Arrow batch. Decimal JSON constructors use that route as well.
`ENCODE` stays on Flink because that release declares `BINARY(1)` for a variable-length result;
UTF-8 and UTF-16 fallback tests retain the complete host bytes. See
[Flink line compatibility](../flink-compatibility.md) for host-only syntax differences.
