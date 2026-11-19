# Calc coverage by topic: strings

[Topic index and original links](calc-filter.md). Sections retain their recorded scope, dates and evidence.

## LIKE ESCAPE and SIMILAR TO {#like-escape-and-similar-to}

Direct input/literal LIKE ESCAPE and SIMILAR TO calls have a certified Rust batch path.
The certificate requires ASCII inputs whose original Java UTF-16 cache agrees with the
Arrow bytes. LIKE admits valid one-character ESCAPE values; SIMILAR admits ordinary literals,
`%`, `_`, `.`, anchors, grouping, alternation and simple quantifiers. Character classes,
POSIX names, counted/adjacent quantifiers, special Java groups and special SIMILAR escape characters
(`[]()|^-+*_%?{}:.$` and backslash) retain Flink evaluation. `%` includes
newlines, whereas `_` and `.` follow Java's line-terminator exclusions.

An uncertified batch runs the retained Flink evaluator, including its invalid-pattern failures.
Original UTF-16 sidecars travel with those arguments. AND/OR consumers remain fused so
short-circuited rows do not evaluate invalid patterns. Empty filtered batches do not evaluate
projections. Existing two-argument LIKE admission is unchanged.

The retained evaluator preserves original literal operands and Flink's constant-pattern
specializations, including its distinct constant versus dynamic empty-ESCAPE behavior.
Released Flink 2.2 accepts the verified literal empty-escape forms; 1.18 rejects them during
planning. The regression compares independent executions and checks that same 1.18 rejection.

[Prepared kernel and complete-job measurements](../benchmarks/native-certified-scalars.md)
retain the stock and previous-implementation controls and their timing limits.


## COALESCE {#coalesce}

`COALESCE` retains the first non-NULL operand without evaluating it again. When an operand
contains a scalar UDF, a volatile expression, or a known potentially failing native expression
(such as a strict STRING-to-INT cast), the complete COALESCE expression uses Flink's
generated code through the existing columnar JVM bridge. This preserves call counts, nullable
results and Flink's evaluation of later operands inside native Calc, including failures from
operands hoisted by host code generation. This includes casts hoisted from later operands
even when an earlier operand is non-NULL. Other pure expressions retain their
existing native CASE lowering. Runtime tests cover INT/STRING stateful UDFs, predicates, nested
expressions, seeded random calls and multiple batches, including NOT NULL output constraints.


## IF {#if}

`IF(condition, then_value, else_value)` is admitted by resolved built-in identity. A true
condition selects the first value; false or NULL selects the second. Pure branches whose
types already match Flink's result type use the existing native searched-CASE expression,
so unselected branches are not evaluated. Empty filtered batches skip the projection.

Mixed integer/DECIMAL and DECIMAL branches also use native selection when both branches are direct
inputs/literals, fit the resolved DECIMAL integral capacity, require no scale reduction and resolve
to a noncompact DECIMAL result (precision greater than 18).
The typed Arrow selector converts only each row's selected value to that resolved precision/scale.
It avoids filtered input batches, separate cast columns and a result merge. Unlike GREATEST/LEAST,
released IF checks the selected branch's NULL flag before conversion, so verified nullable
DECIMAL metadata changes remain native here. Typed NULLs and TRUE/FALSE/NULL conditions retain
their branch selection. The [qualification](../benchmarks/scalar-functions.md#mixed-exact-if)
records stock/actual-previous/native measurements and complete jobs with both transposes.

Mixed normalization to a compact result retains generated evaluation: even the fused selector
remains slower than released Flink for the measured compact control. Matching-type compact
branches continue to use the existing native CASE path.
Other branch normalization, scalar UDFs and volatile calls use the existing generated-expression
bridge inside native Calc. Computed/unverified mixed branches, narrowing and scale reduction
retain generated evaluation.
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


## IFNULL {#ifnull}

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


## UUID {#uuid}

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


## RAND and RAND_INTEGER {#rand-and-rand_integer}

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
zero, infinities, NaN and NULL. Verified integer and DECIMAL unary negation also runs in Rust;
the [exact unary coverage](calc-filter-numeric.md#exact-unary-and-integral-functions) describes the admitted producers
and the composed expressions that retain Flink-generated evaluation inside native Calc.

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


## Type and database metadata {#type-and-database-metadata}

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


## String ordering {#string-ordering}

General relational character-string comparisons (`<`, `<=`, `>`, `>=`) fall back, including
inside CASE and casts. One direct comparison is proven representation-independent: both
operands are qualified primitive ASCII PRINTF results or ASCII/NULL character literals, with
at least one formatter. The six built-in `=`, `<>`, `<`, `<=`, `>`, `>=` comparisons then use
native kernels, including native filters; arbitrary STRING inputs and composed consumers
retain their existing guards. Built-in operator identity is required.

Flink compares retained Java strings by UTF-16 code units,
but serialized strings by UTF-8 bytes. These orders disagree for some supplementary Unicode
characters. Arrow retains the UTF-8 value without the Java-representation information needed
to select Flink's order, so admission cannot safely depend on the column's SQL type alone.
Outside the proven formatter subset, the gate also applies to ASCII/BMP inputs and serialized
sources because the planner cannot prove every runtime operand's representation and range. Equality and inequality
(`=`, `<>`) remain native; this gate does not change Top-N's serialized sort order.


## String concatenation and hashes {#string-concatenation-and-hashes}

These functions run entirely in Rust by default, in projections, predicates, and nested expressions.
The ordinary text path requires valid UTF-8; arbitrary Flink string bytes have the narrower
verified transport described below ([remaining coverage](https://github.com/datafusion-contrib/StreamFusion/issues/622)).

| Function | Native behavior and admission |
|---|---|
| `CONCAT(s, ...)`, `s \|\| t` | Character-string arguments; any NULL argument makes the result NULL. Empty strings are preserved. |
| `CONCAT_WS(separator, ...)` | A literal or column separator; a NULL separator makes the result NULL. NULL values are skipped, empty strings are preserved, and no values or all-NULL values produce an empty string. |
| `MD5(s)` | Lowercase hexadecimal MD5 of the string's UTF-8 bytes; NULL input produces NULL. |
| `SHA1(s)` | Lowercase hexadecimal SHA-1 of the character string's UTF-8 bytes; NULL input produces NULL. |
| `SHA224(s)`, `SHA256(s)`, `SHA384(s)`, `SHA512(s)` | Lowercase hexadecimal SHA-2 of the UTF-8 bytes; NULL input produces NULL. |
| `SHA2(s, bit_length)` | Literal 224/256/384/512 retains the fixed-width kernel. Direct STRING/INT inputs or literals also use a verified runtime-width kernel; a NULL argument returns NULL. |

A direct runtime INT width dispatches 224/256/384/512 to the existing RustCrypto digests.
`CAST(NULL AS INT)` is admitted and produces NULL without discarding evaluation of the
source expression. Non-NULL invalid runtime widths retain the released Flink evaluator and
JDK unsupported-algorithm exception. Computed arguments and other resolved runtime widths
retain generated evaluation; there is no new switch. Hashing reads the original UTF-8 bytes,
as Flink's BinaryStringData hash does, so a decoded Java string cache does not change the
digest. Literal values are checked exactly, including `BIGINT`, without truncating to 32 bits;
unsupported non-NULL literal widths retain their existing fallback.
For example, `SHA2(s, CAST(4294967520 AS BIGINT))` falls back and retains Flink's unsupported-algorithm
failure instead of being treated as SHA-224. Flink 2.2 does not expose hash
overloads with an explicit character set. Binary/collection concatenation is outside this
character-string admission.

`CONCAT` computes Flink's strict NULL propagation from the input validity bitmaps, without
re-evaluating its arguments. Batches without NULL results use DataFusion's kernel directly; batches
with NULL results append only surviving rows. The masked path builds from whole valid strings;
the delegated path retains DataFusion's output validation. The Utf8 `CONCAT_WS` path delegates to DataFusion.
Hashes use the released RustCrypto libraries used by
DataFusion and Comet, writing lowercase hex directly into presized UTF-8 Arrow buffers without intermediate
binary or string-view columns or per-row heap allocations. See [string copy reduction](../optimizations/string-copy-reduction.md).
SQL parity tests cover NULLs, empty strings, embedded zero bytes, Unicode, long inputs, nested
calls, filters, valid BIGINT widths, runtime/NULL widths, conditional and empty-batch behavior,
and oversized bit-length fallback. The runtime kernel has a compiled-Calc no-callback witness;
see [release measurements](../benchmarks/scalar-functions.md#runtime-sha2-widths).

### Byte-preserving string input {#byte-preserving-string-input}

A streaming native Calc can carry STRING inputs as Arrow Binary when every consumer has a
verified byte contract or a compatible text path. Supported byte operations include direct
projection, `IS NULL`,
`IS NOT NULL`, `CONCAT` / `||`, `CONCAT_WS`, `MD5`, `SHA1`, `SHA224`, `SHA256`,
`SHA384`, `SHA512`, `TO_BASE64`, and `ASCII`.
Nested concatenations, runtime separators, character constants and simple
independent numeric projections/filters are supported. These hashes consume the original bytes and return
valid ASCII hex. Untyped NULL arrays and literals also produce typed STRING NULL results, including
empty batches. TO_BASE64 also consumes the original bytes and returns valid ASCII. These results
can feed text predicates or another expression within the Calc.
ASCII reads only the first byte and sign-extends it like Flink's Java byte; empty strings yield zero
and NULL remains NULL. Its integer output can also be used in a native filter over malformed input.
`CHAR_LENGTH` / `CHARACTER_LENGTH` and two-argument `POSITION(needle IN s)` have
specialized native kernels for byte-backed strings. An ASCII length batch uses the Arrow
offsets directly. Other inputs use Unicode code points for valid values and Flink's
leading-byte traversal for malformed ones. Malformed `E0 51 51`, for example, has length 1,
and neither `51` is a POSITION match because neither begins a character in Flink.
An empty search string has position 1, a miss has position 0, and NULL propagates.
SHA2 with a literal width of 224, 256, 384 or 512 lowers to the matching digest and has the same byte
contract. Runtime, NULL and invalid SHA2 widths retain their existing representation and error path;
they are not included in this arbitrary-byte admission.
Raw concatenation output still requires a byte-capable parent or the final row exit.
The planner selects this representation before execution; the schema does not change between
batches. `EXPLAIN` identifies the input columns with `byteStrings=[...]`. There is no new setting.

The SQL type remains STRING. The transpose copies the original `StringData` bytes into a Binary
vector marked `streamfusion.logical_string=1`; the native concatenation kernels consume bytes directly,
preserve their NULL rules, and return Binary to the final row exit without scanning strings for UTF-8.
The exit can borrow Binary-backed string bytes for synchronous consumers,
with the same ownership rule as Utf8. CONCAT output is byte-backed without a Java string cache,
matching Flink's concatenation helpers. `CONCAT_WS` returns NULL only when its separator is NULL;
it skips NULL values but retains empty values and their separators. A separator alone, or only
NULL values after it, produces an empty string for a non-NULL separator.

For example, a runtime STRING containing `FF` produces `FF21` for `CONCAT(s, '!')`.
Truncated `41E4` produces `41E421`; neither is replaced with `EFBFBD`.
Tests compare raw bytes with released Flink and require positive NativeCalc consumption, including
multiple batches, NULLs, embedded zeros, Unicode, nested CONCAT and all-filtered batches.

At an unverified row ingress, the planner tracks byte-backed STRING columns through compatible
Calcs and native filters, watermark and mini-batch assigners, UPDATE_BEFORE filters, and UNION ALL
when every branch has the same byte-backed column positions and no surviving passthrough can
expose a Java cache across the union. A numeric filter can forward
a STRING unchanged. A projection can drop a byte-backed column or consume it in an operation
that produces a different representation, such as ASCII. The planner can then admit other
native operators that only see the resulting ordinary Arrow columns. Surviving byte-backed
STRING columns can leave through the final row exit. Passthrough projections
preserve the original bytes and their optional Java UTF-16 caches. A filter that projects the
same column under multiple names stores those names separately from the deployed filter's
existing serialized configuration.
`FROM_BASE64` can also read this representation in final projections. Its decoded result and
wider compositions follow the existing [FROM_BASE64](#from_base64) rules.

### On-demand UTF-8 for text expressions {#on-demand-utf8-for-text-expressions}

Streaming Calc uses the same UTF-8 adapter for supported text expressions over byte-backed
scalar input columns, including predicates and nested expressions. The planner identifies
which input bytes reach expressions that require text. Passthrough and byte-only inputs
can remain Binary. `EXPLAIN` shows `onDemandUtf8=[true]` when the adapter is selected.

A SIMD scan checks the bytes and each value boundary. Consecutive valid rows with no
original Java string cache can reuse the same Arrow buffers with a temporary
`Utf8` schema and run through the existing native Calc. Malformed rows and rows with an
observable Java cache use the generated Flink evaluator over the original bytes. Ranges
run in input order, preserve changelog tags, and write string results as Arrow Binary.
Calcs that require a single generated evaluator across batches use that evaluator directly
on the bytes. Their plans show `byteBackedFlinkEval=[true]`.

The adapter uses the existing expression and boundary checks for nested input, user-defined
functions, stateful operators and compiled plans. See the
[release measurements](../benchmarks/scalar-functions.md#on-demand-utf-8-validation).

Generic Flink row sources can emit arbitrary string bytes, including strings nested in collections.
For unsupported compositions or relational consumers, the planner requires a verified UTF-8
producer at the row boundary or leaves the query on Flink. Further byte-safe consumer coverage
is tracked in [#498](https://github.com/datafusion-contrib/StreamFusion/issues/498).

The implementation shares the existing string writer's direct segment copy and cached Java-string
encoding. Ordinary Utf8 concatenation continues to use its existing kernel. Representation selection
is decided during planning and remains consistent across batches.
See the [prepared and complete-job measurements](../benchmarks/scalar-functions.md#byte-preserving-concat)
for its performance relative to stock Flink and the existing Utf8 path.

### Utf8 concatenation and hash diagnostics {#utf8-concatenation-and-hash-diagnostics}

The earlier Utf8-path throughput diagnostics compare stock Flink against native execution with a
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


## HASH_CODE {#hash_code}

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


## String and calendar functions {#string-and-calendar-functions}

The following list describes admission and fallback. Per-function Flink/native timings and
workload details are on the [scalar function benchmark page](../benchmarks/scalar-functions.md).

Admission requires verified semantics; it does not promise a speedup for every isolated query.
Retained coverage with slower standalone results is a precursor to the concrete optimizations
in this PR: scalar parameter reuse, direct output construction, primitive extrema, and shared
text scans. Cheap expressions can also remain inside a larger native Calc without another
host boundary. This is not a measured whole-query speedup claim. STARTSWITH/ENDSWITH remain
native for this composition benefit; their standalone regressions are documented in the
benchmark results and do not disable otherwise verified expressions.

### STARTSWITH {#startswith}

On released Flink 2.2, binary `STARTSWITH` uses a native byte-comparison kernel for
literal and runtime BINARY/VARBINARY operands, including mixtures of the two. It never
decodes bytes as text. Empty prefixes match every non-NULL input; a longer prefix does
not match, and either NULL operand returns NULL. Fixed-width bytes retain their padding;
the comparison creates variable offsets without copying their values buffer. SQL casts
that produce those bytes retain their separately documented admission rules. Scalar
operands are evaluated once rather than expanded to a full batch. Sliced inputs retain
their offsets and validity; empty filtered batches do not evaluate projections.
These binary overloads are absent from released Flink 1.18 and are not an additional
StreamFusion SQL extension. Binary ENDSWITH uses the same byte contract at the suffix boundary.

Two character arguments, literal or column. Matches a literal prefix, including Unicode and
empty strings; any NULL argument returns NULL. Wildcard characters have no special meaning.

### ENDSWITH {#endswith}

On released Flink 2.2, BINARY/VARBINARY arguments use the native byte-comparison kernel
described under STARTSWITH, comparing the trailing bytes. Empty suffixes, longer needles,
NULLs, arbitrary byte values, fixed padding, scalar arguments and slices follow the same
rules. No generated JVM evaluator is bound for either binary comparison. Released Flink
1.18 does not register these binary overloads.

Two character arguments with the same NULL and type rules as STARTSWITH. Matches a literal
suffix; an empty suffix matches every non-NULL string. Wildcards have no special meaning.

### INSTR {#instr}

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

### LOCATE {#locate}

Both LOCATE(needle, s) and LOCATE(needle, s, start) are native. Character inputs and TINYINT/SMALLINT/INTEGER starts are admitted; BIGINT starts fall back without narrowing. Positions count Unicode codepoints. Empty needles return 1 for every non-NULL start. Zero/negative starts search from the beginning, except INTEGER minimum: start - 1 wraps to maximum, matching Flink. Out-of-range starts return zero; any NULL argument returns NULL.

### ASCII {#ascii}

Character input returns its first UTF-8 byte widened as a signed Java byte, not a Unicode
code point. Empty strings return zero and NULL propagates. For example, `ASCII('é') = -61`.
The native kernel preserves these rules in projections, predicates and group keys.

### CHR {#chr}

Integer inputs use Flink's low-byte rule: negative values return an empty string;
non-negative values produce the character at `value & 255`, including the NUL character
when the low byte is zero. NULL propagates. All four signed integer widths are native;
`CHR(353)` returns `a`, rather than the character at Unicode code point 353.

### BIN {#bin}

TINYINT, SMALLINT, INTEGER, and BIGINT inputs are admitted. Returns binary digits without leading zeros; zero is `0`. Negative values have 64 two's-complement digits even for narrow input types. NULL returns NULL. Folded string NULL literals retain their declared type.

### HEX {#hex}

Integer and character inputs are admitted. All four signed integer widths preserve Long.toHexString behavior: no leading zeros, uppercase digits, and 16 digits for negative values. Character strings encode their UTF-8 bytes as uppercase hex. NULL returns NULL.

### TO_BASE64 {#to_base64}

Character strings and binary columns are encoded as padded RFC 4648 Base64 without line wrapping.
Strings use their UTF-8 bytes; binary inputs preserve every byte, including invalid UTF-8.
Both overloads share the direct-output encoder. Empty input stays empty and NULL propagates.
VARBINARY and fixed-size BINARY inputs and literals are native, including typed NULLs.
Fixed inputs use their physical bytes: the encoder accepts both length-delimited Arrow Binary
and FixedSizeBinary without changing widths. Non-legacy CAST/TRY_CAST applies the requested
truncation or zero padding before encoding; legacy casts preserve their actual byte lengths,
even when these differ from the declared BINARY width. Binary selection and character-to-binary
casts can feed the same encoder. This does not remove the remaining intermediate fixed-BINARY
boundary restrictions tracked in [#235](https://github.com/datafusion-contrib/StreamFusion/issues/235).

### FROM_BASE64 {#from_base64}

Final scalar `FROM_BASE64` projections decode CHAR, VARCHAR, VARBINARY and fixed BINARY inputs
natively, without a scalar JVM callback. Arrow Utf8 input buffers are viewed as bytes without
decoding them; Arrow Binary and FixedSizeBinary are consumed directly. Empty input stays empty
and NULL propagates. The decoder matches the released JDK basic decoder used by Flink: standard
alphabet, optional padding and accepted nonzero unused tail bits. Whitespace, URL-safe alphabet
characters, malformed units and trailing data after padding raise `IllegalArgumentException`
with the JDK diagnostics, including signed-byte hexadecimal values. Complete groups use the
released SIMD decoder; the last group and invalid-input diagnostics follow Flink's rules.

Decoded STRING values may contain arbitrary bytes. Final scalar STRING projections therefore
travel as Arrow Binary and are read as Flink StringData without UTF-8 normalization. Consumers such as
comparison, length, casts and re-encoding fuse in Flink before Arrow export. Sensitive STRING
results crossing another relational operator, feeding a native columnar sink, or contained in
whole-Calc complex outputs retain fallback rather than exposing invalid Arrow Utf8. Row sinks
can consume the final decoded bytes through the Arrow-to-RowData transpose. Unselected branches
retain lazy generated evaluation; an all-filtered batch never invokes the decoder. These
consumer and relational restrictions remain tracked in
[#498](https://github.com/datafusion-contrib/StreamFusion/issues/498).

### IS_DECIMAL, IS_DIGIT and IS_ALPHA {#is_decimal-is_digit-and-is_alpha}

`IS_ALPHA` and `IS_DIGIT` on character arguments use native Arrow kernels. They return non-nullable FALSE
for NULL or empty input and require every UTF-16 `char` to satisfy the actual worker JDK's
`Character.isLetter` or `Character.isDigit`, respectively. Supplementary letters/digits therefore
return FALSE, as do isolated surrogates, combining marks and replacement characters. `IS_DIGIT`
accepts BMP decimal digits such as Arabic-Indic and fullwidth digits, but rejects signs,
decimal points, superscripts and Roman numerals. These kernels do not use Rust's Unicode version
or code-point classification. ASCII letters/digits have specialized byte fast paths.

Each JVM copies its own 8 KiB BMP property bitmap once per classifier when first compiled;
workers obtain their own tables rather than inheriting the planner JDK's classification.
JNI setup uses a bounded local-reference frame and retains only immutable Rust bytes. Batch
and row evaluation make no JVM calls. Replacing an isolated surrogate with `?` cannot change
these predicates, because both fail the letter/digit tests; existing sensitive-expression and arbitrary
binary STRING boundaries retain their separate admission rules. Already-generated operands
may still contain their own JVM evaluators.
`IS_ALPHA`, `IS_DIGIT` and `IS_DECIMAL` also run natively for direct TINYINT, SMALLINT, INT, BIGINT and
DECIMAL inputs/literals. `IS_ALPHA` returns nonnullable FALSE, including runtime/literal
NULLs. `IS_DIGIT` and `IS_DECIMAL` return nonnullable TRUE for integers regardless of sign, including
runtime/literal NULLs; they return FALSE for internal `DecimalData`, including NULL. Released
Flink boxes primitive defaults for nullable integers and does not treat `DecimalData` as a
Java Number. These are the observed SQL/codegen semantics on both released Flink lines.
The native call keeps the input in the expression. Recursively proven failure-free integer
trees (the bounded integer-rounding producer proof) fold to their type-based result without
evaluating unused intermediates. Division, remainder,
narrowing, UDFs and unverified producers retain generated evaluation of the complete call,
so exceptions and state changes are not discarded. Folded results are not evidence
of executed predicate-kernel work. Conditional branches and empty filtered batches retain
their existing guards. Floating classification signatures and other unqualified object
overloads remain generated.
The [classification measurements](../benchmarks/scalar-functions.md#string-is_alpha)
include stock Flink and the actual previous callback as well as complete row-source jobs.
The [digit measurements](../benchmarks/scalar-functions.md#string-is_digit) cover the separate
digit corpora and preserve small-batch and complete-job costs.
The [numeric alpha measurements](../benchmarks/scalar-functions.md#numeric-is_alpha)
separate predicate execution, bounded folds, tiny batches and full jobs with both transposes.
The [numeric digit measurements](../benchmarks/scalar-functions.md#numeric-is_digit)
qualify that overload separately, including its integer-NULL and internal-DECIMAL behavior.
The [numeric decimal measurements](../benchmarks/scalar-functions.md#numeric-is_decimal)
retain independent predicate, fold, small-batch and complete-job controls.

`IS_DECIMAL` on character arguments also runs in a native Arrow kernel, with nonnullable
FALSE for NULL/empty strings. Its released meaning is successful Java integer, long or
double parsing, not a Decimal SQL type check. Native ASCII lexical parsing accepts signs,
fractions, decimal/hexadecimal exponents, floating suffixes, NaN and Infinity, including
overflow to infinity. Only leading/trailing characters at or below U+0020 are trimmed on
the double path. Non-ASCII integers require every UTF-16 unit to have a worker-JDK radix-10
digit value and the signed result to fit BIGINT, without trimming. ASCII integers beyond
BIGINT still succeed through double syntax, whereas Unicode-digit overflow fails.

The worker snapshots a separate 32 KiB packed radix-10 digit-value table once. No row/batch
JVM calls or floating conversion are needed to answer the predicate. Unqualified
noncharacter overloads retain generated evaluation. Operand failures and conditional/
empty-batch rules remain unchanged. See the [decimal classification measurements](../benchmarks/scalar-functions.md#string-is_decimal).

### UNHEX {#unhex}

Character inputs produce BYTES. Either hex letter case is accepted; invalid bytes, whitespace, `0x` prefixes, and non-ASCII digits return NULL. Empty input produces empty bytes. Flink validates but discards an odd leading digit, emitting zero: `UNHEX('A') = 00`, `UNHEX('ABC') = 00 BC`. Folded binary constants carry bytes directly as typed binary literals, including fixed BINARY values and typed NULLs.

### GREATEST {#greatest}

Integers, BOOLEAN and matching-precision/scale DECIMAL retain their Rust kernels and strict NULL
propagation. Mixed exact-numeric GREATEST also uses explicit casts to Flink's resolved DECIMAL
type before the existing extrema kernel when every argument is a direct input or literal,
integer capacity is sufficient and no DECIMAL loses scale. A DECIMAL argument whose metadata
changes must be NOT NULL: released Flink can dereference it before checking its NULL flag.
Nullable DECIMAL metadata changes, scale reduction and unverified producers retain generated
evaluation. Verified ASCII primitive-text GREATEST/LEAST use the Rust subset below. Other
character results, mixed exact numerics, TIMESTAMP and TIMESTAMP_LTZ use
Flink-generated expressions through the batch JVM bridge, preserving coercions and nanoseconds.
Runtime strings use that bridge when the Calc reads an external DataStream whose conversion
produces Java-backed strings. Unknown or binary-backed input representations retain fallback:
Flink can use UTF-16 ordering before serialization and byte ordering afterward. Floating-point
extrema retain fallback.

[Release qualification](../benchmarks/scalar-functions.md#mixed-exact-greatest) retains
stock/previous/native comparisons, slower tiny batches and complete jobs with both transposes.

The genuine built-in also executes in Rust when every character operand is an ASCII/NULL
literal, a total verified integer/BOOLEAN cast to unbounded STRING, qualified primitive
ASCII PRINTF (2.2), or qualified locale-independent UPPER/LOWER of those producers. Both
modern and legacy cast spellings are preserved. Matching Boolean-text casts select their
strict Boolean extremum before formatting once; mixed casing keeps text comparison.
The typed UTF-8 kernel propagates any argument NULL and shares buffers for uniform winners.
This proof does not admit bounded/unverified/fallible producers, ordinary source strings,
Unicode literals, or same-name catalog functions. The
[primitive-text qualification](../benchmarks/scalar-functions.md#primitive-text-greatest)
records bulk gains, tiny/full-job losses and actual preceding routes.

### LEAST {#least}

Uses the same numeric kernels, generated-expression paths and source string-representation
gates as GREATEST, with strict NULL propagation and minimum comparison. It also admits
the same proven ASCII primitive-text producers, matching Boolean-word literals and
locale-independent case results described above. BOOLEAN extrema combine Arrow value
bitmaps directly while independently unioning argument NULLs; no comparison/zip arrays
or per-row scalar expansion are needed. A scalar false absorbs LEAST and a scalar true
absorbs GREATEST while preserving strict NULL propagation.
The [independent LEAST qualification](../benchmarks/scalar-functions.md#primitive-text-least)
compares its actual preceding encoder and runtime against stock, retains tiny/full-job
losses, and reruns both extrema against released Flink. Verified mixed exact-numeric LEAST uses
the same lossless casts to Flink's resolved DECIMAL type. It retains the same nullable-metadata,
scale-reduction and producer guards; it does not remove Flink's nullable-conversion failures.
The [separate qualification](../benchmarks/scalar-functions.md#mixed-exact-least) records
stock/previous/native measurements, slower small batches and complete-job variability.

### INITCAP {#initcap}

Character strings only. Only ASCII letters and digits form words; every other character separates words and is preserved. NULL returns NULL. This follows Flink rather than DataFusion word boundaries.

### TRANSLATE {#translate}

Three character arguments. Mappings use Unicode codepoints, not graphemes. The first duplicate mapping wins, but duplicates consume target positions. Missing target characters delete; a NULL target acts as empty. NULL/empty `from` leaves the source unchanged. A NULL source returns NULL.

### BTRIM {#btrim}

One-argument space trimming and two-argument character-set trimming with a literal set retain
their Rust kernels. Empty sets preserve the input and NULL propagates. Per-row sets use Flink's
generated evaluator when the Calc reads a proven external Java-string conversion. Unknown or
binary-backed representations retain fallback because Flink can change set behavior after an
exchange when the first set character is a space.

### TRIM {#trim}

The SQL `TRIM([BOTH | LEADING | TRAILING] [characters] FROM s)` forms are native with
a literal trim set, including Unicode, empty and NULL sets. The default set is the ASCII
space, not all whitespace. These forms reuse BTRIM/LTRIM/RTRIM's character-set kernels.
Runtime trim sets are also admitted when the Calc reads a proven external Java-string
conversion, with the same representation gate as BTRIM. Direct input/literal operands use
a Rust batch kernel when every non-null string is ASCII, its original Java UTF-16 cache
agrees with those bytes, and every evaluated trim set is nonempty. All three directions
interpret the operand as a character set, not a regular expression. NULL propagates.

Unicode, empty sets, computed operands and scalar consumers retain a fused generated Flink
evaluator. In particular, an empty set's outcome can depend on its decoded-cache state;
the Rust certificate declines the complete batch instead of normalizing that state.
Filters and CASE suppress unselected failures, and all-filtered batches skip projections.
Unknown or binary-backed source representations still fall back under
[#508](https://github.com/datafusion-contrib/StreamFusion/issues/508).
See the [SQL TRIM release measurements](../benchmarks/scalar-functions.md#runtime-sql-trim).

### ELT {#elt}

The binary-result overload uses a native columnar selector. Dynamic INT indices retain
one-based selection, out-of-range NULLs and NULL operands. Multiple binary result columns retain
independent byte arrays across rows and batches. Fixed-length BINARY arguments and results
are admitted alongside BYTES; embedded zero/high bytes remain binary. Expressions requiring
Flink's rowwise exception or shared-function ordering retain the generated callback.

An INTEGER index and character alternatives are admitted. The index is 1-based; out-of-range and NULL indices return NULL. Only the selected alternative's NULL matters. Other index types fall back: Flink casts its boxed index to Integer after its bounds check. Explicit casts to INTEGER follow the existing cast rules.

Binary ELT runs by default, including fixed-declaration inputs whose raw payload lengths
differ from their SQL widths. Explicit false overrides retain tested stock execution; the
unverified BIGINT-index overload keeps its fallback check.

### URL_ENCODE {#url_encode}

Character strings use Java form encoding: space becomes `+`, ASCII alphanumerics and `-_. *` are preserved apart from space, and other UTF-8 bytes use uppercase percent escapes. NULL returns NULL.

### OVERLAY {#overlay}

Character strings and integer positions, widened to BIGINT without losing bits. Preserves Java UTF-16 positions, length narrowing/overflow, and substring errors. Non-positive or beyond-end starts return the source; zero/negative lengths omit the suffix. Any NULL argument returns NULL.

A standalone final projection keeps the native kernel: split surrogate pairs encode as `?`
at final output, matching Flink. Direct equality/inequality against a string literal and
REPLACE with single-character ASCII search/replacement literals have a
[conditional Rust fusion path](../optimizations/native-string-fusion.md). Producer arguments
must be direct inputs, literals or losslessly widened integer inputs. The Rust path retains
UTF-16 cuts until after the consumer; overflowing indexes retain the exact Flink evaluator.
REPLACE also retains that evaluator for non-ASCII original Java caches or producer literals. Other consumers,
including LIKE and surrounding CASE expressions, fuse through the batch JVM bridge.
Sensitive character
results crossing another relational operator retain the shared STRING identity fallback;
[issue #519](https://github.com/datafusion-contrib/StreamFusion/issues/519) tracks that boundary.

### PARSE_URL {#parse_url}

Direct input/literal calls use a certified Rust path for ASCII hierarchical `http://`
and `https://` URLs with a nonempty ordinary host, optional user info and numeric port.
Raw case, dot segments, percent escapes, `+`, absent/empty components and the first literal
query-key occurrence are preserved. Components and query keys can vary per row. Keys containing
`&`, IPv6, other protocols, whitespace, unusual authorities and non-ASCII batches retain Java URL
parsing. No URL normalization or query decoding is substituted. Composed generated consumers
retain their existing exact route.

### PRINTF {#printf}

On Flink 2.2, ASCII literal formats of at most 512 characters have a JVM-free path when
all arguments are verified integer inputs/literals, arithmetic, lossless widening or
admitted integer rounding trees, or BOOLEAN inputs/literals and verified logical/integer-comparison
trees. Integer and BOOLEAN arguments may be mixed. Supported conversions are `%s`/`%S`, `%d`, `%x`/`%X`,
`%o`, `%b`/`%B` and `%%`, including argument indices/reuse, alignment, sign/zero padding and string
precision. Width and precision are bounded at 256. The planner checks the running JDK's
flags/indexing and argument-type rules during planning with `Locale.US`; data rows execute only the Arrow formatter.
Explicit index zero retains its preceding route because JDK 11 accepts it while JDK 17 rejects it.
NULL arguments retain their formatted `null`/`NULL` text, including precision and padding.
For `%b`/`%B`, every non-NULL integer, including zero, formats as `true`/`TRUE`;
NULL formats as `false`/`FALSE`. Precision and space padding apply to those words.
BOOLEAN objects instead format their actual true/false value with `%b`/`%B`; NULL also
formats as false/FALSE. Their `%s`/`%S` conversion uses true/false words, with null/NULL for
NULL inputs. Integer formatting does not interpret zero as SQL FALSE. Boolean arguments
at numeric conversions or invalid boolean-format flags retain Flink's NULL result through
their preceding route, including the running JDK's index-zero behavior.
Signed widths and two's-complement formatting remain exact. All child producers must be
failure-free even when the format does not use an argument.

Unbounded STRING CAST/TRY_CAST of those verified total integer producers can also feed
this path. The planner validates the format against the logical STRING argument before
eliding its intermediate text: `%s`/`%S` and `%b`/`%B` have the same result when the
underlying integer is formatted directly. Numeric conversions such as `%d`/`%x` remain
invalid for the STRING argument, including index/reuse rules and NULL-specific formatter
behavior on the preceding route. Bounded CHAR/VARCHAR, BOOLEAN/DECIMAL text casts and
unverified or failing producers retain their existing paths. In particular, casting BOOLEAN
false to STRING would make `%b` true; that cast is not elided. The resulting ASCII proof
also applies to the direct length/comparison consumers below. See the
[integer-text qualification](../benchmarks/scalar-functions.md#integer-text-producers-for-printf).

The same verified BOOLEAN producer proof used by character casts also applies to PRINTF:
integer-to-BOOLEAN TRY_CAST over total integer producers, direct DECIMAL-to-BOOLEAN TRY_CAST,
direct DECIMAL comparisons with matching precision/scale, and their verified logical/NULL
wrappers. Ordinary DECIMAL-to-BOOLEAN CAST benefits from its existing comparison rewrite
where released Flink supports it; DECIMAL(p,p) retains its released planning failure.
Computed/mixed DECIMAL predicates, failing/stateful producers, and unverified string consumers
keep their preceding routes. This shares the proof without widening integer-target or
rounding-position admission.
The [numeric BOOLEAN producer qualification](../benchmarks/scalar-functions.md#numeric-boolean-producers-for-printf)
records exact previous-encoder controls, prepared trials and complete jobs.
The [BOOLEAN/mixed-integer qualification](../benchmarks/scalar-functions.md#boolean-object-and-mixed-integer-printf)
retains the slower shared-loop prototypes, final old-native integer and certified DECIMAL controls,
allocation diagnostics and complete jobs with both transposes.

This path borrows the literal format and reuses row/conversion buffers, appending each
result to Arrow directly. It binds no JVM evaluator. Dynamic/non-ASCII formats, unverified
STRING/DECIMAL/other arguments, other conversions, larger widths/precision and unverified
producers retain their existing paths. Direct built-in CHAR_LENGTH and CHARACTER_LENGTH over
this verified primitive formatter also bind no JVM evaluator: its ASCII result has identical
byte, character and UTF-16 length. The producer proof includes the numeric BOOLEAN subset above.
This does not bypass a catalog UDF with the same name. Dynamic/non-ASCII/invalid formats,
character conversions, unverified producers and relational boundaries retain
their existing representation guards. The
[length-consumer qualification](../benchmarks/scalar-functions.md#length-of-primitive-ascii-printf)
records stock, actual-previous and native controls. Flink 1.18 still rejects PRINTF,
which that release does not provide.

Direct six-way built-in comparisons of qualified primitive formatter results against another
such result or ASCII/NULL character literals also bind no JVM evaluator. ASCII compares
identically under Flink's cached Java UTF-16 and serialized UTF-8 representations. At least
one operand must be the formatter; arbitrary character inputs, non-ASCII literals, unverified
formatters/producers and enclosing consumers retain their preceding paths. This includes
native predicates filtering formatted values, without widening the general string ordering
gate. See the [comparison qualification](../benchmarks/scalar-functions.md#comparisons-of-primitive-ascii-printf).
The [integer-format qualification](../benchmarks/scalar-functions.md#integer-printf-with-a-literal-format)
retains both release controls, stock/actual-previous/native measurements and the initial
prototype that did not improve over every existing certified path.
The separate [integer boolean-format qualification](../benchmarks/scalar-functions.md#integer-boolean-printf-formats)
replays the actual preceding encoder and whole runtime, retaining independent stock and
shared numeric-format controls.

Other direct input/literal calls have a certified Rust path for ASCII formats and arguments:
STRING/integer/DECIMAL `%s`, signed integer `%d`, two's-complement `%x`/`%X`/`%o`, ASCII integer
`%c`/`%C` over TINYINT/SMALLINT/INT, integer `%b`/`%B`, `%%`, argument indices/reuse, width, string precision,
alignment and sign/zero padding. BIGINT character conversions retain the formatter, which
rejects non-NULL Long arguments.
BOOLEAN-object arguments outside the verified literal-format path retain their released evaluator.
DECIMAL `%s`/`%S` conversion uses `DecimalData.toString()`'s plain fixed-scale text,
including leading/trailing zeros: DECIMAL(19,19) with unscaled value 1 renders as
`0.0000000000000000001`, and zero as `0.0000000000000000000`. Formatter string precision
and width apply to that exact text; scientific notation would change both projections and
predicates. The released SQL evaluator passes internal `DecimalData`, so DECIMAL numeric
formats such as `%f`, `%d` and `%x` retain Flink's NULL result rather
than introducing BigDecimal formatting. Non-ASCII, temporal/hash/floating formats and unverified
flags retain Flink's Locale.US formatter; invalid formats still return NULL.

Certificates apply to complete batches. Retained evaluator descriptors indicate that a call
**may** use Java, not that a certified batch performed an upcall. Unverified string consumers
still fuse with PRINTF because character formatting can produce isolated UTF-16 surrogates. A sensitive
string crossing another operator retains the existing representation-protection fallback.

The [DECIMAL formatting regression record](../benchmarks/results/decimal-printf-plain-2026-11-05.json)
compares 78 precision/scale combinations with released Flink 2.2.1 on JDK 11/17,
including NULLs, dynamic formats, string precision/width and empty filtered batches.
All 230 focused invocations and 920 release Rust core tests pass; the 1.18 controls
verify that this release does not provide PRINTF. This repairs already admitted
certified behavior without broadening JVM-free admission or claiming a new speedup.

### URL_DECODE {#url_decode}

One character argument is native. Form decoding preserves JDK UTF-8 replacement grouping and returns NULL for malformed escapes. The planner selects the runtime JDK rule: JDK 17/21 (and pre-25 runtimes) use Integer.parseInt, accepting signed one-digit escapes and BMP Unicode hex digits; JDK 25+ uses ASCII-only HexFormat rules.

The JDK rule is selected on the JobManager during planning, so the JobManager and TaskManagers must use the same URL-decoding rule (pre-25 or 25+); mixed JDK groups can produce results that differ from Flink on the TaskManager.

### ENCODE {#encode}

Character input and a literal UTF-8, US-ASCII, ISO-8859-1, UTF-16, UTF-16BE, or UTF-16LE
charset (including JDK aliases). Returns BYTES, preserves NULL, and replaces unmappable
characters with `?` in ASCII/Latin-1. UTF-16 emits a big-endian BOM for non-empty strings;
UTF-16BE/LE emit no BOM. Empty strings produce empty bytes in all six charsets.
On Flink 2.2, other literal charsets fall back. A runtime character expression for the charset runs through
Flink's generated evaluator in the native Calc's batch JVM bridge. It uses the TaskManager
JDK's charset implementations and aliases, including non-UTF charsets, without a Rust
charset-name approximation. Runtime charset kernels are not independently encoded: the complete
Calc uses the generated program and its binary/string representation checks, including consumers
of charsets that produce isolated UTF-16 surrogates. On Flink 2.2, Calcs using only the six verified literal
charsets retain their Rust kernels. NULL arguments return NULL; illegal or unsupported runtime names
raise Flink's `UnsupportedEncodingException` only when the expression is evaluated. Flink 1.18
declares direct ENCODE results as BINARY(1); its generated Calc carries the complete bytes as
Arrow Binary at final projections (see [legacy representation](calc-filter-temporal.md#legacy-encode-result-representation)).

### DECODE {#decode}

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
filtered and empty batches, untaken CASE branches, and invalid-name exceptions.

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

### SQL/JSON evaluation {#sqljson-evaluation}

SQL/JSON first tries the verified native expressions below. When a Calc containing JSON
cannot be encoded through that route, StreamFusion generates its complete filter and
projection list with released Flink code through the batch JVM bridge. Native fast paths
remain unchanged; new grammar does not require another Rust parser extension. EXPLAIN
marks the generated route with `jsonEvaluation=[JVM]`. A native Calc marker identifies the
columnar operator, not the language evaluating its JSON expressions.

The generated route covers JSON_QUERY outside its definite-path projection gate below,
dynamic JSON_VALUE/JSON_QUERY paths, recursive
descent, filters, slices, multi-selectors, path functions, and invalid paths handled by
Flink's policies. It also admits additional JSON_STRING/JSON_OBJECT/JSON_ARRAY scalar types and nested
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
results are copied into owned Arrow output vectors before that batch closes. SQL/JSON programs
preserve cached Java UTF-16 strings through that boundary without widening the existing container
type gates. In particular, an isolated surrogate member name must remain distinct from `?` until
after JSON member selection, duplicate-key resolution and serialization. The callback still
crosses JNI once per batch, including multi-column results and filtering.
The existing character-key/character-value MAP boundary remains admitted. Other MAP
keys and MULTISET elements must be declared non-null; nullable forms retain fallback,
including inside ARRAY/ROW. Nested fixed BINARY leaves retain their existing restriction.
Programs containing admitted collection functions or runtime constructors can instead use the
broader [generated collection route](#collection-expressions), including `ROW(m)` with a typed MAP.
Its representation and boundary gates apply to the complete program. These collection-preserving
programs also admit nullable MULTISET elements inside ROW/ARRAY constructors; standalone
JSON_STRING retains the non-null-element gate above.
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

### JSON_QUERY {#json_query}

Final VARCHAR projections over a direct character field or literal use Rust for verified literal
definite paths: root, member names (including quoted/escaped names), and signed array indexes.
The default WITHOUT ARRAY WRAPPER, CONDITIONAL ARRAY WRAPPER and UNCONDITIONAL ARRAY WRAPPER are
native, with NULL, EMPTY ARRAY or EMPTY OBJECT independently specified ON EMPTY and ON ERROR.
Strict/lax mode, SQL NULLs, JSON null, missing selections, wrong-type selections, duplicate members
and validation of unselected values retain the released Flink behavior. Empty filtered batches
do not evaluate the projection.

The reader validates the document with the shared Jackson runtime profile, then serializes the
selected tree into Arrow. It does not return a raw JSON substring: 2.2 sorts member names by their
original Java UTF-16 units; 1.18 retains insertion order, including the original position of a
replaced key. The last duplicate name wins on both lines. Strings use Jackson-compatible escapes,
and numeric spelling follows the selected Flink line (1.18 Double or 2.2 BigDecimal semantics).
The input string carrier preserves cached UTF-16 through selection and member ordering; replacement of
isolated surrogates happens at the final UTF-8 output boundary. There is no JVM JSON evaluator on
this route. The existing per-batch JNI synchronization of Jackson recycler metadata remains.
Wide compatible integer/boolean/null documents write directly from the existing recycled SIMD
tape. Floating-point nodes, Unicode escape spelling, oversized tapes/documents and Jackson
edge cases retain the exact reader and a flat arena for the selected tree. Both paths apply
the same ordering, duplicate-key, wrapper and error-policy rules.

Multiple unconditional reader projections over direct character fields/literals preserve
row-major Jackson buffer history while evaluating Arrow columns. Calc prepares each reader's
per-row capacity in projection order, then each kernel uses that capacity for its row. Ordinary
short documents avoid per-row capacity-table allocation. The readers share one batch JNI
metadata acquisition/release and publish the final capacity; no document/result is evaluated
by the JVM. SQL NULLs and filtered-out rows do not grow the buffer, and empty filtered batches
skip the entire schedule.

On Flink 2.2, multiple readers with JSON calls in the condition, computed document inputs or
non-final/conditional consumers retain generated row-major evaluation. A native parser combined
with a JVM evaluator also retains that route because a callback can change shared recycler
state. These restrictions protect successful values near Jackson's numeric token limits,
independently of competing exception order. They apply to JSON_VALUE, JSON_EXISTS and IS JSON
as well as JSON_QUERY. The scoped correctness repair is tracked in
[#608](https://github.com/datafusion-contrib/StreamFusion/issues/608); remaining native coverage
is part of [#506](https://github.com/datafusion-contrib/StreamFusion/issues/506).
Flink 1.18 lacks the later token-limit interaction and retains its previous admission for
computed/conditional readers; compatible direct reader projections can share the batch metadata.
Mixed expressions whose kernels cannot consume the represented string carrier stay generated;
their types are checked during encoding, before physical-plan substitution.

Dynamic/NULL paths, wildcards, index/member unions, slices, recursive descent, filters, path
functions, ERROR policies, computed document inputs, ARRAY return types, unverified container
boundaries in other projections, and non-final string
consumers retain the generated route. The intermediate STRING identity restriction remains the
same as JSON_VALUE, including non-final operator boundaries. Non-Calc expression encoders do not
admit this new JSON_QUERY kernel.

### JSON_QUOTE {#json_quote}

Character input, including NULL. Matches Flink 2.2.1's actual spelling: slash is escaped, non-ASCII values use lowercase Unicode escapes, and supplementary characters emit a full code-point escape followed by a low-surrogate escape. Unlisted ASCII controls are retained.

### JSON_UNQUOTE {#json_unquote}

One character argument is native. Valid quoted values are unescaped with Flink/Jackson first-token validation; invalid input is preserved and NULL propagates. A truncated Unicode escape after a valid first token fails the job, matching Flink 2.2.1's uncaught bounds exception. A truncated escape inside the first token is invalid JSON and is preserved.

Scalar consumers use the fused JVM expression path, and strings passed to another operator
fall back under the [JSON_VALUE identity restrictions](#json_value).

### JSON_STRING {#json_string}

One character, BOOLEAN, TINYINT, SMALLINT, INTEGER, BIGINT, or DECIMAL scalar is native by default.
SQL NULL returns SQL NULL; other scalars serialize to JSON text. Strings use Jackson's
escaping: quote/backslash and ASCII controls are escaped, other controls use uppercase
`\u00XX`, and slashes and Unicode remain unescaped. Integer widths retain their exact
decimal spelling. Output is written directly into the Arrow string builder.

DECIMAL retains its declared scale and trailing zeros, using Jackson's BigDecimal spelling:
`1.2300` remains `1.2300`, while sufficiently small values use scientific notation
(`0.000000100` becomes `1.00E-7`). Precision and scale through 38 are native, including NULLs.

Final Calc projections over direct ARRAY/ROW input columns also serialize in Rust. Containers
may nest through 32 levels with nullable elements/fields and character, BOOLEAN, TINYINT,
SMALLINT, INTEGER or BIGINT leaves. Flink 2.2 additionally admits DECIMAL leaves; 1.18's
different decimal normalization retains generated evaluation. Outer containers may be nullable
or NOT NULL. Empty arrays and nested NULLs retain their distinct JSON spelling. ROW members
sort by Java UTF-16 field-name order on both released lines; nested changelog metadata is not
serialized. The schema and escaped keys are prepared once per batch, and values write directly
to the Arrow output without a JSON tree or JVM evaluator.

Computed/intermediate container operands, nonnullable nested elements/fields, depth above 32,
MAP/MULTISET, additional leaf types, and ROW names containing unpaired surrogates, ASCII controls,
quotes or backslashes retain generated evaluation. Existing owned-string and relational-boundary
identity gates still apply. Disabling collection expressions retains the preceding generated route.
Plans that combine string-container serialization with cache-observing comparisons or other
representation-sensitive outputs retain generated evaluation. Flink's serializer materializes
the original strings; for example, `JSON_STRING(a), ARRAY_MAX(a)` can change which string wins
when the same ARRAY contains cold UTF-8 strings. This is successful-value behavior, not exception
ordering, and is checked in both projection orders.
Additional scalar inputs and direct nested JSON_OBJECT, JSON_ARRAY and JSON(value) calls also
use the generated Calc, preserving Flink's raw-JSON handling. JSON_STRING applied to an ordinary
string column containing JSON text quotes it normally. No compatibility opt-in is needed.

NULL values must have a supported scalar type, for example `CAST(NULL AS STRING)`.
An operand whose type remains SQL NULL falls back: Flink's JSON node generator cannot
serialize that type. The same typed-NULL rule applies to JSON_OBJECT values.

### JSON_OBJECT {#json_object}

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

### JSON_ARRAY {#json_array}

Flat arrays of character, BOOLEAN, TINYINT, SMALLINT, INTEGER and BIGINT scalar arguments
execute in Rust. DECIMAL precision/scale through 38 is also native on Flink 2.2, sharing
the exact scale-preserving/scientific rendering of JSON_STRING. Released 1.18's different
decimal rendering retains the generated evaluator. Argument order and repeated values
are preserved. The default ABSENT ON NULL omits SQL NULL elements; explicit NULL ON NULL
writes JSON nulls. Empty or entirely omitted arrays return `[]`, never SQL NULL. NULL
arguments must retain a supported scalar type.

The kernel prepares typed column views and scalar arguments once per batch, then writes
each result directly into the Arrow string builder with the existing Jackson-compatible
scalar serializer. It handles sliced validity, constant/column mixtures and empty inputs
without building per-row JSON trees or calling the JVM. Ordinary scalar consumers and
CASE-selected strings remain ordinary string values. Direct nested JSON_OBJECT, JSON_ARRAY
and JSON(value) arguments retain the generated route because Flink inserts these as raw
JSON. Other scalar/container types also retain their existing routes. Remaining JSON
kernel coverage is tracked by [#506](https://github.com/datafusion-contrib/StreamFusion/issues/506).

The [scalar-array qualification](../benchmarks/scalar-functions.md#flat-scalar-json_array-2026-11-06)
records both released lines/JDKs, actual consumed Rust execution, previous-path controls
and release measurements. All 36 prepared bulk cells beat stock by 1.90-5.86x; the seven
million-row jobs retain both perimeter transposes and take 0.603-0.723 of stock elapsed
time. These results apply to the measured flat scalar shapes, not all SQL/JSON expressions.

### IS JSON {#is-json}

`s IS JSON [VALUE | OBJECT | ARRAY | SCALAR]` and their `IS NOT JSON` forms are native
for character input. Omitting the type means VALUE. Results are non-nullable: SQL NULL
and invalid JSON return FALSE (TRUE for the negated form). The JSON literal `null` is
a valid VALUE and SCALAR, but is neither an OBJECT nor an ARRAY.

Parsing matches Flink 2.2.1's Jackson first-document validation, including trailing
content, token boundaries, escaped surrogates, and limits in every nested field.
It shares the streaming/SIMD reader and JDK profiles described below; no compatibility
opt-in is needed. This validates the document directly, without applying JSON path policies.

### JSON_VALUE {#json_value}

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
member-name unions, path functions and branching JSON_QUERY use the generated Calc described above.

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

Direct JSON string projections retain the Rust kernel. Direct character JSON_VALUE projections
with a column input may also feed final UNION ALL forwarding into a row sink. Native parsing
retains the original input Java cache, including raw surrogate member names, then produces
ordinary UTF-8 to match Flink's serialized fan-in edge. A sink-side UTF-16 capture verifies this
boundary separately from result collection. See [Union](union.md#final-json_value-projections)
for the verified scope and representation restrictions.

Other Calcs projecting a JSON-derived STRING (including nested character fields) into another
operator make the whole query fall back, with
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

### JSON_EXISTS {#json_exists}

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
or probe failure decline the native parser for JSON_VALUE, JSON_QUERY, JSON_EXISTS and IS JSON; Calc
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

### Collection expressions {#collection-expressions}

The following extensions are enabled by default. Set
`streamfusion.experimental.collectionExpressions=false` to disable them. The maintainer accepted
the measured standalone regressions on 2026-10-17;
[collection representation work](../optimizations/collection-representation-work.md) records
the measured boundary improvements and remaining standalone costs.
Existing admitted collection subscripts and Boolean/integer ARRAY_DISTINCT retain their ordinary paths.

| Functions | Execution inside NativeCalc |
| --- | --- |
| Collection CAST / TRY_CAST | Rust recursive conversion for nullable signed-integer/DECIMAL ARRAY elements, STRING-to-integer arrays, numeric-to-BOOLEAN arrays, integer-key MULTISET conversions, and nullable STRING/integer-key MAPs with numeric values. Numeric/BOOLEAN/DECIMAL arrays format to STRING/CHAR/VARCHAR in Rust. Other verified container conversions retain generated Flink evaluation; see the bounds below |
| SPLIT with dynamic, empty or NULL separator | Rust represented-STRING kernel; literal whole separators, strict NULLs and Java UTF-16 unit splitting for an empty separator. The existing non-empty literal separator retains its ordinary Rust path |
| ARRAY_REVERSE, ARRAY_SLICE | Rust kernels for nullable elements, including represented STRING/ROW/MAP-bearing values. Reverse retains the original array category for zero/singleton arrays; slice retains it for a complete range. Copied ranges preserve string caches and nested RowKinds while recording their new generic containing array. NOT NULL elements, fixed BINARY and unverified consumers retain generated evaluation |
| CARDINALITY on ARRAY/MAP | Rust outer-offset count, returning INT; NULL container produces NULL |
| ARRAY_CONTAINS, ARRAY_POSITION | Rust scans for matching BOOLEAN/integer/DECIMAL or represented STRING element/needle types with nullable elements. STRING comparisons use original UTF-16 only when both operands have Java caches, otherwise their encoded bytes. Non-NULL literal or nonnullable CONTAINS needles retain Flink evaluation; DECIMAL arrays with such needles require whole-Calc fallback. Unverified consumers and fallible arguments retain generated evaluation |
| ARRAY_MIN, ARRAY_MAX | Released DataFusion Rust kernels for nullable integer/DECIMAL elements; native represented-STRING reductions preserve Flink's cache-dependent ordering and first retained value. Other types, NOT NULL elements, unverified consumers and fallible arguments retain the generated program |
| ARRAY_APPEND, ARRAY_PREPEND | Rust offset-based copying for nullable BOOLEAN/integer/DECIMAL and represented STRING arrays with a matching element, including NULL. Original UTF-16 caches survive in copied STRING elements; fallible arguments and unverified consumers retain generated evaluation |
| ARRAY_REMOVE | Rust stable selection for matching nullable BOOLEAN/integer/DECIMAL and represented STRING arrays; removes every matching element, including NULL when the needle is NULL. STRING equality preserves original Java-cache identity; other signatures retain generated evaluation |
| ARRAY_CONCAT | Rust concatenation for matching nullable BOOLEAN/integer/DECIMAL and represented STRING arrays; any NULL operand returns NULL. Copied STRING elements retain their original caches; other signatures retain generated evaluation |
| ARRAY_UNION | Released DataFusion Rust kernel for matching nullable BOOLEAN/integer/DECIMAL arrays; a native represented-STRING kernel preserves cache-sensitive equality. Both retain first-occurrence order and one NULL element; either NULL container returns NULL |
| ARRAY_EXCEPT | Rust stable set difference for matching nullable BOOLEAN/integer/DECIMAL and represented STRING arrays, retaining the first left-side occurrence of each value absent on the right, with NULL treated as a value; either NULL array returns NULL |
| ARRAY_INTERSECT | Rust stable intersection for matching nullable BOOLEAN/integer/DECIMAL and represented STRING arrays; distinct matches follow left-side order even when the left array is shorter. STRING equality retains Java caches; NULL is a value and either NULL container returns NULL |
| ARRAY_SORT | Proven short/distinct nullable integer/DECIMAL arrays retain released DataFusion for non-NULL constant flags and the typed batch kernel for runtime/NULL flags. Other integer/DECIMAL and represented STRING arrays use native TimSort at any length, including runtime/NULL Boolean flags. Preserves the released comparator, stable identities and invalid-comparator exception; other types, fallible operands and unverified consumers remain generated |
| ARRAY_JOIN | Rust UTF-8 join for total nullable string arrays and string options, including strict explicit NULL replacement; co-projected sensitive values and unverified consumers remain generated |
| ELEMENT | Rust extraction for nullable BOOLEAN/integer/DECIMAL and represented STRING arrays; NULL/empty arrays return NULL, a singleton retains its exact element representation, longer arrays raise the selected Flink line's exception with the released message. Fallible operands and unverified consumers retain generated evaluation |
| MAP_FROM_ARRAYS | Rust construction for nullable BOOLEAN/integer/DECIMAL and represented STRING key/value arrays, preserving duplicate and NULL keys, entry order and strict parent NULLs; mismatched lengths retain Flink's typed exception; other signatures remain generated |
| MAP_KEYS | Rust shared-key view for total maps with nullable BOOLEAN/integer/DECIMAL keys and values. A final direct input-reference projection additionally admits verified nullable keys/values containing STRING, temporal or nested ARRAY/MAP/ROW values, retaining their represented children. Existing nullable BINARY-key/basic-value projections retain length-delimited transport. NOT NULL primitive key arrays and unverified representations retain generated evaluation |
| MAP_VALUES | Rust shared-value view under the same verified map-projection admission; a value-only Calc with no condition or sensitive co-projection need not preserve the unused keys' Java caches. Direct VARCHAR-key projections also admit nullable BINARY values through the length-delimited transport |
| MAP_ENTRIES | Rust shared entry-row list under the verified MAP_KEYS/MAP_VALUES input-projection admission, including nullable nested key/value types. Fresh entry rows have INSERT kind; nested values retain their own RowKinds and string caches. Unverified consumers retain generated evaluation |
| MAP_UNION | Rust stable gather for total nullable basic maps or nullable STRING keys with basic/STRING values, preserving right duplicates, left-only NULL rules and empty-right identity; other signatures remain generated |
| Runtime ARRAY[...] | Rust construction for matching nullable BOOLEAN/integer/DECIMAL/DATE/TIME/TIMESTAMP/LTZ elements and total arguments. STRING arrays additionally admit literal/input-reference elements, retaining literal UTF-16 units (including isolated surrogates) and original input caches. Other projected signatures retain the generated program; UNNEST retains its existing wider scalar admission |
| Runtime MAP[...] | Rust construction for total nullable basic pairs and STRING/one-level ROW-bearing pairs with distinct literal/input key expressions. Preserves binary STRING/ROW identity; 1.18 represented constructors use input order for its nondeterministic grouping. Repeated/fallible key expressions and unverified types remain generated |
| Runtime ROW(...) | Rust construction for total BOOLEAN/integer/DECIMAL/STRING fields, preserving binary ROW semantics; other fields and unverified consumers remain generated |
| OBJECT_OF | Rust construction for validated metadata and total basic/STRING fields, preserving generic object STRING caches; other signatures remain generated |
| OBJECT_UPDATE | Native field selection for one unshared inline basic/STRING OBJECT_OF in the complete Calc and atomic literal/input values; shared objects and other signatures remain generated |

Representation-sensitive operations outside the verified native kernels use the generated route
for the complete filter and projection, preserving NULLs, empty filtered batches, shared function
invocation order and released Flink overloads. Native STRING collection kernels retain the marked
UTF-8/optional UTF-16 representation; they do not normalize isolated surrogates before comparisons.
The existing final-row-sink and unverified-producer/state/consumer guards remain in force.
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
The existing collection switch and whole-Calc representation safeguards still apply.
Availability still follows the released Flink catalog: Flink 1.18 has ARRAY_MAX but no ARRAY_MIN;
native registration does not introduce a SQL function missing from the host release.

An ARRAY materialized solely for [UNNEST](unnest-correlate.md) can use released DataFusion's scalar
array constructor because the container's generic/binary identity does not escape that consumer.
The admitted scalar element types preserve raw floating values and the released timestamp writer's
compact/expanded storage rules. BIGINT millisecond epoch conversion and TIMESTAMP_LTZ-to-DATE/TIME
casts also have native kernels in that operand context, with SQL epoch bounds and per-version TIME
truncation preserved. Non-UTC offsets come from JDK ZoneRules through the batch bridge and are
cached between transitions, preserving historical and daylight-saving behavior. This total lookup
does not trigger whole-Calc row evaluation. Independent JVM calls keep their scalar batch bridge;
shared stateful calls retain their invocation-order safeguards.
Ordinary projected arrays of verified nullable basic and temporal elements reuse the same
constructor; floating, NOT NULL-element, character and representation-sensitive projected
arrays retain generated evaluation.

Primitive bridge types and recursively supported ARRAY/MAP/ROW values are admitted, including
nullable typed map keys, decimal and timestamp values. Generated ARRAY/ROW containers and nullable MAP
values support fixed BINARY leaves through the length-delimited transport described above;
NOT NULL fixed BINARY MAP keys/values are verified on Flink 2.2 and remain excluded on 1.18;
unpaired UTF-16 field names remain excluded. STRUCTURED results from inline OBJECT_OF/OBJECT_UPDATE are supported;
external STRUCTURED inputs use the [verified built-in collection boundary](calc-filter-temporal.md#external-structured-collection-inputs). Unsupported host signatures or generated programs
fall back during planning.

Generated collection evaluation admits nullable MAP values declared BINARY(n) while retaining their actual
byte lengths, including lengths outside the declaration. In generated evaluation, MAP_VALUES,
MAP_ENTRIES, MAP_UNION, map casts and character formatting use released Flink; casts keep its padding,
truncation and legacy rules. Nested scalar callbacks use the same verified map conversion.
NOT NULL fixed BINARY MAP values use the generated route on Flink 2.2 and remain on stock Flink
on 1.18: released 1.18 can distinguish a NULL
slot from the empty bytes returned by an unchecked getter while formatting the same input.
This support requires `streamfusion.experimental.nestedScalarUdfs=true` (the default) so the Arrow
boundary uses length-delimited bytes. Disabling collection expressions or that transport retains
fallback for the new collection signatures; scalar callbacks retain their separate UDF controls.
Runtime-source parity covers STRING, integer, BYTES and ROW keys, NULL keys/values/containers,
empty maps, actual payloads from zero to 4097 bytes and generated callback composition. NativeCalc
consumption is required on both supported releases. Representation-sensitive MAP outputs retain
the existing state/exchange/native-writer gates; this adds no retained-row equality.
A direct final MAP_VALUES of an input-reference MAP with VARCHAR keys and nullable BINARY values
uses the existing Rust shared-buffer view. Other collection consumers retain generated evaluation.
The length-delimited transport is also required for the Rust view so actual widths survive.
[Release complete-job measurements](../benchmarks/scalar-functions.md#fixed-binary-map-values)
compare the Rust view, generated evaluator, stock Flink and previous fallback under the accepted
collection-extension tradeoff.

Nullable BINARY(n) MAP keys also use the same length-delimited generated bridge. Runtime-source
regressions verify NULL/empty maps, duplicate binary contents, actual widths outside the declaration,
map casts, direct formatting, entry/array consumers, shared binary inputs and all input changelog kinds.
Tests use ordered internal source entries because external byte-array keys have identity hashes.
Both CAST and TRY_CAST keep released key/value conversion behavior; returned maps retain their
logical entries even when a narrowing cast produces equal key bytes. Flink's generated map casts
use Java HashMap, so binary-key cast iteration can depend on object identity; it is not an ordered-map
contract. Direct input-map formatting is compared in its original entry order. NOT NULL binary keys
and values use the verified generated route on Flink 2.2, described below; on 1.18 they retain
fallback. External STRUCTURED inputs follow the separate built-in collection boundary below; unverified representation boundaries retain fallback.
On released 1.18, CASE returning a binary array can fail in Flink's own compiler; the explicit
failure/fallback contract remains. No absent catalog function is introduced on either line.
A direct final MAP_KEYS projection of an input-reference map with nullable BINARY keys and nullable
BOOLEAN/integer/DECIMAL values uses the existing Rust shared-child view. Other key consumers retain
generated evaluation. The default length-delimited transport must remain enabled for actual widths.
[Release comparisons](../benchmarks/scalar-functions.md#fixed-binary-map-keys) record costs and the
bounded optimization investigation.

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
operators. Representation-sensitive final results require a row sink or a native writer with a
verified collection canonicalization contract. The Doris Arrow writer declares that capability;
other native sink rewrites with extended layouts retain whole-query fallback until verified.
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
[Issue #234](https://github.com/datafusion-contrib/StreamFusion/issues/234) tracks remaining
collection aggregates, cast signatures and representation boundaries outside this extension.

### Exact numeric ARRAY CAST and TRY_CAST {#exact-numeric-array-cast-and-try_cast}

Standalone CAST and TRY_CAST between arrays of TINYINT, SMALLINT, INT, BIGINT and DECIMAL evaluate
in Rust. Every array level must have nullable elements, with matching nesting depth. This uses
the existing default-enabled collection option; no additional switch is required. Other verified
standalone ARRAY-to-ARRAY, MAP-to-MAP, MULTISET-to-MULTISET and ROW-to-ROW casts now enter the same generated Flink
program used when a collection constructor or function is present. STRING, floating, temporal
and NOT NULL-element conversions keep Flink's conversion rules and resolved types. The bridge's
type gates and the representation-boundary restrictions above still apply; external STRUCTURED inputs use the verified built-in collection boundary below. Flink 1.18 NOT NULL fixed BINARY MAP keys/values remain excluded. Direct BINARY MULTISET
elements use the verified boundary below; NOT NULL elements remain guarded on Flink 1.18.
The existing DECIMAL-to-FLOAT/DOUBLE ARRAY CAST also retains its Rust path.

Signed-integer MULTISET element casts execute in Rust using the released default HashMap
iteration schedule. Other MULTISET casts retain Flink's generated map-shaped evaluator and the existing Arrow
`MAP<element,INT NOT NULL>` transport. Nullable elements and containers, multiplicities, empty
multisets, narrowing collisions, nested ROW/ARRAY elements and DECIMAL conversion retain the
released rules, including legacy formatting and whole-container TRY_CAST failures. Counts are
carried without recomputing, merging or expanding them. Flink's generated cast keeps its map construction and collision semantics; no new multiset
aggregation implementation is implied.
Runtime sources require consumed NativeCalc work on both supported releases, including all
four input changelog kinds, filtered/CASE paths and empty batches. Direct BINARY elements
use the length-delimited generated boundary below. MULTISET outputs use the same final row-sink/fused-consumer restriction as
MAP outputs; unverified state, exchange, ordering and native-writer boundaries still fall back.
A verified integer/BOOLEAN or [DATE/TIME COLLECT](group-by.md#date-and-time-collection-elements) producer can feed this preserving Calc through its existing map-shaped
output; character aggregate results and other unverified aggregate representations retain fallback.
Verified character grouping fields retain the released serialized-key representation through these
consumers; their pre-group Java caches do not become grouped output caches.
The updating MAP/MULTISET payload extension in
[#437](https://github.com/datafusion-contrib/StreamFusion/issues/437) remains deferred.
[Release comparisons](../benchmarks/scalar-functions.md#multiset-casts) document the accepted
collection tradeoff and the two-pass optimization limit. COLLECT and ARRAY_AGG remain separate
[aggregate coverage](group-by.md).

The conversion follows the recursive list structure used by Comet, preserving NULL containers,
NULL elements and the resolved target schema. Integer widening uses Arrow; integer narrowing
and DECIMAL conversion reuse StreamFusion's released-Flink scalar rules. Narrowing wraps low
bits, DECIMAL-to-integer truncates toward zero before narrowing, and DECIMAL rescaling uses
HALF_UP with overflow represented by a NULL element. Empty arrays remain empty. Arrow slices
convert only their referenced child range, rebasing offsets instead of repeatedly converting
the complete shared child buffer.

Arguments that can fail per row retain Flink evaluation. The numeric cast itself is total;
it must not introduce eager evaluation of an unselected or filtered input expression.
Numeric TRY_CAST uses the same total numeric conversion, including overflow-to-NULL DECIMAL elements;
it does not catch errors raised while evaluating its input. STRING-to-integer ARRAY casts also execute in Rust. TRY_CAST and legacy CAST make the complete
outer result array NULL when any visible element fails, including within nested arrays; strict
CAST preserves the scalar conversion exception. Hidden children under NULL containers are never
converted. Numeric-to-BOOLEAN elements compare against zero. The complete Calc retains filter/CASE short-circuiting and
input evaluation errors, including with legacy cast behavior enabled. Other element conversions retain the verified generated path. Nonnullable elements and fallible
input expressions retain their existing guards.
The released upstream ARRAY<BIGINT> cast fixtures preserve their original assertions. The two
Flink 1.18.1 invocations consume native Calc input with `jvmFunctions=[]`; their input is an
inline array constructor. The three Flink 2.2.1 invocations still fall back because their input
wrapper is an ARRAY-returning scalar UDF. Dedicated runtime-source regressions independently
require consumed NativeCalc input and an empty JVM-function diagnostic on both releases.
Upstream fixture fallback is not counted as native CAST evidence.

The [release-mode functionality measurements](../benchmarks/scalar-functions.md#exact-numeric-array-casts)
compare the same prepared data with stock Flink and the previous StreamFusion fallback.
The new generated standalone casts and SPLIT signatures have separate
[conversion-inclusive measurements](../benchmarks/scalar-functions.md#standalone-collection-programs).

### Collection casts to character types {#collection-casts-to-character-types}

Nullable integer/DECIMAL/BOOLEAN ARRAY elements, including nested arrays, format to STRING, VARCHAR
and CHAR in Rust. Other ARRAY, MAP, MULTISET and ROW character casts retain the generated program. Flink supplies brackets, map entry
order, nested formatting, NULL spellings, CHAR padding and VARCHAR truncation for the selected
legacy-cast setting. Bounded casts count Java UTF-16 units, including truncation between surrogate
halves; the represented string result preserves those Java units. CAST and TRY_CAST retain NULL
containers, filtered/CASE short-circuiting and errors from evaluating their input. Runtime tests
require consumed NativeCalc input on both supported releases, including all four outer changelog
kinds and generic/binary collection representations. Unverified state, exchange and native-writer
boundaries and Flink 1.18 NOT NULL fixed BINARY MAP keys/values still fall back. External STRUCTURED inputs follow the separate verified boundary below.
These casts share the accepted collection-extension performance tradeoff; see the
[conversion-inclusive character-cast measurements](../benchmarks/scalar-functions.md#collection-character-casts).

### ARRAY_DISTINCT {#array_distinct}

Boolean and integer (`TINYINT`, `SMALLINT`, `INT`, `BIGINT`) arrays use typed kernels following
DataFusion's ordered membership and batch-gather structure. Boolean membership uses two bits;
integer arrays of at most eight elements use a bounded stack search with a
collision-checked fingerprint, and larger integer arrays use a reusable hash set. The first occurrence of each value is retained,
including one NULL element. NULL containers remain NULL and empty arrays remain
empty. The result preserves the element width, field metadata, and nullability. Boolean equality
compares values; string equality compares UTF-8 bytes without trimming or Unicode normalization.
Nullable VARCHAR/STRING and CHAR arrays now use the represented STRING kernel. It compares
original UTF-16 units when both values have Java caches and encoded bytes otherwise, preserving
isolated-surrogate identity. A retained-candidate scan preserves this potentially nontransitive equality. Stable first-occurrence selection retains the entire original value;
copy-category metadata records the generic result array. Floating, decimal, temporal, binary and
nested element types retain the generated route for verified bridges.
Runtime SQL parity checks pass against released Flink 2.2.1.
[Whole-job measurements](../benchmarks/scalar-functions.md#integer-array_distinct-2026-09-28)
show large-array gains and small-array sensitivity to run-to-run variation. With
[synchronous entry borrowing](../optimizations/projection-pruning-transpose.md#borrowing-at-the-synchronous-consumer),
the nullable width-eight Boolean diagnostic also beats stock and previous production in two
ten-million-row candidate runs. The earlier UTF-8-only STRING prototype remains disabled;
represented STRING execution is a separate implementation under the maintainer's two-pass
optimization agreement for [#505](https://github.com/datafusion-contrib/StreamFusion/issues/505).

### SPLIT {#split}

Character input and a literal non-empty separator use the Rust kernel. The separator is literal
text, including regex metacharacters. NULL input returns NULL, empty input returns an empty array,
and leading/repeated/trailing separators retain empty tokens.

Dynamic, empty and NULL separators use the default-enabled represented native kernel. NULL input or
separator returns NULL. An empty separator splits Java UTF-16 units, including the two surrogate
halves of a supplementary character. The represented collection output retains those units
and their Java string caches through fused consumers and the final row projection. Unverified
stateful, ordering, exchange and native-writer boundaries retain planning fallback. Disabling
`streamfusion.experimental.collectionExpressions` retains fallback for these new signatures and
does not disable the existing non-empty literal Rust path.
An empty separator can also populate a source StringData object's Java cache. A Calc that combines
such materialization with cache-sensitive comparisons of original character-bearing inputs stays
on the generated route, preserving aliases and evaluation order. Comparisons of the newly
produced SPLIT array alone, such as ARRAY_DISTINCT(SPLIT(s,'')), remain native.
SPLIT is absent from the released Flink 1.18.1 catalog, so this SQL coverage is N/A on that line.
Temporary user functions named SPLIT keep their own signatures and the ordinary UDF admission
rules; builtin separator checks do not apply to them.

### SUBSTRING {#substring}

SUBSTRING/SUBSTR accepts dynamic TINYINT, SMALLINT, or INT starts and optional lengths. Positive positions are one-based, zero starts at the first character, negative positions count from the end, and a position before the beginning returns empty. Negative length returns NULL; input NULLs propagate.

### LEFT {#left}

Character input with a dynamic TINYINT, SMALLINT, or INT count. Non-positive counts return empty, large counts return the full string, and NULL propagates. Counts measure Unicode code points.

### RIGHT {#right}

Character input with a dynamic TINYINT, SMALLINT, or INT count. Non-positive counts return empty, large counts return the full string, and NULL propagates. Counts measure Unicode code points.

### LPAD {#lpad}

STRING, dynamic INT-width length, and literal or dynamic STRING padding. NULL, negative length, or empty padding returns NULL; zero length otherwise returns empty. Flink 2.2.1 counts UTF-16 units, including truncation through surrogate pairs.

Standalone final LPAD/RPAD projections retain their native kernels. Direct equality/inequality
against a string literal and REPLACE with single-character ASCII search/replacement literals
have a [conditional Rust fusion path](../optimizations/native-string-fusion.md). This path
requires direct input/literal producer arguments (or lossless integer widening). Equality
also handles growth by comparing the literal's UTF-16 length and repeating padding period
without constructing the padded string. Growing REPLACE maps one whole padding period,
copies repetitions directly into the final Arrow buffer and consumes the partial UTF-16
period without encoding split surrogates early. Equality supports
original Unicode Java caches; REPLACE retains Flink for non-ASCII caches or producer literals. Dynamic consumers,
other replacements, LIKE, and surrounding CASE expressions retain generated Flink evaluation
through the batch JVM bridge. Both paths preserve isolated surrogates. For example, padding a supplementary
character to one UTF-16 unit does not equal the literal `?`, even though both encode to `?`
at final output. The original input string caches are retained; NULL and all-filtered empty
batches preserve Flink behavior. A padding result crossing an intermediate relational operator
retains the shared STRING identity fallback until that boundary can carry the exact value
([issue #519](https://github.com/datafusion-contrib/StreamFusion/issues/519)).

### RPAD {#rpad}

Same input and boundary rules as LPAD, with padding appended on the right. Dynamic lengths and padding are admitted, with Flink 2.2.1 UTF-16 counting.

### SPLIT_INDEX {#split_index}

Character separators and TINYINT/SMALLINT/INTEGER indices may be dynamic. Indices are zero-based; negative/out-of-range indices, empty input, or any NULL produce NULL. Whole separators preserve empty tokens. An empty separator uses Java Character.isWhitespace, including tabs and line separators but excluding non-breaking spaces. BIGINT indices with character separators fall back.

The resolved `(STRING, INT, INT)` overload has a cache-aware Rust path for direct
input/literal arguments. The integer delimiter denotes one character in 1..255;
other values return NULL. It scans only to the requested field, preserving empty
tokens, and supports non-ASCII UTF-8 and equivalent original Java UTF-16 caches.
A non-equivalent original cache (for example an isolated surrogate encoded as
`?`), computed operands and unverified resolved widths retain the exact Flink
evaluator. This uses the existing per-batch certificate without a new switch.
See the [release measurements](../benchmarks/scalar-functions.md#integer-delimiter-split-index).

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

### Temporal parsing, extraction and rounding {#temporal-parsing-extraction-and-rounding}

`TO_DATE`, `TO_TIMESTAMP`, all `TO_TIMESTAMP_LTZ` overloads, calendar fields, and temporal
`FLOOR`/`CEIL` now have expression implementations. Formatted parsing and recurring-DST LTZ
calendar fields use Flink's own generated code. Timestamp results retain the complete millisecond/fraction pair and run
by default. Direct integer epoch inputs to `TO_TIMESTAMP_LTZ` with literal precision
0 or 3 execute in Rust; TINYINT/SMALLINT/INT widen losslessly before the existing
BIGINT kernel. Verified integer arithmetic, widening and rounding trees also use these
kernels in ordinary Calc; the computed child retains its resolved integer width before
widening. Ordinary Calc also evaluates verified runtime INT precision CASE trees through
a batch kernel selecting seconds/milliseconds when every result is 0, 3 or NULL and conditions
are total admitted BOOLEAN/integer trees. Arbitrary precision columns, invalid/unverified
leaves, fallible conditions, division, narrowing, unknown producers and string/formatted signatures retain
their existing routes, as does UNNEST admission. Ordinary Calc also admits direct/literal
DECIMAL epochs at literal precision 0/3: truncate toward zero, wrap to signed 64 bits,
wrap seconds-to-milliseconds multiplication, then check the final millisecond range.
Verified alternating DECIMAL ABS/negation chains and same-scale widening also execute natively.
Arithmetic, narrowing/scale changes and runtime DECIMAL precision retain generated evaluation. The integer
original-unit range check must not be applied to this distinct released DECIMAL overload.
Flink 2.2 single-argument numeric `TO_TIMESTAMP_LTZ` uses the same verified integer/DECIMAL
producer proofs at its released default precision 3. Flink 1.18's validation rejection,
unverified producers and existing UNNEST routes remain intact.
See the complete [temporal function inventory](temporal-functions.md).
Direct/literal LTZ FLOOR/CEIL in fixed-zero session zones also runs in Rust for DAY/HOUR
on both releases and MINUTE/SECOND/MILLISECOND on 2.2. The latter three forms retain
released 1.18's code-generation failure. LTZ CEIL uses a dedicated signed-remainder
kernel that discards fractional nanos; ordinary TIMESTAMP rounding stays unchanged.
UTC FLOOR also consumes verified total built-in integer epoch conversions, with literal
0/3 or separately proven runtime CASE precision 0/3/NULL, using those same release-supported
units and the existing Rust kernels. CEIL/CEILING accepts the same producer proof; fixed-unit
CEIL reuses plain-timestamp rounding only because numeric epochs have zero fractions and
bounded milliseconds that exclude its differing overflow cases. General LTZ inputs retain
their dedicated kernel. Other computed timestamp producers and
nonzero/regional zones retain their existing routes. See [computed UTC FLOOR](temporal-functions.md#native-utc-floor-over-integer-epochs).

In fixed zero-offset sessions, ordinary Calc FLOOR/CEIL/CEILING additionally compose genuine verified
DECIMAL epochs: direct fields/literals, total ABS/negation chains and same-scale widening
at literal precision 0/3 or the 2.2 default. Decimal truncation, signed-long/seconds wrapping
and final millisecond bounds remain distinct from integer bounds. YEAR/MONTH/DAY/HOUR
qualify on both releases, with MINUTE/SECOND/MILLISECOND on 2.2. The 1.18 default-signature
rejection and sub-hour initialization failures remain unchanged. CEIL preserves the
calendar rules and reuses bounded-epoch fixed-unit rounding because verified DECIMAL
epochs have zero fractional nanos and final milliseconds far from long overflow.
Runtime DECIMAL precision, binary arithmetic/narrowing/rescaling, unverified
producers, other zones/units and UNNEST keep their existing routes. See
[DECIMAL UTC FLOOR](temporal-functions.md#native-utc-floor-over-decimal-epochs) and
[DECIMAL UTC CEIL](temporal-functions.md#native-utc-ceil-over-decimal-epochs).

### LTRIM {#ltrim}

One-argument space trimming and literal Unicode sets retain their Rust kernels. Per-row sets
use the same generated evaluator and source-representation gate as BTRIM. Empty sets preserve
the input and NULL propagates.

### RTRIM {#rtrim}

Uses the same literal kernels and per-row set admission as LTRIM, trimming from the right.


## Case folding & regex {#case-folding-regex}

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
calls to preserve Flink evaluation order. Direct input/literal calls also have a certified Rust batch path, including REGEXP_EXTRACT.
It accepts ASCII text/patterns, ordinary literals, dot, simple classes, grouping/alternation,
`*`/`+`, and verified ASCII escapes. Anchors, lookaround, backreferences, Unicode properties,
counted/lazy/possessive quantifiers, repeated capture groups and uncertain class syntax retain
Java. Empty matches advance like Matcher.find, including the empty match following a nonempty
one; replacement text is literal, not a capture expansion. Compilation is shared by equal
patterns within a batch. Non-ASCII batches and mismatching original UTF-16 caches retain Java.
REGEXP_EXTRACT_ALL and STR_TO_MAP remain generated collection expressions; see
[issue #495](https://github.com/datafusion-contrib/StreamFusion/issues/495).

**Verified primitive UPPER/LOWER.** Both released lines execute `UPPER`/`LOWER` directly in
Rust for unbounded `CAST`/`TRY_CAST` to STRING of proven total integer or BOOLEAN expressions. On
Flink 2.2 this also includes the qualified primitive ASCII `PRINTF` producer when its
literal format contains no lowercase `i` for UPPER or uppercase `I` for LOWER.
Primitive conversions themselves cannot write `i`/`I`; all admitted output therefore has
the same case result under every worker locale, with or without a Java cache. Signed
integer text has no letters and reuses the
native CAST result without a second scan or output buffer; BOOLEAN text writes its final
case directly. Formatter output uses the existing Arrow case kernel.
The built-in operator identity is checked, NULLs
and empty batches are retained, and there is no bound JVM evaluator for these calls.
Bounded character casts, failing/unverified producers, ordinary STRING inputs, other
formatting contracts and unverified dependent consumers retain their existing paths. This is a
producer-specific proof, not general Unicode casing support. See the
[UPPER](../benchmarks/scalar-functions.md#primitive-text-upper) and
[LOWER](../benchmarks/scalar-functions.md#primitive-text-lower) qualifications and complete-job limits.

Built-in CHAR_LENGTH and CHARACTER_LENGTH also consume these qualified UPPER/LOWER results
directly in Rust. The same producer/locale proof guarantees ASCII, so UTF-8 codepoint and
Java UTF-16 lengths agree. This includes proven integer/BOOLEAN text, direct DECIMAL truth/
predicate BOOLEAN text, and qualified Flink 2.2 PRINTF compositions. Signed integer case
conversion still reuses its CAST result; the length consumer adds no JVM evaluator or
transport change. Bounded casts, general Unicode, locale-sensitive formats, fallible
producers and further dependent expressions retain their existing routes. The
[case-length qualification](../benchmarks/scalar-functions.md#length-of-proven-primitive-case-results)
retains separate stock, actual-prior and native controls.

**Default compatibility path.** Other `UPPER`/`LOWER` calls use Flink-generated scalar
expressions through the columnar JVM bridge. Original Java UTF-16 caches are passed beside the UTF-8 arguments;
an absent cache remains a binary-only string. This distinction matters even for ASCII:
under a Turkish or Azerbaijani runtime JVM locale, Java-backed `LOWER('I')` produces dotless i,
while Flink's cold binary ASCII path produces ordinary i. Dependent consumers remain in the
same generated scalar expression. Other projections and the Calc operator remain columnar;
this is a correctness path, not a pure-Rust case-conversion acceleration. See
[issue #494](https://github.com/datafusion-contrib/StreamFusion/issues/494) for remaining native
Unicode coverage. Uncertified `REGEXP_EXTRACT` retains its JVM regex evaluator.

Each of these also has a faster **pure-Rust** alternative — Rust's own case folding, and the `regex`
crate — that is **opt-in** under
`-Dstreamfusion.expression.<NAME>.allowIncompatible=true` (or the blanket flag; see
[Configuration](../configuration.md)). It's opt-in rather than default because it can diverge from
the JVM behavior on non-ASCII case folding and on advanced regex features (backreferences,
lookaround, some Unicode character classes) — real correctness differences, not just a performance
trade-off, which is why parity comes first by default.

An explicit REGEXP_EXTRACT incompatible opt-in keeps its existing direct native opcode and
literal/index admission rules; the exact batch certificate applies to the default route.

Neither path falls back to the host for a supported argument type. What *does* fall back: a
non-string argument, or — specifically on the pure-native (opt-in) `REGEXP_EXTRACT` — a non-literal
pattern or index.

