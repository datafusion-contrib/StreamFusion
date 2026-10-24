# Unnest / Correlate

**Status:** native, with gaps.

`Correlate` is Flink's plan node for lateral cross-application — most commonly `UNNEST`, which
explodes a collection-typed column into rows. Verified UNNEST shapes and basic synchronous JVM
table functions run inside columnar islands. Other shapes retain fallback (see
[Unsupported operators](unsupported.md)).

## What's native

**INNER or LEFT** `UNNEST` of one collection operand of type:

- `ARRAY` (a scalar or `ROW` element, flattened),
- `MAP` (unnested to key + value columns), or
- `MULTISET` (unnested to the element, repeated by its count),

optionally with `WITH ORDINALITY`, including pushed element predicates for INNER and LEFT joins.

The operand may be a direct column, a nested array field, or an encodable computed ARRAY such as
`ARRAY[values_array[1], id]` or `ARRAY_APPEND(values_array, id)`. Sequential UNNESTs compose. LEFT
`ON TRUE` followed by a separate encodable WHERE filter also composes; the WHERE remains a filter
after expansion.

Computed MAPs outside the verified native constructor signatures, STRING/ROW/MAP-bearing ARRAYs
and LEFT element predicates use the released Flink correlate generator inside an
Arrow-input/Arrow-output operator. This preserves the host's map
constructor ordering, duplicate-key policy, generic/binary container behavior and LEFT padding
after predicate evaluation. It covers verified builtin expressions; it does not expand admission
for user functions. The existing Arrow take kernel remains the first choice for ordinary direct
collections and native scalar ARRAY operands. On Flink 2.2, computed MAPs with total nullable basic
key/value pairs and distinct literal/input-reference key expressions instead use the
[native MAP constructor](calc-filter.md#collection-expressions) in a native Calc followed by
ordinary Arrow UNNEST. This route retains NULL keys/values and runtime duplicate keys in
constructor order; repeated syntactic keys, sensitive representations and unverified
signatures retain generated evaluation. Its SQL regression matrix checks INNER/LEFT,
append/updating inputs and ordinality where the released Flink catalog supports it, comparing
ordered rows and result types and requiring Rust MAP diagnostics plus consumed Calc/UNNEST
metrics. Flink 1.18 uses unordered generated-expression grouping for multi-entry MAP
constructors, so those operands retain the released generator and compare row multisets
without assuming construction order. Verified single-entry MAP constructors still use Rust.
The `streamfusion.experimental.collectionExpressions` switch also controls computed collection
operands on the generated route; `streamfusion.operator.unnest.enabled` controls both routes.

The [MAP expansion validation inventory](../benchmarks/collection-map-expansion-validation-2026-10-20.json)
records 93 related checks on Flink 2.2/JDK 17 and 70 passes with 23 capability skips
on Flink 1.18/JDK 11. The 24-case MAP matrix includes runtime duplicate and NULL
keys, two-row batches, both join modes and updating input. Native RocksDB recovery
methods remain enabled in CI but are excluded from this local stateless check.

The source transpose preserves collection representation and can prune unused input columns before
generated expansion. Generated collection Calc results and other generated expansions are verified
inputs; DropUpdateBefore, mini-batch assignment and watermark assignment preserve that contract.
Complex nested outputs require a verified preserving consumer or a final row boundary. Flink
1.18's legacy `toAppendStream` sink is also a final row boundary; nested ROW arguments and results
retain the same external conversion and values as the stock API. Unverified state, ordering,
exchange and native-sink boundaries retain whole-query fallback. Scalar expanded
values can feed ordinary native aggregates, including checkpoint/recovery.

Scalar ARRAY constructors consumed here use the native constructor for resolved homogeneous
integer, floating, Boolean, decimal, DATE, TIME and timestamp elements. FLOAT/DOUBLE values retain
their primitive bits, including NaN payloads and signed zero. Compact timestamps (precision at
most three) discard hidden nanoseconds as Flink's binary-array writer does; higher precision keeps
both timestamp components. Timestamp elements remain a single column despite their physical
two-component struct. Ordinary collection projections retain their existing evaluation policy.

Within the computed operand, BIGINT `TO_TIMESTAMP_LTZ(value, 3)` preserves Flink's SQL epoch bounds
and NULL results outside them. TIMESTAMP_LTZ-to-DATE/TIME casts run in Arrow. Non-UTC casts obtain
offsets from JDK ZoneRules through the existing batch bridge, caching each transition interval;
UTC requires no callback. This retains the worker JDK's historical and daylight-saving rules.
TIME casts retain
the released version's precision behavior: 1.18 keeps milliseconds, while 2.2 truncates them to
the target precision. Other temporal forms retain generated Flink evaluation.

Pure projections, including repeated, reordered or empty projections, are folded into expansion.
Only selected columns are taken for each element; source collections and hidden computed operands
that the consumer discards are not copied into the expanded batch. Pushed INNER filters retain
their predicate inputs. The changelog tag is carried independently of the selected SQL columns.

NULL MAP keys and MULTISET elements remain nullable when expanded, even though Arrow marks the
physical map key field as required. NULL values are preserved. Empty and NULL collections emit
no rows for INNER UNNEST and one padded row for LEFT UNNEST.

Without ordinality, NULL ROW elements are skipped as in Flink. With ordinality, a nullable ROW with
one nullable field retains its NULL element and original position. Other nullable ROW shapes with
ordinality retain fallback because released Flink can fail with an output-arity or serializer-type
error for NULL ROW elements; they must not silently produce a different result natively.

## What's not

- **JVM table functions outside the verified scalar boundary below**, specialized functions,
  Python/async functions and legacy SQL wrappers retain fallback.
- **Types outside the Arrow bridge, user expressions outside their existing admission gates,
  or an unverified collection producer/consumer boundary** retain fallback. A builtin condition
  can use generated evaluation when it cannot use the Arrow expression engine.
- **Nullable ROW elements with ordinality** retain fallback unless the element has exactly one
  nullable field, as described above.

Flink 1.18 cannot plan `WITH ORDINALITY`. Both released lines reject some TIME(3) casts inside
UNNEST during host planning (TIME(3)/TIME(0) type assertion); native substitution does not bypass
that error. Multi-operand UNNEST also remains subject to the released host planner's restrictions.
Flink 1.18 cannot plan nontrivial LEFT element predicates or some nested MAP/ROW constructor
signatures. Its nonliteral TINYINT-key MAP constructor can fail in generated initialization.
Flink 2.2 rejects LEFT predicates referencing outer columns (for example `ON element > id`).
These host planning/initialization failures remain unchanged; they are not acceleration gaps.

An unsupported shape falls the operator back, dragging the whole query with it per the
[all-or-nothing island rule](index.md#the-all-or-nothing-island). See
[Unsupported operators](unsupported.md) for the full accounting of these cases.
An updating source may insert [DropUpdateBefore](drop-update-before.md) before UNNEST when its
consumer omits update-before images. That surrounding filter runs columnarly inside the native
chain and no longer prevents an otherwise admitted UNNEST from accelerating. Its page records
exact changelog/recovery validation and complete-job comparisons against the preceding fallback.

## Generated builtin validation and performance

The generated expansion and its shared conversion paths pass 324 selected checks on Flink 2.2.1;
Flink 1.18.1 verifies 265 checks with 59 version skips. Coverage includes constructor ordering
and duplicate keys, NULL/empty containers, LEFT predicates, repeated/empty projections, exact
getter failures, ROW equality across chained expansions, watermark-carried caches and all four
RowKinds. COUNT/SUM consumers survive two restores on both memory and native RocksDB state.
Owned binary ROW and UTF-16 tests cover IPC, source mutation/closure, allocation failure and
late cache creation.

All 17 final complete-job profiles beat both stock Flink and actual preceding revision `8eb53c8b`
on median time, retaining runtime row sources, both transposes, production IPC and row sinks.
Flink 2.2.1/JDK 17 examples include numeric MAP expansion at **1.325 s versus 2.753 s stock**,
ROW-valued MAP at **3.258 s versus 3.870 s**, and LEFT element predicates at **0.677 s versus
1.124 s**, each with two million source rows and four entries. The wider updating STRING MAP
initially lost; checked bulk UTF-16 copies and lazy scalar cache sidecars reversed that loss.
Its four-million-row confirmation is **7.402 s versus 7.693 s stock and 7.655 s prior**, with
separated trial ranges. Smaller ranges can overlap, and separate-JVM controls expose drift.
See the [method, full comparisons and profiles](../optimizations/collection-representation-work.md#generated-builtin-expansion-and-owned-binary-row-output)
and [all 540 unprofiled jobs](../benchmarks/results/generated-unnest-2026-10-18.json), including
the rejected intermediate implementations. Generated evaluation remains JVM work inside the
Arrow island; these results do not imply every scalar expression runs in Rust.

## JVM table functions

Synchronous JVM `TableFunction` definitions carried by Flink's bridging SQL function support
INNER/LEFT application with top-level CHAR, STRING/VARCHAR, BOOLEAN, TINYINT, SMALLINT, INT, BIGINT,
DECIMAL, FLOAT/DOUBLE, BINARY/VARBINARY, DATE, TIME, TIMESTAMP, TIMESTAMP_LTZ and
day-time/year-month interval input, output and argument types.
These types are admitted by default under the accepted UDF performance exception, without
an additional type-specific opt-in. Their user functions still run on the JVM; measured
standalone costs can exceed stock Flink.
The [bounded conversion investigation](../optimizations/decimal-arrow-reads.md)
records the retained improvement and measured limits. ARRAY, MAP and ROW may recursively contain
these admitted scalar types. Container signatures support Flink's default external Java conversions
(boxed Java arrays, `java.util.Map`, external `Row`, and their default scalar children).
Direct ARRAY arguments and direct result columns also support `java.util.List` with default
scalar element conversions, and `boolean[]`, `byte[]`, `short[]`, `int[]`, `long[]`, `float[]`
and `double[]` with matching non-null element types. ARRAY parents can still be NULL or empty;
Lists can contain NULL elements. Direct result fields and atomic ARRAY results additionally
support `java.util.ArrayList` with default scalar element conversions, including both interval
families. This is result-only: released validation rejects concrete List subclass arguments.
In signatures containing containers, direct non-null BOOLEAN, TINYINT, SMALLINT, INT, BIGINT,
FLOAT and DOUBLE arguments may also use their corresponding
Java primitive classes. Direct non-null DATE arguments can also use Java `int` epoch days.
This DATE bridge is argument-only: boxed `Integer`, result and nested DATE bridges retain
fallback in container signatures. Released type inference still validates argument nullability; this
does not coerce a nullable source argument to a non-null primitive. Direct fields of an
external Row result may also use those seven non-null primitive scalar classes alongside
container result fields. Atomic results using those classes are supported too; Flink wraps
them in a single inferred result field. Alternate bridges nested inside a result field
retain fallback.
For these conversions on Flink 1.18, LEFT queries selecting a declared non-null field from
an external Row result retain the release's SQL-to-rel nullability assertion. Container-only
LEFT projections and atomic-result LEFT queries are verified to execute.
Released converters own these Java values, and the existing
typed-array adapter supplies primitive conversion after nullable Arrow copies. FLOAT/DOUBLE
retain signed zero and NaN payloads through chained function inputs and results.

Released type inference determines the conversions. Other concrete/custom List result classes,
alternate bridges nested inside a ROW/MAP argument or nested result column, Lists with
ARRAY/MAP/ROW elements, internal
`ArrayData`/`MapData`/`RowData`, MULTISET/STRUCTURED/RAW, specialized functions, Python/async
functions and legacy SQL wrappers retain fallback. The ordinary `streamfusion.operator.tableFunction.enabled`
switch can disable this operator.

Fixed BINARY uses the existing default variable-byte Arrow transport, preserving the released
function's actual bytes even when their length differs from BINARY(n). Values are copied into
output batches before the collector can mutate or reuse them. INNER/LEFT fanout, NULLs,
widths 1/4/128, empty/oversized byte arrays and memory/RocksDB recovery are checked against
stock Flink on both supported lines. Disabling both `nestedScalarUdfs` and `binaryElt`
restores the earlier fixed-width transport and keeps these table signatures on fallback.

Container inputs use the existing preserving Arrow boundary: nested Java string contents,
NULL map keys, nullable parents/elements, nested RowKinds and array copy representations survive
the callback. Released converters remain responsible for external values; the same typed-array
adapter used by scalar UDFs handles nullable arrays copied into generic Object arrays.
Collected nested values are written before the user reuses or mutates the result.
Folded column projections with non-null scalar fields execute Flink's released projection
generator before those owned writes. If a function emits NULL under non-null primitive result
metadata, this preserves the generated projection's default primitive value. Queries without
that projection retain stock sink constraint enforcement; LEFT padding remains nullable.
A verified source transpose, preserving Calc, generated UNNEST or preserving table function may
supply sensitive values. Plain source-column projections, including metadata-only AS aliases
and reordered columns, can fold into the preserving entry transpose when types match and
source columns are unique. Original source names select the entry copy; Flink's ordinal
correlate semantics retain the declared output aliases. This fold is verified for boolean
and signed-integer fields and ROWs recursively containing those fields. Other projected
containers, filters, computed projections and duplicate input columns do not use that fold;
their input must independently satisfy the preserving guard. Their results must reach a preserving Calc/correlate or final row
boundary; unverified stateful consumers and columnar sinks retain complete-query fallback.
For example, a non-preserving MAP `CARDINALITY` Calc or a grouped count that still carries an
ARRAY-of-ROW or MAP payload stays on fallback even when that payload is absent from the final result.
Focused SQL checks cover typed and custom-inferred default conversions, retained arguments,
mutable collected arrays/rows, INNER/LEFT fanout, pushed filters, chained functions and UNNEST,
empty projections, failures, all changelog kinds and repeated memory/RocksDB recovery on both
supported Flink lines. The unchanged released Table API `CorrelateITCase.testRowType` additionally
requires the table-function operator to consume input in every backend invocation.

### Primitive table-function results (2026-10-24)

The result-conversion matrix adds 133 checks per supported Flink line, alongside 235 existing
table-function checks. It covers all seven primitive scalar result types with boxed ARRAY,
primitive ARRAY, List, MAP and ROW signatures, using both external Row fields and atomic
collectors. Types, raw floating bits, mutable/retained values, literal NULL emissions, empty
input, failures, changelog kinds and two forced restores with memory/native RocksDB state
match the corresponding released Flink execution. Nested alternate result bridges retain
explicit fallback. The unchanged upstream ROW contracts keep their native-work requirement.

Flink's generated scalar projection turns a NULL emitted under non-null primitive metadata
into false or a typed -1. A fused projection must execute that evaluation before owned Arrow
writes. External Row results without the projection retain stock sink constraint errors;
LEFT padding stays nullable. Atomic collectors apply their own released wrapping/default
semantics. Tests independently check these behaviors and a boxed non-null result control.

On Flink 1.18, selecting the declared non-null external Row result field through LEFT fails
in the released SQL-to-rel converter before physical planning. Both host and native-enabled
queries must raise the same datatype/nullability assertion, with no substitutions. Separate
LEFT projections omitting that field still require consumed native table-function input.
Atomic-result LEFT queries execute on both lines. Recovery checks keep external Row result
fields observable with INNER and exercise atomic results through LEFT on both backends.

The [complete-job artifact](../benchmarks/results/jvm-table-function-primitive-results-2026-10-24.json)
retains stock, unchanged canonical `01b24b62`, and candidate trials, ranges and source/native
hashes. Four external Row signatures and two atomic signatures require native execution in
the candidate and verified preceding-production fallback. An existing INT/ARRAY control
requires native execution in both revisions, exposing the cost of preserving fused projection
semantics. Each revision has separately paired stock controls.

Jobs use JDK 17, release/mimalloc native code, 262,144 runtime input rows, parallelism one,
2 GiB heap, 1,024-row batches, both perimeter transposes and a blackhole row sink. Every job
has a fresh cluster and symmetric untimed EXPLAIN. Two warmup pairs precede five alternating
measured pairs. Builds and tests from this goal do not overlap timings; host activity and
resource pressure are sampled once per second and retained in the artifact.

| Flink / result signature | Stock median (range), s | Previous native-enabled median (range), s | New median (range), s |
|---|---:|---:|---:|
| 2.2.1 / Row INT / ARRAY | 0.121 (0.112–0.210) | 0.183 (0.104–0.188) | 0.228 (0.169–0.259) |
| 2.2.1 / Row INT / List STRING | 1.015 (1.011–1.027) | 1.003 (0.995–1.028) | 2.349 (1.802–2.476) |
| 2.2.1 / Row DOUBLE / MAP | 0.245 (0.237–0.257) | 0.231 (0.227–0.240) | 0.374 (0.369–0.380) |
| 2.2.1 / Row FLOAT / ROW | 0.160 (0.157–0.166) | 0.144 (0.139–0.152) | 0.301 (0.293–0.308) |
| 2.2.1 / Atomic INT / ARRAY | 0.099 (0.093–0.101) | 0.091 (0.086–0.100) | 0.123 (0.118–0.134) |
| 2.2.1 / Atomic DOUBLE / MAP | 0.162 (0.154–0.175) | 0.150 (0.144–0.152) | 0.213 (0.206–0.228) |
| 2.2.1 / Existing INT / ARRAY control | 0.119 (0.109–0.122) | 0.167 (0.155–0.174) | 0.186 (0.179–0.206) |
| 1.18.1 / Row INT / ARRAY | 0.119 (0.114–0.195) | 0.122 (0.103–0.217) | 0.170 (0.165–0.182) |
| 1.18.1 / Row INT / List STRING | 1.010 (0.992–1.037) | 1.009 (0.989–1.018) | 1.768 (1.732–2.306) |
| 1.18.1 / Row DOUBLE / MAP | 0.240 (0.235–0.255) | 0.227 (0.220–0.234) | 0.366 (0.344–0.373) |
| 1.18.1 / Row FLOAT / ROW | 0.162 (0.155–0.189) | 0.142 (0.132–0.145) | 0.303 (0.289–0.309) |
| 1.18.1 / Atomic INT / ARRAY | 0.095 (0.089–0.101) | 0.090 (0.088–0.097) | 0.119 (0.114–0.143) |
| 1.18.1 / Atomic DOUBLE / MAP | 0.157 (0.150–0.160) | 0.148 (0.144–0.153) | 0.203 (0.189–0.211) |
| 1.18.1 / Existing INT / ARRAY control | 0.122 (0.117–0.126) | 0.161 (0.160–0.169) | 0.171 (0.163–0.182) |

The sampler observed zero other Java/build processes, a maximum CPU-pressure 10-second
average of zero, and a minimum of about 6 GiB available memory. The machine was not reserved
exclusively. These short jobs
include startup/JIT variability and separate-JVM stock drift. Existing-control native medians
increase from 0.167 to 0.186 s on 2.2 and 0.161 to 0.171 s on 1.18. The 2.2 control range
is above its prior range; 1.18 ranges overlap. These figures do not isolate a steady-state
projection cost. All new result cases remain
slower than stock; coverage follows the accepted UDF exception, with no acceleration claim.

One bounded JFR pass of INT / List STRING used 1,048,576 rows, one warmup pair and one measured
pair. Forty sampled stacks contained the table-function operator, four including projection
application; this is too sparse for precise attribution. One measured attempt deferred the
generated projection and used a validity-scanning column view for normal rows. Its control
medians improved 6.3% / 2.4%, but 1.18 Row INT / ARRAY and atomic INT regressed 28.0% / 8.6%.
The attempt was reverted and the investigation stopped. The artifact retains all 588 trials,
including the rejected variant, and its source hashes. Profile timings are excluded.
[The bounded rejection record](https://github.com/datafusion-contrib/StreamFusion/blob/main/.claude/wontdos/237-lazy-primitive-result-projections.md)
sets reopening criteria. No optimization change is retained in this increment.

Reproduce with `SF_BENCHMARK=true mvn -pl streamfusion-runtime -Pbench
-Dtest=PrimitiveResultTableFunctionBenchmark -Dboundary.rows=262144 -Dboundary.warmups=2
-Dboundary.runs=5 -Dsf.testForks=1 -Dstreamfusion.exchange.zeroCopyLocal=false
surefire:test@default-test`; add `flink-1.18` to the profile for that line. Prior controls copy
only the test fixtures and set `-Dboundary.expectNative=false`; the existing native control
continues to require the table-function operator and both transposes.

### Concrete ArrayList results (2026-10-24)

Direct external Row result fields and atomic ARRAY results can use `ArrayList` when their
elements use verified default scalar conversions. List arguments still use the exact
`java.util.List` bridge. These are separate contracts: released Flink accepts List subclasses
for external-to-internal result conversion, but rejects their internal-to-external argument
conversion while planning/generating the job. The converter materializing an ArrayList does
not override that validation rule. Independent ArrayList and LinkedList argument controls
retain the same `ValidationException`, diagnostic and planning phase with zero native
substitutions or consumed native work. The [scoped rejection record](https://github.com/datafusion-contrib/StreamFusion/blob/main/.claude/wontdos/237-concrete-list-arguments.md)
contains the released source evidence; it does not exclude concrete List results.

The expanded suite has 82 conversion checks and four interval-List checks, alongside the
existing primitive-result, DATE-argument, nested, typed and basic table-function coverage: 428
checks on each supported Flink line. The 39 additions cover all admitted scalar families,
INNER/LEFT mutable fanout, actual ArrayList result objects, non-null parent metadata (including
emitted NULLs), retained values across closed batches, empty inputs, failures, direct and atomic
collectors, DATE/int argument composition, and two forced restores with memory/native RocksDB.
Nested ArrayList result fields and Lists/ArrayLists of containers retain fallback. Scalar
membership uses Flink’s predefined logical-type family, so zero-field ROW elements remain
containers even when wrapped in an AtomicDataType. Other concrete/custom List result classes
remain outside this exact-class whitelist. Admission reuses Flink's result converter and the
existing owned Arrow writes; no runtime or native-memory path changes.

The [complete-job artifact](../benchmarks/results/jvm-table-function-arraylist-results-2026-10-24.json)
records the exact final source/native hashes, all validation outcomes and 784 timing trials.
The final matrix uses 262,144 rows, one task, a 2 GiB heap, 1,024-row transpose batches, two
warmup pairs and five measured alternating pairs. Inputs cycle 64 prebuilt arrays; within
that fixture, index multiples of 13 give NULL parents, other multiples of seven give empty
parents, and the remaining parents have three elements including a NULL middle element.
INT, STRING (including unpaired UTF-16 surrogates), and DECIMAL(38,18) cases keep the same
List arguments and mutable function code across stock, prior and candidate. The unchanged
control returns the List interface instead of ArrayList.

Fresh-cluster timing includes submission, generated callbacks, conversions, production
transport and both perimeter transposes into a row blackhole sink. Setup and symmetric
EXPLAIN are outside timing. Prior production (`e5c3b4d0`) uses full stock fallback for the
new result bridges, while the List STRING control is native on both revisions. Every
candidate run requires consumed table-function work and both transposes. Seconds below
are median (minimum–maximum).

| Flink / result bridge | Stock in candidate JVM | Prior native enabled | Candidate native enabled |
|---|---:|---:|---:|
| 2.2.1 / ArrayList INT | 0.148 (0.131–0.241) | 0.176 (0.141–0.198) | 0.213 (0.206–0.290) |
| 2.2.1 / ArrayList STRING | 0.252 (0.244–0.262) | 0.247 (0.242–0.252) | 0.470 (0.466–0.476) |
| 2.2.1 / ArrayList DECIMAL(38,18) | 0.275 (0.261–0.282) | 0.257 (0.251–0.267) | 0.443 (0.438–0.452) |
| 2.2.1 / List STRING control | 0.260 (0.251–0.270) | 0.476 (0.468–0.482) | 0.474 (0.469–0.475) |
| 1.18.1 / ArrayList INT | 0.223 (0.141–0.310) | 0.139 (0.125–0.170) | 0.358 (0.252–0.434) |
| 1.18.1 / ArrayList STRING | 0.284 (0.253–0.323) | 0.241 (0.236–0.265) | 0.582 (0.556–0.652) |
| 1.18.1 / ArrayList DECIMAL(38,18) | 0.297 (0.286–0.314) | 0.252 (0.248–0.255) | 0.497 (0.477–0.587) |
| 1.18.1 / List STRING control | 0.278 (0.267–0.313) | 0.490 (0.476–0.499) | 0.518 (0.496–0.668) |

These native callbacks remain slower than stock. The unchanged native control differs from
its prior median by -0.5% on 2.2 and +5.6% on 1.18. Separate-JVM stock controls and
short-job startup/JIT variability remain visible in the ranges. These results do not establish
sustained throughput or a causal speedup. Verified UDF coverage follows the accepted
performance exception, without an extra opt-in or acceleration claim.

The final 224-trial matrix retains all 560 earlier trials and their original source hashes.
Initial 2.2 candidate timings overlapped two other Java processes (CPU pressure avg10 up to
0.67%, about 4 GiB minimum available memory), followed by one quieter 2.2 check. Scalar
classification was then refined to exclude empty ROW elements, including AtomicDataType
wrappers, and the final matrix measures the exact logical-family guard. These are validation
snapshots; the callback, Arrow implementation and timed fixture bodies are unchanged.

In the final matrix, the 2.2 forks had no sampled other Java/build processes. Other Java
activity overlapped parts of both 1.18 forks, with CPU pressure avg10 up to 1.05% and at least
5.3 GiB available memory. All observations and unfavorable trials remain in the artifact.
No build or test from this goal overlaps a timing run; CPUs were not reserved.

Optimization stopped after one 16-second JFR pass over a separate 1,048,576-row String case,
with one warmup pair and one observed pair. Its 48 table-operator stack samples included 27
stacks in owned Arrow writes and 14 leaves in integer-writer null checks, alongside generated
callbacks and released conversion. This sparse profile exposed no isolated concrete-result
bridge hotspot or easy safe class-specific change. No optimization prototype or fixture
optimization was attempted. The later classifier refinement changed no callback or Arrow
implementation; the profile's original provenance and diagnostic timings remain separate
from the cost matrix.

Reproduce with `SF_BENCHMARK=true mvn -pl streamfusion-runtime -Pbench
-Dtest=ArrayListTableFunctionBenchmark -Dboundary.rows=262144 -Dboundary.warmups=2
-Dboundary.runs=5 surefire:test@default-test`; add `,flink-1.18` to the profile for 1.18.1.
`arraylist.boundaries` defaults to `arraylist:INT;arraylist:STRING;arraylist:DECIMAL(38,18);CONTROL_list:STRING`.
For the actual prior revision, copy the conversion, interval and benchmark test fixtures only
and set `-Dboundary.expectNative=false`; the control still requires native work. Production
sources must remain unchanged.

### DATE epoch-day arguments and containers (2026-10-24)

Twenty-one checks verify direct non-null DATE arguments bridged to Java `int`, alongside
boxed ARRAY, primitive ARRAY, scalar List, MAP and ROW values. Functions return the received
epoch day as an INT, so parity observes the conversion rather than merely ignoring the
argument. Values span dates before and after the epoch and SQL's year 0001/9999 boundaries.
INNER/LEFT fanout, nullable containers and children, DATE literals, rejected nullable input,
mutable results, retained arguments across closed batches, lifecycle and failure order are
checked independently against released Flink. Updating input survives two forced restores
with both memory and native RocksDB backends. Negative controls preserve fallback for boxed
DATE/Integer arguments, primitive DATE result fields and atomic results, and DATE/int bridges
nested inside ROW or ARRAY arguments. The shared primitive-array/result whitelist is unchanged.

At the DATE-argument increment, the local regression set contained 389 checks on each
supported Flink line, including the 368 preceding table-function checks. Admission uses the released identity DATE converter
and correlate generator; it adds no runtime implementation or native-memory path.

The [complete-job artifact](../benchmarks/results/jvm-table-function-date-int-arguments-2026-10-24.json)
compares unchanged prior production (`6f5dae00`) with the candidate, each with its own stock
control. Each case uses 262,144 rows, one task, a 2 GiB heap, 1,024-row transpose batches, two
warmup pairs and five measured alternating pairs. Every job starts a fresh Flink cluster;
timing includes submission, the released generator, callbacks, owned conversions, production
transport and both perimeter transposes into a row blackhole sink. Setup and symmetric EXPLAIN
are outside timing. All 224 trials, including warmups, source/native hashes, exact routes and
sampled host observations are retained. No local build or test overlaps these timings.

Seconds are median (minimum–maximum). Prior production uses full stock fallback for the first
three cases; the LocalDate control already executes the native table-function operator.

| Flink / argument + container | Stock in candidate JVM | Prior native enabled | Candidate native enabled |
|---|---:|---:|---:|
| 2.2.1 / DATE/int + ARRAY | 0.126 (0.120–0.186) | 0.125 (0.115–0.191) | 0.193 (0.185–0.270) |
| 2.2.1 / DATE/int + List | 1.014 (1.011–1.021) | 1.022 (1.016–1.032) | 2.341 (1.791–2.365) |
| 2.2.1 / DATE/int + MAP | 0.234 (0.225–0.248) | 0.228 (0.221–0.239) | 0.369 (0.361–0.396) |
| 2.2.1 / LocalDate + ARRAY control | 0.129 (0.121–0.132) | 0.191 (0.182–0.199) | 0.189 (0.181–0.200) |
| 1.18.1 / DATE/int + ARRAY | 0.117 (0.109–0.185) | 0.123 (0.103–0.186) | 0.181 (0.164–0.251) |
| 1.18.1 / DATE/int + List | 1.019 (1.009–1.026) | 1.018 (1.000–1.041) | 1.806 (1.766–2.357) |
| 1.18.1 / DATE/int + MAP | 0.235 (0.230–0.236) | 0.226 (0.221–0.233) | 0.375 (0.366–0.377) |
| 1.18.1 / LocalDate + ARRAY control | 0.128 (0.114–0.154) | 0.186 (0.175–0.201) | 0.187 (0.180–0.195) |

The new native cases are slower than stock. The unchanged native control is within about 1.6%
of its prior median on both lines, while separate-JVM stock controls also show drift. These
short complete-job measurements retain startup/JIT variability and do not establish sustained
throughput. The sampled host had no other Java/build processes or CPU pressure during the
matrix; this does not reserve CPUs. Verified coverage follows the accepted UDF performance
exception, with no acceleration claim or additional opt-in.

Optimization was timeboxed to one 36-second JFR pass over a separate 1,048,576-row List
case (one warmup pair and one observed pair). Its 37 table-operator stack samples included 19
leaves in the user callback, four in the released List-to-array copy, and shared Arrow writes;
only two stacks included the folded projection. This sparse profile exposed no isolated
DATE-argument hotspot or easy safe runtime change. The argument uses an identity converter;
removing owned copies or changing the user fixture would change the measured contract. No
optimization prototype was attempted. The profile summary is in the artifact; its diagnostic
timings are excluded from the 224-trial cost matrix.

Reproduce with `SF_BENCHMARK=true mvn -pl streamfusion-runtime -Pbench
-Dtest=DateIntArgumentTableFunctionBenchmark -Dboundary.rows=262144 -Dboundary.warmups=2
-Dboundary.runs=5 surefire:test@default-test`; add `,flink-1.18` to the profile for 1.18.1.
The benchmark defaults to `array;list;map;CONTROL_array` through `date.boundaries`. To benchmark
the actual previous revision, copy only the two DATE test/benchmark fixtures into that checkout
and set `-Dboundary.expectNative=false`; keep production sources unchanged.

### Mixed primitive arguments and containers (2026-10-24)

Forty-eight additional checks cover all seven non-null primitive scalar argument types with
boxed ARRAY, primitive ARRAY, List, MAP and ROW values. Both INNER and LEFT application compare
released-Flink results and resolved types, including signed zero and raw NaN payloads. Literal
binding, nullable-input planning failures, empty-input evaluation, exception causes, mutable
fanout, retained containers across closed batches, all changelog kinds and two forced restores
with memory/native RocksDB state retain parity. Existing boxed arguments keep their route.
Direct non-null DATE-as-int arguments in container signatures are now covered by the
DATE argument checks on this page; other DATE bridges retain fallback. Primitive result
metadata is now covered by the result-conversion checks on this page.
The 172 existing List/primitive-array/nested/typed checks remain in the regression set on each
supported Flink line. Existing unchanged upstream ROW contracts keep their native-work
requirement; this increment does not change an upstream execution expectation.

The [complete-job artifact](../benchmarks/results/jvm-table-function-mixed-primitives-2026-10-24.json)
records all trials and source/native hashes. It compares stock Flink, canonical `8eb2b004`
with unchanged production code (full fallback), and the new argument admission. Jobs use the
same runtime row source, blackhole row sink, both perimeter transposes, release/mimalloc
native payload, JDK 17, parallelism one, 2 GiB heap, 262,144 input rows and 1,024-row batches.
Each job has a fresh cluster and symmetric untimed EXPLAIN; two warmup pairs precede five
alternating measured pairs. Prior production has separately paired stock controls.

| Flink / signature | Stock median (range), s | Prior fallback median (range), s | New median (range), s |
|---|---:|---:|---:|
| 2.2.1 / INT / ARRAY | 0.188 (0.103–0.201) | 0.173 (0.099–0.183) | 0.169 (0.140–0.237) |
| 2.2.1 / INT / List STRING | 1.017 (1.000–1.032) | 1.014 (1.010–1.037) | 2.355 (1.803–2.404) |
| 2.2.1 / DOUBLE / MAP | 0.233 (0.228–0.246) | 0.232 (0.217–0.250) | 0.366 (0.359–0.369) |
| 2.2.1 / FLOAT / ROW | 0.153 (0.146–0.157) | 0.144 (0.136–0.147) | 0.288 (0.278–0.292) |
| 1.18.1 / INT / ARRAY | 0.111 (0.105–0.230) | 0.110 (0.103–0.186) | 0.163 (0.157–0.169) |
| 1.18.1 / INT / List STRING | 1.031 (1.005–1.037) | 1.021 (1.005–1.034) | 1.839 (1.809–2.404) |
| 1.18.1 / DOUBLE / MAP | 0.258 (0.231–0.280) | 0.222 (0.217–0.224) | 0.402 (0.353–0.417) |
| 1.18.1 / FLOAT / ROW | 0.155 (0.153–0.159) | 0.141 (0.135–0.143) | 0.289 (0.281–0.298) |

These short jobs include startup/JIT variability and separate-JVM drift. The other-Java-process
count sampled once per second was zero, and this investigation's builds/tests did not overlap
timings; the machine was not reserved exclusively. Most measured signatures are slower than
stock Flink. This is verified UDF coverage under the accepted performance exception, with no
acceleration or sustained-throughput claim.

One bounded JFR profile of INT / List STRING used 1,048,576 rows, one warmup pair and one
measured pair. Only 41 sampled stacks contained the table-function operator, spread among
callbacks, released List conversion and owned Arrow writes. This is too sparse for precise
attribution or an easy safe improvement; investigation stopped after one pass without an
optimization change. Profile timings are excluded from the comparison.

Reproduce with `SF_BENCHMARK=true mvn -pl streamfusion-runtime -Pbench
-Dtest=MixedPrimitiveTableFunctionBenchmark -Dboundary.rows=262144 -Dboundary.warmups=2
-Dboundary.runs=5 -Dsf.testForks=1 -Dstreamfusion.exchange.zeroCopyLocal=false
surefire:test@default-test`; add `flink-1.18` to the profile for that line. Prior controls copy
only test fixtures and set `-Dboundary.expectNative=false`, requiring zero substitutions.

### List and primitive array validation and costs (2026-10-23)

Forty-seven new checks pass on each supported Flink line, alongside 125 existing nested/typed
table-function regressions. They cover scalar List elements (including both interval families),
all seven primitive array types, invocation of concrete Java signatures, atomic-array result collectors,
retained arguments, mutable fanout, NULL/empty values, exact floating bits through chained calls,
error causes, all RowKinds and two forced restores on memory and native RocksDB state.
Unverified source filters, nested alternate bridges, internal containers and Lists of containers
keep explicit fallback checks. Existing unchanged upstream ROW contracts still require consumed
native table-function input on both lines.

[The complete-job artifact](../benchmarks/results/jvm-table-function-conversions-2026-10-23.json)
retains all stock, candidate and actual prior-production trials, ranges and source hashes.
Release/mimalloc jobs use JDK 17, 262,144 runtime input rows, parallelism one, 2 GiB heap,
1,024-row batches, both transposes and a blackhole row sink. Each job gets its own cluster and
untimed EXPLAIN; two warmup pairs precede five alternating measured pairs. Builds and tests
from this investigation do not overlap timings; the sampled other-Java-process count was zero.

| Flink / value | Stock median (range), s | Prior fallback median (range), s | New median (range), s |
|---|---:|---:|---:|
| 2.2.1 / INT primitive array | 0.159 (0.147–0.238) | 0.176 (0.147–0.223) | 0.321 (0.234–0.343) |
| 2.2.1 / DOUBLE primitive array | 0.154 (0.140–0.187) | 0.155 (0.140–0.166) | 0.253 (0.247–0.261) |
| 2.2.1 / List STRING | 0.264 (0.258–0.284) | 0.258 (0.251–0.274) | 0.482 (0.474–0.502) |
| 2.2.1 / List DECIMAL(38,18) | 0.270 (0.264–0.286) | 0.257 (0.249–0.265) | 0.457 (0.450–0.462) |
| 1.18.1 / INT primitive array | 0.153 (0.144–0.160) | 0.151 (0.139–0.219) | 0.256 (0.234–0.294) |
| 1.18.1 / DOUBLE primitive array | 0.152 (0.134–0.159) | 0.145 (0.141–0.166) | 0.245 (0.240–0.249) |
| 1.18.1 / List STRING | 0.256 (0.253–0.284) | 0.251 (0.244–0.257) | 0.501 (0.499–0.507) |
| 1.18.1 / List DECIMAL(38,18) | 0.264 (0.263–0.268) | 0.262 (0.260–0.267) | 0.447 (0.445–0.455) |

Prior canonical `189edb21` retains unchanged production code and executes full stock fallback;
its separately paired stock controls remain in the artifact. Short jobs include startup/JIT
variation and separate-JVM drift. These slower conversions are verified UDF coverage under the
accepted performance exception, with no acceleration or sustained-throughput claim.
A separate, bounded List STRING JFR pass observed generated callbacks and owned Arrow writes
in 28 operator samples, too sparse for precise attribution. No easy safe improvement was
established, so profiling stopped after one pass with no optimization change. Those profile
timings are excluded from the comparison.

Reproduce with `SF_BENCHMARK=true mvn -pl streamfusion-runtime -Pbench
-Dtest=TableFunctionConversionBenchmark -Dboundary.rows=262144 -Dboundary.warmups=2
-Dboundary.runs=5 -Dsf.testForks=1 -Dstreamfusion.exchange.zeroCopyLocal=false
surefire:test@default-test`; add `flink-1.18` to the profile for that line. Prior controls copy
only the test fixtures and set `-Dboundary.expectNative=false`, requiring zero substitutions.

### Container callback costs (2026-10-23)

Release/mimalloc complete jobs on Intel i7-12650H/Linux WSL2, JDK 17.0.20.1 and Flink 2.2.1 use
262,144 runtime rows, parallelism one, a 2 GiB heap, 1,024-row batches, both transposes and a
blackhole row sink. Both engines receive untimed EXPLAIN. Two warmup pairs precede five
alternating measured pairs; no other local builds/tests/timings run concurrently.

| Fanout value | Stock median (range), s | Previous fallback median (range), s | New median (range), s |
|---|---:|---:|---:|
| ARRAY INT | 0.175073 (0.091724–0.196773) | 0.166436 (0.098027–0.177716) | 0.205787 (0.145973–0.223976) |
| MAP STRING/INT | 0.239401 (0.231195–0.254029) | 0.218140 (0.213235–0.237609) | 0.388336 (0.368596–0.454621) |
| ROW with DECIMAL(38,18) | 0.128294 (0.122698–0.142771) | 0.114790 (0.101556–0.159279) | 0.219902 (0.212385–0.241299) |

Previous canonical `1bd9daa8` executes full stock fallback. Its separately paired stock medians
are 0.149210/0.232581/0.106898 s; every sample, source hash and configuration is retained in the
[container callback artifact](../benchmarks/results/nested-jvm-table-functions-2026-10-23.json).
These short jobs show startup/JIT variation and slower callback paths; they do not establish
sustained throughput or a causal speedup. Verified coverage follows the accepted UDF exception.
A separate bounded JFR pass sampled released MAP conversion and immediate owned Arrow writes
(20 operator CPU samples, too sparse for precise attribution). No easy optimization preserving
those contracts was established, so the investigation stopped after one profile pass without
an optimization change. Profile timings are excluded from the table.

Reproduce with `SF_BENCHMARK=true mvn -pl streamfusion-runtime -Pbench
-Dtest=TypedTableFunctionBenchmark -Dtablefunc.rows=262144 -Dtablefunc.warmup=2
-Dtablefunc.runs=5 '-Dtablefunc.types=nested:array_int;nested:map;nested:row_DECIMAL(38,18)'
-Dtablefunc.cases=project -Dstreamfusion.exchange.zeroCopyLocal=false test`.
The previous-production control copies only these test fixtures and sets
`-Dtablefunc.expectNative=false`; it must report zero substitutions.

The operator consumes and produces `ArrowBatch`. It borrows input row views only during the
current batch and invokes the released Flink correlate generator, retaining its conversions,
pushed predicates, NULL/empty behavior, LEFT padding and function lifecycle. Each collected row
is written immediately, before a function can mutate or reuse it. Output batches have the normal
transpose row limit and carry all four RowKinds. No function invocation crosses JNI; surrounding
native operators retain their existing boundaries. A pure column projection is fused into the
output writer, including repeated or empty projections, so discarded correlate columns never
enter an intermediate Arrow batch.

CHAR uses the same string transport as VARCHAR and the released correlate generator's
declared CHAR type. SQL casts retain Flink's padding and truncation; the bridge does not
normalize a function's returned strings. Typed runtime tests cover CHAR(1)/CHAR(4), empty
strings, trailing spaces, over-width values, NULL, INNER/LEFT fanout, repeated output reuse,
and heap/RocksDB recovery. Java table-function execution remains subject to the accepted
UDF performance exception; admission does not claim a throughput improvement.

A release complete-job check on Apple Silicon/JDK 11 with Flink 2.2.1, parallelism one,
262,144 runtime rows, 1,024-row batches, both transposes and a blackhole row sink measured
CHAR(4) fanout at 0.124551s stock (0.118949-0.133813s) and 0.177585s native
(0.167164-0.184397s), medians of five alternating pairs after two warmups. These short jobs
include startup and are a callback-cost diagnostic, not a sustained-throughput claim.
Before admission the same query used the stock fallback. Reproduce with `TypedTableFunctionBenchmark`,
`-Pbench`, `tablefunc.types=CHAR(4)`, `tablefunc.cases=project` and `tablefunc.rows=262144`.

The exact and temporal types use the same released correlate generator and existing Arrow
readers/writers; their user function still executes on the JVM. TIMESTAMP and TIMESTAMP_LTZ
retain precision 0/3/6/9, pre-epoch fractions and years outside Arrow's nanosecond epoch range.
DECIMAL results retain the host's scale rounding and NULL on precision overflow, including
precision-38 inputs and scale-38 outputs. Released SQL resolves TIME inputs to TIME(0), whose
internal value can retain milliseconds; a function explicitly demanding TIME(3) retains Flink's
code-generation type mismatch rather than silently changing its signature.

Floating cases include signed zeros, subnormal/extreme values, infinities and NaNs; VARBINARY
includes empty values, embedded zero/high bytes and 4,097-byte payloads. Interval functions use
the released planner's canonical SECOND(3)/MONTH signatures and inferred input types. A declaration
that requires an unsupported implicit interval cast or differs from the planner's normalized
output type retains the host's planning/code-generation failure. Native substitution does not
rewrite a user function's signature.

The expanded suite passes 54 checks on each released Flink line, including all-row-kind
memory/RocksDB recovery for the newly admitted floating, byte-array and interval signatures.
[Validation details](../benchmarks/results/typed-table-function-scalars-2026-10-19.json) retain
the source/library hashes, cases and released interval-planner fixture limitations.

The typed SQL fixtures compare independent host/native executions across UTC, Asia/Shanghai and
America/Los_Angeles, including DST boundary values, INNER/LEFT padding, NULL arguments/results,
zero/two-row fanout, pushed predicates, repeated projections and mutable output reuse across
127-row batches. Recovery fixtures propagate all four RowKinds through the function into a keyed
aggregate and force two restores on each of memory and native RocksDB state.

### Exact and temporal boundary measurements

The type expansion is correctness groundwork with an accepted performance tradeoff. All eighteen
projection/grouped cases at two million runtime rows remain slower than their paired Flink
controls; adding types does not move the JVM function into Rust. Flink 2.2.1/JDK17 uses the
fresh baseline release+mimalloc library, parallelism one, 2 GiB heap, 1,024-row batches,
production IPC, two warmups and five alternating measured pairs. Both transposes and the
blackhole row sink remain inside timing. The grouped case uses 64 groups and one-phase
COUNT aggregation; source typed values are prepared outside execution.

| Type / shape | Stock median, s | Native median, s | Actual preceding fallback median, s |
| --- | ---: | ---: | ---: |
| BOOLEAN / projection | 0.288124 | 0.432154 | 0.296479 |
| BOOLEAN / grouped | 0.671458 | 0.875822 | 0.736061 |
| DECIMAL(38,18) / projection | 0.268254 | 0.714318 | 0.240361 |
| DECIMAL(38,18) / grouped | 0.762733 | 0.942262 | 0.741088 |
| TIMESTAMP_LTZ(9) / projection | 0.310736 | 0.627484 | 0.266546 |
| TIMESTAMP_LTZ(9) / grouped | 0.681617 | 1.013311 | 0.676114 |

Previous-code controls run the unchanged production code at `adbdafe4`, with identical new
benchmark fixtures. They fall back completely with zero native substitutions. Separate JVMs
show control drift, so differences between candidate and previous medians are not isolated
cost attribution. All trial arrays, warmups, narrower types, initial one-million-row results,
source/library hashes and CPU samples are in the
[raw evidence](../benchmarks/results/typed-table-functions-2026-10-19.json).

CPU profiling identifies decimal conversion and owned row serialization as the main boundary
costs. Lazy RowKind allocation and a generated wide-decimal binary exit failed to establish
improvement and were removed; the
[rejected experiments](https://github.com/datafusion-contrib/StreamFusion/blob/main/.claude/wontdos/379-typed-table-function-boundaries.md) retain
both measurements. Profiling is diagnostic; its durations are excluded from speed claims.
The [bounded follow-up](../optimizations/decimal-arrow-reads.md) reduces decimal read costs;
further conversion work is deferred after that pass found no other simple change.
The ordinary `streamfusion.operator.tableFunction.enabled=false` override disables
table-function substitution for all admitted types.

Arrow decimal reads now avoid temporary byte arrays and BigInteger construction when the
actual unscaled value fits a signed long, including in wide-precision columns. The
[bounded conversion investigation](../optimizations/decimal-arrow-reads.md) records the
complete-job gains and remaining stock-Flink deficit. Larger values, temporal conversion
and owned exit copies retain their existing paths; no collector or ownership contract changes.

This does not complete [#237](https://github.com/datafusion-contrib/StreamFusion/issues/237): JVM
two-phase/OVER aggregate functions, additional scalar conversion classes and specialized/async/legacy
table-function contracts remain open. Performance evidence includes release jobs with row sources, both transposes, row
sinks, stock Flink and the actual preceding revision; profiles include Java and symbolized Rust
CPU stacks plus sampled Java allocations. Profiled durations are excluded from speed claims.

The following v1 results used native-only pre-clock EXPLAIN and metrics clusters per trial
pair. They remain historical diagnostics; corrected comparisons use EXPLAIN for both engines
and a fresh cluster for every job. [Method correction](../benchmarks/jvm-aggregates.md#symmetric-preflight-correction)
records the change. Both Flink lines pass all four corrected table-function shapes in fixture
smokes and full-size reruns. SQL/data/resources/transposes are unchanged.

Released Flink 2.2.1/JDK 17 release jobs measured 2M-row splitting at **1.055 s native versus
1.348 s stock and 1.393 s previous StreamFusion**, and 500k-row split-plus-JSON aggregation at
**1.012 s versus 4.409 s stock and 4.324 s previous**. Each has two warmups and five alternating
pairs, parallelism one, 2 GiB heap and 1,024-row batches. Each trial pair closes its metrics
cluster to release completed aggregate state; cluster boot is outside timing, while planning,
job submission, source/sink and state costs remain inside. Previous-code controls use actual
revision `b965b8bf`; separate JVMs show control drift. See the
[raw trials and profiles](../benchmarks/results/jvm-table-functions-2026-10-15.json).
The same released Flink 1.18.1 configuration measured split at 1.077 s native versus 1.352 s
stock and 1.400 s previous, and grouped output at 1.047 s versus 4.440 s stock and 4.263 s previous.
The grouped gain includes existing native JSON aggregation. Every candidate range is below its
paired stock range; separate-JVM controls still limit attribution between implementations.

Further 2M-row release cases retain the same resources, warmups, five pairs and actual preceding
revision, including both transposes:

| Flink / case | Stock median (range), s | Native median (range), s | Previous median, s |
| --- | ---: | ---: | ---: |
| 2.2.1 / 128-character split tokens | 2.149 (2.147–2.219) | 1.905 (1.889–1.966) | 2.177 |
| 1.18.1 / 128-character split tokens | 2.116 (2.089–2.154) | 1.918 (1.876–1.943) | 2.122 |
| 2.2.1 / INT/BIGINT arguments and results | 0.767 (0.747–0.796) | 0.525 (0.512–0.600) | 0.796 |
| 1.18.1 / INT/BIGINT arguments and results | 0.794 (0.749–0.813) | 0.530 (0.520–0.642) | 0.749 |

The numeric source emits an INT key and nullable BIGINT value; each non-NULL invocation emits
two rows with BIGINT/INT results. The width parameter is unused there. These are representative
function boundaries, not a guarantee that every user function runs faster.

### Corrected complete-job results

[The symmetric-preflight matrix](../benchmarks/results/jvm-aggregates-symmetric-isolated-2026-10-16.json)
uses a fresh cluster for each job, 2 GiB heap, parallelism one, production IPC, both 1,024-row
transposes, two warmups and five alternating pairs on separate four-CPU Ubuntu/JDK17 runners:

| Case | Rows | Flink 2.2 stock/native median, s | Flink 1.18 stock/native median, s |
| --- | ---: | ---: | ---: |
| STRING split, width 16 | 2M | 2.398 / 1.889 | 2.385 / 1.927 |
| INT/BIGINT arguments and results | 2M | 1.476 / .902 | 1.420 / .914 |
| Split plus JSON grouping | 500k | 3.668 / 1.302 | 3.604 / 1.436 |
| STRING split, width 256 | 2M | 4.729 / 4.570 | 4.441 / 4.571 |

Native ranges lie below stock for the first three shapes on both lines and wide output on
2.2. Wide output on 1.18 regresses by 2.9%, with every paired native trial slower; its ranges
overlap slightly. All trials and ranges are preserved. The grouped gain includes existing
native JSON aggregation. These runs use the ordinary copying STRING exit; the default-off
[borrowed STRING exit investigation](../optimizations/zero-copy-exit-transpose.md#experimental-borrowed-string-exit)
targets that profiled copy and requires separate performance evidence. This corrected matrix
does not yet repeat the previous-fallback revision control.

The separate, default-off [borrowed-exit matrix](../benchmarks/results/jvm-aggregates-string-exit-isolated-2026-10-16.json)
also completes both lines with all trials retained. Stock/native medians are 2.403/1.907 s
(split), 1.485/.931 s (numeric), 3.470/1.528 s (grouped) and 4.500/4.497 s (wide) on 2.2;
on 1.18 they are 1.563/1.332, 1.114/.704, 3.030/1.215 and 3.503/3.481 s respectively.
Wide ranges overlap on both lines, so no clear wide-output gain is established. Other shapes
retain separated native/stock ranges. Host/control drift limits comparisons across matrices,
and the new exit path remains default off. Actual previous-runtime controls now use the same corrected fixtures in the completed
comparison below.

[The subsequent completed comparison](../benchmarks/results/jvm-aggregates-key-projection-isolated-2026-10-16.json)
includes actual prior-runtime `b965b8bf` controls on both lines. Stock/candidate/previous medians
on 2.2 are 2.457/1.809/2.267 s for split, 1.466/.951/1.514 s for numeric, 3.862/1.428/3.561 s
for grouped and 4.793/4.863/4.415 s for wide output. On 1.18 they are 2.326/1.891/2.318,
1.414/.904/1.379, 3.381/1.422/3.446 and 4.433/5.068/4.364 s respectively. Previous controls
run in separate JVMs; their own stock trials and all ranges are retained. The wide regression
remains unresolved, especially on 1.18. The borrowed exit stays default off; the aggregate
key-projection change is not a causal explanation for table-function timings.

The writer reuses one short lazy Java String's UTF-8 encoding in the shared Arrow
writer, with bounded retention and cold-workload bypass. It retains normal output ownership and
Flink conversion. [Matched profile evidence](../benchmarks/results/jvm-table-repeated-encoding-profiles-2026-10-16.json)
reduces sampled table-function allocations by 34%/32% on 1.18/2.2; all profile timings are excluded.
The [encoding technique](../optimizations/string-copy-reduction.md#repeated-lazy-string-encodings)
describes the bounds. Forty-one focused checks pass on each line, including table-function
recovery, mutable rows, UTF-8/null handling, array cells and writer reset. Default portable/JSON
regressions pass 73 on 2.2 and 71 with two existing skips on 1.18.

The [completed isolated encoding comparison](../benchmarks/results/jvm-encoding-isolated-2026-10-16.json)
measures repeated wide text at 2.817 s native versus 4.802 stock on 2.2, and 2.941 versus 4.911 s
on 1.18 (1.70×/1.67× stock). Actual previous-native medians are 4.663/4.846 s. Every runtime retains
its own stock controls, raw trials and ranges. Width-256 row-distinct text still regresses on 2.2:
4.776 s native versus 4.564 stock; 1.18 is 5.158 versus 5.186 with overlapping ranges. These jobs
enable the borrowed exit and retain both transposes and production IPC; they do not establish a
blanket default admission.

The new writer policy keeps its single encoding useful when changing rows produce adjacent equal
results, resetting its miss counter on a hit. [Mixed profiles](../benchmarks/results/jvm-encoding-adaptive-profiles-2026-10-16.json)
reduce sampled cold-text encoding allocation by about half and inclusive table-function allocation
by 14%/18% on 1.18/2.2. Forty-one focused checks pass on each line. Every profile duration is excluded;
whole-job stock and actual previous-runtime comparisons for this rule remain unmeasured.
The maintainer accepted shipping the correctness-tested implementation on October 16
with further performance work deferred; no speed claim is made for this reset rule.

Validation passes 67 selected checks on Flink 2.2.1 and 64 on 1.18.1, with three existing UNNEST
version skips on 1.18.1. Coverage includes mutable output reuse, 8,193 results from one invocation,
all RowKinds, lifecycle errors, repeated/zero-column projection, numeric consumers and a
parallelism-three pipeline with keyed aggregates before and after the function. Four recovery
scenarios per line cover memory/native RocksDB and append/retracting input, each with two forced
restores and consumed-row/memory-release assertions. Container signatures with unverified external
conversions or representation consumers, and an explicitly disabled table-function operator,
retain complete-query fallback with zero native substitutions. Other JVM function contracts remain
tracked in [#237](https://github.com/datafusion-contrib/StreamFusion/issues/237).
