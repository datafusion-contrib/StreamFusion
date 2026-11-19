# Calc coverage by topic: jvm collections

[Topic index and original links](calc-filter.md). Sections retain their recorded scope, dates and evidence.

## Nested Java scalar results {#nested-java-scalar-results}

`streamfusion.experimental.nestedScalarUdfs` enables synchronous external Java `Row`
arguments and results, using Flink's complete generated Calc evaluator through the existing batch
bridge. It defaults to true under the accepted UDF performance exception; explicit false
retains the preceding transport and whole-query fallback. Computed ROW-valued programs and filters use whole-Calc generation,
including SQL ROW constructors and comparisons, so physical metadata never becomes a logical SQL
field. Input-reference columns carried beside unrelated scalar work keep the existing native path;
whole-Calc generation is required when an expression produces or consumes a ROW value. Recursive
external ROW/ARRAY/List/MAP conversions, including primitive and ROW-array results, use the
[verified conversion boundary](#recursive-external-scalar-conversions) below. Nested fixed BINARY
uses the transport described below. Custom inference may select those verified conversions;
explicit and delegating default inference remain admitted. Specialization, internal Java
conversion classes and signatures outside that whitelist retain whole-query fallback.
Existing logical-type, expression and producer/consumer guards continue to apply.

The earlier default-inference increment changed planning admission, not runtime conversion or ownership.
The released Calc generator retains resolved types, NULLs, named Row field mapping, nested
RowKinds, Java STRING contents, callback order and immediate ownership of mutable results.
Its initial selection passed 48 checks on released Flink 2.2.1 and 47 on Flink 1.18.1 with JDK 17
and the release/mimalloc native library. One existing ARRAY_SORT check is N/A on 1.18;
all 18 checks passed on both lines. These cover explicit/delegating inference,
shared-call, retention, failure and heap/RocksDB recovery alongside the existing nested
scalar controls. Populated admitted cases require consumed native Calc input. Custom internal
RowData remains an explicit whole-query fallback control; alternate external List children now
use the recursive conversion qualification below.
`InferredNestedScalarBenchmark` retains complete stock/previous/native jobs and both transposes;
this coverage uses the accepted UDF performance exception without an acceleration claim.
The earlier combined temporal/primitive/inference selection passed 255 checks on 2.2.1 and 254
on 1.18.1, plus the existing ARRAY_SORT N/A, with zero failures/errors.
Existing delegating ARRAY/MAP inference regressions also require native Calc input with
matching values and resolved types; their internal `MapData` alternative retains fallback.

Flink's released converters own external callback inputs and normalize mutable results before the
next invocation. This retains named-row field mapping, reusable ROW/array results, NULLs, Java
string conversion and callback failures. No generic-array conversion shortcut is enabled: a
profiled experiment reduced allocations but changed Unicode ARRAY_SORT results.

### Recursive external scalar conversions {#recursive-external-scalar-conversions}

Synchronous scalar calls use the actual argument and result DataTypes selected by released
Flink inference. Generated calls delegate method/overload/varargs validation, external conversion
and evaluation to Flink, including when the Java method returns `Object` or an annotated List.
The verified directions match the JVM table-function conversion rules:

- External `Row` and `Map` interface conversions recurse through every field, key and value.
- ARRAY arguments/results admit Java object arrays and supported primitive arrays. Arguments
  also admit the `List` interface; results additionally admit List implementations, including
  nested custom implementations and their actual `toArray` behavior.
- Children may be ARRAY/List/ROW/MAP recursively, with the documented boolean, signed numeric,
  decimal, character, binary, date/time/timestamp and interval leaves. Verified primitive and
  JDBC/boxed temporal bridges may occur beneath these containers. Primitive leaves require
  the released NOT NULL signature.

Before the released argument converter, the shared adapter repairs copied `Object[]` into the
required internal array class, recursively through ROW and aligned MAP key/value arrays. It
preserves actual NULL slots and RowKinds and does not replace Flink's converters. ROW calls
select preserving collection transport only when their nested logical types require it;
existing nullable scalar-array field projections retain their preceding native route.

Primitive DATE/TIME int[] and interval int[]/long[] results require an enclosing ARRAY/List or
MAP converter that serializes them first. Results reached only through ROW retain fallback
because later released copies can reject that backing. TIME/long[] and LTZ/int[]/long[] retain
matching released failure controls on fallback. Concrete List arguments retain released planning
rejection; internal ArrayData/MapData/RowData, MULTISET/STRUCTURED/RAW signatures and specialized
functions remain outside this whitelist.

Within a generated Calc, Flink retains external values until all predicates and projections
finish, including shared calls and nested functions. Across operators, a producer may retain
primitive/boxed array backing or binary leaves that a later callback can also retain. Owned
Arrow output cannot reproduce those Java aliases. Scalar/table chains therefore use the shared
producer/argument guard and retain whole-query fallback for such aliases. ARRAY/List/MAP
converters that serialize non-identity children establish the verified copying boundary;
paired retaining-consumer controls require native work for those admitted chains. Unverified
stateful consumers, exchanges and columnar sinks retain the existing representation guards.

The conversion matrix checks 1,134 signatures on each released Flink line with independent
stock/native values and resolved types, NULLs, empty containers, nested RowKinds and mutable
results. Additional checks cover concrete signatures, overloads/varargs, exact floating-bit
observers, shared/retained calls, short-circuiting, lifecycle/failures, every changelog kind,
repeated heap/native-RocksDB recovery and task-JVM deployment. The shared adapter is also
serialized into a fresh worker without Flink planner or Calcite artifacts. Each populated
admitted query requires consumed NativeCalc work; safe mixed scalar/table chains require both
operators. Flink 1.18 rejects untyped NULL in the verified nested CASE fixtures; explicit failure
controls pin that validation, while typed NULLs verify successful short-circuiting on both lines.

The consolidated local selection after integration with current production passes 2,277 checks
on Flink 2.2.1 and 2,276 checks on 1.18.1, with its one existing ARRAY_SORT capability skip.
This includes 176 mixed scalar/container cases per line, exercising primitive/temporal
arguments and scalar results alongside recursive arguments. The unchanged upstream Calc and
table-function execution contracts remain part of the required full fork workflow.

The [complete-job record](../benchmarks/results/jvm-recursive-scalars-rebased-2026-11-04.json) retains
all 168 trials, including warmups, and the separately paired stock controls for previous
production `624cbd7e`. That checkout has unchanged production code and only the new fixture and
benchmark overlays. Both checkouts use the same release/mimalloc native library, Temurin 17,
Apple M4, one task, 2 GiB heap, 1,024-row Arrow batches, Arrow IPC for local exchanges, JVM UTC
and session America/New_York. Each case/revision/line runs in a separate serial JVM with two
warmup pairs and five measured alternating engine pairs. All 262,144 runtime payloads are
fresh, cycling 48 logical shapes; source generation is included in timing. EXPLAIN is symmetric
and outside timing. Both transposes, released conversions, JNI, owned output and the blackhole
row sink remain in the complete job.

Seconds are median (minimum–maximum):

| Flink / boundary | Stock | Previous fallback | Candidate |
| --- | ---: | ---: | ---: |
| 1.18.1 / `list/row:default-STRING:both` | 0.461162 (0.455096–0.462044) | 0.453978 (0.447643–0.460726) | 0.684589 (0.680930–0.687352) |
| 1.18.1 / `row/primitive:primitive-DOUBLE:both` | 0.478104 (0.476085–0.480573) | 0.479264 (0.469668–0.480655) | 0.613123 (0.610577–0.618799) |
| 1.18.1 / `row/custom/map:default-DECIMAL(38,18):result` | 0.758683 (0.735057–0.799432) | 0.746120 (0.737385–0.766570) | 1.221537 (1.197329–1.521920) |
| 2.2.1 / `list/row:default-STRING:both` | 0.469785 (0.463296–0.473863) | 0.450527 (0.444817–0.456875) | 0.705300 (0.697150–0.867304) |
| 2.2.1 / `row/primitive:primitive-DOUBLE:both` | 0.486453 (0.476910–0.491680) | 0.486247 (0.481284–0.493673) | 0.634913 (0.633328–0.642800) |
| 2.2.1 / `row/custom/map:default-DECIMAL(38,18):result` | 0.747586 (0.744495–0.754269) | 0.740942 (0.735807–0.749522) | 1.204868 (1.200834–1.207984) |

All candidate medians are slower than stock and preceding fallback. These short jobs retain
startup/JIT and all unfavorable samples. This is conversion coverage under the accepted UDF
performance exception, with no standalone acceleration claim. The record also retains total
Maven invocation time separately from each complete-job interval; compilation, JVM launch and
symmetric EXPLAIN/setup remain outside the trial timer.

The [earlier record](../benchmarks/results/jvm-recursive-scalars-2026-11-04.json) retains all
168 pre-integration trials against `0f5fc167`, including the substantial Flink 1.18 variability
and drift between its paired stock controls. Those raw revision ratios do not isolate the
feature's cost. Both records retain their original native-library and fixture hashes.

Reproduce an individual candidate case after building the released native profile:

```bash
SF_BENCHMARK=true mvn -Pbench -pl streamfusion-runtime -am \
  -Dtest=RecursiveScalarBenchmark -Dsf.testForks=1 \
  '-Dsf.extraJvmArgs=-Xmx2g -Duser.timezone=UTC' \
  -Dboundary.rows=262144 -Dboundary.warmups=2 -Dboundary.runs=5 \
  '-Drecursive.scalar.bench.signatures=list/row:default-STRING:both' test
```

Use `-Pbench,flink-1.18` for the other released line. Overlay the same benchmark/fixtures onto
`624cbd7e` and set `-Dboundary.expectNative=false` for the actual previous fallback control.
Remaining JVM contracts stay in [#237](https://github.com/datafusion-contrib/StreamFusion/issues/237).

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
#237 stays open. {#237-stays-open}

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


## User scalar functions {#user-scalar-functions}

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
External Java `Row`, `Map`, List and verified Java-array signatures use the default-enabled generated paths described
below. Scalar-only fixed-size `BINARY`, root MULTISET, and alternate Java
conversion classes outside the verified mappings retain fallback. TIMESTAMP/TIMESTAMP_LTZ
and DATE/TIME signatures have the default-enabled generated path described below.

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
or outputs use the default-enabled nested scalar option above and its verified recursive
ROW/ARRAY/List/MAP boundary. Unsupported UDF signatures and specialized functions still fall back. Temporal
columns and builtin expressions are supported here; temporal **user-function signatures** retain
the separate signature checks below. The path preserves columnar operator boundaries and avoids a separate
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

The [historical release comparison](../optimizations/udf-columnar-upcall.md#complete-generated-calcs-with-primitive-inputs)
reduced the then-opt-in INT/BIGINT/STRING job times by about 28%/32%/10%. These isolated jobs
remain slower than stock Flink. Default admission now follows the accepted UDF exception;
the [bounded shared-call optimization](../optimizations/udf-columnar-upcall.md#generated-batch-loops-for-shared-stateful-calls)
adds a generated batch loop for numeric/boolean programs while retaining the existing row
evaluator, writer and admission rules. STRING-result programs keep the existing direct row loop.
Older serialized evaluators retain row evaluation. Both Flink lines pass 223 focused checks and
six isolated-worker checks; final release trials and their variability are linked from the
optimization page. These complete jobs still trail stock Flink.

Runtime tests compare ordered values, types, every changelog tag and invocation counts, including
nullable inputs/results, nested and conditional calls, all-filtered and empty inputs, exceptions,
5,003-row sources and balanced task lifecycle. Native operator metrics establish actual consumption.

Release end-to-end measurements retain both transposes and a rowwise blackhole sink. At five
million rows, paired medians for two shared calls were 0.457/0.843 s stock/native for INT,
0.453/0.833 s for BIGINT, and 1.064/1.558 s for STRING. Native throughput is about 0.54× stock
for the integers and 0.68× for strings. One-million-row comparisons also retain the previous
fallback; all completed trials are in the [structured results](../benchmarks/results/shared-stateful-scalar-2026-10-14.json).
The report's `raw_log` paths retrieve the exact original log bodies through the
[evidence archive](../benchmarks/evidence.md#source-repository-and-archived-run-outputs).
A diagnostic CPU profile records 972 JVM-callback, 2,433 entry-transpose and 673 exit-transpose
samples among 6,769 total samples. These inclusive stack counts overlap; they identify boundary
and callback work, not an additive cost breakdown. The row evaluator already reuses its input
row and batches JNI calls. The bounded [boundary investigation](../optimizations/calc-schema-once-c-data.md#cache-calc-output-schemas)
caches immutable Calc output schemas; broader conversion and ownership work is deferred there.
No standalone speedup is claimed for this historical UDF workload. Default UDF admission supports composition with surrounding native operators,
whose performance must be measured for the complete query.


### Temporal scalar functions {#temporal-scalar-functions}

`streamfusion.calc.temporalUdf.enabled=true` admits verified scalar signatures using
`LocalDate` for DATE, `LocalTime` for TIME, `LocalDateTime` for TIMESTAMP and `Instant`
for TIMESTAMP_LTZ arguments/results. Precision
0/3/6/9 is preserved through the existing full-range millisecond/nanosecond Arrow representation.
The complete Calc uses Flink's generated code and released external/internal converters, including
its predicate and sibling projections. This retains calendar fields for TIMESTAMP, absolute time
for LTZ, declared result types, NULL callbacks, shared function order and one task lifecycle.
Results remain Arrow columns for downstream native operators, including GROUP BY.

DATE/TIME use the released epoch-day/millisecond-of-day converters, including negative dates,
leap dates, midnight, end of day, NULL containers and nullable results. SQL TIME signatures
must match the released planner's TIME(0) normalization; native admission does not make an
inferred TIME(9) or explicitly incompatible TIME(3) signature executable. Runtime millisecond
values are preserved. Alternate `java.sql.Date`/`java.sql.Time` conversion classes retain fallback.
Runtime tests compare types and values across batches, lazy/filtered calls, shared state and
heap/RocksDB recovery, and require NativeCalc to consume input. These Java functions retain
the accepted UDF performance exception; this coverage does not claim a speedup.

The `UdfTypeBoundaryBenchmark` release diagnostic (Apple M4 Pro, Flink 2.2.1/JDK 11,
parallelism one, 262,144 rows, 1,024-row batches, both transposes and blackhole row sink)
uses two warmups and five alternating stock/native pairs. DATE job medians are 0.092154s
stock (0.079492-0.101028s) and 0.132566s native (0.127392-0.137835s); TIME job medians
are 0.095451s stock (0.089436-0.097501s) and 0.143299s native (0.126271-0.148375s).
The previous behavior used stock fallback. These short complete-job timings include startup
and establish callback costs, not sustained acceleration. Run with `-Pbench` and
`SF_BENCHMARK=true`; input counts and repetitions use the `boundary.*` test properties.

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
and the [current conversion measurements](../optimizations/udf-columnar-upcall.md#complete-temporal-calc-batches).

#### Temporal scalar conversion {#temporal-scalar-conversion}

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
The [current direct-batch comparisons](../optimizations/udf-columnar-upcall.md#complete-temporal-calc-batches)
cover both released Flink lines, cardinalities eight/16,384, ordinary ID columns and a surrounding
native GROUP BY. Standalone projections take 11.2–31.0% less time than the preceding callback;
grouped jobs show no consistent gain. Stock Flink remains faster for identity and epoch, while
the text fixtures beat stock. Correctness-verified UDF admission does not require a speedup.

A direct Java Arrow-batch path removes the Rust round trip when the entire Calc
is already generated JVM code and its input columns contain timestamps, optionally alongside
BOOLEAN, TINYINT, SMALLINT, INT, BIGINT, FLOAT or DOUBLE columns. This path now
defaults on under the accepted UDF performance exception. Changelog-tagged input, other argument
expressions, and results requiring representation metadata retain the existing callback.
The task-local function binding carries the verified input-column mapping; runtime code does
not decode the expression ABI. Predicate evaluation, function lifecycle and row-major call
order use the same released Flink evaluator.

Timestamp output sharing additionally defers writes
for unconditional, timestamp-only projections of matching width. Every function still runs.
A byte-equal result column retains the input buffers; the first changed value materializes its
unchanged prefix and all subsequent values. Preinitialized Arrow fields preserve timestamp
component metadata through transfers. Sharing also requires a common allocator root; inputs
from another root use owned copies. Other result shapes use the ordinary flat Arrow writer.
The existing system properties `streamfusion.experimental.directTemporalCalc` and
`streamfusion.experimental.temporalAliases` both default to true. Explicit false selects the
previous callback or disables output sharing, respectively; neither changes verified type coverage.
`streamfusion.calc.temporalUdf.enabled=false` remains the stock-Flink control.

In a historical unprofiled 2M-row repeat with the configuration above, direct batches plus timestamp
aliasing measured identity **0.651 s versus stock 0.645 s**, text **0.830 versus 0.969 s**, and
epoch conversion **0.536 versus 0.570 s**. The identity result still misses performance admission;
these separate JVMs also do not establish an improvement against the previous correct fallback.
All trials and raw CPU/allocation profiles are retained in the results artifact. CPU profiling
covers both Java and symbolized Rust frames with async-profiler 4.0, a 2 ms software CPU timer
and DWARF native unwinding. It identifies Arrow FFI/ownership work in the old Rust path and
extra Java TimestampData allocation at callback input and row exit in the improved path.
Profiled job durations are excluded from acceleration claims. See the
[optimization and profiling details](../optimizations/udf-columnar-upcall.md#complete-temporal-calc-batches).

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

JNI-checked release qualification passes **163 focused checks on each of Flink 2.2.1 and
1.18.1**. This includes direct temporal evaluation at all four precisions in UTC,
America/Los_Angeles and Asia/Shanghai, shared call order, selected failures, mutable-result
changes, retained output, both allocator-root cases, key groups and alternating insert/changelog
batches. Both the previous-callback and copy-only execution controls also pass.
Reproduce the benchmark with:

```sh
SF_BENCHMARK=true mvn test -Pbench -pl streamfusion-runtime -am \
  -Dtest=TemporalUdfBenchmark -Dsurefire.failIfNoSpecifiedTests=false \
  -Dtemporal.rows=2000000 -Dtemporal.warmup=2 -Dtemporal.runs=5 \
  -Dsf.extraJvmArgs=-Xmx2g
```

`-Dtemporal.cardinality=16384` selects a precomputed runtime corpus with distinct seconds and
precision-aligned fractional nanoseconds, every eighth value NULL. The default cardinality of
eight retains the full-range calendar, negative-epoch and timezone boundary corpus above.

The benchmark rotates stock Flink, the preceding production JNI callback, current direct batches
and explicit stock fallback in each trial. `-Dtemporal.cases=pipeline` measures timestamp UDFs
feeding native GROUP BY and asserts work in both operators. `-Dtemporal.engines=FLINK,NATIVE`
selects only the stock/current pair. `-Dtemporal.cases=mixed` keeps an ordinary ID column beside
the timestamp results. Every complete native job retains both transposes.

Use `-Dtemporal.cases=perimeter,identity -Dtemporal.rows=5000000` for the matching control.
For JFR, compile first and run `mvn surefire:test@default-test` with the same profile and test
properties: the ordinary `test` lifecycle also launches a separate ORC test JVM, which would
overwrite a shared recording filename after the benchmark completes.



## Collection subscripts {#collection-subscripts}

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


## Generated helper performance {#generated-helper-performance}

The table below is historical. Direct certified regex, LIKE ESCAPE, PARSE_URL and PRINTF
subsets now avoid Java; see the [2026-10-26 comparison](../benchmarks/native-certified-scalars.md)
for current measurements and remaining evaluator routes. Verified DECIMAL SIGN also now uses
the [typed native kernel](../benchmarks/scalar-functions.md#decimal-sign); its historical
generated-helper row below does not describe the new implementation.
Direct BOOLEAN character CAST also now uses the
[native formatter](../benchmarks/scalar-functions.md#boolean-cast-to-character);
the BOOLEAN-to-STRING row remains historical callback evidence.

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

The IS_ALPHA row records the previous generated evaluator. Current native alpha/digit
coverage and paired measurements are documented in the classification section and scalar
benchmark page; this historical table does not describe their current implementation.

