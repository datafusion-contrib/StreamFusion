# Calc coverage by topic: temporal

[Topic index and original links](calc-filter.md). Sections retain their recorded scope, dates and evidence.

## FROM_UNIXTIME literal formats {#from_unixtime-literal-formats}

Integer epoch seconds and constant numeric patterns (`yyyy`, `MM`, `dd`, `HH`, `mm`, `ss`,
quoted literals) execute in Rust for fixed-offset zones and regional zones with finite
historical offset tables, including `Asia/Shanghai`. Planning serializes the running JDK's
`java.util.TimeZone` offsets, so execution needs neither per-row JVM callbacks nor a second
native time-zone database. This preserves JDK-specific historical behavior, including the
pre-1900 raw offset, former daylight-saving transitions, the Gregorian calendar cutover,
BC dates and Java long multiplication overflow.
The finite table also includes the running JDK's final synthetic raw-offset boundary.
Its date is discovered using `TimeZone.getOffset`, covering both the older 2037 and
newer 2100 boundaries (including `Africa/Windhoek`) without reflective module access.
JobManagers and TaskManagers must use matching formatter locales and JDK time-zone rules.

Zones with recurring future rules, unverified regional TimeZone implementations, dynamic or
unsupported patterns, non-Gregorian/default numbering locales and other unverified signatures
retain the Flink evaluator. Existing
fixed-zone plans retain their encoded format. In an admitted regional zone, composing the
formatter with SUBSTR, integer/string CAST, constant nonzero integer division and LPAD
keeps the date/hour/quarter-hour partition projection entirely native. Output formatting
reuses one scratch string per batch and reuses the formatted value for consecutive equal
epoch seconds; it does not change the SQL expression or its substring/cast semantics.
See [correctness and performance evidence](../benchmarks/native-sync-calc.md) for the
complete date-partition expression, wide-row parity checks and both timing boundaries.


## Generated processing time {#generated-processing-time}

An unobserved PROCTIME indicator uses Flink's typed physical NULL representation. Native Calc
preserves that marker; PROCTIME_MATERIALIZE reads the existing execution clock when SQL observes
the value. Logical stock types remain unchanged. Retained state must not receive an eagerly
materialized clock value, because later changelog retractions must match the saved row. Empty
and all four changelog row kinds have direct placeholder/materialization checks, with observed
native times bounded by actual execution. Complete-job coverage includes the
[retracting-rank handoffs](top-n.md#retained-processing-time-indicators-in-retracting-rank).

Generated scalar expressions and complete-row Calc evaluators bind processing time to the owning
Calc or Filter task's live `ProcessingTimeService` during operator open. Flink's generated
`PROCTIME_MATERIALIZE` access therefore works inside CASE/COALESCE selections and predicates,
including complete-row evaluation needed for shared function state or representation contracts.
Simple TIMESTAMP_LTZ COALESCE selections over input columns, NULLs and processing-time clocks
with widening precision casts use the existing native CASE path at precisions above three.
No UDF or other nondeterministic/fallible expression enters this path. More complex expressions
retain Flink generation where required by their function or representation contract. Multiple
independent failure sites alone do not require complete-row generation. A lower Calc containing only input references and a lazy
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
generated scalar execution alongside independent fallible projections in UTC, Asia/Shanghai and America/Los_Angeles.

Released Flink 2.2.1 rejects the direct precision-3 COALESCE form combining a nullable timestamp
and materialized PROCTIME with a generated argument-nullability mismatch. Its valid CASE form
and precision-9 COALESCE are used for the SQL comparisons; this runtime binding does not alter
host SQL validation or repair that separate host code-generation error.

Release comparisons and the remaining 4.1% sustained-filter limitation are recorded in
[the optimization ledger](../optimizations/identity-expression-passthroughs.md#processing-time-indicators).


## Date/time {#datetime}

The [temporal functions page](temporal-functions.md) lists every supported scalar family, clock,
watermark and window helper, with the exact range and runtime-context gates. Temporal expressions
use Flink's code through the batched JVM upcall unless a verified Rust kernel already exists.
Adjacent temporal calls fuse into one upcall, retaining intermediate TimestampData values inside
Flink rather than converting each one to an Arrow timestamp.

`DATE_FORMAT` and `EXTRACT` retain their existing opt-in Rust LTZ paths. Dynamic patterns, additional
extraction fields and other temporal functions use Flink's implementation. Timestamp-producing
expressions run by default with the full Flink millisecond range and fractional nanos.
Plain-TIMESTAMP numeric `DATE_FORMAT` also handles Java quoted literals, doubled quotes and
escaped literal percents natively. Optional sections and unsupported or invalid Java pattern
syntax keep generated Flink evaluation, preserving output, failures and empty-batch laziness.
Direct TIMESTAMP_LTZ columns and verified total integer/DECIMAL epoch producers in ordinary Calc reuse
this exact renderer in fixed zero-offset sessions. Integer producers compose the existing
width, arithmetic/rounding and original-unit bounds proof at literal precision 0/3, verified
runtime INT CASE precision 0/3/NULL, and Flink 2.2's default precision. Verified DECIMAL fields,
literals, total ABS/negation chains and same-scale widening compose at literal precision 0/3
or the 2.2 default, preserving DECIMAL truncation, signed-long/seconds wrapping and final
millisecond bounds. Runtime DECIMAL precision, other zones and unverified
derived producers keep their existing generated routes, including arithmetic DECIMAL epochs and general
timestamp CASE/casts. Flink 1.18 retains its validation rejection of single-argument numeric
epochs. See the [composition qualification](../benchmarks/scalar-functions.md#utc-date_format-over-verified-integer-epochs-2026-11-05).
The [DECIMAL qualification](../benchmarks/scalar-functions.md#utc-date_format-over-verified-decimal-epochs-2026-11-05)
records its independent correctness/performance scope and retained small-batch and complete-job costs.


## Flink 1.18 compatibility {#flink-118-compatibility}

The 1.18 development build uses the shared native SQL/JSON reader with its verified Jackson
2.14.2 profile on JDK 11 or 17. Decimal JSON constructors retain the whole-Calc JVM route, once per
Arrow batch, to preserve that release's decimal spelling. See
[Flink line compatibility](../flink-compatibility.md) for host-only syntax differences.

### Legacy ENCODE result representation {#legacy-encode-result-representation}

On Flink 1.18, SQL `ENCODE` reports `BINARY(1)` despite returning variable-length bytes.
The shared generated Calc evaluator keeps its original expressions and external schema, but
uses Arrow Binary for affected scalar output columns. This preserves charset resolution,
nulls, byte arrays, casts and row/projection evaluation order without changing ordinary fixed
BINARY columns. Final projections and fused scalar consumers accelerate; an affected fixed-width
result crossing into another operator or a native sink falls back before execution. Casting the
result to BYTES establishes a normal variable-length boundary.


## Flink 1.18 mixed timestamp comparison compatibility {#flink-118-mixed-timestamp-comparison-compatibility}

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

### Exact DOUBLE TRUNCATE {#exact-double-truncate}

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
Rust-side functionality gate described below. STRING ARRAY_DISTINCT subsequently gained the
represented native kernel described above; fixed binary casts, Boolean ARRAY_DISTINCT,
and the bounded DOUBLE TRUNCATE helper remain admitted.


The maintainer accepted shipping the verified broader SQL/JSON MAP/MULTISET boundary
on October 16 while deferring performance improvements. Historical MAP whole-job results
remain slower than stock Flink; see the [recorded measurements](../benchmarks/scalar-functions.md#map-sqljson-batch-boundaries-2026-09-19).
This acceptance does not widen nullable-key, fixed-BINARY or non-Calc admission.


### Binary ELT and raw payload boundaries {#binary-elt-and-raw-payload-boundaries}

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

### Operator-format performance admission, 2026-10-19 {#operator-format-performance-admission-2026-10-19}

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

### Binary ELT default admission, 2026-10-20 {#binary-elt-default-admission-2026-10-20}

The verified INTEGER-indexed binary selector now runs without an opt-in. Explicit
`streamfusion.experimental.binaryElt=false` restores stock execution, independently of the
nested-scalar flag. Planner and worker use the same default helper. CI exercises this default
without forcing the flag. The unverified index overloads and representation guards
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


### Map lookup after retained-row operators {#map-lookup-after-retained-row-operators}

Logical MAP `ITEM` retains an explicit map tag when a preceding Top-N, regular join or first-N
operator stores maps as physical Arrow Lists. Literal and dynamic key lookup use the same borrowed
entry offsets/key/value view for physical Maps and tagged Lists; integer map keys cannot be
misinterpreted as one-based array positions. Existing compact timestamp-key and nullable-key
compatibility guards remain in effect. ARRAY subscript behavior is unchanged.

Retained MAP/MULTISET output reaching an unverified native Arrow sink uses the existing row-sink
route. Direct external Java MAP scalar callbacks use the verified generated path below;
retained-row carriage alone does not expand callback conversion coverage.


### Java ARRAY scalar signatures {#java-array-scalar-signatures}

The default-enabled `streamfusion.experimental.nestedScalarUdfs` option admits top-level
external Java ARRAY scalar arguments and results with verified scalar leaves and recursive
ARRAY/List/ROW/MAP children. Java object arrays, supported primitive arrays, external List
arguments and List implementation results use the [recursive whitelist](calc-filter-jvm-collections.md#recursive-external-scalar-conversions).
Custom inference and annotations may select those conversions. MULTISET/STRUCTURED/RAW,
internal classes, specialized functions and unverified conversion classes retain fallback.

On released Flink 1.18.1, CASE selecting a `byte[][]` ARRAY-of-BYTES UDF result or the input
array fails in the host's generated Java allocation (`'{' expected instead of '['`). The
suite asserts that same host failure and generation fallback, rather than skipping it or
claiming CASE coverage on that line. Single/nested calls, CARDINALITY and filters retain
positive NativeCalc work on both lines; the CASE fixture also executes natively on Flink 2.2.1.

The complete Calc uses the existing generated Arrow batch callback. Flink's copy of a nullable
columnar array can have an `Object[]` backing even when the user method expects `Integer[]`.
The converter restores the typed internal backing, recursively through ARRAY/ROW/MAP, before
invoking the released external converter. It retains NULL versus empty arrays, NULL elements,
Java string code units, owned retained arguments and the released result conversion. It does
not replace array evaluation with Rust user-function execution.

Runtime tests cover BOOLEAN, TINYINT, SMALLINT, INT, BIGINT, FLOAT/DOUBLE, DECIMAL(38,18), STRING,
DATE, TIME(0), TIMESTAMP(9), TIMESTAMP_LTZ(9), BYTES and nested INT arrays. Consumers include
CARDINALITY, nested UDFs, CASE and filters. Reused mutable outputs, shared state/lifecycle, retained inputs after batch release,
empty/all-filtered input, failures and heap/RocksDB recovery are verified against stock Flink.
Each populated admitted query requires consumed NativeCalc work; existing collection
representation guards continue to restrict unverified state/exchange consumers. Scalar
results from ARRAY arguments can compose with native scalar operators.

The release `UdfTypeBoundaryBenchmark` diagnostic uses Apple M4 Pro, Flink 2.2.1/JDK 11,
262,144 rows, 1,024-row batches, parallelism one, both transposes and a blackhole row sink.
After two warmups, five alternating pairs yield INT-array medians of 0.080996s stock
(0.074598-0.093563s) and 0.139827s native (0.128676-0.145702s). DECIMAL-array medians
are 0.241181s stock (0.227901-0.247675s) and 0.361928s native (0.358151-0.383200s).
The prior route was stock fallback. These short jobs include startup; they document conversion
costs under the accepted UDF exception and make no sustained-acceleration claim. Reproduce
with `-Pbench`, `SF_BENCHMARK=true` and `-Dtest=UdfTypeBoundaryBenchmark#arrayCompleteJobs`.

### Fixed BINARY MULTISET elements {#fixed-binary-multiset-elements}

CAST/TRY_CAST and character formatting admit direct BINARY(n) MULTISET elements through the
existing generated Flink evaluator and Arrow MAP<element,INT NOT NULL> transport. Nullable
elements run on both released lines; NOT NULL elements run on 2.2. Flink 1.18 NOT NULL
binary elements retain fallback because unchecked NULL getters can expose empty bytes where
its original container preserves NULL spelling. Declared SQL widths remain unchanged, while
transport retains actual byte lengths. The existing nested-scalar length-delimited option
must remain enabled. No accumulator, state codec or ownership protocol is added.

Fixed-binary MULTISET CASE expressions also use released generated evaluation: selected
branches retain Flink's implicit binary casts, including padding observed on 1.18. Direct
native selection would skip that transformation. Disabled collection/length-delimited options
keep this CASE on stock Flink; direct native selection is rejected. This does not change ordinary scalar CASE.
Runtime internal sources verify NULL/empty parents and elements, lengths0–6 and4097 despite
BINARY(4), zero/negative multiplicities, colliding cast keys, all four changelog kinds,
legacy/current cast rules, CHAR/VARCHAR bounds, filtered/empty batches, CASE and nested
ARRAY/ROW consumers. CAST and TRY_CAST preserve input arithmetic failures and conditional
short-circuiting. Formatting checks use ordered internal inputs; casts that allocate new
byte-array map keys can have identity-dependent HashMap iteration, so their map results are
compared as maps rather than claiming a deterministic formatted order. Native checks require consumed Calc work; 1.18 NOT NULL forms and disabled
length-delimited/collection options require fallback.

Unverified state, exchange and native-writer boundaries retain the represented-collection
restrictions. This does not enable updating retained MULTISET payloads deferred in
[#437](https://github.com/datafusion-contrib/StreamFusion/issues/437).
[Release comparisons and bounded profiles](../benchmarks/scalar-functions.md#fixed-binary-multiset-elements)
retain conversion and source/sink costs without a standalone acceleration claim.

### NOT NULL fixed BINARY map leaves {#not-null-fixed-binary-map-leaves}

Flink 2.2 admits MAP keys and values declared BINARY(n) NOT NULL through the existing
length-delimited generated evaluator. Either or both leaves may be NOT NULL. MAP_KEYS,
MAP_VALUES, MAP_ENTRIES, MAP_UNION, CAST/TRY_CAST, character formatting and composed array
consumers keep released Flink evaluation. This does not widen the Rust shared-child view's
nullable-element whitelist. Flink 1.18 retains planning-time fallback: its unchecked NULL
binary getters can expose empty bytes where the original map preserves NULL formatting.
The same release capability already governs NOT NULL binary MULTISET elements.

Runtime-source regressions exercise both GenericMapData and released-serializer BinaryMapData,
NULL/empty parents, actual NULL leaves despite declared NOT NULL hints, lengths zero through
six and 4097 despite BINARY(4), all four changelog kinds, legacy/current casts, CHAR/VARCHAR
bounds, CASE and empty filtered batches. Each populated admitted query requires consumed
NativeCalc work. Retained Java Map callbacks preserve owned inputs and shared function state
across closed batches for both source representations. The callbacks also retain verified
stock behavior on 1.18; they do not add a new conversion protocol.

The default collection and nested-scalar length-delimited options must remain enabled for the
new collection functions. Disabling either retains Flink evaluation of those functions; unrelated
native projections can still execute. Existing final-projection/fused-consumer restrictions and
unverified state, exchange, ordering and native-writer guards remain. No retained-payload support
is added for [#437](https://github.com/datafusion-contrib/StreamFusion/issues/437).
[Release comparisons and two bounded profiles](../benchmarks/scalar-functions.md#not-null-fixed-binary-map-leaves)
document complete-job costs under the accepted collection tradeoff.

### Java MAP scalar signatures {#java-map-scalar-signatures}

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
Runtime millisecond TIME values are preserved. Root DATE/TIME UDF signatures use the temporal
path. NOT NULL fixed BINARY map leaves use the verified generated boundary on Flink 2.2 and retain
fallback on 1.18. Custom inference may select the verified recursive external conversions;
arbitrary conversion classes and internal classes retain their signature gates.

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

### Verified native map keys {#verified-native-map-keys}

Within the collection extension, MAP_KEYS evaluates in Rust for total MAP operands
whose keys and values are nullable BOOLEAN, integer or DECIMAL. Parent NULLs,
NULL keys, duplicates and entry order survive as a shared Arrow list view. Direct input maps additionally admit nullable verified nested/string key and value types. Other
signatures and fallible arguments retain generated Flink evaluation. This does not change default
collection admission or the standalone conversion limitations recorded in the
[collection representation ledger](../optimizations/collection-representation-work.md).

Direct final projections of input-reference MAPs with STRING/VARCHAR keys and nullable
BOOLEAN/integer/DECIMAL values also use these shared views. Key and entry outputs preserve
the represented string buffers. A value-only Calc without a condition or sensitive
co-projection can omit the unused keys' Java caches. Composed consumers and unverified
state, exchange and sink boundaries retain their existing representation guards.

MAP_VALUES uses the same verified map-projection admission and shared-buffer view, selecting the value child without changing NULLs or entry order.

MAP_ENTRIES uses the same verified map-projection admission and reuses entry children
as a list of nullable key/value rows, preserving order and parent NULLs. Consumers
that inspect row representation or RowKind retain generated evaluation. A direct
projection creates fresh INSERT-kind entry rows, so it does not require the
whole-Calc ROW metadata callback. This exception applies only to the verified
basic maps, verified nullable nested input maps and native one-level ROW-bearing MAP
constructors. Unverified ROW-valued consumers keep the generated callback.

MAP_UNION folds verified total basic maps and nullable STRING-key maps with basic/STRING values
left to right, propagating any NULL
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
STRING-bearing constructors and one-level ROW keys/values also run natively. Their STRING fields
become cold binary strings, and nested rows retain the binary category and RowKind required by
Flink's BinaryMapData construction. Flink 1.18's generated-expression grouping order varies
between independently compiled identical queries. Represented constructors use input order on
that line; deterministic 2.2 construction preserves input order exactly. The older primitive-only
multi-entry 1.18 constructor path retains generation. Repeated key expressions, fallible operands
and released narrow-integer compilation failures retain their existing guards.

Constant MAP constructors, MAP_FROM_ARRAYS, MAP_UNION and MAP casts retain the complete
nullable-key entry-list field when returned as a scalar. This includes its physical MAP
marker and resolved decimal precision/scale; broadcasting a constant must not turn the
carrier into an ordinary ARRAY of rows. The unchanged upstream mixed-precision decimal MAP
case and focused constant/runtime-source regressions exercise this boundary.

The UPDATE_BEFORE row filter retains the exact input Arrow fields after filtering. Arrow
Java may rename list children during C Data import; restoring the original schema over the
owned result vectors keeps UTF-16 carriers compatible with native literals and constructors.

Basic MAP_ENTRIES projections keep their existing native fresh-row route, including
MAP_FROM_ARRAYS with the same array on both sides. Existing fallible-operand guards remain.

ROW constructors with total BOOLEAN/integer/DECIMAL and STRING fields reuse DataFusion's
released named-struct builder, sharing field arrays. Fields retain their SQL
names and nullable values; the constructed row itself is present with INSERT
row kind. STRING fields lose their Java caches, matching binary ROW construction. These fresh basic rows and their scalar field projections bypass the
whole-Calc ROW metadata callback. Other fields and representation-dependent row
consumers remain generated; nested ROW arguments still trigger metadata handling.

OBJECT_OF with validated class/field metadata and total basic fields reuses the
released named-struct builder. Runtime values and field order match Flink's fresh
GenericRowData construction; metadata validation still belongs to Flink planning.
STRING fields also run natively and preserve their Java caches in the fresh generic object.
Nested/temporal fields and fallible operands retain generated evaluation.

OBJECT_UPDATE admits only one unshared inline basic/STRING OBJECT_OF construction in the
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

### Native container cast bounds {#native-container-cast-bounds}

The [remaining collection measurements](../benchmarks/native-collections.md) compare stock
Flink, the previous evaluator and these native kernels with both row/Arrow transposes. Slower
cases ship under the maintainer's explicit two-pass agreement for #505; native execution does
not imply a complete-job speedup.

MAP casts admit nullable STRING keys whose character type is unchanged, or signed-integer keys
cast to another signed-integer type, with nullable numeric or numeric-to-BOOLEAN values. STRING
values cast to signed integers and nested nullable numeric ARRAY values use the same child
conversion. New maps use Flink's Java HashMap iteration schedule: the first equal key object
survives, the last value wins, and target integer keys are rehashed after conversion. Generated MAP input-field access takes a binary serializer snapshot before a call. Native
MAP-reference operands reproduce its cold nested strings and binary row/array categories using
owned Arrow views. Computed MAP operands retain their original caches through identity key casts. Integer MULTISET element casts use the same schedule
and retain INT occurrence counts. Nonnullable element/key/value constraints, richer casts and
representation-sensitive mixed consumers retain their verified generated path.


A MAP_FROM_ARRAYS call with different operands remains a potential length-error site. Placing
it inside CAST and MAP_KEYS, for example, retains the complete generated Calc under the existing
fallible-operand guard. The direct MAP_FROM_ARRAYS kernel still runs in Rust and preserves its
released length exception. A NativeCalc for that composed query is explicitly mixed execution.


Function-engine diagnostics include TRY_CAST explicitly, alongside CAST. Both native and
JVM-backed TRY_CAST calls therefore name their actual route; an empty function-name list
cannot serve as evidence that a runtime TRY_CAST kernel executed. The collection tests require
the named root function plus an empty JVM list and consumed native Calc input. Runtime STRING
ELEMENT checks use source arrays, including cardinality failures, rather than relying only on
a singleton constructor expression.


### Temporal collection copy categories {#temporal-collection-copy-categories}

Direct column projections retain source field metadata through pruning, aliases, reordering
and repeated references. Generated JVM argument batches retain that metadata as well. This
preserves the generic copy category of NOT NULL DATE/TIME arrays emitted by collection
aggregates, including mixed array projections, formatting, conditional expressions and
retained/shared Java callbacks. Logical output types and element nullability remain Flink's;
computed expressions use their own result representation. Unverified stateful, UNNEST and
native-writer collection boundaries keep their existing planning guards.

Collection aggregation retains host execution when an input Calc reads a NOT NULL supported
scalar hint in a predicate or materializes it in a non-identity projection. Released Flink can expose
the source row's unchecked primitive payload beneath an actual NULL slot, which Arrow validity
does not retain. This guard is scoped to collection aggregate admission. Direct source hints,
key-only identity filters and COALESCE from nullable input remain verified aggregate contexts.


Collection aggregate input guards also cover TIMESTAMP/TIMESTAMP_LTZ and both interval families.
A materializing Calc over unsupported NOT NULL source hints retains released Flink execution;
nullable inputs and explicitly matching timestamp comparison kinds retain their verified paths.
The [timestamp/interval aggregate contract](group-by.md#timestamp-and-interval-collection-elements)
also documents the released four-digit DAY precision planning error and native exchange copy
semantics. The existing 1.18 mixed timestamp comparison normalization remains unchanged.


### External STRUCTURED collection inputs {#external-structured-collection-inputs}

Built-in collection programs admit source fields declared STRUCTURED, recursively inside
ARRAY, ROW, MAP keys/values and MULTISET elements, through the existing represented-ROW
converter. Resolved implementation classes, attributes and nullability stay in Flink's logical
types. The existing Arrow struct fields and kind/string/array metadata carry their internal
RowData values; no new operator, native kernel, JNI ownership handoff or state format is added.
The planner validates this input context separately from the default converter admission used
by other operators.

CAST/TRY_CAST between ROW and STRUCTURED, matching structured/collection containers and character
targets use the existing generated evaluator inside NativeCalc when they require Flink's
representation. Runtime ROW-to-STRUCTURED casts also work through the Table API on both released
lines. Constructors and composed built-in collection functions use the same boundary. On 2.2,
source objects can feed OBJECT_OF/OBJECT_UPDATE; those functions remain absent on 1.18. This
adds coverage without describing the Java evaluator as a Rust cast kernel. A final MAP_VALUES
that reads primitive values without observing STRUCTURED keys can use the existing Rust child
view and omit the unobserved key caches.

This admission requires an enabled built-in collection program and a verified encoded input
boundary. Plain structured projections/filters and programs containing user callbacks retain
their previous fallback. Unverified leaves, fixed-BINARY/nullability release gates, metadata
names, columnar producers, state/order/exchange consumers and native writers keep their existing
restrictions. In particular, this does not admit structured grouping keys or retained structured
payloads. Derived scalar results can feed native grouping and its shuffle, including repeated
heap/native RocksDB restore. The updating MAP/MULTISET extension in
[#437](https://github.com/datafusion-contrib/StreamFusion/issues/437) stays deferred.

Runtime-source checks verify inherited Java fields, NULL/empty parents and children, repeated
object references, nested objects and arrays, maps with structured keys/values, multiset counts,
resolved types and ordered per-key RowKinds. Twelve profiles per line additionally retain
released behavior for actual NULLs under NOT NULL hints on structured parents/fields, array
elements, map keys/values and multiset elements. Strict casts retain conversion failures; TRY_CAST
retains whole-result NULLs and errors evaluating its input. CASE/filter and empty-source checks
retain evaluation boundaries. Flink 1.18 rejects a CASE branch containing `ROW(p,10/id)` during
its own SQL validation; that original query retains an explicit host/native planning-failure
assertion. A separate CASE inside the ROW argument verifies division short-circuiting on both
lines. NativeCalc consumption is required for populated admitted queries; unverified callbacks,
structured state and a disabled collection option require fallback.

The structured suite passes 53 checks on 2.2 and 51 with two absent-function skips on 1.18.
The existing six computed-object regressions pass on 2.2 and are catalog skips on 1.18. The
unchanged released miscellaneous-cast suite additionally passes 58/41 result regressions;
these are not counted as positive evidence for the new source boundary.

[Release comparisons and the closed two-capture investigation](../benchmarks/scalar-functions.md#external-structured-collection-inputs)
retain the slower stock/previous/native results and production conversion costs under the
accepted collection-extension tradeoff. Further standalone performance work stays in #392.

### Scalar arrays across native expansion {#scalar-arrays-across-native-expansion}

Verified collection-preserving Calc outputs can feed the
[preserved scalar-array UNNEST boundary](unnest-correlate.md#preserved-scalar-array-expansion).
Nullable STRING/CHAR arrays retain their Arrow representation through Rust fan-out and native
NULL filters. Cache-sensitive scalar consumers use the existing generated evaluator, preserving
UTF-16 behavior rather than treating a string carrier as a logical ROW or discarding its cache.
Native ARRAY construction matches BinaryArrayWriter by clearing string caches and retaining its
binary-copy marker under NULL elements. Unverified nested/state/writer boundaries remain guarded.

### Character search endpoints {#character-search-endpoints}

Native expansion of `IN` and `NOT IN` search predicates preserves the exact
character endpoints stored by the released Flink planner, including the spaces
in explicitly cast `CHAR` constants. This uses the same search normalization
across filters, projections, UNNEST predicates and join conditions. It does not
change string ordering admission or add unsupported comparison signatures.

### Collection references beside native scalar expressions {#collection-references-beside-native-scalar-expressions}

A pure native projection can pass a verified collection input column through beside
unrelated scalar numeric expressions, retaining the input field metadata and Arrow
buffers. The structural check requires a verified columnar producer, no condition,
no JVM calls and direct references for sensitive fields. It does not widen generated
collection evaluation or state/writer boundaries. COLLECT formatting over binary or
nested elements
retains the whole stock plan because Java key order is observable; see
[binary](group-by.md#binary-collection-elements) and
[nested collection elements](group-by.md#nested-collection-elements).

The verified exchange into a JVM collection group preserves supported nested input
representations. This shares the generated-container schema and released value
serializers with expression evaluation. It admits the documented MAP and MULTISET
aggregate elements without widening unrelated stateful consumers or user-function
argument contracts. ARRAY, ROW and STRUCTURED aggregate elements remain unverified;
array support requires a container copy-class representation that also covers empty
arrays.

A preserving consumer's layout requirement also upgrades an existing source transpose
through a direct native projection and exchange. An earlier pruning pass may have
created that transpose before the consumer was selected; the transition pass carries
the inherited requirement into its final schema. Existing batch-size, source-pruning,
JSON and scalar string-cache settings survive the upgrade. This fixes the requested
representation boundary without changing collection element admission.

Generated collection values may also enter a columnar sink that explicitly accepts and
canonicalizes their representation for its wire format. This preserves the existing Doris
Arrow writer and sink-materializer contract; ordinary native sinks retain their row-sink
guard. Native DISTINCT-array groups retain their separately verified input contract.

Batch projections with no output columns preserve the filtered row count, including existential and scalar-subquery intermediates.

Flink multiple-input fusion stays outside native batch regions because its internal serializers require `RowData`. Arrow boundaries are preserved for every native batch task, including plans without joins.

The native Calc type probe admits expression trees up to 32 levels deep. Deeper trees, including large `IN` predicates expanded into nested conditions, retain Flink evaluation before crossing JNI. Wide projections and grouped reductions with shallow expressions remain eligible.
