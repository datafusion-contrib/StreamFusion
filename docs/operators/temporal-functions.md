# Temporal functions

**Status: partial.** All Flink 2.2.1 temporal scalar families have expression implementations.
Timestamp results retain Flink's complete range. Pattern time functions still require the
unsupported `MATCH_RECOGNIZE` operator.

## Function inventory

| Functions | Execution and admission |
|---|---|
| `CURRENT_DATE`, `CURRENT_TIME`, `CURRENT_TIMESTAMP`, `LOCALTIME`, `LOCALTIMESTAMP`, `NOW()`, `CURRENT_ROW_TIMESTAMP()` | Rust clock functions; sampled during each batch evaluation, using the session zone for local fields. These nondeterministic functions are admitted by default. |
| `PROCTIME()`, internal `PROCTIME_MATERIALIZE` | Direct native calls and verified simple COALESCE selections use the Rust execution-time clock. Inside Flink-generated scalar or complete-Calc expressions, materialization reads the owning task's live processing-time service, bound at open and cleared at close. CASE/COALESCE input branches preserve their timestamp precision; materialized values follow Flink's millisecond clock and declared casts. See [generated processing time](calc-filter.md#generated-processing-time). |
| `EXTRACT`, `YEAR`, `QUARTER`, `MONTH`, `WEEK`, `DAYOFYEAR`, `DAYOFMONTH`, `DAYOFWEEK`, `HOUR`, `MINUTE`, `SECOND` | All forms Flink accepts for DATE, TIME, TIMESTAMP, TIMESTAMP_LTZ and intervals. DATE/plain-TIMESTAMP calendar fields and HOUR/MINUTE/SECOND over DATE, plain TIMESTAMP and TIME execute in Rust, preserving Java signed arithmetic. Direct LTZ extraction uses the planning JDK's serialized fixed/finite offset history; recurring-DST zones retain Flink. Interval fields and unverified wrappers remain generated. |
| `TO_DATE(text)`, `TO_DATE(text, format)` | The one-argument parser stays in Rust; formatted parsing uses Flink. Dynamic formats, NULL behavior and invalid-input failures follow Flink. |
| `TO_TIMESTAMP(text[, format])` | Flink parsing, including dynamic formats and full-range timestamp results. |
| `TO_TIMESTAMP_LTZ` | Direct/literal integer epoch seconds or milliseconds with literal precision 0 or 3 execute in Rust. TINYINT/SMALLINT/INT widen losslessly to BIGINT first. Verified ordinary-Calc integer arithmetic, widening and rounding trees also execute in Rust; widening happens after the computed integer result. Unverified producers, other numeric signatures and string/format/explicit-zone overloads retain Flink evaluation. Existing UNNEST coverage is unchanged. Released Flink validates numeric precision (0 or 3). |
| `DATE_FORMAT`, `UNIX_TIMESTAMP(text[, format])`, `CONVERT_TZ` | Flink evaluation supports dynamic patterns and zones. The existing literal numeric-pattern DATE_FORMAT fast path for plain timestamps remains in Rust. |
| `FROM_UNIXTIME` | Constant numeric formats in fixed-offset zones execute in Rust; other forms retain Flink's evaluator. See the exact subset below. |
| `UNIX_TIMESTAMP()` | Rust current Unix-seconds clock. |
| `TIMESTAMPADD`, `TIMESTAMPDIFF` | Planner rewrites can use native interval arithmetic and comparisons. Verified runtime MONTH/DAY addition, MONTH difference and LTZ SECOND difference have no generated children; other rewrites retain their existing expression admission. Month-end and precision rules follow Flink. |
| `FLOOR(timepoint TO unit)`, `CEIL`, `CEILING` | DATE/plain-TIMESTAMP MONTH/YEAR and plain TIMESTAMP DAY/HOUR/MINUTE/SECOND/MILLISECOND use Rust kernels. Direct/literal LTZ FLOOR/CEIL to MONTH/YEAR also uses the existing kernel in fixed zero-offset zones. In the same zones, direct/literal LTZ FLOOR/CEIL uses Rust for DAY/HOUR on both releases and MINUTE/SECOND/MILLISECOND on 2.2; LTZ CEIL has its own remainder/overflow kernel. Released 1.18 cannot compile those three sub-hour LTZ forms; its failure stays intact. Nonzero/regional zones, computed inputs and other units retain their existing routes. Numeric FLOOR/CEIL admission is unchanged. |
| Temporal `CAST`, date/time/timestamp/interval literals, interval arithmetic, temporal comparisons, `OVERLAPS` | Timestamp +/- day-time/year-month intervals, DATE +/- month intervals and verified day literals, and TIME +/- day-time intervals execute in Rust. Timestamp results preserve nanoseconds. Matching temporal GREATEST/LEAST operands use native null-propagating comparisons. OVERLAPS can rewrite to native comparisons, including the verified runtime plain-timestamp/day-time-interval form. Other casts, comparisons and interval computations retain their existing expression admission. |
| `CURRENT_WATERMARK(time_attribute)` | Calc projections and predicates read the last watermark received by that operator; NULL before its first watermark. Join/UNNEST residuals without a Calc watermark context fall back. |
| `SOURCE_WATERMARK()` | Source declaration, not a scalar calculation. Existing DataStream/source watermarks are forwarded into the native pipeline. Native connector scan admission is unchanged. |
| `TUMBLE`, `HOP`, `CUMULATE`, `SESSION` | Native window implementations with released-Flink calendar conversion for regional LTZ zones and nonaligned fixed offsets; existing shape/changelog restrictions remain. See [window aggregate](window-aggregate.md) and [windowing TVF](window-aggregate.md#windowing-tvf-window-assignment). |
| `TUMBLE_START`, `TUMBLE_END`, `TUMBLE_ROWTIME`, `TUMBLE_PROCTIME`; corresponding `HOP_*` and `SESSION_*` helpers | Existing legacy window properties; see [window aggregate](window-aggregate.md). |
| `MATCH_ROWTIME`, `MATCH_PROCTIME` | Fall back with [MATCH_RECOGNIZE](unsupported.md). A scalar implementation cannot supply pattern-match context. |

Flink still rejects invalid signatures, invalid units and expressions outside their SQL context.
The inventory describes Flink's functions, not extra overloads supplied by other SQL dialects.

DATE `HOUR(d)`/`MINUTE(d)`/`SECOND(d)` and their EXTRACT aliases follow released Flink's physical-integer
arithmetic: `(epochDays % 86400000) / 3600000` for HOUR and
`(epochDays % 3600000) / 60000` for MINUTE, and `(epochDays % 60000) / 1000` for SECOND,
with signed remainder and truncation
towards zero. This can be nonzero for large internal DATE values; interpreting every DATE
as midnight would produce different results. The Arrow kernel preserves NULLs, sliced
offsets and empty batches without introducing a JVM callback. Runtime regression sources
carry internal DATE integers directly so the full signed range reaches SQL evaluation.
For example DATE '2024-01-31' produces SECOND 19 and DATE '0001-01-01' produces
MINUTE -11 and SECOND -59. These match both supported released Flink lines rather
than an assumed SQL-midnight interpretation.
The [correctness record](../benchmarks/results/date-clock-extraction-2026-11-02.json)
retains twelve failing before-fix assertions and twelve passing final SQL invocations across
Flink 1.18.1/2.2.1 and JDK 11/17, plus the unchanged released extraction oracle and 886
passing Rust core tests. This repairs existing native results; it is not an acceleration claim.

## Native BIGINT epoch milliseconds

`TO_TIMESTAMP_LTZ(n,3)` for a direct BIGINT column or literal reuses the existing epoch
millisecond kernel in ordinary Calc expressions. The conversion does not depend on the
session zone. Both supported released Flink lines return NULL outside the inclusive physical
millisecond range `[-62167219200000,253402300799999]`; the kernel preserves these exact
bounds, NULL inputs and declared TIMESTAMP_LTZ(3) output. It does not reinterpret them as
an assumed year-1-to-year-9999 proleptic range or wrap an invalid long into a valid timestamp.

The two-component Arrow timestamp shares the input's immutable millisecond buffer, creates
zero nanoseconds and keeps combined input/range validity on the parent. Non-null component
children remain consistent with the existing schema, including sliced inputs and empty
batches. No input bytes are modified and the output owns its retained Arrow buffer reference.
This removes temporary value vectors and their copies without changing JNI or timestamp
transport. The verified computed-integer subset below also uses this kernel. Unverified
producers, non-integer numeric types and runtime/other precisions keep their existing routes.
No production switch is added.

The [qualification](../benchmarks/scalar-functions.md#bigint-epoch-millisecond-conversion)
retains stock/actual-previous/native measurements, the initial slower pilot, profiling and
allocation evidence, and complete jobs with both transposes. Broader conversion work stays
in [#503](https://github.com/datafusion-contrib/StreamFusion/issues/503).

`TO_TIMESTAMP_LTZ(n,0)` also accepts direct BIGINT columns/literals. It checks the original
seconds against the inclusive released range `[-62167219200,253402300799]` before scaling
to milliseconds. In particular, an invalid long must not wrap into the timestamp range
when multiplied by 1000. It uses the same parent validity/two-component representation,
with a new immutable millisecond buffer and zero sub-millisecond nanos. Session zones do
not affect the numeric conversion. The admission does not extend computed UNNEST inputs,
numeric precision or the string/format overloads.

Direct TINYINT/SMALLINT/INT columns and literals also use these kernels for precision
0 and 3. A lossless signed Arrow cast supplies the existing BIGINT input contract;
there is no narrowing or floating conversion, and all such values fit both released
epoch ranges. Output remains TIMESTAMP_LTZ(3), with input NULLs retained. Both Flink
lines widen these inputs before applying their long-based timestamp conversion.
This direct-input extension leaves existing UNNEST admission unchanged; the additional
ordinary-Calc computed subset is specified below.
The BIGINT buffer-sharing and overflow-bound rules above remain unchanged.
The [narrow-integer qualification](../benchmarks/scalar-functions.md#narrow-integer-epoch-conversion)
records bulk function gains alongside slower tiny batches and complete jobs.

## Native computed integer epochs

Ordinary Calc also accepts verified integer trees as the first operand of
`TO_TIMESTAMP_LTZ(...,0)` or `(...,3)`: integer addition/subtraction/multiplication,
lossless widening, identity FLOOR/CEIL/ROUND and the existing verified ROUND/TRUNCATE
literal-position subset (-38 through 38 or NULL). Every child must satisfy the same
recursive proof. Division, remainder, narrowing, unknown/stateful producers and other
signatures keep their existing correct evaluator. UNNEST admission is unchanged.

The integer child executes at its resolved width before any widening for the BIGINT
epoch kernel. This retains narrow and signed-long overflow, rounding NULLs and the
released epoch bounds. For example, an overflowing INT addition must wrap as an INT
before conversion; it must not become a wider addition. Existing millisecond/second
kernels retain their parent validity, range checks and timestamp schema. Session zones
do not affect the numeric conversion; a recurring-DST extraction consumer may still
require generated evaluation independently of this epoch conversion.

This reuses the existing verified integer proof and native kernels without a new
switch, JNI bridge, transport or Rust implementation. The
[computed-epoch qualification](../benchmarks/scalar-functions.md#computed-integer-epoch-conversion)
retains prepared stock/actual-previous/native controls and conversion-inclusive jobs.

## Native UTC LTZ calendar rounding

FLOOR/CEIL to MONTH/YEAR over a direct LTZ input or literal reuses the plain-timestamp calendar
kernel when the configured session zone normalizes to the fixed UTC offset. This includes
UTC, Z and fixed zero-offset aliases; a regional zone with a current/raw offset of zero
does not qualify. Released Flink adds/subtracts no offset in these zones and uses the same
Julian-date arithmetic, including signed millisecond division, int overflow and the Gregorian
cutover. Output fractions are zero and the declared SQL precision is retained.

Computed inputs and nonzero/regional zones keep their existing paths; fixed-unit
rounding has the separate verified subsets below.
In particular, fixed-unit LTZ CEIL has different submillisecond/overflow behavior from plain
TIMESTAMP CEIL, so the plain kernel is not a blanket replacement. Calendar admission adds
no new native kernel, JNI bridge or switch. The separate
[FLOOR](../benchmarks/scalar-functions.md#utc-ltz-calendar-floor) and
[CEIL](../benchmarks/scalar-functions.md#utc-ltz-calendar-ceil) qualifications record
stock/actual-previous/native timings and complete jobs with both transposes.

## Native UTC LTZ fixed-unit FLOOR

Direct/literal LTZ FLOOR reuses the existing plain-TIMESTAMP fixed-width kernel when
the configured zone normalizes to fixed UTC. DAY/HOUR qualify on both released Flink
lines; MINUTE/SECOND/MILLISECOND qualify only on 2.2. Released 1.18 generates invalid
Java casts for these three smaller LTZ units, so they keep the existing code-generation
failure through Flink fallback. A version capability preserves that released behavior;
it is not a runtime option.

With zero offset, Flink floors the signed millisecond field using Java remainder and
wrapping subtraction, and resets fractional nanoseconds to zero even for MILLISECOND.
The existing Rust kernel has the same floor behavior, including full signed-long
extrema; timestamp schema, declared precision and NULLs are unchanged. The extension
changes no production Rust kernel, JNI contract or transport. Computed operands and
nonzero/regional zones retain their preceding routes. LTZ CEIL ignores fractional nanos
and uses different overflow arithmetic from plain TIMESTAMP CEIL; its separate kernel
below preserves those rules.
The [fixed-unit FLOOR qualification](../benchmarks/scalar-functions.md#utc-ltz-fixed-unit-floor)
retains bulk improvements, slower tiny controls and conversion-inclusive complete jobs.

## Native UTC LTZ fixed-unit CEIL

Direct/literal LTZ CEIL in a fixed-zero session zone uses a dedicated Rust kernel for
DAY/HOUR on both releases and MINUTE/SECOND/MILLISECOND on 2.2. The same source, zone
and released-version safeguards as fixed-unit FLOOR apply. Computed operands, nonzero/
regional zones and other units retain their existing evaluation routes.

The kernel follows released `DateTimeUtils.timestampCeil`: take the original signed
millisecond remainder, subtract it, then add the width only for a positive remainder.
Subtraction/addition wrap as Java long arithmetic. Fractional nanos are discarded for
all admitted units, including MILLISECOND; an input millisecond of -1 with positive
fractional nanos still produces -1 milliseconds at that unit. With width 1, Long.MAX_VALUE
stays Long.MAX_VALUE. These differ from plain TIMESTAMP CEIL, whose original kernel is
unchanged. Output fractions are zero, input NULLs are retained and SQL precision stays
unchanged.

The UDF accepts the existing component and primitive Arrow timestamp inputs without
coercing primitives into ordinary ROW values. Scalar broadcasts, slices and empty arrays
use the existing timestamp helpers and return the normal component timestamp schema.
No JNI, transport, dependency or configuration switch changes.
The [fixed-unit CEIL qualification](../benchmarks/scalar-functions.md#utc-ltz-fixed-unit-ceil)
records bulk improvements and the slower tiny/complete-job controls.

## Native FROM_UNIXTIME

The default format and literal formats composed of `yyyy`, `MM`, `dd`, `HH`, `mm`, `ss`,
punctuation and quoted literals execute in Rust for integer epoch seconds in a fixed-offset
session zone. This includes `yyyyMMddHHmm`, Unicode literals and doubled apostrophes.
The format is compiled once when constructing the expression; evaluation registers no JVM
callback and performs no Arrow/JVM handoff for formatting.

The native calendar reproduces Java's Julian/Gregorian cutover in October 1582, year-of-era
formatting before year 1, and the full BIGINT input range. Seconds-to-milliseconds conversion
wraps like Java long arithmetic; adding the zone offset retains the calendar's wider range.
NULL seconds produce NULL, including when the format is empty.

Dynamic or unsupported patterns, NULL patterns, invalid patterns and zones with historical
or recurring transitions retain the existing Flink evaluator. The native subset also requires
the planning JVM's default format locale to use a Gregorian calendar and ASCII digits;
other locale calendars/digit systems stay on Flink. Supported native formatting can feed
an outer host temporal evaluator without moving formatting itself back across the bridge.

## Exact evaluation and expression fusion

Temporal functions outside the native subsets use Flink 2.2.1's expression generator, compiled once when the native Calc is
planned. The generated evaluator travels in the existing serializable scalar-function binding,
opens on the task manager, and receives argument columns through the existing batched JVM upcall.
Native operators continue to exchange Arrow batches. No additional row/Arrow operators are inserted.
The relational node supplies the session zone and cast settings, including in filters and join residuals.

Adjacent temporal calls are fused into one generated evaluator. For example,
`DATE_FORMAT(TO_TIMESTAMP(text), pattern)` parses and formats inside one upcall, with only the
strings crossing the bridge. This preserves Flink's wider intermediate date range and avoids
marshalling an intermediate timestamp. Calendar arithmetic, fractions, NULLs, invalid patterns,
and DST rules follow the running Flink/JDK implementation.

The existing `DATE_FORMAT` and `EXTRACT` `allowIncompatible` options still select their limited
Rust LTZ fast paths. Dynamic formats, recurring-DST LTZ extraction and unverified units retain Flink even with those options.

TIME columns use Arrow millisecond storage even for a declared `TIME(0)`, because Flink's internal
TIME values can retain milliseconds. This prevents precision loss during expression evaluation.

## Timestamp range

TIMESTAMP and TIMESTAMP_LTZ columns retain Flink's signed epoch milliseconds and nanoseconds within
the millisecond in separate Arrow buffers. Timestamp-producing expressions, literals and rounding
run by default across Flink's supported range, including years 0001 and 9999. The former
`TIMESTAMP_RANGE.allowIncompatible` option is no longer needed and has no effect.

Declared precision does not discard fractions already present in a runtime TimestampData value.
Event-time operations read milliseconds, while comparisons, expression results and payloads retain
both components. Other function-specific gates still apply. Pattern matching remains tracked in
[issue #78](https://github.com/datafusion-contrib/StreamFusion/issues/78).

## Validation and performance

Parity tests cover dynamic patterns and zones, NULLs and errors, pre-epoch fractions, leap days,
month ends, DST gaps/overlaps, temporal casts and intervals, filters, join predicates, grouping,
computed rowtime windows, and watermark timing. Expanded-year tests cover fused string/numeric
results and full-range timestamp outputs, grouping keys and window boundaries. Clock tests check execution-time
bounds rather than equality between two runs.

Regression tests also cover temporal admission alongside TRIM and JSON option symbols, text-field
DATE_FORMAT patterns through the upcall, and the legacy native timestamp-minus-interval encoding.
The day-time interval regressions cover signed/subsecond literals, nullable grouping keys,
filters after Top-N, and outer-join residual arithmetic. They retain canonical millisecond
integers throughout the native pipeline. Timestamp subtraction with nanosecond payloads and
year-month CASE results also pass released-Flink parity through the current temporal evaluator;
these forms no longer require the older duration/interval output fallbacks.

These changes expand the queries that can remain in an accelerated pipeline. They do not establish
that isolated temporal projections are faster than stock Flink. Release-mode measurements, including
both row/Arrow transposes, are on the [scalar benchmark page](../benchmarks/scalar-functions.md).

## Remaining native boundaries

[Issue #504](https://github.com/datafusion-contrib/StreamFusion/issues/504) still tracks
LTZ rounding outside the direct/literal fixed-zero calendar/fixed-unit subsets above and
recurring-DST LTZ extraction. Nonzero/regional zones, computed inputs and unverified units
retain generated rounding; fixed-zero DAY/HOUR and release-supported sub-hour forms no
longer require it. MONTH/YEAR rounding in nonzero/regional zones also retains it.
America/New_York extraction retains it because the zone has recurring future DST transitions.

The [expanded route audit](../benchmarks/results/calendar-residual-routes-2026-11-02.json)
compares 128 independent queries, each with 5,003 runtime rows, across Flink 1.18.1/2.2.1,
JDK 11/17 and those four zones. It predates the bounded UTC calendar rounding admission;
its original CEIL(LTZ TO MONTH) route is retained as historical evidence.
Direct runtime TIMESTAMPADD(MONTH/DAY),
TIMESTAMPDIFF(MONTH), TIMESTAMPDIFF(SECOND over LTZ) and the inspected plain-timestamp
OVERLAPS form are already native, with complete parity, consumed Calc work, no fallback
and empty evaluators. The two TIMESTAMPDIFF arguments come from distinct runtime columns
so the difference cannot fold to zero. These forms are not remaining blanket gaps.

Other units, mixed temporal coercions, interval extraction and rowtime wrappers require their
own expression-route checks. A native outer arithmetic kernel does not establish that a
generated child also ran in Rust, and this audit does not qualify every possible rewrite.

Matching logical timestamp extrema accept both compact millisecond arrays and full
millisecond/nanosecond component arrays, including mixtures produced by collection expressions.

[Prepared native calendar measurements](../benchmarks/native-certified-scalars.md) include
YEAR(DATE), MONTH rounding, timestamp month arithmetic and matching-timestamp extrema.
They exclude perimeter transposes; the separate whole-job results retain those costs.
