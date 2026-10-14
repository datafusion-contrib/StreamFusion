# Temporal functions

**Status: partial.** All Flink 2.2.1 temporal scalar families have expression implementations.
Timestamp results retain Flink's complete range. Pattern time functions still require the
unsupported `MATCH_RECOGNIZE` operator.

## Function inventory

| Functions | Execution and admission |
|---|---|
| `CURRENT_DATE`, `CURRENT_TIME`, `CURRENT_TIMESTAMP`, `LOCALTIME`, `LOCALTIMESTAMP`, `NOW()`, `CURRENT_ROW_TIMESTAMP()` | Rust clock functions; sampled during each batch evaluation, using the session zone for local fields. These nondeterministic functions are admitted by default. |
| `PROCTIME()`, internal `PROCTIME_MATERIALIZE` | Rust processing-time clock, evaluated at execution time. |
| `EXTRACT`, `YEAR`, `QUARTER`, `MONTH`, `WEEK`, `DAYOFYEAR`, `DAYOFMONTH`, `DAYOFWEEK`, `HOUR`, `MINUTE`, `SECOND` | All forms Flink accepts for DATE, TIME, TIMESTAMP, TIMESTAMP_LTZ and intervals. The existing DATE/plain-TIMESTAMP quarter/week/day kernels stay in Rust; other cases use Flink's generated expression code. |
| `TO_DATE(text)`, `TO_DATE(text, format)` | The one-argument parser stays in Rust; formatted parsing uses Flink. Dynamic formats, NULL behavior and invalid-input failures follow Flink. |
| `TO_TIMESTAMP(text[, format])` | Flink parsing, including dynamic formats and full-range timestamp results. |
| `TO_TIMESTAMP_LTZ` | Flink's numeric and string overloads, including format and explicit-zone arguments. Flink validates numeric precision (0 or 3). |
| `DATE_FORMAT`, `UNIX_TIMESTAMP(text[, format])`, `CONVERT_TZ` | Flink evaluation supports dynamic patterns and zones. The existing literal numeric-pattern DATE_FORMAT fast path for plain timestamps remains in Rust. |
| `FROM_UNIXTIME` | Constant numeric formats in fixed-offset zones execute in Rust; other forms retain Flink's evaluator. See the exact subset below. |
| `UNIX_TIMESTAMP()` | Rust current Unix-seconds clock. |
| `TIMESTAMPADD`, `TIMESTAMPDIFF` | Flink calendar and elapsed-time arithmetic, including its month-end and precision rules. |
| `FLOOR(timepoint TO unit)`, `CEIL`, `CEILING` | Flink rounding; plain TIMESTAMP DAY/HOUR/MINUTE/SECOND/MILLISECOND use a Rust kernel. Numeric FLOOR/CEIL admission is unchanged. |
| Temporal `CAST`, date/time/timestamp/interval literals, interval arithmetic, temporal comparisons, `OVERLAPS` | Flink-generated temporal subexpressions plus typed Arrow literals, with lossless timestamp results. |
| `CURRENT_WATERMARK(time_attribute)` | Calc projections and predicates read the last watermark received by that operator; NULL before its first watermark. Join/UNNEST residuals without a Calc watermark context fall back. |
| `SOURCE_WATERMARK()` | Source declaration, not a scalar calculation. Existing DataStream/source watermarks are forwarded into the native pipeline. Native connector scan admission is unchanged. |
| `TUMBLE`, `HOP`, `CUMULATE`, `SESSION` | Native window implementations with released-Flink calendar conversion for regional LTZ zones and nonaligned fixed offsets; existing shape/changelog restrictions remain. See [window aggregate](window-aggregate.md) and [windowing TVF](window-aggregate.md#windowing-tvf-window-assignment). |
| `TUMBLE_START`, `TUMBLE_END`, `TUMBLE_ROWTIME`, `TUMBLE_PROCTIME`; corresponding `HOP_*` and `SESSION_*` helpers | Existing legacy window properties; see [window aggregate](window-aggregate.md). |
| `MATCH_ROWTIME`, `MATCH_PROCTIME` | Fall back with [MATCH_RECOGNIZE](unsupported.md). A scalar implementation cannot supply pattern-match context. |

Flink still rejects invalid signatures, invalid units and expressions outside their SQL context.
The inventory describes Flink's functions, not extra overloads supplied by other SQL dialects.

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
Rust LTZ fast paths. Dynamic patterns and newly supported fields use Flink even with those options.

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
