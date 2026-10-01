# Host-exact builtins over the same upcall, with a faster pure-Rust opt-in

**Applies to:** REGEXP_EXTRACT, UPPER/LOWER, temporal parsing/formatting/arithmetic/casts,
DOUBLE TRUNCATE (under validation)

Builtins whose Rust implementation can diverge from the JVM's — REGEXP_EXTRACT (regex dialects),
UPPER/LOWER (locale case folding), DATE_FORMAT/EXTRACT over `TIMESTAMP_LTZ` (time-zone database
edges) — default to calling Flink's own implementation through the same batch upcall used for user
UDFs: byte parity, island preserved.

The pure-Rust path stays available behind `allowIncompatible` (see
[Configuration](../configuration.md)) for callers who want the faster path and can accept the
divergence risk.

q21 measures the honest price of the guarantee: 0.76x via the upcall vs 1.57x pure-native. For the
zone-aware datetime functions the two paths measure within noise of each other — the call itself
isn't the bottleneck there.

Temporal expressions also use Flink's generated expression code through this bridge. Adjacent
temporal calls fuse into a single evaluator: for `DATE_FORMAT(TO_TIMESTAMP(text), pattern)`,
only the input strings and final formatted result cross the bridge. The intermediate timestamp
stays in Flink's internal representation, preserving its full range without another upcall.
This expands coverage; the original generic-callback measurements did not demonstrate an
end-to-end speedup for isolated temporal projections. The exact canonical TRY_CAST subset below
is measured separately.
See [the temporal contract](../operators/temporal-functions.md) and
[release measurements](../benchmarks/scalar-functions.md#temporal-coverage-diagnostic-2026-09-15).

## Canonical timestamp TRY_CAST

Profiles identify general temporal parsing and repeated timestamp-vector lookup as avoidable
costs. Timestamp writers cache validated vectors. Verified DATE, TIME, TIMESTAMP and
TIMESTAMP_LTZ forms use direct parsing inside the existing batch callback, retaining JNI and
both perimeter transposes. Character arguments retain Java-backed materialization to preserve
UTF-16 and identity semantics; generated non-string arguments can borrow Arrow rows.

Direct character input references use a small fast-path prefix before the released generated
conversion. Composed character children execute their generated code exactly once, outside
conversion failure handling, before calling the parser. Retaining the direct-input prefix matters:
a common wrapper regressed canonical TIMESTAMP from 0.632s to 0.729s; the direct path restores
0.622s while retaining the composed-expression improvement.

DATE recognizes four-digit years and one- or two-digit month/day fields. TIME recognizes
`HH:mm:ss` with optional 1–3 fractional digits, then applies the released line's logical-type
precision rule: Flink 2.2 truncates while 1.18 preserves parsed milliseconds. SQL parity checks
caught why using the raw parser or SQL expression precision alone was insufficient.

TIMESTAMP recognizes `yyyy-MM-dd HH:mm:ss[.fraction]`, years 1–9999 and 1–9 fractional digits.
It preserves Flink's SMART clamping of days 29–31 and next-day normalization of zero-fraction
`24:00:00`, checking components before Java time construction. This removes an exception and
second parse previously paid by normalized values. Fraction truncation, session `TimeZone`,
DST gaps/overlaps and `TimestampData` conversion follow Flink. Other formats and year zero use
the released parser. Legacy mode retains generated DATE/TIME conversion and only the direct
input timestamp prefix; complete-row evaluators retain generated code.

On September 27, 2026, Linux x86-64/Core i7-12650H, JDK 17, Flink 2.2.1, release+mimalloc,
2M runtime rows, parallelism one, 2 GiB heap, default 1,024-row batches, system session zone
America/New_York, no injected NULLs, two warmups and five alternating trials per engine:
median complete-job seconds, including the row source/sink, JNI and both transposes.

| Query | Previous native | Updated native (range) | Stock Flink (range) |
| --- | ---: | ---: | ---: |
| Direct TIMESTAMP | 0.632 | 0.622 (0.613–0.632) | 3.496 (3.458–3.515) |
| Direct TIMESTAMP_LTZ | 0.656 | 0.677 (0.662–0.684) | 3.590 (3.577–3.626) |
| TIMESTAMP of TRIM | 3.796 | 0.862 (0.852–0.883) | 3.674 (3.611–3.699) |
| SMART-normalized TIMESTAMP | 7.298 | 0.621 (0.617–0.628) | 3.504 (3.500–3.547) |
| Direct DATE | 0.756 | 0.545 (0.531–0.568) | 0.611 (0.583–0.625) |
| DATE of TRIM | 0.925 | 0.735 (0.725–0.747) | 0.768 (0.765–0.772) |
| Unpadded DATE | 0.748 | 0.554 (0.542–0.559) | 0.587 (0.569–0.597) |
| Direct TIME | 0.788 | 0.544 (0.534–0.670) | 0.642 (0.632–0.657) |
| TIME of TRIM | 0.951 | 0.750 (0.734–0.754) | 0.780 (0.773–0.789) |
| Short-fraction TIME | 0.790 | 0.544 (0.539–0.554) | 0.628 (0.620–0.641) |

These are separate runs with source-matched identity controls retained in the CSV output.
Direct timestamp controls are 0.479s native / 0.400s Flink; SMART controls are 0.470s / 0.392s.
DATE/TIME identity medians range from 0.455–0.546s native and 0.385–0.402s Flink; the
short-fraction identity run was more variable (native 0.486–0.565s). Reported expression
ranges retain outliers, including the direct TIME run.
The previous direct TIMESTAMP_LTZ run used Flink 3.500s versus the current 3.590s; its small
native difference is within similar control movement. Composed and SMART timestamps improve
substantially over the previous native implementation, while direct timestamps preserve their
existing speedup. These fixed fixtures do not establish a speedup for every temporal spelling.
Unrecognized and malformed text still use released parsing.

Boolean remains a performance blocker. A separate 20M-row length-dispatch experiment measured
3.497s native versus 3.402s Flink; control drift did not establish an improvement, so that kernel
change was removed. The PR remains draft pending its remaining performance blockers.

Fixtures alternate `2000-2-29` / `1969-1-2` for unpadded dates,
`12:34:56.1` / `23:59:59.12` for short fractions, and
`2024-02-30 00:00:00` / `2024-02-29 24:00:00` for SMART timestamps. Canonical timestamp
fixtures alternate `2000-02-29 12:34:56` / `1969-12-31 23:59:59`. Composed queries apply TRIM
before TRY_CAST. Plans require native Calc and both transposes. Reproduce with:

```sh
SF_BENCHMARK=true mvn test -Pbench -pl streamfusion-runtime -am \
  '-Dtest=ScalarFunctionBenchmark#individualFunctions' -Dsurefire.failIfNoSpecifiedTests=false \
  -Dscalar.functions=TRY_STRING_TO_DATE,TRY_STRING_TO_TIME,TRY_STRING_TO_TIMESTAMP,TRY_STRING_TO_TIMESTAMP_LTZ,TRY_DATE_COMPOSED,TRY_TIME_COMPOSED,TRY_TIMESTAMP_COMPOSED,TRY_DATE_NONCANONICAL,TRY_TIME_NONCANONICAL,TRY_TIMESTAMP_NONCANONICAL \
  -Dscalar.rows=2000000 -Dscalar.warmup=2 -Dscalar.runs=5 -Dsf.extraJvmArgs=-Xmx2g
```

All 91 parser/SQL checks pass on each released Flink line. They cover precisions 0–9, four zones,
random dates, range limits, pre-epoch fractions, DST, normalization, short fractions, unpadded
dates, legacy settings, stateful child evaluation, input failures, NULLs and guarded branches.

## Bounded DOUBLE TRUNCATE shortcut

The existing JVM scalar upcall can avoid decimal conversion for DOUBLE TRUNCATE when
outward-rounded adjacent-double bounds give the same truncated integer. Absolute values
1 through 1e9 and scales -6 through 6 keep the intermediate integer exactly representable.
Ambiguous boundaries and values outside that domain use released Flink evaluation. NULL
handling and row short-circuit/order gates remain in place; the Arrow/JNI bridge is unchanged.

September 30, 2026, JDK 17, Flink 2.2.1, release Criterion, 1,024 sliced non-NULL rows:
production Rust Calc exports both arguments, invokes Java, imports the Arrow result and disposes
of it inside measurement. Fixture setup, JVM startup and registration are outside it. Each case
uses three seconds of warmup and 100 samples over at least five seconds. Reference runs precede
shortcut runs in one embedded JVM. These are Criterion **means**, with 95% confidence intervals
partially recovered in the [task-log excerpts](../benchmarks/recovered-historical-diagnostics-2026-10-01.txt).

| Profile | Released Flink upcall | Bounded shortcut | Change |
| --- | ---: | ---: | ---: |
| Bounded values, dynamic scales -3 through 3 | 368.029 µs | 168.769 µs | -54.1% |
| Exact decimal boundaries (fallback) | 316.862 µs | 317.230 µs | +0.1%, overlapping intervals |
| Values outside the domain (fallback) | 238.473 µs | 232.739 µs | -2.4% |

Both paths request 5,248 Rust bytes across 84 allocation calls and produce a new 8,192-byte
output payload. Rust counters do not measure Java allocations, so these figures do not quantify
the avoided decimal objects or copied bytes. All 36 fixtures validate nullable slices and three
batch sizes; only the six cases above were timed. Exact helper tests pass against released
Flink 2.2.1 and 1.18.1. Runtime SQL parity now passes on Flink 2.2.1; complete-job comparisons with stock Flink and
previous StreamFusion remain pending.
These measurements establish an upcall improvement, not end-to-end acceleration admission.

```sh
python3 bin/bench-native.py --bench jvm_truncate --smoke
python3 bin/bench-native.py --bench jvm_truncate --filter '/1024/.*/nulls=false$'
```

An expanded run registers the production generated Flink expression evaluator alongside the
reflective reference and shortcut. All 54 fixtures compare exact outputs. JVM generation/opening
remain outside measurement; the generated evaluator uses the existing imported Arrow-row reader
inside the production upcall. Same machine, heap, batch size and sampling settings as above, fresh
JVM, with reference/shortcut/generated cases run in that order. Remeasured means:

| Profile | Reflective Flink | Shortcut | Generated Flink |
| --- | ---: | ---: | ---: |
| Bounded values | 351.421 µs | 155.387 µs | 351.387 µs |
| Decimal boundaries | 326.910 µs | 321.720 µs | 295.411 µs |
| Outside domain | 227.612 µs | 228.559 µs | 224.495 µs |

The bounded shortcut improves 55.8% against the generated evaluator. Decimal-boundary fallback
is 8.9% slower than generated evaluation, with disjoint mean confidence intervals: this remains
an optimization blocker, rather than an admitted default acceleration. The generated path borrows
Arrow rows, while reflective evaluation materializes argument columns; Java allocations are not
captured by the Rust counter. All three retain identical 5,248-byte/84-call Rust probes and
8,192-byte new output payloads. [All nine means and confidence intervals](../benchmarks/recovered-historical-diagnostics-2026-10-01.txt)
are retained, including unfavorable profiles. The earlier two-way run remains above; movement
between JVM runs does not establish a code improvement. Whole-job gates remain pending.

### Borrow input rows in the shortcut evaluator

The shortcut now runs inside the existing generated-expression class, using the same imported
Arrow-row reader as the generated reference. Generated primitive operands call the exact helper
directly, eliminating reflective argument-column materialization. NULL guards precede the helper;
an omitted scale is primitive INT zero, with no additional Arrow argument. Lazy consumers and
multi-failure row programs retain the existing generated row-order path. Comet's owned C Data
import/evaluate/export contract is unchanged.

Fresh four-way release Criterion run, same configuration and sampling, reference/reflective
shortcut/generated/borrowed order, 1,024 sliced non-NULL rows, means:

| Profile | Reflective shortcut | Generated Flink | Borrowed shortcut |
| --- | ---: | ---: | ---: |
| Bounded values | 167.718 µs | 364.370 µs | 163.116 µs |
| Decimal boundaries | 315.334 µs | 292.829 µs | 302.161 µs |
| Outside domain | 226.298 µs | 222.645 µs | 227.079 µs |

Borrowing improves bounded and decimal-boundary times 2.7% and 4.2% over the same-run reflective
shortcut. Bounded values remain 55.2% faster than generated Flink. Decimal-boundary fallback is
still 3.2% slower than generated Flink, and outside-domain values 2.0% slower, with disjoint
confidence intervals. This is partial optimization, not removal of the performance blocker.
[All twelve means and confidence intervals](../benchmarks/recovered-historical-diagnostics-2026-10-01.txt)
retain the unfavorable cases. Rust allocation/payload probes remain unchanged and do not count
Java argument objects. All 72 upcall fixtures pass bit-exact checks. Twenty in-process checks pass
on Flink 2.2.1; nineteen pass on 1.18.1 with unsupported ELT skipped. They include generated default
and dynamic scale values, NULLs, signed zero, boundaries and exception messages. Eleven runtime SQL checks pass on Flink 2.2.1, including native CASE/COALESCE results and
exceptions and the existing AND/OR fallback. Complete-job performance gates remain pending.


The environment reset removed temporary benchmark artifacts. Historical links above now point to surviving task-log excerpts; complete raw CSVs and Criterion samples must be regenerated.

## Whole-job DOUBLE TRUNCATE controls

`ScalarFunctionBenchmark` includes `DOUBLE_TRUNCATE_BOUNDED`, `DOUBLE_TRUNCATE_BOUNDARY`,
and `DOUBLE_TRUNCATE_OUTSIDE`. Runtime sequence sources generate alternating signs, dynamic
INT scales and optional NULL operands. Boundary inputs use half-integers at scale one; outside
inputs use absolute value 0.46. Each has a source-matched DOUBLE identity control. The existing
blackhole sink, release JNI library and both transposes stay in the native measured path, and
plan assertions require native Calc. These fixtures provide end-to-end evidence for the same
three profiles as the four-way JNI Criterion controls; no whole-job speedup is assumed.

Run with `SF_BENCHMARK=true`, `-Pbench`,
`-Dtest=ScalarFunctionBenchmark#individualFunctions`, and
`-Dscalar.functions=DOUBLE_TRUNCATE_BOUNDED,DOUBLE_TRUNCATE_BOUNDARY,DOUBLE_TRUNCATE_OUTSIDE`.
Use identical rows, warmups and trials for stock Flink and previous/candidate StreamFusion,
and retain the per-trial CSV with `-Dscalar.output=...`.

All three profiles and their identity controls execute successfully with 5,003 runtime rows
on released Flink 2.2.1. This single-trial fixture smoke check validates plan admission and
execution only; its startup-dominated durations are not performance-admission evidence.

When the previous production revision falls back for a newly supported function, run the same
fixture with `-Dscalar.native.expected=false`. This explicit control requires the selected
function to lack native Calc and runtime substitution while identity controls still require
native execution. The default remains strict native admission. Label those native-enabled
trials as previous-version fallback in retained comparisons; they do not represent native
acceleration or replace stock-Flink measurements.

## Recovered release whole-job measurements, 2026-10-01

Released Flink 2.2.1, JDK 17 and the candidate at `ee58ff55`, with release Rust/mimalloc,
run two million runtime rows, two warmups and five measured trials per engine. Execution
alternates engine order each trial. The existing blackhole sink, JNI and both transposes
remain measured; plan assertions require native Calc. The host was quiet before starting.
Source values have alternating signs, dynamic scales, no NULLs and the three profiles above.
[All trials and source-matched identity controls](../benchmarks/expression-wholejob-candidate-2026-10-01.csv)
are retained.

| Expression/profile | Stock Flink median | Candidate median | Stock/candidate |
| --- | ---: | ---: | ---: |
| STRING to BINARY(16), 264-byte source strings | 0.950 s | 1.007 s | 0.94× |
| Fixed BINARY ELT | 0.334 s | 0.416 s | 0.80× |
| DOUBLE TRUNCATE, bounded | 0.786 s | 0.457 s | 1.72× |
| DOUBLE TRUNCATE, half-integer boundaries | 0.691 s | 0.766 s | 0.90× |
| DOUBLE TRUNCATE, outside domain | 0.518 s | 0.591 s | 0.88× |

These durations include startup and deployment. Retain the slow standalone binary and
TRUNCATE fallback profiles; a bounded-domain win does not satisfy their admission gate.
The source-matched native identity controls also lose to stock Flink, showing the remaining
row/Arrow conversion floor. Profile and optimize before treating these candidates as complete.
Previous-production comparisons and broader nullable/composition profiles remain pending.

The previous production revision (`1b1b5ed8`, including canonical main `0b38269e`) runs
identical fixture-only benchmark code with explicit expected fallback for the new functions.
[All previous-version trials](../benchmarks/expression-wholejob-previous-2026-10-01.csv)
retain stock and native-enabled fallback controls on the same host and configuration.
Previous native-enabled medians are 0.939 s (cast), 0.350 s (ELT), 0.774 s (bounded TRUNCATE),
0.689 s (boundary) and 0.529 s (small fractions). Thus the initial candidate's bounded
TRUNCATE beats both references, while every other listed profile still requires optimization.
The native identity controls are verified separately rather than bypassing all route assertions.
Linux/Core i7-12650H, Rust 1.94.0 and JDK 17; the native library uses production mimalloc.
