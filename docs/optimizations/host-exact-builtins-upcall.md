# Host-exact builtins over the same upcall, with a faster pure-Rust opt-in

**Applies to:** REGEXP_EXTRACT, UPPER/LOWER, temporal parsing/formatting/arithmetic/casts

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
