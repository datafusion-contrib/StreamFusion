# Recorded scalar workloads: temporal

[Topic index and original links](scalar-functions.md). Sections retain their recorded scope, dates and evidence.

## TO_DATE {#to_date}

`TO_DATE`: `TO_DATE(s)`

| Case | Input | Flink (s) | Native (s) | Flink / native |
|---|---|---:|---:|---:|
| `TO_DATE` | date strings, non-null | 0.427 | 0.529 | 0.81x |
| `TO_DATE` | date strings, NULL/8 | 0.435 | 0.544 | 0.80x |


## QUARTER {#quarter}

`QUARTER`: `QUARTER(ts)`

| Case | Input | Flink (s) | Native (s) | Flink / native |
|---|---|---:|---:|---:|
| `QUARTER` | TIMESTAMP(9), non-null | 0.368 | 0.434 | 0.85x |
| `QUARTER` | TIMESTAMP(9), NULL/8 | 0.378 | 0.460 | 0.82x |


## WEEK {#week}

`WEEK`: `WEEK(ts)`

| Case | Input | Flink (s) | Native (s) | Flink / native |
|---|---|---:|---:|---:|
| `WEEK` | TIMESTAMP(9), non-null | 0.367 | 0.452 | 0.81x |
| `WEEK` | TIMESTAMP(9), NULL/8 | 0.385 | 0.472 | 0.81x |


## DAYOFYEAR {#dayofyear}

`DAYOFYEAR`: `DAYOFYEAR(ts)`

| Case | Input | Flink (s) | Native (s) | Flink / native |
|---|---|---:|---:|---:|
| `DAYOFYEAR` | TIMESTAMP(9), non-null | 0.370 | 0.439 | 0.84x |
| `DAYOFYEAR` | TIMESTAMP(9), NULL/8 | 0.373 | 0.464 | 0.80x |


## DAYOFWEEK {#dayofweek}

`DAYOFWEEK`: `DAYOFWEEK(ts)`

| Case | Input | Flink (s) | Native (s) | Flink / native |
|---|---|---:|---:|---:|
| `DAYOFWEEK` | TIMESTAMP(9), non-null | 0.364 | 0.420 | 0.87x |
| `DAYOFWEEK` | TIMESTAMP(9), NULL/8 | 0.382 | 0.444 | 0.86x |


## Temporal coverage diagnostic (2026-09-15) {#temporal-coverage-diagnostic-2026-09-15}

This run measures the new temporal expression paths on Apple M4 Pro, JDK 17, UTC, Flink 2.2.1
and DataFusion 54.0.0. It uses the release core library with mimalloc, 1,000,000 rows, parallelism 1,
one warmup and three measured trials per engine/case. Cases run serially in one JVM with alternating
engine order; no other tests or benchmarks run concurrently. Medians include SQL planning and
execution. Every plan is checked for `NativeCalc`, `RowDataToArrow`, and `ArrowToRowData`.

That run enabled the former timestamp-range opt-in and used the previous nanosecond layout; it does
not measure the current component layout. Inputs are non-null. Parsing uses
`2000-02-29 12:34:56` and `1969-12-31 23:59:59`; other cases use the existing TIMESTAMP(9)
fixtures, including negative fractional epochs. These cases do not measure dynamic formats, time
zones, or every temporal overload. The 264-byte payload setting is inherited from the general
harness and does not pad these date/time fixtures. Identity controls use the same input sources
and are reported without subtraction.

Reproduce in a fresh JVM:

```sh
TZ=UTC SF_BENCHMARK=true mvn -pl :streamfusion-runtime test -Pbench \
  -Dnative.cargo.packages='-p streamfusion' \
  '-Dnative.cargo.args=build --release --features mimalloc' \
  '-Dtest=ScalarFunctionBenchmark#individualFunctions' \
  -Dscalar.functions=TO_TIMESTAMP,TIMESTAMP_FLOOR,TIMESTAMP_CEIL,TIMESTAMP_ADD,TIMESTAMP_DIFF \
  -Dscalar.rows=1000000 -Dscalar.warmup=1 -Dscalar.runs=3 \
  -Dscalar.output=target/scalar-temporal.csv
```

| Case | Flink (s) | Native (s) | Flink / native |
|---|---:|---:|---:|
| Identity control: timestamp text | 0.328 | 0.549 | 0.598x |
| Identity control: TIMESTAMP(9) | 0.304 | 0.440 | 0.692x |
| `TO_TIMESTAMP(s)` | 1.965 | 2.219 | 0.886x |
| `FLOOR(ts TO MINUTE)` | 0.298 | 0.435 | 0.684x |
| `CEIL(ts TO SECOND)` | 0.300 | 0.428 | 0.701x |
| `TIMESTAMPADD(MONTH, 1, ts)` | 0.350 | 0.584 | 0.600x |
| `TIMESTAMPDIFF(DAY, ts, TIMESTAMP '2024-01-01 00:00:00')` | 0.302 | 0.528 | 0.572x |

All five standalone temporal projections are slower than Flink in this diagnostic. Even the
identity controls pay more for native execution, so these measurements do not isolate kernel or
upcall cost. This is coverage work: temporal expressions can now remain between native operators,
and adjacent temporal calls can share one upcall. A longer pipeline needs its own benchmark before
claiming a throughput improvement. The current results justify no standalone temporal speedup claim.


## FROM_UNIXTIME literal formats {#from_unixtime-literal-formats}

`ScalarFunctionBenchmark` selects `FROM_UNIXTIME_DEFAULT`, `FROM_UNIXTIME_LITERAL` and
`FROM_UNIXTIME_BRIDGE`. The first two use the native fixed-zone formatter; the third reads
`yyyyMMddHHmm` from a runtime pattern column and retains the JVM bridge. All use a nullable
BIGINT source, modern epoch seconds varying over one day, UTC, parallelism 1 and a rowwise
blackhole sink, with both row/Arrow transposes verified in the native plan.

An M4 Pro/JDK 17 release build (`-Pbench`, mimalloc), 2 million rows, NULL every eighth row,
two warmups and five interleaved Flink/native measurements gave:

| Query | Flink seconds | Native seconds | Flink/native |
| --- | ---: | ---: | ---: |
| Identity control | 0.801519 | 0.900607 | 0.890x |
| Default format | 1.542533 | 1.303721 | 1.183x |
| Literal `yyyyMMddHHmm` | 1.245461 | 1.251630 | 0.995x |
| Dynamic pattern, JVM bridge | 1.204726 | 2.209705 | 0.545x |

The default native format is faster in this run; the compact literal is roughly tied with
Flink. The dynamic control additionally carries a pattern column, so it does not isolate
callback cost.

A separate before/after run compared identical literal queries against the previous
implementation at `a4b9902f`, using that revision's release binary and planner with the same
benchmark fixtures. Each revision ran with `-Dscalar.engine=native`, two warmups and five
measured trials, keeping all other settings unchanged:

| Query | Previous JVM bridge seconds | Native formatter seconds | Before/after |
| --- | ---: | ---: | ---: |
| Identity control | 0.655586 | 0.651136 | 1.007x |
| Default format | 1.696912 | 1.003518 | 1.691x |
| Literal `yyyyMMddHHmm` | 1.543752 | 0.958994 | 1.610x |

These are sequential native-only runs, separate from the interleaved Flink comparison above.
The nearly unchanged identity control supports attributing the improvement to removing the
formatting handoff, but the result remains a local end-to-end measurement rather than a
kernel-only speed claim.


## UTC LTZ calendar FLOOR {#utc-ltz-calendar-floor}

The FLOOR qualification snapshot admits direct/literal TIMESTAMP_LTZ to MONTH/YEAR in
fixed zero-offset session zones using the existing calendar kernel. Released Flink uses
the same Julian-date arithmetic there, including signed division, int overflow and the
Gregorian cutover. Regional zones, nonzero offsets, computed inputs and other units retain
their existing routes. CEIL is qualified separately below. Production Rust, JNI, ownership
and configuration are unchanged.

The [result record](results/utc-ltz-floor-2026-11-02.json) and
[4,000 raw trials](results/utc-ltz-floor-2026-11-02.csv.gz) retain stock controls, an independently
recompiled actual preceding encoder at `11e13486`, source/DSO hashes and unfavorable results.
Both paths use the same unchanged release/default-feature DSO and the same expanded
timestamp fixture/executable; this is not relabeled as the preceding historical executable.
On Apple M4 Pro, JDK 17.0.12, one thread, 16 warmups/nine measurements and 131,072 rows/trial,
TIMESTAMP_LTZ(3)/(9), NULL/NOT NULL and GenericRowData/BinaryRowData/Arrow controls consume
`YEAR(FLOOR(v TO MONTH|YEAR))>0 OR CAST(NULL AS BOOLEAN)`. Inputs include full signed
millisecond extrema, pre-epoch fractions, cutover and year boundaries. The timer starts/stops
inside Rust, including intrinsic work and output consumption; initial JNI/import/export and
perimeter transposes are outside this small-function timer. Checksums agree in every trial,
and admitted candidate expressions require empty JVM evaluators.

| Flink | Workload | Cells | Stock/native median range | Previous/native median range | Slower than stock |
| --- | --- | ---: | ---: | ---: | ---: |
| 1.18.1 | bulk | 16 | 1.182-1.551x | 2.286-2.821x | 0 |
| 1.18.1 | tiny control | 4 | 0.193-0.253x | 8.358-8.609x | 4 |
| 2.2.1 | bulk | 16 | 1.224-1.632x | 2.312-3.178x | 0 |
| 2.2.1 | tiny control | 4 | 0.195-0.247x | 8.123-8.654x | 4 |

All 32 bulk medians beat stock and previous. All eight 16-row controls remain slower than
stock. Eight complete million-row source/blackhole configurations retain startup, JNI and
both transposes, at parallelism one with two warmups/five measurements. Seconds are median
(min-max), with independent stock controls retained.

| Flink / inputs | Candidate stock | Native | Previous stock | Previous |
| --- | ---: | ---: | ---: | ---: |
| 1.18.1 / millis-month | 0.188040 (0.186749-0.205863) | 0.221072 (0.214229-0.226698) | 0.193261 (0.185584-0.202123) | 0.245723 (0.235856-0.248278) |
| 1.18.1 / millis-year | 0.195539 (0.186410-0.203306) | 0.220040 (0.211578-0.263018) | 0.180761 (0.175119-0.191061) | 0.238446 (0.236861-0.267134) |
| 1.18.1 / nanos-month | 0.180207 (0.177152-0.188744) | 0.212674 (0.211109-0.219351) | 0.185782 (0.179583-0.188839) | 0.235585 (0.235366-0.246597) |
| 1.18.1 / nanos-year | 0.181047 (0.176542-0.182064) | 0.216663 (0.211255-0.223228) | 0.177336 (0.172870-0.186011) | 0.229550 (0.228119-0.237410) |
| 2.2.1 / millis-month | 0.179602 (0.175361-0.193702) | 0.212927 (0.209005-0.226058) | 0.183578 (0.179895-0.192829) | 0.246426 (0.234361-0.263776) |
| 2.2.1 / millis-year | 0.183784 (0.182589-0.197169) | 0.231608 (0.228845-0.251674) | 0.180244 (0.179069-0.201296) | 0.239005 (0.228666-0.274734) |
| 2.2.1 / nanos-month | 0.185797 (0.180933-0.190350) | 0.218771 (0.216324-0.227968) | 0.189567 (0.181581-0.202551) | 0.239713 (0.235645-0.246484) |
| 2.2.1 / nanos-year | 0.178197 (0.172020-0.185764) | 0.218076 (0.212453-0.226665) | 0.176826 (0.174746-0.191225) | 0.237412 (0.233253-0.262087) |

Native/stock elapsed ratios span **1.125-1.260**: every measured complete job is slower than
stock. Native/previous ratios span **0.864-0.969**. The small-function improvement does not
establish whole-job acceleration, and tiny batches remain an unfavorable workload.

All 68 focused JVM invocations pass across released Flink 1.18.1/2.2.1 and JDK 11/17,
compiled for Java release 11. Complete 5,003-row runtime maps compare values, declared
precision, NULLs, unique IDs/cardinality and full-range inputs across eight session zones;
only normalized fixed-zero zones require empty evaluators. Negative admission controls at
this snapshot cover other units, CEIL and computed inputs. The unchanged released
`ScalarFunctionsTest.testTemporalFloor` stock oracle passes on all four pairs, separately
from native-route witnesses. All 28 fixture smokes pass, including unchanged original-corpus
checksums. The unchanged production DSO reuses the recorded 886 Rust core passes; the
expanded release benchmark executable is rebuilt. Further LTZ scope remains in
[#504](https://github.com/datafusion-contrib/StreamFusion/issues/504).


## UTC LTZ calendar CEIL {#utc-ltz-calendar-ceil}

CEIL/CEILING to MONTH/YEAR admits the same direct/literal LTZ and fixed-zero session-zone
subset, independently of FLOOR qualification. Released Flink drops submillisecond fractions
before its calendar calculation; the existing kernel reproduces this behavior at exact month
boundaries and for negative/full-range milliseconds. Fixed-unit LTZ CEIL is different and
remains outside this admission, together with nonzero/regional zones and computed inputs.
No production Rust, DSO, JNI, ownership or option changes are needed.

The [result record](results/utc-ltz-ceil-2026-11-02.json) and
[4,000 raw trials](results/utc-ltz-ceil-2026-11-02.csv.gz) compare stock with independently
recompiled actual preceding encoder `bc9156b3` and candidate. They use the same timestamp
fixture, production DSO and release/default-feature executable as FLOOR, with
`YEAR(CEIL(v TO MONTH|YEAR))>0 OR CAST(NULL AS BOOLEAN)`. The prepared-input Rust-side
timer, types, signed extrema, NULLs, GenericRowData/BinaryRowData controls, batch sizes,
16 warmups/nine measurements and 131,072 rows/trial are unchanged. Every checksum agrees;
candidate native expressions have empty JVM evaluators. All unfavorable controls and
source/runtime hashes remain public.

| Flink | Workload | Cells | Stock/native median range | Previous/native median range | Slower than stock |
| --- | --- | ---: | ---: | ---: | ---: |
| 1.18.1 | bulk | 16 | 1.177-1.672x | 2.539-4.442x | 0 |
| 1.18.1 | tiny control | 4 | 0.203-0.249x | 8.403-8.773x | 4 |
| 2.2.1 | bulk | 16 | 1.157-1.665x | 2.380-3.261x | 0 |
| 2.2.1 | tiny control | 4 | 0.201-0.258x | 8.495-8.830x | 4 |

All 32 bulk medians beat stock and previous; all eight tiny controls lose to stock. The
eight million-row source/blackhole configurations retain startup, JNI and both transposes,
two warmups/five measurements and independent stock controls. Seconds are median (min-max).

| Flink / inputs | Candidate stock | Native | Previous stock | Previous |
| --- | ---: | ---: | ---: | ---: |
| 1.18.1 / millis-month | 0.186132 (0.178269-0.186337) | 0.208164 (0.206946-0.214929) | 0.182699 (0.181872-0.216983) | 0.243392 (0.241391-0.262012) |
| 1.18.1 / millis-year | 0.188171 (0.175289-0.197888) | 0.219593 (0.209214-0.226519) | 0.185199 (0.178695-0.188247) | 0.250715 (0.231815-0.271578) |
| 1.18.1 / nanos-month | 0.190383 (0.182234-0.201164) | 0.209850 (0.205938-0.228390) | 0.180241 (0.176636-0.205446) | 0.249955 (0.244625-0.276685) |
| 1.18.1 / nanos-year | 0.196046 (0.178773-0.199058) | 0.217153 (0.208887-0.223411) | 0.196122 (0.187562-0.199845) | 0.268520 (0.251645-0.291934) |
| 2.2.1 / millis-month | 0.180832 (0.177707-0.195561) | 0.208945 (0.200662-0.229296) | 0.188054 (0.184059-0.194765) | 0.245933 (0.238274-0.261214) |
| 2.2.1 / millis-year | 0.184349 (0.181639-0.193377) | 0.216672 (0.213815-0.233547) | 0.187517 (0.181014-0.198702) | 0.248477 (0.234889-0.258829) |
| 2.2.1 / nanos-month | 0.188166 (0.177473-0.191057) | 0.215282 (0.212135-0.229300) | 0.196054 (0.183815-0.204254) | 0.244518 (0.239370-0.254048) |
| 2.2.1 / nanos-year | 0.183150 (0.180889-0.203244) | 0.218773 (0.213459-0.236328) | 0.189499 (0.181699-0.192151) | 0.240628 (0.237761-0.265100) |

Native/stock elapsed ratios span **1.102-1.195**; every measured complete job still loses
to stock. Native/previous ratios span **0.809-0.909**. These results qualify bulk function
work and quantify the remaining complete-job cost, without asserting whole-job acceleration.

All 68 new focused JVM invocations pass across released Flink 1.18.1/2.2.1 and JDK 11/17,
compiled for Java release 11. Full 5,003-row maps cover eight zones, NULLs, declared precision,
millisecond extrema and nanoseconds, with consumed intended native work and empty evaluators
only in qualified zones. Other units and computed round operands remain negative admission
controls. The unchanged released temporal FLOOR/CEIL stock oracle is reused from FLOOR's
four passing release/JDK controls; these are not claimed as a second new oracle run.
All 28 fixture smokes pass. The production DSO and its recorded 886 Rust core passes are
unchanged. A further **152 passing JVM invocations** (38 per release/JDK pair) exercise
the final FLOOR/CEIL implementation together with existing calendar/interval controls;
that source/result record accompanies the CEIL artifact. Broader calendar work remains in
[#504](https://github.com/datafusion-contrib/StreamFusion/issues/504).


## BIGINT epoch millisecond conversion {#bigint-epoch-millisecond-conversion}

Direct/literal `TO_TIMESTAMP_LTZ(n,3)` with BIGINT input reuses the existing epoch-millisecond
kernel in ordinary Calc. Released bounds are inclusive -62167219200000..253402300799999;
NULL and out-of-range values stay NULL. The timestamp shares the immutable input value
buffer, uses zero nanoseconds, and places combined input/range validity on its parent.
A packed range mask plus wordwise validity intersection replaces per-row Option handling.
At this qualification, computed ordinary-Calc inputs, seconds and other numeric signatures
retain existing routes; the later seconds qualification extends the direct BIGINT subset.

The initial unchanged kernel's nullable Flink 1.18 IS NULL pilot loses: stock 3.160 versus
native 4.490 ns/row. CPU samples identify conversion, Vec growth/realloc and Vec<bool>
validity copying. Sharing the millisecond buffer reduces a 1,024-row probe from 55 allocation
requests/28,575 requested bytes to 29/6,047, but one isolated pilot still measures 0.990x
stock/native. The final packed-mask pass uses 31 requests/6,231 bytes and measures 1.269
ns/row in that final pilot. These are request counts, not peak memory or bytes copied.
The BOOLEAN output itself owns a new bitmap; timestamp buffer sharing is independently
asserted for sliced inputs after input owners are dropped.

The [result record](results/epoch-millis-2026-11-02.json) preserves the failed initial pilot,
first-pass matrix/jobs, allocation/profile observations and final qualification;
[4,000 final raw trials](results/epoch-millis-2026-11-02.csv.gz) retain all unfavorable controls.
The release/default-feature prepared executable and both core DSOs use **System**, verified
against retained Mach-O allocator symbols; these are not mimalloc-specific measurements.
The actual preceding encoder/runtime at `871f269e` is preserved separately, with its own
DSO/executable hashes; both sides use the same expanded logical BIGINT corpus. Inputs include
signed extrema, both exact conversion boundaries and adjacent invalid values, cutover and
modern dates, NULL/NOT NULL and GenericRowData/BinaryRowData/Arrow. Apple M4 Pro, JDK 17.0.12,
one thread, 16 warmups/nine measurements and 131,072 rows/trial follow the prepared Rust-side
method: production operation and output consumption are timed inside Rust; initial JNI/import/
export and perimeter transposes are excluded. Intrinsic callback costs remain included in
previous controls. Candidate expressions require empty evaluators and every checksum agrees.
Consumers are YEAR/MONTH/SECOND comparisons and IS NULL, each planned independently.

| Flink | Workload | Cells | Stock/native median range | Previous/native median range | Slower than stock |
| --- | --- | ---: | ---: | ---: | ---: |
| 1.18.1 | bulk | 16 | 1.343-3.910x | 5.438-42.181x | 0 |
| 1.18.1 | tiny control | 4 | 0.078-0.117x | 9.571-23.935x | 4 |
| 2.2.1 | bulk | 16 | 1.405-5.014x | 5.860-40.166x | 0 |
| 2.2.1 | tiny control | 4 | 0.111-0.124x | 9.757-24.940x | 4 |

All 32 bulk medians beat stock and previous. All eight 16-row controls remain slower than
stock. Eight complete million-row source/blackhole configurations retain startup, JNI and
both transposes, with two warmups/five measurements and independent stock controls. Seconds
are median (min-max).

| Flink / consumer | Candidate stock | Native | Previous stock | Previous |
| --- | ---: | ---: | ---: | ---: |
| 1.18.1 / year | 0.172933 (0.160338-0.174794) | 0.185525 (0.180171-0.207699) | 0.173175 (0.163452-0.180049) | 0.208390 (0.206822-0.222593) |
| 1.18.1 / month | 0.162458 (0.157122-0.182099) | 0.186845 (0.182849-0.190759) | 0.166398 (0.163162-0.172407) | 0.212426 (0.204017-0.217323) |
| 1.18.1 / second | 0.161529 (0.157477-0.173762) | 0.176890 (0.173200-0.186999) | 0.166245 (0.163281-0.172074) | 0.224440 (0.221124-0.231372) |
| 1.18.1 / null | 0.185503 (0.176530-0.189883) | 0.195437 (0.187705-0.200961) | 0.180688 (0.175215-0.185593) | 0.227803 (0.224631-0.240018) |
| 2.2.1 / year | 0.178724 (0.165996-0.183075) | 0.182469 (0.181150-0.194421) | 0.165492 (0.157553-0.174396) | 0.221700 (0.213897-0.251977) |
| 2.2.1 / month | 0.171128 (0.163341-0.172744) | 0.177046 (0.174130-0.185138) | 0.171559 (0.166280-0.172917) | 0.216314 (0.211774-0.223302) |
| 2.2.1 / second | 0.161398 (0.159844-0.170333) | 0.181007 (0.176693-0.183741) | 0.166190 (0.159351-0.171686) | 0.208888 (0.204120-0.223793) |
| 2.2.1 / null | 0.192193 (0.185861-0.195798) | 0.199313 (0.196734-0.209371) | 0.186166 (0.180513-0.191239) | 0.231576 (0.230858-0.242957) |

Native/stock elapsed ratios span **1.021-1.150**: every measured complete job still loses
to stock. Native/previous ratios span **0.788-0.890**. Bulk function work improves; these
short complete jobs do not establish whole-job acceleration.

All **148 focused JVM invocations** pass across released Flink 1.18.1/2.2.1 and JDK 11/17,
compiled for Java release 11. Complete 5,003-row maps verify values, types, NULLs, unique
IDs/cardinality, both nullabilities, four zones, calendar consumers, typed conditional selection,
all-filtered batches and the then-generated seconds/computed controls. Direct BIGINT seconds
are admitted by the separate qualification below. Admitted
paths require consumed NativeCalc work and empty evaluators. Existing UNNEST temporal
bounds, zones and failure contracts remain covered. All **888 Rust core tests** pass without
default features, zero ignored, including sliced buffer ownership, parent/child NULL contracts,
scalars and empty arrays. The unchanged released `TemporalTypesTest` Shanghai, UTC, boundary
and invalid TO_TIMESTAMP_LTZ methods pass in **16 stock-oracle invocations**, separately
from native-route witnesses. All 28 fixture smokes pass, including old/new original-corpus
checksum controls. Broader conversions remain in
[#503](https://github.com/datafusion-contrib/StreamFusion/issues/503).


## BIGINT epoch second conversion {#bigint-epoch-second-conversion}

Direct/literal `TO_TIMESTAMP_LTZ(n,0)` with BIGINT input reuses the epoch conversion structure.
Both released Flink jars check original seconds against inclusive
`[-62167219200,253402300799]` before multiplying by 1000. Invalid values stay NULL even if
their scaled long would wrap into a valid timestamp. The new millisecond buffer and zero
nanoseconds use the existing component schema; input/range validity stays on the parent.
Numeric conversion is zone independent. Computed ordinary-Calc/UNNEST inputs and other numeric,
string, format or precision signatures keep their existing routes. There is no new switch.

The [record](results/epoch-seconds-2026-11-02.json) and
[4,000 raw trials](results/epoch-seconds-2026-11-02.csv.gz) compare the independently compiled
actual preceding encoder `5d78fd4a` with candidate on one current release/default-feature/System
runtime, allocator, transport and expanded seconds fixture. Both final linked artifacts import
System malloc and have no mimalloc allocator symbols. This isolates the encoder/kernel route
change; it is not a replay of a complete historical DSO. The preceding encoder still binds
its generated evaluator, while candidate requires empty evaluators. All checksums agree.

On Apple M4 Pro, Oracle JDK 17.0.12, one thread, 16 warmups/nine measurements and 131,072
rows/trial, equal GenericRowData/BinaryRowData/Arrow contain exact seconds bounds and adjacent
invalid values, signed extrema and multiplication-overflow controls, cutover/modern/pre-epoch
values and NULL/NOT NULL. Production work/output consumption are timed inside Rust with
intrinsic callback costs retained in the preceding control. Initial JNI/C Data entry/export
and perimeter transposes are excluded only from this prepared-function timer. Independently
planned nullable BOOLEAN consumers are YEAR/MONTH/SECOND comparisons and IS NULL.

| Flink | Workload | Cells | Stock/native median range | Prior encoder/native median range | Slower than stock |
| --- | --- | ---: | ---: | ---: | ---: |
| 1.18.1 | bulk | 16 | 1.342-3.448x | 5.967-39.244x | 0 |
| 1.18.1 | tiny control | 4 | 0.078-0.106x | 9.399-21.834x | 4 |
| 2.2.1 | bulk | 16 | 1.350-4.511x | 5.777-37.158x | 0 |
| 2.2.1 | tiny control | 4 | 0.099-0.110x | 9.369-22.675x | 4 |

All 32 bulk medians beat stock and the preceding encoder; all eight 16-row controls lose
to stock. Complete million-row source/blackhole jobs retain startup, JNI, production conversions
and both transposes, with two warmups/five measurements and independent stock controls.
Seconds are median (min-max).

| Flink / consumer | Candidate stock | Native | Prior-control stock | Prior encoder |
| --- | ---: | ---: | ---: | ---: |
| 1.18.1 / year | 0.178340 (0.170356-0.219641) | 0.184620 (0.178544-0.200105) | 0.162921 (0.161419-0.182398) | 0.213423 (0.206437-0.226048) |
| 1.18.1 / month | 0.172563 (0.166091-0.180995) | 0.188816 (0.184118-0.202078) | 0.185944 (0.168941-0.196921) | 0.220979 (0.212758-0.236827) |
| 1.18.1 / second | 0.169010 (0.163062-0.184871) | 0.185623 (0.175359-0.191390) | 0.162442 (0.157704-0.167774) | 0.216820 (0.208115-0.234500) |
| 1.18.1 / null | 0.178418 (0.169744-0.192398) | 0.203734 (0.197714-0.234369) | 0.176524 (0.171268-0.184801) | 0.230295 (0.224920-0.239758) |
| 2.2.1 / year | 0.178993 (0.168456-0.192806) | 0.191535 (0.186340-0.223990) | 0.173104 (0.168654-0.181658) | 0.216082 (0.213046-0.241996) |
| 2.2.1 / month | 0.181048 (0.166665-0.188743) | 0.183546 (0.177635-0.202007) | 0.171645 (0.162908-0.181974) | 0.223246 (0.208871-0.239910) |
| 2.2.1 / second | 0.159443 (0.155857-0.194190) | 0.184733 (0.170795-0.189403) | 0.162390 (0.159129-0.170958) | 0.210622 (0.208602-0.224377) |
| 2.2.1 / null | 0.194371 (0.178394-0.201297) | 0.201201 (0.196232-0.232690) | 0.191489 (0.185210-0.207430) | 0.229590 (0.226761-0.258798) |

Native/stock elapsed ratios span **1.014-1.159**; all eight measured complete jobs remain
slower than stock. Native/prior-encoder ratios span **0.822-0.886**. These prepared bulk
function wins do not establish complete-job acceleration.

All **212 focused JVM invocations** pass on released Flink 1.18.1/2.2.1 x JDK 11/17 with
Java release 11. Complete 5,003-row maps compare values, resolved types, NULLs, IDs/cardinality,
exact seconds/millisecond bounds, signed extrema, four zones, calendar consumers, typed
conditional selection, all-filtered batches and explicit generated controls for computed/
other-numeric inputs. The intended NativeCalc must consume runtime work and admitted functions
have no JVM evaluator. Existing millisecond and UNNEST coverage stays covered. All **890 Rust
core tests** pass without default features and zero ignored, including parent/child validity,
sliced/scalar/empty inputs and pre-scaling range checks. Sixteen freshly executed unchanged
released TemporalTypesTest stock-oracle invocations and 28 fixture smokes pass separately;
original-corpus controls compare the retained preceding executable with the expanded fixture.
Broad temporal parsing/formatting and other conversions stay in
[#503](https://github.com/datafusion-contrib/StreamFusion/issues/503).

### Retained millisecond controls {#retained-millisecond-controls}

A first seconds implementation passed its bulk gate but added redundant buffer-reference
clones and selected range bounds at runtime. A retained Flink 1.18 millisecond IS NULL pilot
then took 1.274 versus preceding-native 1.151 ns/row, with nonoverlapping measured ranges.
The final implementation moves the scaled buffer directly and specializes fixed bounds by
unit. Both complete seconds qualifications, the first
[4,000 trials](results/epoch-seconds-initial-2026-11-02.csv.gz) and all 800 first/final
retained-millisecond raw trials remain in the result record. These diagnostics compare the
retained actual previous native executable/DSO with candidate; their meaning differs from
the preceding-encoder-only seconds control above.

Final millisecond medians (min-max) in ns/row:

| Flink / consumer | Retained preceding native | Candidate native | Candidate / retained median |
| --- | ---: | ---: | ---: |
| 1.18.1 / year | 7.597 (7.446-7.771) | 7.710 (7.575-9.072) | 1.015 |
| 1.18.1 / null | 1.251 (1.197-1.298) | 1.221 (1.194-1.271) | 0.976 |
| 2.2.1 / year | 7.704 (7.678-7.897) | 7.678 (7.516-7.769) | 0.997 |
| 2.2.1 / null | 1.161 (1.119-1.183) | 1.238 (1.208-1.483) | 1.067 |

The original 1.18 NULL regression is not stable in the final control, but the final 2.2 NULL
pilot is still 6.7% slower than retained native, also with nonoverlapping ranges. The other
controls overlap. These isolated short probes do not establish a universal unchanged-path
speedup or absence of regressions; remaining small-operation overhead stays visible.


## Narrow integer epoch conversion {#narrow-integer-epoch-conversion}

Direct TINYINT/SMALLINT/INT `TO_TIMESTAMP_LTZ(v,0/3)` widens signed input losslessly
with the existing Arrow integer cast, then reuses the qualified BIGINT epoch kernel.
The two released Flink lines use the same long-based conversion. Computed inputs,
non-integer signatures and existing UNNEST restrictions retain their preceding routes.
No production Rust implementation, JNI contract, timestamp schema or switch changes.

The [2026-11-03 record](results/integral-epoch-2026-11-03.json) compares released
Flink 1.18.1/2.2.1 on JDK 17.0.12, Apple M4 Pro, one thread, and release/mimalloc-linked
artifacts. Allocator symbols are checked for both executable and JNI DSO. Stock uses
prepared BinaryRowData; Rust uses the same signed values in prepared Arrow, with and
without NULLs, over all three input widths, precision 0/3 and YEAR/IS NULL consumers.
Each engine processes 131,072 rows per trial, with 16 warmups and nine measurements.
The Rust timer includes widening, allocation, result consumption and intrinsic callbacks;
perimeter import/export is excluded only for this prepared function comparison.

The actual preceding encoder `490f458a` is compiled against the same current runtime,
fixture and allocator. It retains a JVM evaluator. Both its independent stock control
and the candidate's paired stock control are retained separately, avoiding an overwritten
stock baseline. This does not replay an entire historical native library.

| Flink | Bulk cells | Stock / native median | Previous / native median |
| --- | ---: | ---: | ---: |
| 2.2.1 | 24 | 2.313-4.193x | 6.536-61.804x |
| 1.18.1 | 24 | 1.682-3.062x | 6.133-44.228x |

All 48 1,024-row cells improve against both controls. All twelve 16-row controls
lose to stock, with stock/native ratios 0.246-0.277x. Keep these small-batch limits:
replacement of a JVM callback alone is not evidence of a stock-Flink speedup.
[All trials](results/integral-epoch-2026-11-03.csv.gz) include warmups, checksums,
paired/independent stock and prior evaluator measurements.

Complete jobs use the existing INT row source and blackhole sink, one million rows,
UTC, two warmups and five measurements per engine with alternating order. Planning,
startup, production JNI and both transposes remain timed. Candidates require consumed
NativeCalc input and empty evaluators; previous controls require the generated route.
Seconds, median, for the same YEAR/IS NULL consumers:

| Flink | Unit / consumer | Paired stock | Native | Independent stock | Previous evaluator |
| --- | --- | ---: | ---: | ---: | ---: |
| 2.2.1 | 0 / YEAR | 0.168406 | 0.174265 | 0.182519 | 0.217596 |
| 2.2.1 | 0 / NULL | 0.180299 | 0.192963 | 0.177445 | 0.218229 |
| 2.2.1 | 3 / YEAR | 0.169664 | 0.175953 | 0.171438 | 0.206674 |
| 2.2.1 | 3 / NULL | 0.184774 | 0.197030 | 0.183020 | 0.227416 |
| 1.18.1 | 0 / YEAR | 0.169060 | 0.178559 | 0.177995 | 0.204989 |
| 1.18.1 | 0 / NULL | 0.168649 | 0.190953 | 0.168474 | 0.216864 |
| 1.18.1 | 3 / YEAR | 0.172733 | 0.180505 | 0.166589 | 0.205832 |
| 1.18.1 | 3 / NULL | 0.173585 | 0.194221 | 0.173924 | 0.223086 |

All eight complete native medians remain slower than stock (native/stock elapsed
1.035-1.132), while improving over prior generated execution (native/prior elapsed
0.801-0.884). The JSON retains every trial and range. These measurements qualify the
bulk function path; they do not establish complete-job acceleration or timestamp
serialization throughput.

A before-change runtime control matches values but fails six narrow-integer pure-route
assertions with `jvm=[TO_TIMESTAMP_LTZ]`; BIGINT and computed controls pass. The final
44 focused JVM invocations pass on both releases and JDK 11/17, targeting Java release
11, with no skips. Full 5,003-row maps compare values, schema, NULLs and unique IDs;
UTC/regional sessions, signed extrema, both precisions, CASE and all-filtered batches
have runtime work/empty-evaluator assertions. Computed operands retain explicit
generated-route controls. The 892-test Rust core result uses the unchanged production
Rust implementation; the new example metadata checks actual BIGINT result types even
when input is narrow. Full hosted CI is separate.
Sixteen freshly executed, unchanged released `TemporalTypesTest` stock-oracle
invocations pass separately across both Flink lines and JDKs. They confirm released
behavior and do not count as native-runtime coverage.



## UTC LTZ fixed-unit FLOOR {#utc-ltz-fixed-unit-floor}

Direct/literal LTZ FLOOR in a fixed-zero session zone reuses the existing plain-timestamp
fixed-width floor kernel. DAY/HOUR qualify on both released lines; MINUTE/SECOND/MILLISECOND
qualify only on 2.2. Released 1.18 generates invalid Java casts for those sub-hour LTZ forms;
explicit failure controls preserve that behavior. Regional/nonzero zones, computed operands
and fixed-unit LTZ CEIL retain their preceding routes. No production Rust, JNI, timestamp
schema or runtime switch changes.

The [2026-11-03 record](results/utc-ltz-fixed-floor-2026-11-03.json) uses released
Flink 1.18.1/2.2.1, Oracle JDK 17.0.12, Apple M4 Pro, one thread and release/mimalloc-linked
artifacts. Equal prepared GenericRowData/BinaryRowData and Arrow contain signed-long extrema,
normal/pre-epoch values, LTZ(3/9) fractional inputs and nullable/NOT NULL columns. The Rust-side
timer retains production work, output consumption and intrinsic callbacks; initial import/export
and perimeter transposes are excluded only from this prepared comparison. Each trial processes
131,072 rows with 16 warmups and nine measurements. The consumer is
`YEAR(FLOOR(v TO unit))>0 OR CAST(NULL AS BOOLEAN)`, not timestamp output serialization.

The independently compiled actual preceding encoder `db05513e` retains generated FLOOR on
the shared current runtime, fixture and allocator. Candidate paired stock and independent
prior-stock controls remain distinct; this is not a complete historical DSO replay. Symbol
checks confirm both the prepared executable and JNI DSO resolve malloc to mimalloc.

| Flink | Bulk cells | Stock / native median range | Previous / native median range |
| --- | ---: | ---: | ---: |
| 2.2.1 | 40 | 1.099-1.612x | 3.520-4.682x |
| 1.18.1 | 16 | 1.160-1.479x | 3.504-4.551x |

All 56 bulk cells improve against stock and the previous encoder. All fourteen 16-row
controls lose to stock (stock/native 0.146-0.175x); their overhead remains visible.
[All 7,000 raw trials](results/utc-ltz-fixed-floor-2026-11-03.csv.gz) retain warmups,
checksums and both stock controls. Reproduce with the existing `integral_expression`
fixture, TIMESTAMP_LTZ(3/9), the expression above, and each admitted unit.

Ten complete million-row jobs use the existing row source/blackhole fixture, UTC, LTZ(3/9),
two warmups/five measurements and alternating engine order. Startup, JNI and both transposes
remain in the timing. Candidate runs require consumed native Calc work and empty evaluators;
previous runs require generated FLOOR. Seconds, median:

| Flink | Precision / unit | Paired stock | Native | Independent stock | Previous evaluator |
| --- | --- | ---: | ---: | ---: | ---: |
| 2.2.1 | 3 / DAY | 0.176608 | 0.211898 | 0.182302 | 0.241057 |
| 2.2.1 | 3 / HOUR | 0.171679 | 0.204881 | 0.174518 | 0.231262 |
| 2.2.1 | 3 / MILLISECOND | 0.170209 | 0.215291 | 0.170087 | 0.242998 |
| 2.2.1 | 9 / DAY | 0.180133 | 0.205462 | 0.173028 | 0.235460 |
| 2.2.1 | 9 / HOUR | 0.177703 | 0.207438 | 0.179124 | 0.231418 |
| 2.2.1 | 9 / MILLISECOND | 0.174788 | 0.211231 | 0.174412 | 0.238524 |
| 1.18.1 | 3 / DAY | 0.174992 | 0.209729 | 0.178677 | 0.242289 |
| 1.18.1 | 3 / HOUR | 0.177254 | 0.209395 | 0.175687 | 0.231452 |
| 1.18.1 | 9 / DAY | 0.175201 | 0.216809 | 0.178319 | 0.241498 |
| 1.18.1 | 9 / HOUR | 0.184392 | 0.211330 | 0.178049 | 0.258221 |

Native/stock elapsed ratios span **1.141-1.265**;
native/previous ratios span **0.818-0.905**.
These complete-job results remain separate from bulk function qualification. The JSON retains
every trial and range; prepared gains do not establish complete-job acceleration.

A before-change runtime control matches complete values/schema, then fails six zero-offset
pure-route assertions with `jvm=[FLOOR]`. The final 44 focused JVM invocations and 136 adjacent
calendar FLOOR/CEIL/encoder regressions pass across both releases and JDK 11/17, targeting Java
release 11, with no skips. The runtime matrix compares full 5,003-row maps, types, NULLs,
IDs/cardinality, signed-long extrema, fractions, precisions 0/3/9, zero-offset aliases,
nonzero/regional generated controls, CASE and all-filtered batches. Admitted paths require
consumed NativeCalc work and empty evaluator descriptors; computed/CEIL/unsupported-unit
encoder controls remain explicit. Released 1.18's three smaller LTZ units fail independently
under both stock and native-enabled fallback with the same code-generation cause/phase.
Four freshly executed unchanged released `ScalarFunctionsTest.testTemporalFloor` stock-oracle
invocations pass separately, not as native-runtime coverage. The existing 892-test Rust core
result covers the unchanged production Rust; hosted CI remains separate. Remaining temporal
work stays in [#504](https://github.com/datafusion-contrib/StreamFusion/issues/504).



## UTC LTZ fixed-unit CEIL {#utc-ltz-fixed-unit-ceil}

A separate Rust kernel preserves released LTZ signed-remainder CEIL, including ignored
submillisecond fractions and final long overflow. The ordinary TIMESTAMP CEIL kernel stays
unchanged. Direct/literal inputs qualify only in normalized fixed-zero zones, for DAY/HOUR
on both releases and MINUTE/SECOND/MILLISECOND on 2.2. The latter three forms retain the
released 1.18 code-generation failure. Computed inputs, nonzero/regional zones and other
units retain their existing routes. Component and primitive Arrow timestamps are accepted
without changing JNI, timestamp transport or configuration.

The [2026-11-03 record](results/utc-ltz-fixed-ceil-2026-11-03.json) compares released
Flink 1.18.1/2.2.1, Oracle JDK 17.0.12, Apple M4 Pro, one thread and release/mimalloc-linked
artifacts. Both executable and JNI DSO have malloc/mi_malloc symbol identity checks. Prepared
GenericRowData, BinaryRowData and Arrow contain identical signed-long extrema, normal and
pre-epoch values, LTZ(3/9), submillisecond fractions and nullable/NOT NULL columns. Each trial
processes 131,072 rows with 16 warmups and nine measurements. The Rust-side timer retains
production operation, output consumption and intrinsic prior callbacks; initial import/export
and perimeter transposes are excluded only from this function comparison. The consumer is
`YEAR(CEIL(v TO unit))>0 OR CAST(NULL AS BOOLEAN)`, not timestamp output serialization.

Actual preceding encoder `e7b2a8e4` runs generated CEIL on the same current runtime, fixture
and allocator. Its independent stock control remains distinct from candidate paired stock;
this is not an entire historical DSO replay. The prior native artifacts are retained separately
for provenance; this qualification isolates the encoder/kernel route change.

| Flink | Bulk cells | Stock / native median range | Previous / native median range |
| --- | ---: | ---: | ---: |
| 2.2.1 | 40 | 1.208-1.632x | 3.798-7.901x |
| 1.18.1 | 16 | 1.237-1.581x | 3.862-4.969x |

All 56 bulk cells improve against stock and the prior evaluator. All fourteen 16-row
controls lose to stock (stock/native **0.150-0.179x**). The
[7,000 raw trials](results/utc-ltz-fixed-ceil-2026-11-03.csv.gz) retain warmups,
checksums and both stock controls. Reproduce with `integral_expression`, LTZ(3/9),
the expression above and each admitted unit.

Ten complete million-row jobs retain startup, JNI and both transposes, using the existing
row source/blackhole fixture in UTC with LTZ(3/9), two warmups, five measurements and
alternating engine order. Candidate jobs require consumed NativeCalc work and empty
evaluators; prior jobs require generated CEIL. Seconds, median:

| Flink | Precision / unit | Paired stock | Native | Independent stock | Previous evaluator |
| --- | --- | ---: | ---: | ---: | ---: |
| 2.2.1 | 3 / DAY | 0.182005 | 0.200002 | 0.181019 | 0.235959 |
| 2.2.1 | 3 / HOUR | 0.185597 | 0.203753 | 0.172905 | 0.230548 |
| 2.2.1 | 3 / MILLISECOND | 0.178166 | 0.208781 | 0.174501 | 0.227337 |
| 2.2.1 | 9 / DAY | 0.175395 | 0.206331 | 0.177940 | 0.234895 |
| 2.2.1 | 9 / HOUR | 0.180320 | 0.203949 | 0.181615 | 0.237350 |
| 2.2.1 | 9 / MILLISECOND | 0.170617 | 0.206994 | 0.173575 | 0.235976 |
| 1.18.1 | 3 / DAY | 0.173835 | 0.206550 | 0.184450 | 0.245386 |
| 1.18.1 | 3 / HOUR | 0.174190 | 0.202289 | 0.181930 | 0.233150 |
| 1.18.1 | 9 / DAY | 0.185248 | 0.206611 | 0.173145 | 0.227567 |
| 1.18.1 | 9 / HOUR | 0.173767 | 0.207287 | 0.178209 | 0.232716 |

All ten complete native medians remain slower than stock: native/stock elapsed
**1.098-1.213**, native/previous **0.842-0.918**.
Every trial, range and independent control is retained. The prepared function gate does
not establish complete-job acceleration.

All **896 release Rust core tests** pass without default features, zero ignores. New tests
exercise full signed millis, fractions, final-wrap overflow, distinct ordinary-TIMESTAMP
behavior, primitive/component timestamps without coercion, scalar broadcasts, slices,
NULL widths, empty/all-NULL arrays, ownership and rejected signatures. All **268 focused
JVM invocations** pass on both released lines and JDK 11/17, targeting Java release 11,
with no skips. Complete 5,003-row maps compare values/types/NULLs/IDs/cardinality, precisions
0/3/9, zero-offset aliases, nonzero/regional generated controls, CASE and all-filtered batches.
Admitted paths require consumed NativeCalc work and empty evaluators; computed/unsupported
shapes and 1.18 code-generation failures retain explicit expectations. Adjacent fixed/calendar
FLOOR/CEIL and narrow integer epoch routes are included. The actual before-change control
matches values/schema but fails six zero-offset pure-route assertions with `jvm=[CEIL]`.
Four fresh unchanged released `ScalarFunctionsTest.testTemporalFloor` oracle invocations
pass separately; that upstream method contains both FLOOR and CEIL assertions and does not
provide native-runtime coverage. Hosted CI remains separate. Further calendar/timezone work
stays in [#504](https://github.com/datafusion-contrib/StreamFusion/issues/504).



## Computed integer epoch conversion {#computed-integer-epoch-conversion}

`TO_TIMESTAMP_LTZ(...,0/3)` now reuses the existing exact epoch kernels for recursively
verified integer arithmetic, lossless widening and rounding trees in ordinary Calc.
The child retains its resolved integer width before any widening, preserving intermediate
wrap and the released epoch bounds. Division/remainder, narrowing, unknown/stateful producers,
other signatures and existing UNNEST restrictions retain their preceding routes. No native
implementation, JNI, transport, dependency or configuration changes.

The [2026-11-03 record](results/computed-integral-epoch-2026-11-03.json) compares released
Flink 1.18.1/2.2.1 on Oracle JDK 17.0.12, Apple M4 Pro, one thread, with release/mimalloc-linked
artifacts. The executable and DSO retain verified malloc/mi_malloc identity. Equal prepared
GenericRowData, BinaryRowData and Arrow contain nullable/NOT NULL TINYINT/SMALLINT/INT/BIGINT,
signed extrema and released epoch boundaries. Each trial consumes 131,072 rows, with 16 warmups
and nine measurements. Rust times production operation and equivalent result consumption after
import and before export, including the prior evaluator's intrinsic callbacks. Initial JNI/import/
export and perimeter transposes are excluded only from this prepared function comparison.

The add consumer is `YEAR(TO_TIMESTAMP_LTZ(v+CAST(1 AS $TYPE),unit))`; composed controls use
`TO_TIMESTAMP_LTZ(ROUND((v+CAST(1 AS $TYPE))*v,-2),unit) IS NULL`, at precision 0 and 3.
This is not a timestamp output serialization benchmark. Actual preceding encoder `998df4fd`
runs against the shared current runtime, fixture and allocator; it is not a whole historical
DSO replay. Its independent stock control remains distinct from candidate paired stock.

| Flink | Bulk cells | Stock / native median range | Previous / native median range |
| --- | ---: | ---: | ---: |
| 2.2.1 | 40 | 1.683-9.329x | 5.203-15.737x |
| 1.18.1 | 40 | 1.584-5.455x | 5.411-14.469x |

All 80 bulk cells improve against stock and previous. All sixteen 16-row controls lose to
stock (stock/native **0.146-0.178x**).
The [9,600 raw qualification trials](results/computed-integral-epoch-2026-11-03.csv.gz)
retain warmups, checksums and independent controls. The separate
[800 initial pilot trials](results/computed-integral-epoch-pilot-2026-11-03.csv.gz)
are also retained, with their summaries in the result record. Reproduce with
`integral_expression`, the types and expressions above, and matching epoch-seconds/millis corpora.

Twelve complete million-row jobs use the existing row source/blackhole fixture in UTC,
with two warmups, five measurements and alternating engine order. They retain startup,
JNI and both transposes. Candidate requires consumed NativeCalc work and empty evaluators;
prior requires generated epoch conversion. Seconds, median:

| Flink / input / precision / shape | Paired stock | Native | Independent stock | Previous evaluator |
| --- | ---: | ---: | ---: | ---: |
| 2.2.1 / INT-0-add | 0.170323 | 0.173181 | 0.175443 | 0.207233 |
| 2.2.1 / INT-3-add | 0.176385 | 0.176725 | 0.171943 | 0.207876 |
| 2.2.1 / BIGINT-0-add | 0.166357 | 0.184379 | 0.164921 | 0.213727 |
| 2.2.1 / BIGINT-3-add | 0.165382 | 0.176141 | 0.167736 | 0.221302 |
| 2.2.1 / BIGINT-0-round-product | 0.195209 | 0.193531 | 0.205385 | 0.227742 |
| 2.2.1 / BIGINT-3-round-product | 0.199279 | 0.194703 | 0.206845 | 0.229284 |
| 1.18.1 / INT-0-add | 0.165017 | 0.175784 | 0.172271 | 0.207979 |
| 1.18.1 / INT-3-add | 0.174836 | 0.175597 | 0.174498 | 0.205013 |
| 1.18.1 / BIGINT-0-add | 0.175806 | 0.179489 | 0.173366 | 0.206188 |
| 1.18.1 / BIGINT-3-add | 0.165088 | 0.184378 | 0.170795 | 0.214139 |
| 1.18.1 / BIGINT-0-round-product | 0.196274 | 0.196922 | 0.196744 | 0.221564 |
| 1.18.1 / BIGINT-3-round-product | 0.191578 | 0.192440 | 0.198439 | 0.222426 |

Native/stock elapsed spans **0.977-1.117**; native/previous spans
**0.796-0.889**. Every trial and variability range is retained;
the prepared function gate does not establish complete-job acceleration.

All **380 focused JVM invocations** pass without skips across Flink 1.18.1/2.2.1 and JDK
11/17, targeting Java release 11. Complete 5,003-row maps compare values, resolved types,
NULLs, IDs and cardinality for arithmetic overflow, epoch bounds and adjacent values,
rounding and widening, nullable/NOT NULL inputs, CASE and all-filtered batches. Admitted
paths require consumed NativeCalc work and empty evaluators. Narrowing, division, failing
lazy/empty producers and recurring-DST extraction retain explicit generated controls.
Adjacent direct epochs, UNNEST and fixed-unit LTZ rounding are included. The actual before
control matches values/schema then fails all eight new pure-route assertions with
`jvm=[TO_TIMESTAMP_LTZ]`; its unverified-producer control passes. Sixteen fresh unchanged
released `TemporalTypesTest` oracle invocations pass separately, providing stock semantics,
not native-runtime evidence. No Rust implementation changes from the preceding CEIL commit,
which passed 896 release core tests without ignores. Hosted CI remains separate. Broader
parsing/conversion work stays in [#503](https://github.com/datafusion-contrib/StreamFusion/issues/503).



## Runtime integer epoch precision {#runtime-integer-epoch-precision}

Ordinary Calc admits verified INT CASE precision trees with literal 0, 3 or NULL results
for integer TO_TIMESTAMP_LTZ. The native kernel reads conditions/branches directly, checks
original seconds/milliseconds before scaling, and writes one timestamp column. Literal
millisecond-or-NULL branches share immutable input values and alter parent validity only.
Nested branches reuse the total integer selector. Computed integer children retain their
resolved widths before widening; unproven/failing/stateful inputs and UNNEST keep their routes.

The [record](results/runtime-epoch-precision-2026-11-04.json) retains all 80 final configurations and
[19,800 raw samples](results/runtime-epoch-precision-2026-11-04.csv.gz) across all development stages and the final matrix.
Apple M4 Pro, macOS arm64, JDK 17.0.12, one thread, release/mimalloc, Flink 1.18.1/2.2.1.
Matched prepared GenericRowData/BinaryRowData and Arrow cover all four integer widths,
nullable/NOT NULL inputs, extrema and epoch bounds. Each trial consumes 131,072 rows;
16 warmups/nine measurements alternate engines. Native timing starts inside Rust after
import and stops before export, retaining production Calc/consumption and intrinsic costs.
Stock starts on equivalent prepared RowData. Actual preceding encoder `a90183ec` retains
its evaluator on the same current native runtime/fixture, with independent stock controls.

The two measured shapes consume YEAR over seconds/milliseconds selected by the sign of v,
or IS NULL over millisecond/NULL selection. SQL strings, all medians/ranges and artifacts
are retained in the record. The timings do not qualify every nested composition.

| Scope | Cells | Stock / native | Actual previous / native | Independent stock / native |
| --- | ---: | ---: | ---: | ---: |
| Bulk, 1,024 rows | 64 | 1.145-4.399x | 6.227-38.014x | 1.372-5.028x |
| Tiny, 16 rows | 16 | 0.042-0.197x | 10.826-17.692x | 0.044-0.161x |

Retain 0 paired-stock bulk medians below 1x,
0 independent-stock bulk medians below 1x and
16 tiny paired-stock losses. These are measured diagnostics;
native execution evidence alone does not establish a speedup.

The first timestamp-CASE implementation loses despite correctness. CPU samples identify
CASE batch partitioning and Struct timestamp merging, including child/null-buffer copies.
A first fused kernel removes that merging but still materializes an INT precision column;
another sample identifies selector/materialization work. Direct branch selection removes
that intermediate, and millisecond-or-NULL buffer sharing removes the remaining value copy.
The shared-buffer version still loses bulk cases. Packed Arrow condition/validity masks
replace per-row selection for scalar precision leaves, retain first-true CASE selection and
scale only selected in-range seconds. Final masks exclude NULL predicates. All development
stages, actual source/artifact hashes and slower samples are retained rather
than replaced by the final matrix. Inclusive sample counts overlap; they are not wall-time shares.

Eight complete million-row configurations retain the bounded row source, blackhole sink,
startup, JNI and both transposes. Two warmups/five measurements alternate stock/native;
candidate requires consumed NativeCalc work and empty evaluators, previous keeps its evaluator.
Median seconds:

| Flink / input / shape | Paired stock | Native | Independent stock | Actual previous |
| --- | ---: | ---: | ---: | ---: |
| 2.2.1 / INT-seconds-millis | 0.176752 | 0.177871 | 0.172852 | 0.212247 |
| 2.2.1 / INT-millis-null | 0.177497 | 0.188569 | 0.183063 | 0.234874 |
| 2.2.1 / BIGINT-seconds-millis | 0.169195 | 0.183573 | 0.162459 | 0.218152 |
| 2.2.1 / BIGINT-millis-null | 0.181030 | 0.190784 | 0.182086 | 0.236146 |
| 1.18.1 / INT-seconds-millis | 0.166785 | 0.178363 | 0.170580 | 0.206144 |
| 1.18.1 / INT-millis-null | 0.178303 | 0.187838 | 0.175389 | 0.233447 |
| 1.18.1 / BIGINT-seconds-millis | 0.159494 | 0.179954 | 0.165135 | 0.219383 |
| 1.18.1 / BIGINT-millis-null | 0.179089 | 0.191463 | 0.177121 | 0.223125 |

Native/stock elapsed ranges **1.006-1.128**, retaining
8 stock losses; native/previous ranges
**0.803-0.865**, retaining 0 previous losses.
No universal complete-job gain is claimed.

All **432 final JVM invocations** pass without skips across released Flink 1.18.1/2.2.1
and JDK 11/17, Java release 11: 36 new precision invocations, 136 preceding epoch/rounding
controls and 260 preceding decimal/integer-character controls. Complete 5,003-row maps/schema
cover six precision trees, nested/NULL conditions, all integer widths, resolved overflow,
original epoch bounds, UTC/New York and empty filtered batches. New pure routes require
consumed native work, no fallback and empty evaluator descriptors. Fallible/unknown precision
and epoch producers retain explicit generated controls. Recorded preceding-encoder controls compare all 32
first-query maps/schema before the expected new route assertion fails; their subsequent
empty-query comparisons are not reached. Four retained-route methods pass six controls each.

All **910 release Rust core tests** pass without ignores. New tests cover original-unit
bounds, pre-scaling overflow, independently sliced validity, scalar/array combinations,
first-true/NULL conditions, independently sliced conditions across 64-bit boundaries, empty
arrays, invalid internal signatures and immutable buffer
ownership. Sixteen unchanged released TemporalTypesTest method invocations pass separately;
these stock oracles do not replace the independent native SQL/evidence witnesses.

Reproduce with release integral_expression, SF_INTEGRAL_CORPUS=epoch-millis and the exact
expression/type/NULL/layout/batch from the record. Actual previous supplies the frozen
encoder and SF_INTEGRAL_PREVIOUS=true on the same final native runtime. Complete jobs use
IntegralExpressionPipelineBenchmark with matching expression and INT/BIGINT input settings.
Remaining signatures and timezone/parser work stay in
[#503](https://github.com/datafusion-contrib/StreamFusion/issues/503).



## UTC FLOOR over integer epochs {#utc-floor-over-integer-epochs}

`FLOOR(TO_TIMESTAMP_LTZ(n,p) TO unit)` composes the existing Rust integer, epoch
and rounding kernels in fixed-zero session zones, after proving the genuine built-in
conversion and total integer/precision producers. YEAR/MONTH/DAY/HOUR qualify on both
released lines; smaller verified units remain 2.2-only. Other zones, fallible/unverified
inputs and invalid precision keep their correct preceding routes. See the
[coverage contract](../operators/temporal-functions.md#native-utc-floor-over-integer-epochs).
The fixed-unit FLOOR kernel now writes directly to the existing capacity-sized timestamp
builder. This removes a temporary result vector and repeated growth/splitting, while
retaining Flink arithmetic and the existing JNI/transport/configuration contracts.

The final matrix passes **348 fresh JVM invocations** with zero skips across
released Flink 1.18.1/2.2.1 and JDK 11/17, targeting Java 11. Full 5,003-row maps/schema
cover all signed integer widths, NULLs, epoch limits, wrapping arithmetic, runtime 0/3/NULL
precision, YEAR/NULL consumers, three zero-zone aliases and all-filtered batches. Direct
LTZ FLOOR/CEIL, nanoseconds and released unsupported sub-hour forms are revalidated.
Catalog functions and fallible/unknown producers retain their existing contracts. Four
unchanged released `testTemporalFloor` stock-oracle invocations pass separately.
Actual preceding encoder `b6cccbf1` matches each positive invocation's
first complete map/schema before the intended pure-route assertion fails. Subsequent
queries in those failing invocations are not reached. Admitted SQL consumes NativeCalc
with no fallback and empty evaluators.

The rebuilt release runtime passes **914 fresh Rust core tests**, including sliced parent
validity, independent NULL operands, empty results and fractional rounding in the new
builder regression. The preceding runtime retains its separate archived 912-test qualification. Measured and validated encoder,
build-selected compatibility and fixture hashes are recorded. Setup additionally requires
FLOOR in candidate native descriptors and previous JVM descriptors, preventing the
outer YEAR consumer from disguising a generated or removed round operation.

Prepared timing starts stock on equal prebuilt GenericRowData/BinaryRowData and native
inside Rust on equal Arrow batches. It retains production epoch, rounding and YEAR,
NULL handling, result consumption, allocation and intrinsic conversions/callbacks; initial
JNI/import/export and perimeter transposes remain outside this small-function timer.
One thread, 131,072 rows/trial, 16 warmups and nine measurements, both NULL modes, batches
1,024 and 16. Four shapes consume seconds/YEAR, milliseconds/MONTH, INT milliseconds/DAY,
and runtime CASE precision/HOUR through YEAR. Epoch corpora include released range limits,
invalid values and signed extrema; INT uses the identical signed-width generator in both
runtimes. Candidate and actual preceding encoders use identical fixtures/TableConfig and
their recorded release native artifacts; stock is independently measured in both JVMs.

| Prepared controls | Cells | Stock/native throughput | Previous/native throughput |
| --- | ---: | ---: | ---: |
| 1,024 rows/batch | 32 | 1.017-1.985x | 2.552-5.131x |
| 16 rows/batch | 8 | 0.112-0.321x | 8.137-11.642x |

All 32 final bulk medians beat stock and previous. The two smallest paired gains are
only about 2%, with overlapping ranges; they do not establish a stable speedup. **8/8
tiny controls lose to stock**; they remain in the record. Independent stock medians and
min/max ranges remain visible rather than being replaced with paired controls.

Complete jobs use a bounded million-row source and blackhole sink, one thread, two
warmups and five alternating measured trials. Submission/startup, JNI and both perimeter
transposes stay inside the job timer; surrounding JVM startup is outside. Six configurations
span the three BIGINT shapes on both releases. Values are median elapsed seconds:

| Release-shape | Paired stock | Native | Independent stock | Previous |
| --- | ---: | ---: | ---: | ---: |
| 22-seconds-year | 0.174415 | 0.194423 | 0.174084 | 0.235397 |
| 22-millis-month | 0.179176 | 0.188011 | 0.174567 | 0.222483 |
| 22-runtime-hour | 0.171393 | 0.188866 | 0.168582 | 0.221171 |
| 118-seconds-year | 0.181120 | 0.192063 | 0.178531 | 0.216032 |
| 118-millis-month | 0.177334 | 0.194292 | 0.180732 | 0.225815 |
| 118-runtime-hour | 0.174516 | 0.188233 | 0.173405 | 0.216282 |

Native/stock elapsed is **1.049-1.115**,
with 6/6 paired medians slower than stock; native/previous is
**0.826-0.889**, with
0/6 slower than previous. 6/6 independent
stock medians also beat native. Overlapping ranges and slower complete jobs remain
limitations; prepared bulk gains do not establish universal pipeline acceleration.

The initial 32 bulk cells include one nullable runtime-precision/HOUR cell at **0.978x**
stock. A five-second process sample locates `timestamp_array` and vector growth under
TemporalRound (272 sampled inclusive frames); counts are not elapsed percentages. The
capacity-sized builder follows the same Arrow construction pattern as Comet's temporal
casts, reusing StreamFusion's existing two-component builder rather than Spark semantics.
Builder-only reduces a representative allocation probe from 122/86,917 bytes to 99/48,253
bytes, but still has slower bulk cells (0.871x on one 1.18 control). Disassembly identifies
a runtime `sdiv` inside the per-row loop. The final scalar-width specialization makes the
five supported widths constant to the compiler and avoids materializing width/direction
arrays; arbitrary widths and NULL arguments retain the generic path. This retains all
released signed-overflow and nanosecond rules.
All five specialized implementations have no integer division instruction in the
release binary. The same final allocation probe is 92 requests and 39,541 bytes.
Both initial stages, their twelve complete jobs and source/artifact provenance remain in
the record alongside the final optimization, including every slower result.

All **12,000 prepared raw samples**, allocation probes, complete-job trials,
independent controls, source/bytecode/artifact hashes and exact expressions are in the
[portable JSON](results/epoch-ltz-floor-2026-11-04.json) and
[compressed raw CSV](results/epoch-ltz-floor-2026-11-04.csv.gz). The existing prepared
`integral_expression` fixture selects these shapes through `SF_INTEGRAL_EXPRESSION`,
`SF_INTEGRAL_CORPUS=epoch-seconds|epoch-millis` and
`SF_INTEGRAL_REQUIRED_FUNCTION=FLOOR`; the complete-job fixture uses its corresponding
epoch input properties. The record identifies the actual preceding encoder; an arbitrary
older main/runtime is not an equivalent previous control. Full hosted checks qualify the
exact pushed head separately. Broad [#504](https://github.com/datafusion-contrib/StreamFusion/issues/504)
remains open for other producers/zones, units and recurring-DST extraction.


## UTC CEIL over integer epochs {#utc-ceil-over-integer-epochs}

`CEIL(TO_TIMESTAMP_LTZ(n,p) TO unit)` composes the existing Rust integer, epoch
and rounding kernels in fixed-zero session zones, after proving the genuine built-in
conversion and total integer/precision producers. YEAR/MONTH/DAY/HOUR qualify on both
released lines; smaller verified units remain 2.2-only. Other zones, fallible/unverified
inputs and invalid precision keep their correct preceding routes. See the
[coverage contract](../operators/temporal-functions.md#native-utc-ceil-over-integer-epochs).
The verified numeric epoch bound and zero fractions make plain-timestamp fixed-unit CEIL
exact here, so this extension reuses the existing rounding operation. General LTZ
inputs retain their dedicated signed-remainder/overflow path. The fixed component loop
now maps millisecond values with Arrow unary and reuses parent validity, avoiding per-row
NULL rebuilding and fractional-buffer appends. No opcode, JNI, transport or configuration change.

The final matrix passes **388 fresh JVM invocations** with zero skips across
released Flink 1.18.1/2.2.1 and JDK 11/17, targeting Java 11. Full 5,003-row maps/schema
cover all signed integer widths, NULLs, epoch limits, wrapping arithmetic, runtime 0/3/NULL
precision, YEAR/NULL consumers, three zero-zone aliases and all-filtered batches. Direct/plain
TIMESTAMP and LTZ FLOOR/CEIL, both CEIL aliases, nanoseconds and released unsupported sub-hour forms are revalidated.
Catalog functions and fallible/unknown producers retain their existing contracts. Four
unchanged released `testTemporalFloor` (including CEIL) stock-oracle invocations pass separately.
Actual preceding encoder `0e4ed571` matches each positive invocation's
first complete map/schema before the intended pure-route assertion fails. Subsequent
queries in those failing invocations are not reached. Candidate SQL consumes NativeCalc
with no fallback and empty evaluators.

The rebuilt release runtime passes **915 fresh Rust core tests**, including shared sliced
parent validity and zero fractions. The actual preceding runtime retains its archived
914-test qualification; none of its historical tests are relabeled as fresh. Measured and validated encoder,
build-selected compatibility and fixture hashes are recorded. Setup additionally requires
CEIL in candidate native descriptors and previous JVM descriptors, preventing the
outer YEAR consumer from disguising a generated or removed round operation.

Prepared timing starts stock on equal prebuilt GenericRowData/BinaryRowData and native
inside Rust on equal Arrow batches. It retains production epoch, rounding and YEAR,
NULL handling, result consumption, allocation and intrinsic conversions/callbacks; initial
JNI/import/export and perimeter transposes remain outside this small-function timer.
One thread, 131,072 rows/trial, 16 warmups and nine measurements, both NULL modes, batches
1,024 and 16. Four shapes consume seconds/YEAR, milliseconds/MONTH, INT milliseconds/DAY,
and runtime CASE precision/HOUR through YEAR. Epoch corpora include released range limits,
invalid values and signed extrema; INT uses the identical signed-width generator in both
runtimes. Candidate and actual preceding encoders use identical fixtures/TableConfig and
their recorded release native artifacts; stock is independently measured in both JVMs.

| Prepared controls | Cells | Stock/native throughput | Previous/native throughput |
| --- | ---: | ---: | ---: |
| 1,024 rows/batch | 32 | 1.085-3.083x | 2.614-8.457x |
| 16 rows/batch | 8 | 0.122-0.321x | 9.247-12.703x |

All 32 final bulk medians beat stock and previous. **8/8
tiny controls lose to stock**; they remain in the record. Independent stock medians and
min/max ranges remain visible rather than being replaced with paired controls.

Complete jobs use a bounded million-row source and blackhole sink, one thread, two
warmups and five alternating measured trials. Submission/startup, JNI and both perimeter
transposes stay inside the job timer; surrounding JVM startup is outside. Six configurations
span the three BIGINT shapes on both releases. Values are median elapsed seconds:

| Release-shape | Paired stock | Native | Independent stock | Previous |
| --- | ---: | ---: | ---: | ---: |
| 22-seconds-year | 0.177023 | 0.188216 | 0.175292 | 0.234545 |
| 22-millis-month | 0.182372 | 0.196069 | 0.180223 | 0.225036 |
| 22-runtime-hour | 0.168022 | 0.180388 | 0.168876 | 0.223127 |
| 118-seconds-year | 0.173584 | 0.192806 | 0.176364 | 0.223084 |
| 118-millis-month | 0.177507 | 0.200562 | 0.178427 | 0.221420 |
| 118-runtime-hour | 0.163780 | 0.183824 | 0.168138 | 0.218249 |

Native/stock elapsed is **1.063-1.130**,
with 6/6 paired medians slower than stock; native/previous is
**0.802-0.906**, with
0/6 slower than previous. 6/6 independent
stock medians also beat native. Overlapping ranges and slower complete jobs remain
limitations; prepared bulk gains do not establish universal pipeline acceleration.

The initial candidate includes a nullable runtime-precision/HOUR bulk cell at **0.943x**
stock. A five-second native stack sample finds 1,886 inclusive TemporalRound frames, with
per-row append_nulls/append_value and validity/timestamp reads below the fixed loop. Counts
are sampled frames, not elapsed percentages. Following Comet's timestamp-extraction unary
pattern, the final fixed component loop maps milliseconds and reuses sliced parent validity.
Flink wrapping and fraction rules remain unchanged. Initial trials, all six initial complete
jobs, and their source/artifact provenance remain in the record. The representative
runtime-precision/HOUR allocation probe falls from 92 requests/39,541 bytes to 88/39,165 bytes.

All **8,000 prepared raw samples**, allocation probes, complete-job trials,
independent controls, source/bytecode/artifact hashes and exact expressions are in the
[portable JSON](results/epoch-ltz-ceil-2026-11-04.json) and
[compressed raw CSV](results/epoch-ltz-ceil-2026-11-04.csv.gz). The existing prepared
`integral_expression` fixture selects these shapes through `SF_INTEGRAL_EXPRESSION`,
`SF_INTEGRAL_CORPUS=epoch-seconds|epoch-millis` and
`SF_INTEGRAL_REQUIRED_FUNCTION=CEIL`; the complete-job fixture uses its corresponding
epoch input properties. The record identifies the actual preceding encoder; an arbitrary
older main/runtime is not an equivalent previous control. Full hosted checks qualify the
exact pushed head separately. Broad [#504](https://github.com/datafusion-contrib/StreamFusion/issues/504)
remains open for other producers/zones, units and recurring-DST extraction.



## DECIMAL epoch conversion {#decimal-epoch-conversion}

Genuine ordinary-Calc `TO_TIMESTAMP_LTZ(d,0)` / `(d,3)` over direct DECIMAL
columns/literals executes in Rust. Released Flink 1.18.1/2.2.1 first truncates
toward zero, wraps `BigDecimal.longValue()`, wraps seconds times 1,000, then checks
the final millisecond range. The integer overload's original-unit bounds do not
apply. NULLs, pre-epoch fractions, resolved TIMESTAMP_LTZ(3), session-zone behavior,
empty/lazy branches and catalog function identity are preserved. Computed decimal
producers, runtime precision, UNNEST and other signatures keep their preceding routes.

Arrow unary truncates directly into Int64 milliseconds, using compact 64-bit or
128-bit division as required. The existing epoch kernel builds timestamp components
with strict range/NULL parent validity and immutable child buffers. No new callback,
temporal transport or configuration switch is introduced.

All 580 fresh focused JVM invocations pass on both released Flink
lines and JDK 11/17, targeting Java 11. All 925 release Rust core tests pass, including
an independent wide-integer oracle for every precision/scale, sliced/scalar/NULL/empty
and wrap/range controls. The complete current Java core is freshly compiled (373/375
sources); expected native work must be consumed and admitted routes have empty
evaluator descriptors. The actual preceding encoder/runtime returns the same complete
5,003-row first-query maps/schema before failing 86 expected pure-route assertions per
release/JDK pair, while three retained controls pass. Subsequent SQL in those failing
invocations is not counted as preceding parity evidence. Sixteen unchanged released
temporal method oracles pass separately. The earlier released-utility algorithm oracle
has 806,336 matches; it is algorithm evidence, not native execution evidence.

Prepared and complete-job timings use JDK 17. Prepared inputs match Generic/Binary RowData and Arrow over DECIMAL(18,0), (18,3),
(19,0), (19,19), (38,0), (38,18) and (38,38), seconds/milliseconds, both NULL modes,
1,024/16-row batches and signed-fraction/range/low-64-bit wrapping values. Each trial
processes 131,072 rows, with 16 warmups and nine measurements. Timers start inside Rust
after input setup/import and stop before export, retaining production Calc, allocation,
conversion and equivalent output consumption. Only this small-function gate excludes
perimeter JNI/C Data entry and transposes. Preceding measurement uses its actual
JVM evaluator inside NativeCalc; it is not relabeled as stock fallback. Both processes
retain independent stock controls; process order reverses on 1.18.

All 112 prepared bulk cells beat stock 1.965-12.441x
and actual preceding behavior 11.439-29.473x.
23/28 tiny controls lose to stock
(stock/native 0.218-1.351x), and
0/28 lose to previous. These losses remain;
no universal tiny-batch speedup is claimed.

Eight complete million-row job configurations retain submission/startup, JNI and both
row/Arrow transposes, one thread, two warmups and five alternating measurements per
engine. Native/stock elapsed ranges 0.934-1.165;
native/previous ranges 0.649-0.880.
6/8 complete medians lose to the paired stock control,
5/8 lose to independent stock and
0/8 lose to previous. All complete-job losses and ranges
remain; a prepared bulk gain is not a whole-job acceleration claim.

| Flink/DECIMAL/unit | Stock seconds | Native seconds | Independent stock seconds | Previous seconds |
|---|---:|---:|---:|---:|
| 22-p18s0-u0 | 0.191690 | 0.198154 | 0.189804 | 0.235846 |
| 22-p18s3-u3 | 0.195445 | 0.196903 | 0.198312 | 0.240912 |
| 22-p38s0-u0 | 0.210570 | 0.224670 | 0.200114 | 0.259989 |
| 22-p38s18-u3 | 0.241333 | 0.235233 | 0.248650 | 0.362492 |
| 118-p18s0-u0 | 0.196552 | 0.197247 | 0.192116 | 0.224039 |
| 118-p18s3-u3 | 0.193650 | 0.197908 | 0.193933 | 0.232247 |
| 118-p38s0-u0 | 0.196993 | 0.229459 | 0.208220 | 0.271942 |
| 118-p38s18-u3 | 0.246362 | 0.230224 | 0.241323 | 0.348089 |

The [machine-readable qualification](results/decimal-epoch-2026-11-05.json) and
[all 14,800 raw prepared samples](results/decimal-epoch-2026-11-05.csv.gz) retain
pilot/final medians, full trials, independent controls, source/fixture/encoder bytecodes,
complete current-core compilation, exact preceding/candidate release artifacts and
allocator aliases. Historical qualifications keep their original provenance.
Hosted CI must qualify the exact final head separately.



## DECIMAL unary epoch producers {#decimal-unary-epoch-producers}

Ordinary-Calc `TO_TIMESTAMP_LTZ(...,0/3)` also accepts total same-operator
DECIMAL ABS/negation chains and widening casts that keep the scale. The existing
unary/cast kernels compose with the independently qualified decimal epoch kernel:
truncate toward zero, wrap low signed 64 bits, wrap seconds multiplication, then
check final millisecond bounds. NULLs, resolved types, regional display, lazy branches
and empty batches remain exact. Mixed unary trees, arithmetic, narrowing/scale-changing
casts, runtime precision and UNNEST keep their previous routes.

All 624 focused JVM invocations pass on released Flink 1.18.1/2.2.1
and JDK 11/17, targeting Java 11. Full 5,003-row maps/schema cover signed fractions,
precision/scale extremes, low-64-bit wrapping, final bounds, NULLs, nullable/non-null
sources, modern/legacy cast modes and New York/UTC sessions. Positive routes require
consumed NativeCalc, no fallback and empty evaluator descriptors. The actual preceding
encoder compares complete first-query maps/schema before ten expected pure-route failures
per release/JDK pair; the retained-control method passes. Later unreached queries are
not preceding parity evidence.

This extension changes only planner admission: it reuses the unchanged release runtime
with its preceding 925-core-test qualification and sixteen unchanged released temporal
method oracles. Those historical results are not relabeled as fresh tests. All other
sources match the complete-core compilation from that qualification; each SQL/performance
configuration uses its freshly compiled preceding or candidate encoder with Java release 11.
The source, encoder and runtime hashes are checked independently.

Prepared and complete-job timings use JDK 17. Prepared inputs match Generic/Binary RowData and Arrow over DECIMAL(18,3), (19,0),
(19,19), (38,0), (38,18) and (38,38), ABS/negation/nesting/same-scale widening,
seconds/milliseconds, both NULL modes,
1,024/16-row batches and signed-fraction/range/low-64-bit wrapping values. Each trial
processes 131,072 rows, with 16 warmups and nine measurements. Timers start inside Rust
after input setup/import and stop before export, retaining production Calc, allocation,
conversion and equivalent output consumption. Only this small-function gate excludes
perimeter JNI/C Data entry and transposes. Preceding measurement uses its actual
JVM evaluator inside NativeCalc; it is not relabeled as stock fallback. Both processes
retain independent stock controls; process order reverses on 1.18.

All 48 prepared bulk cells beat stock 1.815-12.115x
and actual preceding behavior 8.100-21.684x.
10/12 tiny controls lose to stock
(stock/native 0.295-1.291x), and
0/12 lose to previous. These losses remain;
no universal tiny-batch speedup is claimed.

Six complete million-row job configurations retain submission/startup, JNI and both
row/Arrow transposes, one thread, two warmups and five alternating measurements per
engine. Native/stock elapsed ranges 0.909-1.025;
native/previous ranges 0.623-0.871.
1/6 complete medians lose to the paired stock control,
1/6 lose to independent stock and
0/6 lose to previous. All complete-job losses and ranges
remain; a prepared bulk gain is not a whole-job acceleration claim.

| Flink/DECIMAL/unit | Stock seconds | Native seconds | Independent stock seconds | Previous seconds |
|---|---:|---:|---:|---:|
| 22-negate-p18s3-u0 | 0.197845 | 0.196491 | 0.197402 | 0.239379 |
| 22-abs-p38s18-u3 | 0.250061 | 0.234997 | 0.251065 | 0.368419 |
| 22-widen-p19s0-u3 | 0.207587 | 0.212784 | 0.196798 | 0.245700 |
| 118-negate-p18s3-u0 | 0.205466 | 0.204706 | 0.205045 | 0.235013 |
| 118-abs-p38s18-u3 | 0.250150 | 0.227381 | 0.249014 | 0.365091 |
| 118-widen-p19s0-u3 | 0.204820 | 0.202692 | 0.203000 | 0.246186 |

The [machine-readable qualification](results/decimal-unary-epoch-2026-11-05.json) and
[all 6,950 raw prepared samples](results/decimal-unary-epoch-2026-11-05.csv.gz) retain
pilot/final medians, full trials, independent controls, source/fixture/encoder bytecodes,
complete current-core compilation, exact preceding/candidate release artifacts and
allocator aliases. The initial double-negation probe folded to an already-native direct input, so it cannot
establish a new-feature gain: its three completed paired cells and fifty unpaired
candidate samples are retained separately. Historical qualifications keep their original provenance.
Hosted CI must qualify the exact final head separately.



## Default numeric epoch precision {#default-numeric-epoch-precision}

Released Flink 2.2.1 single-argument `TO_TIMESTAMP_LTZ` over Number/DecimalData delegates
to precision 3 in its unchanged bytecode. Ordinary Calc therefore reuses the existing
integer and DECIMAL millisecond kernels for their separately verified total producers.
The proof requires genuine built-in identity; omitted precision does not admit new
producer families, floating/text overloads or UNNEST. Integer widths/NULLs/range and
DECIMAL truncation/low-64-bit wrapping/final milliseconds retain their existing contracts.
Flink 1.18.1 rejects this signature during SQL validation; both stock and native controls
preserve that rejection, without skipped tests or a new SQL overload.

All 192 fresh focused JVM invocations pass on released Flink 1.18.1/2.2.1
and JDK 11/17, targeting Java 11. The 2.2 matrix includes complete 5,003-row maps/schema,
all signed integer widths, arithmetic/widening/rounding producers, seven DECIMAL
precision/scale pairs, unary/widening producers, signed fractions/range/wrapping values,
both NULL modes, New York/UTC sessions, lazy/empty consumers and UTC integer-epoch
FLOOR/CEIL. Positive routes require consumed NativeCalc, no fallback and empty
evaluators. Retained invalid/text/catalog/producer controls and existing double-argument,
runtime-precision and UTC-rounding contracts pass separately.

The actual preceding 2.2 encoder compares all eleven first-query maps/schema before its
expected pure-route assertions fail; two retained controls pass per JDK. Later unreached
queries are not preceding-parity evidence. Eight fresh unchanged released 2.2 temporal
method oracles pass as stock-only evidence. A separate 1074-invocation
parent-core matrix qualifies rebase onto main `9954a19c`, including incoming
recursive UDF alias/validation and shared-state contracts. That frozen parent-core result
is not relabeled as testing the later single-argument encoder. No runtime changes are
made; its historical 925 Rust core tests retain their original hashes and are not rerun
or relabeled as fresh tests. Candidate/previous encoders are freshly compiled and checked
against their SQL/performance bytecode and complete parent-core compilation.

Release prepared measurements use Flink 2.2.1/JDK 17, equivalent Generic/Binary RowData
and Arrow, all four signed integer widths, integer multiplication, DECIMAL(18,3),
(19,0), (38,0), (38,18), (38,38), ABS/negation, NULLs and epoch/wrap-boundary domains.
Each trial processes 131,072 rows with 16 warmups and nine measurements. Native timing
starts inside Rust after input setup/import and stops before export, retaining production
Calc, allocation, intrinsic conversions and equivalent output consumption. Only this
small-function gate excludes perimeter JNI/C Data and transposes. Previous behavior is
its actual generated evaluator inside NativeCalc, not host fallback. Both processes retain
independent stock controls.

All 44 bulk cells beat stock **1.412-15.625x** and previous
**11.349-21.918x**. 9/11 tiny cells
lose to stock (stock/native 0.078-1.252x), and
0/11 lose to previous. All small-batch losses and variability remain.

Four complete million-row configurations retain startup/submission, JNI and both
row/Arrow transposes, with two warmups and five alternating measurements per engine.
Native/stock elapsed is 0.894-1.074;
native/previous 0.612-0.866.
2/4 medians lose to paired stock,
2/4 to independent stock and
0/4 to previous. A prepared gain is not a universal
complete-job acceleration claim.

| Input/producer | Stock seconds | Native seconds | Independent stock seconds | Previous seconds |
|---|---:|---:|---:|---:|
| int-square | 0.182350 | 0.181269 | 0.182687 | 0.219454 |
| bigint-direct | 0.186902 | 0.192911 | 0.182297 | 0.222803 |
| decimal-p18s3 | 0.187237 | 0.201149 | 0.191009 | 0.243453 |
| decimal-abs-p38s18 | 0.249972 | 0.223394 | 0.250466 | 0.365267 |

The [machine-readable qualification](results/default-precision-epoch-2026-11-05.json)
and [5,500 raw prepared samples](results/default-precision-epoch-2026-11-05.csv.gz)
retain all measurements, independent controls, exact source/encoder/fixture/runtime
provenance and the separate parent rebase qualification. Historical results keep their
original revisions. Hosted CI must qualify the exact final PR head separately.


## Numeric DATE_FORMAT calendar correctness (2026-11-05) {#numeric-date_format-calendar-correctness-2026-11-05}

This repairs an already-enabled path rather than claiming a new accelerated signature.
All 28 fresh JVM invocations pass on released Flink 1.18.1/2.2.1 and JDK 11/17, targeting
Java 11. They cover complete runtime-source maps and schemas for TIMESTAMP(0/3/6/9),
BCE/year-zero/extended dates, signed-millisecond endpoints, NULLs, filtering and empty
batches. Admitted paths consume NativeCalc with no fallback and empty JVM evaluators.
The actual preceding runtime fails all four new value tests on each released line's
JDK 17 control, while the three existing tests pass. Twelve unchanged released temporal
test-method oracles and all 927 release Rust core tests also pass.

Six complete million-row configurations compare stock, the actual preceding native
runtime and the repaired runtime on modern dates, where the preceding formatter is
correct. Each engine has two warmups and three measured trials at parallelism one.
Both row/Arrow perimeter transposes, planning/submission, JNI and blackhole consumption
remain in the measured jobs. Raw elapsed and job-runtime trials, source/library hashes
and correctness accounting are retained in the
[qualification record](results/date-format-year-2026-11-05.json).
Native/stock median elapsed is 0.744-0.960; native/previous is 0.926-1.001.
The full timestamp pattern on Flink 2.2 is 0.07% slower than the preceding runtime;
the trials are retained rather than presented as a universal speedup. These complete-job
diagnostics do not qualify any additional pattern or timezone admission.


## Java DATE_FORMAT literal syntax (2026-11-05) {#java-date_format-literal-syntax-2026-11-05}

Numeric plain-TIMESTAMP patterns honor quoted literals and doubled quotes without
copying Java syntax into the output. Optional sections and unsupported/invalid patterns
keep Flink's generated evaluator. All 56 fresh JVM invocations pass on released
Flink 1.18.1/2.2.1 and JDK 11/17, targeting Java 11. Runtime-source tests compare complete
maps/schema, NULLs, native consumption, absent fallback and exact evaluator routes;
reserved/malformed patterns preserve released failures and all-filtered laziness.
The preceding encoder fails seven new tests and passes seven retained tests per
released-line JDK 17 control. Later unreached queries are not preceding-parity evidence.

Release prepared measurements cover TIMESTAMP(3/9), full signed-millisecond endpoints,
pre-epoch/extended dates, Generic/Binary RowData, NULL modes and three quoted patterns.
Stock starts on prebuilt RowData and native starts on equivalent Arrow; native timing
starts inside Rust after input preparation/import and stops before export, retaining
production Calc, allocation, intrinsic conversions and equivalent output consumption.
The actual preceding path retains its generated JVM evaluator inside NativeCalc.
Each trial processes 131,072 rows, with 16 warmups and nine measurements per engine.

All 48 bulk cells beat stock **2.499-4.252x** and previous **3.433-5.541x**.
Eleven of twelve 16-row cells lose to stock (stock/native **0.751-1.007x**); all beat
the preceding generated path. These small-batch losses and all 6,000 raw samples are
retained in the [qualification](results/date-format-pattern-2026-11-05.json) and
[prepared trials](results/date-format-pattern-2026-11-05.csv.gz).

Four complete million-row jobs retain both row/Arrow perimeter transposes, JNI,
planning/submission and blackhole consumption, with two warmups and three measured
trials per engine. Native/stock median elapsed is **0.783-0.990** and native/previous
is **0.607-0.707**. The narrowest stock margin is the literal-prefix pattern on
Flink 1.18 (0.270 versus 0.273 seconds); raw startup and measured trials remain in the
qualification rather than being hidden by the prepared speedup.


## Exact UTC DATE_FORMAT over local-zoned timestamps (2026-11-05) {#exact-utc-date_format-over-local-zoned-timestamps-2026-11-05}

Direct TIMESTAMP_LTZ input columns in ordinary Calc reuse the exact numeric formatter
when the planning JDK normalizes the session zone to a fixed zero offset. This removes
the generated evaluator without a separate timezone database or a new performance switch.
Nonzero/regional zones, unverified derived producers, unsupported patterns, catalog
functions and explicit incompatible/UNNEST routes keep their previous handling.

All 68 new focused JVM tests pass across released Flink 1.18.1/2.2.1 and JDK 11/17,
targeting Java 11. Complete runtime-source maps/schema cover TIMESTAMP_LTZ(0/3/6/9),
UTC aliases, BCE/extended years, signed-millisecond endpoints, NULLs, filtering and
empty batches. Pure routes require consumed NativeCalc, no fallback and empty JVM
evaluators; retained routes are checked explicitly. The preceding encoder matches each
first-query map/schema before twelve expected pure-route failures per released-line
JDK 17 control; its nineteen retained tests pass. Later queries are not prior-parity evidence.

The broader final selection has another 162 passing regression invocations and fourteen
unchanged version-gated aborts: seven FlinkTemporalFunctionsSqlHarnessTest assumptions
on each 1.18/JDK combination for absent LTZ text/default-precision signatures, the
pre-epoch TIME external conversion limit and three sub-hour LTZ rounding signatures.
Those aborts are not counted as passes; no new UTC-formatting test is aborted or skipped.
The runtime is unchanged from the calendar repair, whose 927-core qualification remains
separate preceding evidence.

Release prepared measurements use equivalent Generic/Binary RowData and Arrow,
TIMESTAMP_LTZ(3/9), NULL modes, full-range epoch data and three literal numeric formats.
Each trial processes 131,072 rows with sixteen warmups and nine measurements per engine.
Native timing starts inside Rust after input preparation/import and stops before export;
intrinsic production Calc, allocations, conversions and output consumption remain timed.
Previous behavior retains its actual generated evaluator inside NativeCalc.

All 48 bulk cells beat stock **2.861-4.546x** and previous **4.168-5.989x**.
Ten of twelve 16-row cells lose to stock (stock/native **0.676-1.062x**); all beat previous.
Six complete million-row jobs retain submission, JNI and both perimeter transposes, with
two warmups and three measured trials per engine. Native/stock median elapsed is
**0.764-0.961** and native/previous **0.533-0.678**. All startup and measured trials,
variability, source/runtime hashes and evaluator-route evidence remain in the
[qualification](results/utc-date-format-2026-11-05.json) and
[6,000 prepared samples](results/utc-date-format-2026-11-05.csv.gz).



## UTC DATE_FORMAT over verified integer epochs (2026-11-05) {#utc-date_format-over-verified-integer-epochs-2026-11-05}

DATE_FORMAT now composes the verified total integer TO_TIMESTAMP_LTZ producer proof
in ordinary Calc, for supported literal numeric patterns and session zones normalized
to a fixed zero offset. This includes direct integers, admitted wrapping arithmetic
and rounding, literal precision 0/3 and verified runtime INT CASE precision 0/3/NULL.
Flink 2.2's single-argument numeric epoch uses its released default precision three;
Flink 1.18 retains its validation rejection. For example,
`DATE_FORMAT(TO_TIMESTAMP_LTZ(n*n,3),'yyyy-MM-dd')` now stays in Arrow without a
generated temporal evaluator. Resolved integer widths, original-unit epoch bounds,
overflow wrapping, NULLs and the existing exact numeric calendar formatter are reused.
No production Rust/JNI change, new opcode or performance switch is added.

Both released DateTimeUtils implementations format TimestampData through its Instant,
the session ZoneId and cached Java DateTimeFormatter. The fixed-zero proof avoids
introducing a second timezone database. Regional/nonzero zones, DECIMAL epochs,
unverified producers, unsupported/dynamic patterns and existing UNNEST contexts retain
their separate routes. Arbitrary timestamp CASE/casts are not admitted by this proof.

All **314 fresh JVM invocations** pass on released Flink 1.18.1/2.2.1 and JDK 11/17,
targeting Java 11: 62 new SQL invocations and 252 related regressions, no skips or
aborts. Complete 5,003-row maps/schema cover all four integer widths, nullable/NOT NULL
fields, signed bounds, wrapping arithmetic/rounding, runtime precision, quoted patterns,
IS NULL/CHAR_LENGTH/LIKE/CASE consumers, UTC aliases, retained regional zones and
all-filtered batches. Pure routes consume NativeCalc, have no fallback and have empty
JVM evaluator descriptors. Narrowing, division, DECIMAL, unsupported patterns and
unverified precision controls retain generated evaluation. Lazy failing branches and
all-filtered divide-by-zero inputs remain harmless. Twelve fresh unchanged released
TemporalTypesTest DATE_FORMAT/Shanghai/LosAngeles method oracles pass as separate stock
evidence. The byte-identical runtime retains its historical 927-core-test qualification.

Actual preceding encoder `284ebb1a2` compares each first query's full maps/schema and
then fails fifteen expected pure-route assertions on 2.2/JDK17 and eleven on 1.18/JDK17.
Two retained controls pass on both lines; the 1.18 absent-signature control also passes.
Later unreached queries are not preceding-parity evidence. Qualified and performance
encoder instructions match on both lines; current and preceding benchmark fixture
classes are byte-identical.

Prepared release measurements output character values directly and consume equivalent
UTF-8 checksums. Six shapes cover TINYINT seconds, SMALLINT millisecond addition, INT
millisecond multiplication, BIGINT seconds, BIGINT millisecond addition and BIGINT
runtime precision. Each uses nullable/nonnullable and Generic/Binary RowData controls,
1,024-row batches and an additional 16-row nullable/Binary control on each release.
Stock starts on equivalent prebuilt RowData, native on prebuilt Arrow. The timer runs
inside Rust after setup/import and before export; production Calc, allocation and
intrinsic conversions/callbacks remain timed. The actual preceding route is a generated
temporal evaluator inside NativeCalc. Both encoders use the same release runtime and
fixture. Each trial processes 131,072 rows, with sixteen warmups, nine measurements
and 1 GiB prepared JVM heaps. No other tests or benchmarks run concurrently.

All **48 bulk cells** beat stock **2.942-4.855x** and previous **4.190-6.704x**.
All **12 tiny controls lose to stock**, with stock/native **0.382-0.591x**; all beat
previous. Six complete million-row jobs retain submission/startup, production JNI
and both RowData/Arrow transposes. At parallelism one and 2 GiB heaps for every engine,
each has two warmups and five measured repetitions. All six beat paired stock and
the actual preceding evaluator: native/stock elapsed **0.651-0.692**, native/previous
**0.583-0.643**. These results do not establish universal tiny-batch or complete-job gains.

See the [qualification, full-job trials and exact provenance](results/epoch-date-format-2026-11-05.json)
and [6,000 raw prepared samples](results/epoch-date-format-2026-11-05.csv.gz).
Remaining temporal signatures stay open in
[#503](https://github.com/datafusion-contrib/StreamFusion/issues/503).


## UTC DATE_FORMAT over verified DECIMAL epochs (2026-11-05) {#utc-date_format-over-verified-decimal-epochs-2026-11-05}

Ordinary Calc now composes the existing numeric DATE_FORMAT renderer with verified
DECIMAL TO_TIMESTAMP_LTZ producers in sessions normalized by the planning JDK to a fixed
zero offset. Direct fields/literals, total ABS/negation chains and same-scale widening
use literal precision 0/3 or the released Flink 2.2 default. For example,
`DATE_FORMAT(TO_TIMESTAMP_LTZ(-ABS(d),3),'yyyy-MM-dd')` has no generated evaluator.
Flink 1.18 still rejects the absent single-argument overload. Genuine builtin identities,
supported literal numeric patterns, incompatible-option and UNNEST gates remain intact.
Arithmetic/narrowing/rescaling and other unverified producers, runtime DECIMAL precision,
nonzero/regional zones and dynamic/unsupported patterns retain their preceding routes.

Both released DateTimeUtils bytecodes truncate through `BigDecimal.setScale(0, DOWN)`,
wrap the signed long, wrap seconds multiplication when needed, and then bound final
milliseconds. These differ from integer original-unit epoch checks. Their formatter uses
TimestampData.toInstant, LocalDateTime.ofInstant in the session ZoneId, and the cached
Java DateTimeFormatter. A zero-offset session composes the existing exact epoch and
full-range numeric calendar kernels without another timezone database, runtime opcode,
JNI path or performance switch.

All **286 fresh JVM invocations** pass on released Flink 1.18.1/2.2.1 and JDK 11/17,
targeting Java 11: **68 new SQL invocations and 218 related invocations**, no skips or
aborts. Full 5,003-row maps/schema cover ten DECIMAL precision/scale pairs, nullable and
NOT NULL sources, modern/legacy mode, deterministic seeded coefficients, truncation,
wraps and epoch bounds, total unary/widening trees, quoted patterns, IS NULL/CHAR_LENGTH/
LIKE/CASE consumers, UTC aliases, retained routes and lazy/all-filtered failures. Pure
admitted queries consume NativeCalc, have no fallback and empty JVM evaluators. Twelve
unchanged released DATE_FORMAT/Shanghai/LosAngeles stock-oracle runs pass separately.
Actual preceding `f123e4eaf` matches each first-query map/schema before sixteen expected
pure-route failures on 2.2/JDK17 and thirteen on 1.18/JDK17. Two retained controls pass
per line, plus the 1.18 absent-signature control; later unreached queries are not prior
parity evidence. Focused/performance encoder instructions match, including nested
classes; candidate/prior fixture classes are byte-identical. The unchanged runtime
retains its historical 927-core qualification; these are not fresh Rust test runs.

Prepared release timing consumes direct character output with equivalent UTF-8 checksum
work. Stock starts on prebuilt Generic/Binary RowData; native starts inside Rust on
prebuilt Arrow RecordBatch. The timer excludes setup/import/export and retains intrinsic
Calc work, allocations and conversions. Actual previous behavior invokes the generated
temporal evaluator inside NativeCalc. Five shapes cover compact DECIMAL(18,3) seconds,
noncompact (19,0) milliseconds, wrapping (38,0) seconds, negated (38,18) milliseconds and
ABS of (38,38) milliseconds. Each released line uses nullable/nonnullable and Generic/
Binary controls with 1,024-row batches, plus five 16-row nullable/Binary controls. Each
trial processes 131,072 rows with sixteen warmups, nine measurements and matched 1 GiB
prepared JVM heaps. UTC is explicit in the prepared process environment. Independent
stock controls and all variability remain in the record. No other tests or benchmarks
run concurrently with timing.

All **40 bulk cells** beat stock **3.070-5.782x** and actual previous
behavior **4.603-8.379x**. **10/10 tiny controls
lose to stock**; **0/10 lose to previous**. Six complete million-row
character-output jobs retain submission/startup, production JNI and both perimeter
transposes. Every engine uses parallelism one, 2 GiB heaps, two warmups and five measured
repetitions. **0/6 lose to stock** and
**0/6 lose to previous**: native/stock elapsed is
**0.684-0.780**, native/previous
**0.482-0.637**. These bounded function gains do not
establish universal job acceleration; slower tiny/full-job results remain material.

See the [qualification, complete-job trials and exact provenance](results/decimal-epoch-date-format-2026-11-05.json)
and [5,000 raw prepared samples](results/decimal-epoch-date-format-2026-11-05.csv.gz).
The remaining temporal signatures stay open in
[#503](https://github.com/datafusion-contrib/StreamFusion/issues/503).


## UTC FLOOR over verified DECIMAL epochs (2026-11-05) {#utc-floor-over-verified-decimal-epochs-2026-11-05}

Ordinary Calc FLOOR now composes verified DECIMAL TO_TIMESTAMP_LTZ producers with the
existing exact calendar/fixed-width kernels in sessions normalized by the planning JDK
to fixed zero offset. Direct fields/literals, total ABS/negation chains and same-scale
widening use literal precision 0/3 or Flink 2.2 default precision. Preserve DECIMAL
truncation, signed-long/seconds wrapping and final millisecond bounds. YEAR/MONTH/DAY/HOUR
qualify on both released lines, MINUTE/SECOND/MILLISECOND on 2.2. Flink 1.18 retains its
absent default signature and sub-hour initialization failures. Genuine builtins, NULLs,
resolved result precision, conditional/empty batches and existing UNNEST scope stay exact.
DECIMAL CEIL/CEILING, runtime decimal precision, binary arithmetic/narrowing/rescaling,
other unverified producers, regional/nonzero zones and unsupported units retain their
routes. DATE_FORMAT shares its existing DECIMAL producer proof without a behavior change.
No production Rust/JNI/opcode or configuration change is introduced.

Released DateTimeUtils truncates DECIMAL through BigDecimal before signed-long conversion
and wrapped seconds multiplication. It bounds the final milliseconds. timestampFloor
adds the zone offset, applies fixed-width floor or signed-day/int-Julian calendar
conversion, then subtracts the offset. At fixed zero offset the existing kernels preserve
signed division, calendar cutover and zeroed fractions; generic Gregorian truncation is
not a replacement for these released calendar rules.

All **308 fresh JVM invocations** pass on released Flink 1.18.1/2.2.1 and JDK 11/17,
targeting Java 11: **68 new harness invocations and 240 related invocations**, no
skips/aborts. Complete 5,003-row maps/schema cover ten DECIMAL precision/scale pairs,
nullable/NOT NULL sources, total unary/widening trees, wraps, exact fixed-unit boundaries,
pre-epoch and calendar-cutover values, every admitted unit, basic consumers, UTC aliases,
lazy/all-filtered paths and retained routes. Admitted paths consume NativeCalc work, have
no fallback and empty JVM evaluators. Existing fixed/calendar FLOOR, direct CEIL, numeric
producer/version admission and the shared DATE_FORMAT route are requalified. Four
unchanged released ScalarFunctionsTest.testTemporalFloor stock oracles pass separately.
Actual preceding `22f6b94ca` compares each first-query map/schema before sixteen expected
pure-route failures on 2.2/JDK17 and thirteen on 1.18/JDK17; two retained controls per line
and the 1.18 absent-signature control pass. Later unreached queries are not prior parity
evidence. Final parity/performance encoder instructions match, including nested classes;
candidate/prior fixture classes are byte-identical. The unchanged runtime retains its
historical 927-core qualification; this is not a newly rerun Rust count.

Prepared release timing consumes YEAR(FLOOR(...)) as an integer with equivalent checksums.
Four shapes cover compact DECIMAL(18,3) seconds/YEAR, noncompact (19,0) millis/MONTH,
wrapping (38,0) seconds/DAY and negative ABS (38,18) millis/HOUR. Stock starts on prebuilt
Generic/Binary RowData; native starts on prebuilt Arrow RecordBatch, with its timer inside
Rust after setup/import and before export. Production Calc, allocation and intrinsic
conversions remain timed. Actual previous behavior retains generated temporal evaluation
inside NativeCalc. Each released line uses nullable/nonnullable and Generic/Binary controls
at 1,024 rows, plus four 16-row nullable/Binary controls. Each trial processes 131,072
rows, with sixteen warmups, nine measurements, matched 1 GiB heaps and explicit UTC.
Independent stock controls and variability remain recorded; no other performance workload
runs concurrently with timing.

All **32 bulk cells** beat stock **1.291-6.523x** and actual previous
behavior **2.651-9.545x**. **8/8 tiny controls
lose to stock**, **0/8 lose to previous**. Six complete million-row
jobs output TIMESTAMP_LTZ(3) directly, retaining submission/startup, JNI and both perimeter
transposes. Each engine uses parallelism one, 2 GiB heaps, two warmups and five measured
repetitions. **4/6 lose to stock**, **0/6
lose to previous**; native/stock elapsed is **0.921-1.078**,
native/previous **0.658-0.885**. The prepared function gains
do not establish universal complete-job acceleration; all slower cases remain material.

See the [qualification, complete-job trials and exact provenance](results/decimal-epoch-floor-2026-11-05.json)
and [4,000 raw prepared samples](results/decimal-epoch-floor-2026-11-05.csv.gz).
Other temporal/calendar signatures remain open in
[#504](https://github.com/datafusion-contrib/StreamFusion/issues/504).


## UTC CEIL over verified DECIMAL epochs (2026-11-06) {#utc-ceil-over-verified-decimal-epochs-2026-11-06}

Ordinary Calc CEIL/CEILING composes verified DECIMAL TO_TIMESTAMP_LTZ producers with
existing native calendar/fixed-width kernels in session zones normalized by the planning
JDK to fixed zero offset. Genuine builtins over direct fields/literals, total ABS/negation
chains and same-scale widening qualify at literal precision 0/3 or released Flink 2.2
default precision. YEAR/MONTH/DAY/HOUR qualify on both releases; MINUTE/SECOND/MILLISECOND
are 2.2-only. Runtime precision, arithmetic/narrowing/rescaling, unknown/fallible producers,
other zones/units and UNNEST retain preceding routes. Released 1.18 retains its absent
default signature and sub-hour initialization failures. No configuration switch, Rust
opcode, JNI boundary or timestamp transport is added.

Released decimal conversion truncates toward zero through BigDecimal, wraps signed-long
conversion and seconds multiplication, then bounds the final milliseconds. Its admitted
non-NULL outputs have zero fractional nanos and milliseconds in
`[-62167219200000,253402300799999]`. At fixed zero offset the existing CEIL kernels retain
fixed-width rounding and signed-day/int-Julian calendar behavior. Adding at most one DAY
cannot overflow long for this bounded producer. Direct/literal LTZ CEIL keeps the separate
remainder/overflow kernel required for arbitrary signed-long milliseconds and fractions.

All **640 test invocations across four fresh JVM processes** pass
on released Flink 1.18.1/2.2.1 and JDK 11/17, with Java release 11 and no skips/aborts:
**68 new harness and 572 related invocations**.
Complete 5,003-row maps/schema cover ten precision/scale pairs, nullable/NOT NULL fields,
truncation, wrapping and range bounds, total unary/widening trees, pre-epoch and calendar
cutover values, exact fixed-unit boundaries, all supported units, CEILING, consumers,
zero-offset aliases, lazy/all-filtered batches and retained routes. Pure paths require
consumed NativeCalc work, no fallback and empty JVM evaluator descriptors. The latest
canonical-base release native library and example are rebuilt; **927 fresh release Rust
core tests pass**. Preliminary runs against older frozen libraries are excluded.

Four separate unchanged released `ScalarFunctionsTest.testTemporalFloor` oracle runs pass
across both releases/JDKs; despite its name, that upstream method asserts both FLOOR and
CEIL behavior. These stock runs supplement the native parity tests. Actual preceding main
`a9f7c3589` compares each first-query full map/schema, then fails sixteen expected pure-route
assertions on 2.2/JDK17 and thirteen on 1.18/JDK17. The retained controls pass. Later
unreached queries are not preceding-parity evidence. Focused/performance encoder instructions
match, including nested classes; candidate/preceding benchmark fixtures are byte-identical.
Both timing engines use the same freshly built canonical-base native runtime.

Release prepared timing consumes YEAR(CEIL(...)) as an integer with equivalent checksums.
Four shapes cover compact DECIMAL(18,3) seconds/YEAR, noncompact (19,0) millis/MONTH,
wrapping (38,0) seconds/DAY and negative ABS (38,18) millis/HOUR. Stock starts on prebuilt
Generic/Binary RowData; native starts on prebuilt Arrow RecordBatch. The native timer starts
inside Rust after setup/import and stops before export; production Calc, allocation and
intrinsic conversions remain timed. Actual previous behavior retains generated temporal
evaluation inside NativeCalc. Each release has nullable/nonnullable and Generic/Binary
controls at 1,024 rows plus four 16-row nullable/Binary controls. Each trial processes
131,072 rows with sixteen warmups, nine measurements, matched 1 GiB heaps and explicit UTC.
Independent stock measurements, min/max variability and every raw trial are retained.
No other performance workload runs concurrently with these measurements.

All **32 bulk cells** beat stock **1.121-4.472x** and actual previous
behavior **2.681-7.882x**. **8/8 tiny controls lose
to stock**, **0/8 lose to previous**. Six complete million-row jobs
emit TIMESTAMP_LTZ(3) directly and retain submission/startup, JNI and both transposes.
Every engine uses parallelism one, 2 GiB heaps, two warmups and five measured repetitions.
**4/6 lose to stock**, **0/6 lose to
previous**; native/stock elapsed is **0.914-1.126**, native/previous
**0.694-0.923**. These are bounded prepared-function gains;
the measurements do not establish universal complete-job acceleration. Slower jobs and
small-batch controls remain in the record.

See the [qualification, complete-job trials and exact provenance](results/decimal-epoch-ceil-2026-11-06.json)
and [4,000 raw prepared samples](results/decimal-epoch-ceil-2026-11-06.csv.gz).
Remaining temporal signatures stay open in
[#504](https://github.com/datafusion-contrib/StreamFusion/issues/504).

