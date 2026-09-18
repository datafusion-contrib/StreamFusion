# Scalar function benchmarks

Measured on 2026-09-09 against the production implementation in `69122d2b`, using Apple M4 Pro,
JDK 17, UTC, Flink 2.2.1 and DataFusion 54.0.0. The native library uses the standard Maven
`bench` profile: release mode, mimalloc, and the development connector features.

Every query evaluates one function over 2,000,000 rows at parallelism 1. Both engines run
serially in the same JVM for each scenario, alternating which engine runs first on each trial.
Each engine gets two warmups and five measured trials per case; the tables show median elapsed
seconds. Each scenario uses a fresh JVM. No other benchmark or test suite runs concurrently.

Times include SQL planning/execution, a rowwise DataStream source, and a rowwise blackhole
sink. The harness checks that every native function and identity-control plan includes
`NativeCalc`, `RowDataToArrow`, and `ArrowToRowData`. Ratios are Flink time divided by native
time: above 1 means native is faster for that workload; below 1 means it is slower. Small
differences can be run-to-run noise, and these measurements do not isolate kernel cost.

The original 48 cases cover 35 retained functions, including integer widths and literal/column
search parameters. All cases run with the 264-byte ASCII/non-null and Unicode/NULL scenarios;
the ten search cases also run with 8-byte ASCII padding, giving 106 Flink/native comparisons.
TO_TIMESTAMP and temporal FLOOR/CEIL/CEILING were outside that measurement's coverage. Their
subsequent implementation is measured in the [temporal diagnostic below](#temporal-coverage-diagnostic-2026-09-15).
See [Calc / filter](../operators/calc-filter.md) for the complete argument gates.

## SQL/JSON native-first routing (2026-09-18)

The production admission policy tries the existing native encoding first, then generates
an entire JSON Calc with Flink when that fails. Existing simple paths retain their Rust
kernels; slices and JSON_QUERY below use one JVM callback per Arrow batch. The scalar
bridge boundary and intermediate JSON string identity restrictions still apply.

This release/mimalloc measurement ran on Apple M1 Max with the same Flink/JDK versions,
1,000,000 rows, 264-byte budget, two warmups, five alternating trials and row source/sink
with both transposes as the prototype below. No other local test or benchmark ran concurrently.
The unchanged simple functions act as regression controls; their small differences from
the earlier native measurements are not an optimization claim.

| Case | Flink (s) | StreamFusion (s) | Flink / StreamFusion | JSON evaluator |
|---|---:|---:|---:|---|
| Simple-source identity | 0.390342 | 0.655438 | 0.596x | none |
| Array-source identity | 0.474533 | 0.852896 | 0.556x | none |
| JSON_VALUE, simple path | 1.106959 | 0.796195 | 1.390x | Rust |
| JSON_EXISTS, simple path | 1.093376 | 0.739060 | 1.479x | Rust |
| `JSON_QUERY(s, '$.a[0:2]')` | 1.951007 | 2.823455 | 0.691x | JVM |
| `JSON_EXISTS(s, '$.a[0:2]')` | 1.744556 | 2.484502 | 0.702x | JVM |

The newly admitted functions are slower than stock Flink in isolation. This is a coverage
route that avoids more handwritten parser semantics and permits surrounding operators to
remain columnar; it is not evidence of a whole-query speedup. A larger stateful pipeline
has not been benchmarked here. Identity controls are reported without subtraction.
[Raw trials](sql-json-hybrid-2026-09-18.csv) retain every measured iteration.

Use the reproduction command below with
`-Dscalar.functions=JSON_VALUE,JSON_EXISTS,JSON_QUERY_SLICE,JSON_EXISTS_SLICE`.

## SQL/JSON JVM bridge prototype (2026-09-18)

This experiment routes a complete SQL/JSON Calc through Flink-generated JVM code using
StreamFusion's existing batch UDF bridge. It preserves row evaluation order and removes
Calc's native path-grammar gate. It does **not** remove the Rust kernels, alter JSON format
connectors, or establish that a whole-query JVM replacement should ship.

On Apple Silicon, JDK 17, UTC and Flink 2.2.1, each case processes 1,000,000 rows at
parallelism 1, with a 264-byte payload budget, no injected NULLs, two warmups and five
measured trials. The common native library is built in release mode with mimalloc. Each
route runs in a separate JVM, alternating against stock Flink within that JVM. The JVM
route was measured first, then the existing native route. Both row/Arrow transposes,
rowwise source and blackhole sink remain in the measured path; plans assert native Calc
and both transposes. No other local tests or benchmarks ran concurrently. These are
whole-job elapsed medians, including planning, not isolated JSON kernel timings.

| Case | Flink, native run (s) | Rust route (s) | Flink, JVM run (s) | JVM bridge (s) | JVM / Rust time |
|---|---:|---:|---:|---:|---:|
| Simple-path identity | 0.387916 | 0.659267 | 0.391868 | 0.660814 | 1.002x |
| Wildcard-source identity | 0.482458 | 0.900425 | 0.472690 | 0.872888 | 0.969x |
| JSON_VALUE, simple path | 1.108530 | 0.838918 | 1.112615 | 1.755095 | 2.092x |
| JSON_EXISTS, simple path | 1.115314 | 0.748365 | 1.107832 | 1.669865 | 2.231x |
| JSON_VALUE, wildcard | 4.207545 | 1.721668 | 4.243582 | 5.190559 | 3.015x |
| JSON_EXISTS, wildcard | 3.677213 | 1.565452 | 3.683537 | 4.477114 | 2.860x |

The JVM prototype costs 2.09–3.02 times the existing Rust route on these cases and
1.22–1.58 times its paired stock Flink control. Controls are not subtracted. Wildcard
JSON_VALUE takes the lax EMPTY path; this is not a scalar-extraction speedup comparison.
The steady paired Flink and identity controls help compare the separate JVMs, but the
experiment does not measure stateful downstream pipelines or wider/more complex JSON.

The blanket replacement was rejected. Current routing retains native encoding first and
uses the generated JVM Calc only when a JSON Calc exceeds that admission. The bridge adds
dynamic paths, JSON_QUERY, complex selectors and exact error handling without more native
parser extensions. A representative larger-pipeline benchmark remains future work. The
table above records the rejected blanket replacement, not the current routing of those
four cases.

Raw trials: [JVM route](sql-json-jvm-2026-09-18.csv) and
[existing native route](sql-json-native-2026-09-18.csv). In each CSV, `engine=native`
means StreamFusion enabled; in the first file its JSON evaluator runs on the JVM.
The existing route uses planner `c0677de0` and the prototype's common release DSO:
its only native change is filtered row-UDF output handling, which the existing JSON
kernels do not call. The selected simple/wildcard grammar is shared with `286c5b88`;
no union/slice extension is exercised. The benchmark harness is unchanged between runs.

Build the library with `cargo build --manifest-path native/Cargo.toml -p streamfusion
--release --features mimalloc`, then run this in each planner checkout with that same
release library (using distinct output paths):

```bash
SF_BENCHMARK=true mvn -B -ntp -Pbench -pl :streamfusion-runtime -am \
  -Dnative.build.skip=true '-Dtest=ScalarFunctionBenchmark#individualFunctions' \
  -Dsurefire.failIfNoSpecifiedTests=false \
  -Dscalar.functions=JSON_VALUE,JSON_EXISTS,JSON_VALUE_WILDCARD,JSON_EXISTS_WILDCARD \
  -Dscalar.rows=1000000 -Dscalar.bytes=264 -Dscalar.warmup=2 -Dscalar.runs=5 \
  -Dscalar.output=target/sql-json-comparison.csv test
```

Earlier SQL/JSON tables below describe the Rust route before this prototype, including
its former direct-projection admission. Current branch coverage is on
[Calc/filter](../operators/calc-filter.md#sqljson-evaluation).

## Spaced JSON paths diagnostic (2026-09-16)

Definite bracket paths with ASCII spaces now stay in native Calc instead of falling back.
On Apple M4 Pro, JDK 17, UTC and Flink 2.2.1, the release/mimalloc build measured the
following end-to-end medians over 2,000,000 rows at parallelism 1, with a 264-byte ASCII
payload budget, NULL every eighth row, two warmups and five measured trials per engine.
Both row/Arrow transposes are included and asserted in the native plan. Engines alternate
trial order; the source-matched identity control is reported without subtracting it.

| Case | Flink (s) | Native (s) | Flink / native |
|---|---:|---:|---:|
| Identity control | 0.822841 | 1.153666 | 0.713x |
| `JSON_VALUE(s, 'lax $[ ''user'' ][ ''name'' ]')` | 1.682775 | 1.248065 | 1.348x |
| `JSON_EXISTS(s, 'lax $[ ''user'' ][ ''name'' ]')` | 1.652583 | 1.202643 | 1.374x |

Reproduce with the command below, selecting
`-Dscalar.functions=JSON_VALUE_SPACED_PATH,JSON_EXISTS_SPACED_PATH` and
`-Dscalar.nullEvery=8`. These numbers compare newly admitted queries against their previous
Flink fallback; the change normalizes literal paths during planning and does not alter
native JSON parsing. They do not establish a speedup for already admitted compact paths.

## Empty JSON member diagnostic (2026-09-16)

Empty quoted names now use native JSON_VALUE/JSON_EXISTS. On Apple M4 Pro, JDK 17, UTC
and Flink 2.2.1, the release/mimalloc build measured these end-to-end medians over
2,000,000 rows at parallelism 1, with 264-byte ASCII padding, NULL every eighth row,
two warmups and five measured trials per engine. Both row/Arrow transposes are included
and asserted. Engine order alternates; the source-matched identity control is reported
without subtracting it. Documents alternate between an empty key containing `Alice`
and a space-only key, which does not match the selected path.

| Case | Flink (s) | Native (s) | Flink / native |
|---|---:|---:|---:|
| Identity control | 0.633615 | 1.102641 | 0.575x |
| `JSON_VALUE(s, 'lax $['''']')` | 1.559965 | 1.290161 | 1.209x |
| `JSON_EXISTS(s, 'lax $[""]')` | 1.574280 | 1.150832 | 1.368x |

Reproduce with the command below, selecting
`-Dscalar.functions=JSON_VALUE_EMPTY_MEMBER,JSON_EXISTS_EMPTY_MEMBER` and
`-Dscalar.nullEvery=8`. These results compare newly admitted expressions with their
previous Flink fallback. The change removes admission restrictions and reuses the existing
native member selectors; it does not speed up previously admitted paths.

## DECIMAL UDF consumer diagnostic (2026-09-16)

Measured against the retracting OFFSET implementation plus DECIMAL UDF consumer support,
using Apple M4 Pro, JDK 17, UTC, Flink 2.2.1 and the release `bench` profile with mimalloc.
The selection was `UDF_DECIMAL_IS_NULL,UDF_DECIMAL_NESTED`, with 2,000,000 rows,
two warmups and five alternating measured trials per engine. Both transposes and native
Calc substitution were asserted. The source cycles through `999.995`, `-999.995`,
`1.235`, `-1.235`, `0` and `9.99E+8`, with every eighth value NULL. The UDF declares
DECIMAL(5,2), so the inputs exercise rounding and overflow as well as ordinary values.

| Case | Flink (s) | Native (s) | Flink / native |
|---|---:|---:|---:|
| STRING identity control | 0.504798 | 1.106419 | 0.456x |
| `decimal_from_text(s) IS NULL` | 0.496819 | 1.205532 | 0.412x |
| `decimal_external(decimal_from_text(s))` | 0.519048 | 1.362920 | 0.381x |

These expressions are slower in isolation, including the identity control. The consumer
expression uses Flink-generated JVM code through the existing batch UDF bridge so that
external values, internal decimal overflow and generated null flags retain Flink's exact
semantics. This removes an admission blocker inside larger columnar pipelines; it is not
a Rust decimal-kernel optimization or evidence of a speedup for that composition.
The nested consumer returns DECIMAL(38,9). Controls are not subtracted from timings.
Reproduce with the command below, selecting
`-Dscalar.functions=UDF_DECIMAL_IS_NULL,UDF_DECIMAL_NESTED` and `-Dscalar.nullEvery=8`.

## Floating conversion and POWER diagnostic (2026-09-16)

Measured with Apple M4 Pro, JDK 17, UTC, Flink 2.2.1 and the release `bench` profile
with mimalloc. Each query processes 2,000,000 rows at parallelism 1, with two warmups and
five alternating trials per engine. Both transposes and native Calc are asserted; the
source and blackhole sink remain rowwise. No other local tests or benchmarks ran concurrently.

The scalar DECIMAL(38,9) source alternates `12345678901234567890.123456700` and
`-0.000000100`, with every eighth value NULL. Arrays contain both values and a NULL
element, with every eighth container NULL. POWER uses the existing BIGINT number fixture
with a runtime base cast to DOUBLE and exponent `0.5`. The three source-matched controls
run in the same JVM before the functions; controls are not subtracted from timings.

| Case | Flink (s) | Native (s) | Flink / native |
|---|---:|---:|---:|
| DECIMAL identity control | 0.550296 | 0.906067 | 0.607x |
| ARRAY<DECIMAL> identity control | 0.645095 | 1.420928 | 0.454x |
| BIGINT identity control | 0.613385 | 0.858960 | 0.714x |
| `CAST(n AS FLOAT)` | 0.825372 | 1.019472 | 0.810x |
| `CAST(a AS ARRAY<FLOAT>)` | 1.682023 | 1.603254 | 1.049x |
| `POWER(CAST(n AS DOUBLE), 0.5)` | 0.619829 | 1.071053 | 0.579x |

The array conversion has a small advantage in this workload; the scalar conversion and
POWER are slower. The conversions execute in Rust, while default POWER uses Flink-generated
JVM code to preserve its exact Math.pow contract. These results establish coverage costs,
not a general speedup for floating expressions or larger composed pipelines. Reproduce with
`-Dscalar.functions=DECIMAL_TO_FLOAT,DECIMAL_ARRAY_TO_FLOAT,POWER_EXACT` and
`-Dscalar.nullEvery=8` in the command below. CSV output quotes the declared result type so
precision/scale commas remain inside one field.

## Inputs

- Search strings add `row:`/`other:` and `:match`/`:miss` around the padding: total lengths are
  274/275 bytes for the 264-byte case and 18/19 bytes for the 8-byte case. Column needles and
  LOCATE start positions vary independently of the source text.
- String fixtures use a 264-byte payload budget. URL_DECODE and JSON_UNQUOTE repeat complete
  escape groups within that budget; JSON_UNQUOTE adds enclosing quotes. DECODE uses valid
  UTF-8 bytes; malformed input belongs to the semantic tests.
- The Unicode scenario also makes every eighth source value NULL, so Unicode and nullability
  effects are not isolated. Integer, date and timestamp values stay the same across the two
  scenarios; only nullability changes. UNHEX keeps ASCII hex input in both scenarios.
- TO_DATE reads ten-byte date strings. Calendar functions use TIMESTAMP(9) values spanning
  pre-epoch fractions, a leap day and a year boundary. LTRIM/RTRIM use a literal trim set.
- GREATEST/LEAST measure BIGINT inputs. ENCODE/DECODE measure UTF-8. These cases do not claim
  performance for other admitted types or charsets. Source fixtures are in
  `ScalarFunctionBenchmark` and `TextTimeBenchmarkInputs`.

## Reproduce

Use JDK 17 and run one timing process at a time:

```sh
TZ=UTC SF_BENCHMARK=true mvn -pl :streamfusion-runtime test -Pbench \
  '-Dtest=ScalarFunctionBenchmark#individualFunctions' \
  -Dscalar.engine=both -Dscalar.functions=SCALAR,SEARCH,ENCODING,TEXT_TIME \
  -Dscalar.rows=2000000 -Dscalar.warmup=2 -Dscalar.runs=5 \
  -Dscalar.bytes=264 -Dscalar.unicode=false -Dscalar.nullEvery=0 \
  -Dscalar.output=target/scalar-ascii264.csv
```

Repeat in a fresh JVM with `scalar.unicode=true` and `scalar.nullEvery=8`, using a different
output file. Then run `scalar.functions=SEARCH`, `scalar.bytes=8`, `scalar.unicode=false` and
`scalar.nullEvery=0` in another fresh JVM. A function name such as `STARTSWITH_LITERAL` can
select a single case, but a different selection has a different JIT history from the full run.

## Raw data

The current trials, source-matched identity controls, result medians, workload catalog and
checksums are attached to [PR #44](https://github.com/datafusion-contrib/StreamFusion/pull/44)
as `pr44-flink-native-benchmarks.zip`. The repository keeps benchmark code and final result
tables; generated CSVs are not versioned. Controls are retained for checking the run, and are
not subtracted from function times because their result types and lengths can differ.

## IFNULL coverage diagnostic (2026-09-16)

Measured against `ebe550c6` plus IFNULL support, using the release `bench` profile with
mimalloc, JDK 17, UTC and the same rowwise source/sink methodology above. The selection was
`IFNULL_STRING,IFNULL_BIGINT,IFNULL_DECIMAL`, with 2,000,000 rows, two warmups, five measured
trials, a 264-byte ASCII string budget and every eighth input NULL. Source-matched identity
controls ran in the same JVM before the functions; both transpose operators and native Calc
substitution were checked for every plan.

| Case | Flink (s) | Native (s) | Flink / native |
|---|---:|---:|---:|
| STRING identity control | 0.755 | 1.079 | 0.700x |
| BIGINT identity control | 0.259 | 0.419 | 0.618x |
| DECIMAL(38,9) identity control | 0.290 | 0.639 | 0.453x |
| `IFNULL(s, 'missing')` | 0.751 | 1.109 | 0.677x |
| `IFNULL(n, CAST(-1 AS BIGINT))` | 0.267 | 0.436 | 0.612x |
| `IFNULL(n, CAST(0 AS DECIMAL(38,9)))` | 0.283 | 0.642 | 0.441x |

These isolated projections are slower natively, including the identity controls. IFNULL adds
coverage so that a containing filter/Top-1 island can remain columnar; this measurement does
not establish an end-to-end speedup for that larger query. Treat it as a coverage prerequisite,
not a standalone scalar acceleration claim. Controls are not subtracted from function times.

## STRING to BOOLEAN coverage diagnostic (2026-09-16)

Measured against `ebe550c6` plus STRING-to-BOOLEAN support, using JDK 17, UTC and the
release `bench` profile with mimalloc. The source cycles through `true`, `FALSE`, `t`,
`0`, `yes`, `n`, with every eighth value NULL. The selection was `STRING_TO_BOOLEAN`,
with 2,000,000 rows, two warmups and five measured trials. Both transpose operators,
native Calc substitution and the source-matched identity control were checked.

| Case | Flink (s) | Native (s) | Flink / native |
|---|---:|---:|---:|
| STRING identity control | 0.307 | 0.497 | 0.618x |
| `CAST(s AS BOOLEAN)` | 0.316 | 0.473 | 0.667x |

This isolated conversion is slower natively with rowwise input/output. Its purpose is to
remove a cast coverage blocker inside larger native islands, where existing Arrow batches
avoid additional boundaries; these measurements do not claim a speedup for that composition.
The identity control returns STRING rather than BOOLEAN and is not subtracted from the cast.

## INSTR overloads diagnostic (2026-09-16)

Measured against `ebe550c6` plus extended INSTR support, with JDK 17, UTC and the release
`bench` profile with mimalloc. The selection was `INSTR3_COLUMN,INSTR4_FORWARD,INSTR4_REVERSE`,
with 2,000,000 rows, two warmups, five measured trials, a 264-byte ASCII padding budget and
every eighth source value NULL. Both transpose operators and native Calc substitutions were
checked. The existing search fixtures supply runtime needles/starts to the three-argument
case and repeated `x` padding to the forward/reverse third-occurrence cases.

| Case | Flink (s) | Native (s) | Flink / native |
|---|---:|---:|---:|
| Runtime needle/start identity control | 0.886 | 1.248 | 0.710x |
| Literal search identity control | 0.742 | 1.000 | 0.742x |
| `INSTR(s, needle, start_pos)` | 1.428 | 1.305 | 1.094x |
| `INSTR(s, 'x', 1, 3)` | 0.773 | 0.998 | 0.774x |
| `INSTR(s, 'x', -1, 3)` | 3.919 | 0.953 | 4.112x |

Reverse search benefits from avoiding Flink's reversed-string allocations. The short forward
search remains slower with rowwise input/output; these results do not establish a blanket
INSTR speedup. Controls return STRING rather than INT and are not subtracted from timings.

## Exact DECIMAL coverage diagnostic (2026-09-16)

Measured against `ebe550c6` plus exact decimal ROUND/literal/integer-cast support, using JDK 17,
UTC and the release `bench` profile with mimalloc. The selection was
`DECIMAL_ROUND_POS,DECIMAL_ROUND_NEG,DECIMAL_ROUND_EXPAND,DECIMAL_TO_BIGINT`, with 2,000,000
DECIMAL(38,9) source rows, two warmups, five measured trials and every eighth value NULL.
Both transpose operators and native Calc substitutions were checked. The source-matched
identity control ran in the same JVM before the functions.

| Case | Flink (s) | Native (s) | Flink / native |
|---|---:|---:|---:|
| DECIMAL(38,9) identity control | 0.272 | 0.616 | 0.441x |
| `ROUND(n, 2)` | 0.296 | 0.632 | 0.468x |
| `ROUND(n, -3)` | 0.319 | 0.636 | 0.502x |
| `ROUND(n, 12)` | 0.269 | 0.630 | 0.426x |
| `CAST(n AS BIGINT)` | 0.311 | 0.523 | 0.596x |

These isolated row-fed projections are slower natively, including the identity control. They
remove expression blockers from larger native islands, including casts above AVG(DECIMAL),
but this diagnostic does not establish an end-to-end speedup for those composed queries.
Controls are not subtracted from timings. Extreme negative ROUND positions using the JVM
upcall are covered by semantic tests rather than these timing cases.

## STARTSWITH

`STARTSWITH_LITERAL`: `STARTSWITH(s, 'row:')`; `STARTSWITH_COLUMN`: `STARTSWITH(s, needle)`

| Case | Input | Flink (s) | Native (s) | Flink / native |
|---|---|---:|---:|---:|
| `STARTSWITH_LITERAL` | ASCII, 264-byte budget | 0.447 | 0.766 | 0.58x |
| `STARTSWITH_COLUMN` | ASCII, 264-byte budget | 0.554 | 0.938 | 0.59x |
| `STARTSWITH_LITERAL` | Unicode, 264-byte budget, NULL/8 | 0.599 | 0.796 | 0.75x |
| `STARTSWITH_COLUMN` | Unicode, 264-byte budget, NULL/8 | 0.700 | 0.981 | 0.71x |
| `STARTSWITH_LITERAL` | ASCII, 8-byte padding | 0.375 | 0.556 | 0.67x |
| `STARTSWITH_COLUMN` | ASCII, 8-byte padding | 0.504 | 0.773 | 0.65x |

## ENDSWITH

`ENDSWITH_LITERAL`: `ENDSWITH(s, ':match')`; `ENDSWITH_COLUMN`: `ENDSWITH(s, needle)`

| Case | Input | Flink (s) | Native (s) | Flink / native |
|---|---|---:|---:|---:|
| `ENDSWITH_LITERAL` | ASCII, 264-byte budget | 0.445 | 0.759 | 0.59x |
| `ENDSWITH_COLUMN` | ASCII, 264-byte budget | 0.567 | 0.942 | 0.60x |
| `ENDSWITH_LITERAL` | Unicode, 264-byte budget, NULL/8 | 0.579 | 0.832 | 0.70x |
| `ENDSWITH_COLUMN` | Unicode, 264-byte budget, NULL/8 | 0.700 | 1.003 | 0.70x |
| `ENDSWITH_LITERAL` | ASCII, 8-byte padding | 0.380 | 0.552 | 0.69x |
| `ENDSWITH_COLUMN` | ASCII, 8-byte padding | 0.497 | 0.766 | 0.65x |

## INSTR

`INSTR_LITERAL`: `INSTR(s, ':match')`; `INSTR_COLUMN`: `INSTR(s, needle)`

| Case | Input | Flink (s) | Native (s) | Flink / native |
|---|---|---:|---:|---:|
| `INSTR_LITERAL` | ASCII, 264-byte budget | 1.461 | 0.768 | 1.90x |
| `INSTR_COLUMN` | ASCII, 264-byte budget | 1.622 | 0.945 | 1.72x |
| `INSTR_LITERAL` | Unicode, 264-byte budget, NULL/8 | 0.935 | 0.817 | 1.14x |
| `INSTR_COLUMN` | Unicode, 264-byte budget, NULL/8 | 1.107 | 1.012 | 1.09x |
| `INSTR_LITERAL` | ASCII, 8-byte padding | 0.425 | 0.579 | 0.73x |
| `INSTR_COLUMN` | ASCII, 8-byte padding | 0.556 | 0.793 | 0.70x |

## LOCATE

`LOCATE2_LITERAL`: `LOCATE(':match', s)`; `LOCATE2_COLUMN`: `LOCATE(needle, s)`; `LOCATE3_LITERAL`: `LOCATE(':match', s, start_pos)`; `LOCATE3_COLUMN`: `LOCATE(needle, s, start_pos)`

| Case | Input | Flink (s) | Native (s) | Flink / native |
|---|---|---:|---:|---:|
| `LOCATE2_LITERAL` | ASCII, 264-byte budget | 1.427 | 0.737 | 1.94x |
| `LOCATE2_COLUMN` | ASCII, 264-byte budget | 1.520 | 0.935 | 1.63x |
| `LOCATE3_LITERAL` | ASCII, 264-byte budget | 1.437 | 0.854 | 1.68x |
| `LOCATE3_COLUMN` | ASCII, 264-byte budget | 1.586 | 1.048 | 1.51x |
| `LOCATE2_LITERAL` | Unicode, 264-byte budget, NULL/8 | 0.963 | 0.839 | 1.15x |
| `LOCATE2_COLUMN` | Unicode, 264-byte budget, NULL/8 | 1.093 | 0.990 | 1.10x |
| `LOCATE3_LITERAL` | Unicode, 264-byte budget, NULL/8 | 1.023 | 1.015 | 1.01x |
| `LOCATE3_COLUMN` | Unicode, 264-byte budget, NULL/8 | 1.151 | 1.222 | 0.94x |
| `LOCATE2_LITERAL` | ASCII, 8-byte padding | 0.440 | 0.607 | 0.72x |
| `LOCATE2_COLUMN` | ASCII, 8-byte padding | 0.544 | 0.790 | 0.69x |
| `LOCATE3_LITERAL` | ASCII, 8-byte padding | 0.453 | 0.653 | 0.69x |
| `LOCATE3_COLUMN` | ASCII, 8-byte padding | 0.586 | 0.860 | 0.68x |

## BIN

`BIN_TINYINT`: `BIN(n)`; `BIN_SMALLINT`: `BIN(n)`; `BIN_INTEGER`: `BIN(n)`; `BIN_BIGINT`: `BIN(n)`

| Case | Input | Flink (s) | Native (s) | Flink / native |
|---|---|---:|---:|---:|
| `BIN_TINYINT` | non-null | 0.335 | 0.472 | 0.71x |
| `BIN_SMALLINT` | non-null | 0.336 | 0.468 | 0.72x |
| `BIN_INTEGER` | non-null | 0.341 | 0.469 | 0.73x |
| `BIN_BIGINT` | non-null | 0.331 | 0.476 | 0.70x |
| `BIN_TINYINT` | NULL/8 | 0.321 | 0.478 | 0.67x |
| `BIN_SMALLINT` | NULL/8 | 0.324 | 0.468 | 0.69x |
| `BIN_INTEGER` | NULL/8 | 0.332 | 0.464 | 0.71x |
| `BIN_BIGINT` | NULL/8 | 0.325 | 0.468 | 0.69x |

## HEX

`HEX_STRING`: `HEX(s)`; `HEX_TINYINT`: `HEX(n)`; `HEX_SMALLINT`: `HEX(n)`; `HEX_INTEGER`: `HEX(n)`; `HEX_BIGINT`: `HEX(n)`

| Case | Input | Flink (s) | Native (s) | Flink / native |
|---|---|---:|---:|---:|
| `HEX_TINYINT` | non-null | 0.335 | 0.436 | 0.77x |
| `HEX_SMALLINT` | non-null | 0.338 | 0.432 | 0.78x |
| `HEX_INTEGER` | non-null | 0.344 | 0.430 | 0.80x |
| `HEX_BIGINT` | non-null | 0.332 | 0.440 | 0.75x |
| `HEX_STRING` | ASCII, 264-byte budget | 1.348 | 1.057 | 1.27x |
| `HEX_TINYINT` | NULL/8 | 0.321 | 0.438 | 0.73x |
| `HEX_SMALLINT` | NULL/8 | 0.325 | 0.434 | 0.75x |
| `HEX_INTEGER` | NULL/8 | 0.328 | 0.434 | 0.76x |
| `HEX_BIGINT` | NULL/8 | 0.314 | 0.428 | 0.73x |
| `HEX_STRING` | Unicode, 264-byte budget, NULL/8 | 1.857 | 1.103 | 1.68x |

## TO_BASE64

`TO_BASE64`: `TO_BASE64(s)`

| Case | Input | Flink (s) | Native (s) | Flink / native |
|---|---|---:|---:|---:|
| `TO_BASE64` | ASCII, 264-byte budget | 0.657 | 0.963 | 0.68x |
| `TO_BASE64` | Unicode, 264-byte budget, NULL/8 | 0.840 | 1.037 | 0.81x |

## UNHEX

`UNHEX`: `UNHEX(s)`

| Case | Input | Flink (s) | Native (s) | Flink / native |
|---|---|---:|---:|---:|
| `UNHEX` | 528 hex bytes, non-null | 1.203 | 1.295 | 0.93x |
| `UNHEX` | 528 hex bytes, NULL/8 | 1.130 | 1.063 | 1.06x |

## GREATEST

`GREATEST`: `GREATEST(n, m, 17)`

| Case | Input | Flink (s) | Native (s) | Flink / native |
|---|---|---:|---:|---:|
| `GREATEST` | non-null | 0.308 | 0.471 | 0.65x |
| `GREATEST` | NULL/8 | 0.287 | 0.453 | 0.63x |

## LEAST

`LEAST`: `LEAST(n, m, 17)`

| Case | Input | Flink (s) | Native (s) | Flink / native |
|---|---|---:|---:|---:|
| `LEAST` | non-null | 0.294 | 0.449 | 0.65x |
| `LEAST` | NULL/8 | 0.276 | 0.453 | 0.61x |

## INITCAP

`INITCAP`: `INITCAP(s)`

| Case | Input | Flink (s) | Native (s) | Flink / native |
|---|---|---:|---:|---:|
| `INITCAP` | ASCII, 264-byte budget | 1.347 | 1.266 | 1.06x |
| `INITCAP` | Unicode, 264-byte budget, NULL/8 | 1.812 | 1.463 | 1.24x |

## TRANSLATE

`TRANSLATE`: `TRANSLATE(s, 'abcdef', 'ABCDEF')`

| Case | Input | Flink (s) | Native (s) | Flink / native |
|---|---|---:|---:|---:|
| `TRANSLATE` | ASCII, 264-byte budget | 2.377 | 1.350 | 1.76x |
| `TRANSLATE` | Unicode, 264-byte budget, NULL/8 | 3.138 | 1.240 | 2.53x |

## BTRIM

`BTRIM`: `BTRIM(s)`

| Case | Input | Flink (s) | Native (s) | Flink / native |
|---|---|---:|---:|---:|
| `BTRIM` | ASCII, 264-byte budget | 0.566 | 0.868 | 0.65x |
| `BTRIM` | Unicode, 264-byte budget, NULL/8 | 0.728 | 0.937 | 0.78x |

## ELT

`ELT`: `ELT(i, s, t)`

| Case | Input | Flink (s) | Native (s) | Flink / native |
|---|---|---:|---:|---:|
| `ELT` | ASCII, 264-byte budget | 0.753 | 1.342 | 0.56x |
| `ELT` | Unicode, 264-byte budget, NULL/8 | 1.112 | 1.575 | 0.71x |

## URL_ENCODE

`URL_ENCODE`: `URL_ENCODE(s)`

| Case | Input | Flink (s) | Native (s) | Flink / native |
|---|---|---:|---:|---:|
| `URL_ENCODE` | ASCII, 264-byte budget | 1.450 | 1.285 | 1.13x |
| `URL_ENCODE` | Unicode, 264-byte budget, NULL/8 | 4.400 | 1.448 | 3.04x |

## OVERLAY

`OVERLAY`: `OVERLAY(s PLACING t FROM i FOR n)`

| Case | Input | Flink (s) | Native (s) | Flink / native |
|---|---|---:|---:|---:|
| `OVERLAY` | ASCII, 264-byte budget | 0.834 | 1.197 | 0.70x |
| `OVERLAY` | Unicode, 264-byte budget, NULL/8 | 1.201 | 1.415 | 0.85x |

## URL_DECODE

`URL_DECODE`: `URL_DECODE(s)`

| Case | Input | Flink (s) | Native (s) | Flink / native |
|---|---|---:|---:|---:|
| `URL_DECODE` | ASCII, 264-byte budget | 1.713 | 1.507 | 1.14x |
| `URL_DECODE` | Unicode, 264-byte budget, NULL/8 | 1.521 | 1.257 | 1.21x |

## ENCODE

`ENCODE_UTF8`: `ENCODE(s, 'UTF-8')`

| Case | Input | Flink (s) | Native (s) | Flink / native |
|---|---|---:|---:|---:|
| `ENCODE_UTF8` | ASCII, 264-byte budget | 0.454 | 0.755 | 0.60x |
| `ENCODE_UTF8` | Unicode, 264-byte budget, NULL/8 | 0.900 | 0.821 | 1.10x |

## DECODE

`DECODE_UTF8`: `DECODE(b, 'UTF-8')`

| Case | Input | Flink (s) | Native (s) | Flink / native |
|---|---|---:|---:|---:|
| `DECODE_UTF8` | ASCII, 264-byte budget | 0.442 | 0.657 | 0.67x |
| `DECODE_UTF8` | Unicode, 264-byte budget, NULL/8 | 0.846 | 0.735 | 1.15x |

## JSON_QUOTE

`JSON_QUOTE`: `JSON_QUOTE(s)`

| Case | Input | Flink (s) | Native (s) | Flink / native |
|---|---|---:|---:|---:|
| `JSON_QUOTE` | ASCII, 264-byte budget | 1.550 | 1.570 | 0.99x |
| `JSON_QUOTE` | Unicode, 264-byte budget, NULL/8 | 26.048 | 2.972 | 8.77x |

## JSON_UNQUOTE

`JSON_UNQUOTE`: `JSON_UNQUOTE(s)`

| Case | Input | Flink (s) | Native (s) | Flink / native |
|---|---|---:|---:|---:|
| `JSON_UNQUOTE` | ASCII, 264-byte budget | 1.615 | 1.317 | 1.23x |
| `JSON_UNQUOTE` | Unicode, 264-byte budget, NULL/8 | 1.719 | 1.312 | 1.31x |

## SPLIT

`SPLIT`: `SPLIT(s, '|')`

| Case | Input | Flink (s) | Native (s) | Flink / native |
|---|---|---:|---:|---:|
| `SPLIT` | ASCII, 264-byte budget | 3.241 | 6.581 | 0.49x |
| `SPLIT` | Unicode, 264-byte budget, NULL/8 | 2.535 | 4.821 | 0.53x |

## SUBSTRING

`SUBSTRING_DYNAMIC`: `SUBSTRING(s, n, len)`

| Case | Input | Flink (s) | Native (s) | Flink / native |
|---|---|---:|---:|---:|
| `SUBSTRING_DYNAMIC` | ASCII, 264-byte budget | 0.760 | 0.929 | 0.82x |
| `SUBSTRING_DYNAMIC` | Unicode, 264-byte budget, NULL/8 | 0.937 | 1.005 | 0.93x |

## LEFT

`LEFT_DYNAMIC`: `LEFT(s, n)`

| Case | Input | Flink (s) | Native (s) | Flink / native |
|---|---|---:|---:|---:|
| `LEFT_DYNAMIC` | ASCII, 264-byte budget | 0.615 | 0.880 | 0.70x |
| `LEFT_DYNAMIC` | Unicode, 264-byte budget, NULL/8 | 0.800 | 0.967 | 0.83x |

## RIGHT

`RIGHT_DYNAMIC`: `RIGHT(s, n)`

| Case | Input | Flink (s) | Native (s) | Flink / native |
|---|---|---:|---:|---:|
| `RIGHT_DYNAMIC` | ASCII, 264-byte budget | 0.822 | 0.864 | 0.95x |
| `RIGHT_DYNAMIC` | Unicode, 264-byte budget, NULL/8 | 0.983 | 0.912 | 1.08x |

## LPAD

`LPAD_DYNAMIC`: `LPAD(s, n, p)`

| Case | Input | Flink (s) | Native (s) | Flink / native |
|---|---|---:|---:|---:|
| `LPAD_DYNAMIC` | ASCII, 264-byte budget | 0.878 | 1.123 | 0.78x |
| `LPAD_DYNAMIC` | Unicode, 264-byte budget, NULL/8 | 1.199 | 1.400 | 0.86x |

## RPAD

`RPAD_DYNAMIC`: `RPAD(s, n, p)`

| Case | Input | Flink (s) | Native (s) | Flink / native |
|---|---|---:|---:|---:|
| `RPAD_DYNAMIC` | ASCII, 264-byte budget | 0.815 | 1.173 | 0.70x |
| `RPAD_DYNAMIC` | Unicode, 264-byte budget, NULL/8 | 1.189 | 1.410 | 0.84x |

## SPLIT_INDEX

`SPLIT_INDEX_DYNAMIC`: `SPLIT_INDEX(s, p, n)`

| Case | Input | Flink (s) | Native (s) | Flink / native |
|---|---|---:|---:|---:|
| `SPLIT_INDEX_DYNAMIC` | ASCII, 264-byte budget | 2.099 | 1.065 | 1.97x |
| `SPLIT_INDEX_DYNAMIC` | Unicode, 264-byte budget, NULL/8 | 2.029 | 1.121 | 1.81x |

## TO_DATE

`TO_DATE`: `TO_DATE(s)`

| Case | Input | Flink (s) | Native (s) | Flink / native |
|---|---|---:|---:|---:|
| `TO_DATE` | date strings, non-null | 0.427 | 0.529 | 0.81x |
| `TO_DATE` | date strings, NULL/8 | 0.435 | 0.544 | 0.80x |

## QUARTER

`QUARTER`: `QUARTER(ts)`

| Case | Input | Flink (s) | Native (s) | Flink / native |
|---|---|---:|---:|---:|
| `QUARTER` | TIMESTAMP(9), non-null | 0.368 | 0.434 | 0.85x |
| `QUARTER` | TIMESTAMP(9), NULL/8 | 0.378 | 0.460 | 0.82x |

## WEEK

`WEEK`: `WEEK(ts)`

| Case | Input | Flink (s) | Native (s) | Flink / native |
|---|---|---:|---:|---:|
| `WEEK` | TIMESTAMP(9), non-null | 0.367 | 0.452 | 0.81x |
| `WEEK` | TIMESTAMP(9), NULL/8 | 0.385 | 0.472 | 0.81x |

## DAYOFYEAR

`DAYOFYEAR`: `DAYOFYEAR(ts)`

| Case | Input | Flink (s) | Native (s) | Flink / native |
|---|---|---:|---:|---:|
| `DAYOFYEAR` | TIMESTAMP(9), non-null | 0.370 | 0.439 | 0.84x |
| `DAYOFYEAR` | TIMESTAMP(9), NULL/8 | 0.373 | 0.464 | 0.80x |

## DAYOFWEEK

`DAYOFWEEK`: `DAYOFWEEK(ts)`

| Case | Input | Flink (s) | Native (s) | Flink / native |
|---|---|---:|---:|---:|
| `DAYOFWEEK` | TIMESTAMP(9), non-null | 0.364 | 0.420 | 0.87x |
| `DAYOFWEEK` | TIMESTAMP(9), NULL/8 | 0.382 | 0.444 | 0.86x |

## LTRIM

`LTRIM_LITERAL_SET`: `LTRIM(s, ' |ab')`

| Case | Input | Flink (s) | Native (s) | Flink / native |
|---|---|---:|---:|---:|
| `LTRIM_LITERAL_SET` | ASCII, 264-byte budget | 0.756 | 1.107 | 0.68x |
| `LTRIM_LITERAL_SET` | Unicode, 264-byte budget, NULL/8 | 0.851 | 1.072 | 0.79x |

## RTRIM

`RTRIM_LITERAL_SET`: `RTRIM(s, ' |ab')`

| Case | Input | Flink (s) | Native (s) | Flink / native |
|---|---|---:|---:|---:|
| `RTRIM_LITERAL_SET` | ASCII, 264-byte budget | 1.923 | 0.990 | 1.94x |
| `RTRIM_LITERAL_SET` | Unicode, 264-byte budget, NULL/8 | 1.581 | 1.063 | 1.49x |

## SQL/JSON measurements

Measured on 2026-09-10 with the same release profile, JDK 17, Flink/DataFusion versions,
2,000,000 rows, parallelism 1, two warmups, five measured trials, interleaved engines and
both transposes described above. Each scenario starts a fresh JVM; no other test or benchmark
runs concurrently. Both functions use default admission, including synchronization with
Jackson's actual recycled input-buffer capacity; no compatibility flags are enabled.

The input alternates between a document containing `user.name` and a document without that
member. The byte budget controls a separate padding string, excluding JSON syntax and other
fields. Unicode input also includes an escaped newline in the selected name; every eighth
source value is SQL NULL. Each query evaluates one function, using the literal path
`lax $.user.name`. Identity controls use the same source and are not included in the tables.

The multi-member scenarios add 16 or 64 short string members (`field0: value0`, and so on)
between `user` and `padding`. They distinguish repeated key/value parsing from scanning one
long string. Both shapes retain full input validation and last-duplicate-member semantics.
The native reader selects its protected SIMD path for these multi-member scenarios and the
streaming path for the padding-only scenarios. See the [parsing technique](../optimizations/sql-json-parsing.md).

```sh
TZ=UTC SF_BENCHMARK=true mvn -pl :streamfusion-runtime test -Pbench \
  '-Dtest=ScalarFunctionBenchmark#individualFunctions' \
  -Dscalar.engine=both -Dscalar.functions=JSON_VALUE,JSON_EXISTS \
  -Dscalar.rows=2000000 -Dscalar.warmup=2 -Dscalar.runs=5 \
  -Dscalar.bytes=264 -Dscalar.json.fields=0 \
  -Dscalar.unicode=false -Dscalar.nullEvery=0 \
  -Dscalar.output=target/json-functions.csv
```

Repeat with 32 and 1024 ASCII padding bytes, and with 264 bytes plus `scalar.unicode=true`
and `scalar.nullEvery=8`. For multi-member documents, use 32 padding bytes and
`scalar.json.fields=16` or `64`; repeat the 64-member case with Unicode and NULL/8.
Raw trial output stays in the local target directory; the tables contain only the final
Flink/native medians, with no intermediate optimization results.

## JSON_VALUE

`JSON_VALUE(s, 'lax $.user.name')`

| Scenario | Flink (s) | Native (s) | Flink / Native |
|---|---:|---:|---:|
| ASCII, 32-byte padding | 1.212 | 0.831 | 1.46x |
| ASCII, 264-byte padding | 1.796 | 1.311 | 1.37x |
| ASCII, 1024-byte padding | 3.648 | 2.962 | 1.23x |
| Unicode, 264-byte padding, NULL/8 | 1.439 | 1.108 | 1.30x |
| 16 extra members, ASCII, 32-byte padding | 2.805 | 1.786 | 1.57x |
| 64 extra members, ASCII, 32-byte padding | 8.067 | 4.595 | 1.76x |
| 64 extra members, Unicode, 32-byte padding, NULL/8 | 6.429 | 3.321 | 1.94x |

## JSON_EXISTS

`JSON_EXISTS(s, 'lax $.user.name')`

| Scenario | Flink (s) | Native (s) | Flink / Native |
|---|---:|---:|---:|
| ASCII, 32-byte padding | 1.182 | 0.828 | 1.43x |
| ASCII, 264-byte padding | 1.766 | 1.287 | 1.37x |
| ASCII, 1024-byte padding | 3.621 | 3.211 | 1.13x |
| Unicode, 264-byte padding, NULL/8 | 1.438 | 1.091 | 1.32x |
| 16 extra members, ASCII, 32-byte padding | 2.773 | 1.772 | 1.56x |
| 64 extra members, ASCII, 32-byte padding | 8.115 | 4.576 | 1.77x |
| 64 extra members, Unicode, 32-byte padding, NULL/8 | 6.464 | 3.315 | 1.95x |

## TO_BASE64 binary input

Measured on 2026-09-10 with the release profile, 2,000,000 rows, two warmups and
five interleaved trials, including both transposes. Run with
`scalar.functions=TO_BASE64_BINARY`. The `tt_bytes` fixture supplies binary columns
containing the ASCII/Unicode payload bytes; encoding is the only measured function.

| Scenario | Flink (s) | Native (s) |
|---|---:|---:|
| ASCII, 32-byte budget | 0.357 | 0.495 |
| ASCII, 264-byte budget | 0.499 | 0.767 |
| Unicode, 264-byte budget, NULL/8 | 0.485 | 0.753 |

## ENCODE UTF-16 charsets

Measured on 2026-09-10 with the same release, interleaved 2,000,000-row method,
two warmups, five measured trials and both transposes. Each charset runs separately
over `tt_text`; select `ENCODE_UTF16`, `ENCODE_UTF16BE`, or `ENCODE_UTF16LE`.

| Function | Scenario | Flink (s) | Native (s) |
|---|---|---:|---:|
| ENCODE_UTF16 | ASCII, 32-byte budget | 0.440 | 0.627 |
| ENCODE_UTF16BE | ASCII, 32-byte budget | 0.454 | 0.688 |
| ENCODE_UTF16LE | ASCII, 32-byte budget | 0.540 | 0.689 |
| ENCODE_UTF16 | ASCII, 264-byte budget | 0.998 | 1.537 |
| ENCODE_UTF16BE | ASCII, 264-byte budget | 0.997 | 1.544 |
| ENCODE_UTF16LE | ASCII, 264-byte budget | 1.599 | 1.484 |
| ENCODE_UTF16 | Unicode, 264-byte budget, NULL/8 | 1.889 | 1.242 |
| ENCODE_UTF16BE | Unicode, 264-byte budget, NULL/8 | 1.222 | 1.222 |
| ENCODE_UTF16LE | Unicode, 264-byte budget, NULL/8 | 2.063 | 1.227 |

## DECODE UTF-16 charsets

Measured on 2026-09-10 with the release profile, 2,000,000 rows, two warmups,
five interleaved trials and both transposes. Run each of `DECODE_UTF16`,
`DECODE_UTF16BE`, and `DECODE_UTF16LE` independently. Source fixtures pre-encode
text in the matching charset, so only DECODE is measured. The budgets describe
the original text in UTF-8; actual binary input uses UTF-16 code units plus a
BOM for UTF-16. Non-null benchmark inputs are valid encoded text.

| Function | Scenario | Flink (s) | Native (s) |
|---|---|---:|---:|
| DECODE_UTF16 | ASCII, 32-byte text budget | 0.423 | 0.526 |
| DECODE_UTF16BE | ASCII, 32-byte text budget | 0.441 | 0.514 |
| DECODE_UTF16LE | ASCII, 32-byte text budget | 0.557 | 0.507 |
| DECODE_UTF16 | ASCII, 264-byte text budget | 1.020 | 1.342 |
| DECODE_UTF16BE | ASCII, 264-byte text budget | 1.112 | 1.322 |
| DECODE_UTF16LE | ASCII, 264-byte text budget | 1.318 | 1.389 |
| DECODE_UTF16 | Unicode, 264-byte text budget, NULL/8 | 1.025 | 0.975 |
| DECODE_UTF16BE | Unicode, 264-byte text budget, NULL/8 | 1.141 | 0.976 |
| DECODE_UTF16LE | Unicode, 264-byte text budget, NULL/8 | 1.311 | 1.001 |

## JSON_STRING scalars

Measured on 2026-09-10 with the release profile, 2,000,000 rows, two warmups,
five interleaved trials and both transposes. `JSON_STRING_TEXT` uses `tt_text`;
`JSON_STRING_BOOLEAN` alternates boolean values; `JSON_STRING_INTEGER` uses the
BIGINT fixture. Each query contains one JSON_STRING call. Byte budgets affect only
text, so boolean/integer results list non-null and NULL/8 inputs once each.

| Input scenario | Flink (s) | Native (s) |
|---|---:|---:|
| STRING, ASCII, 32-byte budget | 0.605 | 0.644 |
| STRING, ASCII, 264-byte budget | 1.529 | 1.513 |
| STRING, Unicode, 264-byte budget, NULL/8 | 1.176 | 1.262 |
| BOOLEAN, non-null | 0.424 | 0.470 |
| BOOLEAN, NULL/8 | 0.386 | 0.464 |
| BIGINT, non-null | 0.456 | 0.487 |
| BIGINT, NULL/8 | 0.402 | 0.478 |

## JSON_OBJECT scalar values

Measured on 2026-09-10 with the release profile, 2,000,000 rows, two warmups,
five interleaved trials and both transposes. Run `JSON_OBJECT_NULL` or
`JSON_OBJECT_ABSENT` independently. Each query constructs one object from a text
column, a BIGINT row ordinal and an alternating BOOLEAN column, using literal
keys `text`, `id` and `flag`. The two functions differ only in NULL ON NULL versus
ABSENT ON NULL. In the NULL/8 scenario each column is NULL every eighth row, at
staggered positions; the byte budget describes the text column.

| Function | Scenario | Flink (s) | Native (s) |
|---|---|---:|---:|
| JSON_OBJECT_NULL | ASCII, 32-byte text budget | 1.099 | 0.863 |
| JSON_OBJECT_ABSENT | ASCII, 32-byte text budget | 1.102 | 0.863 |
| JSON_OBJECT_NULL | ASCII, 264-byte text budget | 2.009 | 1.742 |
| JSON_OBJECT_ABSENT | ASCII, 264-byte text budget | 2.036 | 1.774 |
| JSON_OBJECT_NULL | Unicode, 264-byte text budget, NULL/8 | 1.644 | 1.484 |
| JSON_OBJECT_ABSENT | Unicode, 264-byte text budget, NULL/8 | 1.598 | 1.475 |

## TRIM directions and literal sets

Measured on 2026-09-10 using the release profile and the interleaved, 2,000,000-row,
two-warmup/five-trial method above, including both transposes. Run with
`scalar.functions=TRIM_LEADING,TRIM_TRAILING,TRIM_LITERAL_SET` and the listed byte budgets.
The queries are `TRIM(LEADING FROM s)`, `TRIM(TRAILING FROM s)`, and
`TRIM(BOTH ' |ab' FROM s)`. Each query measures one function independently.

| Function | Scenario | Flink (s) | Native (s) | Flink / native |
|---|---|---:|---:|---:|
| TRIM_LEADING | ASCII, 32-byte budget | 0.377 | 0.543 | 0.69x |
| TRIM_LEADING | ASCII, 264-byte budget | 0.823 | 1.141 | 0.72x |
| TRIM_LEADING | Unicode, 264-byte budget, NULL/8 | 0.710 | 0.982 | 0.72x |
| TRIM_TRAILING | ASCII, 32-byte budget | 0.377 | 0.546 | 0.69x |
| TRIM_TRAILING | ASCII, 264-byte budget | 0.817 | 1.126 | 0.73x |
| TRIM_TRAILING | Unicode, 264-byte budget, NULL/8 | 0.680 | 0.965 | 0.70x |
| TRIM_LITERAL_SET | ASCII, 32-byte budget | 0.845 | 0.579 | 1.46x |
| TRIM_LITERAL_SET | ASCII, 264-byte budget | 2.911 | 1.154 | 2.52x |
| TRIM_LITERAL_SET | Unicode, 264-byte budget, NULL/8 | 1.605 | 0.985 | 1.63x |

## SHA1

`SHA1(s)` measured on 2026-09-10 with the same release, interleaved method as TRIM,
2,000,000 rows, two warmups and five trials, including both transposes. Run with
`scalar.functions=SHA1`. The input is the `tt_text` fixture.

| Scenario | Flink (s) | Native (s) | Flink / native |
|---|---:|---:|---:|
| ASCII, 32-byte budget | 0.837 | 0.697 | 1.20x |
| ASCII, 264-byte budget | 2.229 | 1.299 | 1.72x |
| Unicode, 264-byte budget, NULL/8 | 1.882 | 1.151 | 1.64x |

## IS JSON predicates

Measured on 2026-09-10 with the same release profile, 2,000,000 rows, two warmups,
five interleaved trials and both transposes. Run with
`scalar.functions=IS_JSON_VALUE,IS_JSON_OBJECT,IS_JSON_ARRAY,IS_JSON_SCALAR`.
Each predicate runs independently over the same mixture: object, array, string scalar,
and malformed object, in equal proportions before SQL NULL injection. Each document contains
a padding string with the listed byte budget. Malformed inputs account for 25% of all rows
and exercise Flink's exception-handling cost; these results are specific to that mixture.
The Unicode case replaces every eighth row with SQL NULL.

| Function | Scenario | Flink (s) | Native (s) | Flink / native |
|---|---|---:|---:|---:|
| IS_JSON_VALUE | ASCII, 32-byte padding | 1.608 | 0.629 | 2.56x |
| IS_JSON_OBJECT | ASCII, 32-byte padding | 1.600 | 0.625 | 2.56x |
| IS_JSON_ARRAY | ASCII, 32-byte padding | 1.645 | 0.625 | 2.63x |
| IS_JSON_SCALAR | ASCII, 32-byte padding | 1.648 | 0.624 | 2.64x |
| IS_JSON_VALUE | ASCII, 264-byte padding | 2.224 | 1.156 | 1.92x |
| IS_JSON_OBJECT | ASCII, 264-byte padding | 2.301 | 1.163 | 1.98x |
| IS_JSON_ARRAY | ASCII, 264-byte padding | 2.284 | 1.133 | 2.02x |
| IS_JSON_SCALAR | ASCII, 264-byte padding | 2.280 | 1.136 | 2.01x |
| IS_JSON_VALUE | Unicode, 264-byte padding, NULL/8 | 2.158 | 0.933 | 2.31x |
| IS_JSON_OBJECT | Unicode, 264-byte padding, NULL/8 | 2.181 | 0.968 | 2.25x |
| IS_JSON_ARRAY | Unicode, 264-byte padding, NULL/8 | 2.177 | 0.949 | 2.30x |
| IS_JSON_SCALAR | Unicode, 264-byte padding, NULL/8 | 2.201 | 0.942 | 2.33x |

## JSON_VALUE RETURNING

Measured on 2026-09-10 with the release, interleaved method above: 2,000,000 rows,
two warmups, five measured trials and both transposes. Run with
`scalar.functions=JSON_VALUE_BOOLEAN,JSON_VALUE_INTEGER,JSON_VALUE_DOUBLE`. Each query
projects `JSON_VALUE(s, '$.v' RETURNING <type>)` independently. Documents contain a
selected `v` member and a separate padding string with the listed byte budget.
BOOLEAN alternates true/false, INTEGER alternates 123456789/-234567890, and DOUBLE
alternates 1.23456789/-2.3456789e12. Unicode changes the padding; every eighth row is
SQL NULL in that scenario. All non-null documents are valid and contain a matching scalar.

| RETURNING | Scenario | Flink (s) | Native (s) | Flink / native |
|---|---|---:|---:|---:|
| BOOLEAN | ASCII, 32-byte padding | 0.862 | 0.698 | 1.24x |
| INTEGER | ASCII, 32-byte padding | 0.899 | 0.715 | 1.26x |
| DOUBLE | ASCII, 32-byte padding | 0.959 | 0.798 | 1.20x |
| BOOLEAN | ASCII, 264-byte padding | 1.472 | 1.226 | 1.20x |
| INTEGER | ASCII, 264-byte padding | 1.417 | 1.244 | 1.14x |
| DOUBLE | ASCII, 264-byte padding | 1.496 | 1.424 | 1.05x |
| BOOLEAN | Unicode, 264-byte padding, NULL/8 | 1.331 | 1.158 | 1.15x |
| INTEGER | Unicode, 264-byte padding, NULL/8 | 1.198 | 1.065 | 1.12x |
| DOUBLE | Unicode, 264-byte padding, NULL/8 | 1.296 | 1.158 | 1.12x |

## Temporal coverage diagnostic (2026-09-15)

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

## Byte characters and JSON extensions (2026-09-15)

These results use the implementation based on `ab4a21c6`, Apple M4 Pro, JDK 17, UTC,
Flink 2.2.1 and DataFusion 54.0.0. Each case/scenario runs in a fresh JVM with the standard
Maven `bench` profile (release + mimalloc), parallelism 1 and 2,000,000 rows. Flink/native
run serially and alternate order, with two warmups and five measured trials per engine.
Tables contain the final median elapsed seconds, including planning, the rowwise source
and sink, and both native transposes. No baseline time is subtracted. Other test suites
and native builds are stopped during measurements.

Small differences are subject to run-to-run variance. Standalone regressions remain
admitted to preserve verified semantics and native composition; the character changes
also correct existing DataFusion/Flink result differences. These are end-to-end results,
not isolated kernel timings or comparisons with an earlier native implementation.

Reproduce each named case separately, then repeat with `scalar.unicode=true` and
`scalar.nullEvery=8`. For integer and decimal inputs, that changes only nullability.
Generated trial CSVs remain local build output and are not versioned.

```sh
TZ=UTC SF_BENCHMARK=true mvn -pl :streamfusion-runtime test -Pbench \
  -Dnative.cargo.packages='-p streamfusion' \
  '-Dtest=ScalarFunctionBenchmark#individualFunctions' \
  -Dscalar.functions=ASCII -Dscalar.engine=both \
  -Dscalar.rows=2000000 -Dscalar.warmup=2 -Dscalar.runs=5 \
  -Dscalar.bytes=264 -Dscalar.unicode=false -Dscalar.nullEvery=0 \
  -Dscalar.output=target/scalar-ascii.csv
```

### ASCII

`ASCII`: `ASCII(s)` over 264-byte strings. ASCII inputs begin with `a`/`c`;
Unicode inputs begin with a Chinese character or accented letter, exercising signed bytes.

| Input | Flink (s) | Native (s) |
|---|---:|---:|
| ASCII, no NULLs | 0.753 | 1.017 |
| Unicode, NULL every eighth row | 0.532 | 0.774 |

### CHR

`CHR`: `CHR(n)` over BIGINT values, including negative values and repeated low bytes.

| Input | Flink (s) | Native (s) |
|---|---:|---:|
| BIGINT, no NULLs | 0.294 | 0.458 |
| BIGINT, NULL every eighth row | 0.292 | 0.431 |

### JSON_STRING DECIMAL

`JSON_STRING_DECIMAL`: `JSON_STRING(n)` over DECIMAL(38,9), alternating
`12345678901234567890.123456700` and `-0.000000100` (scientific notation in JSON).
The 264-byte string budget does not apply to decimal input.

| Input | Flink (s) | Native (s) |
|---|---:|---:|
| DECIMAL(38,9), no NULLs | 0.434 | 0.640 |
| DECIMAL(38,9), NULL every eighth row | 0.415 | 0.599 |

### JSON_OBJECT DECIMAL

`JSON_OBJECT_DECIMAL`: `JSON_OBJECT('n' VALUE n)` with the same two DECIMAL(38,9)
values and default NULL ON NULL policy. The 264-byte string budget does not apply.

| Input | Flink (s) | Native (s) |
|---|---:|---:|
| DECIMAL(38,9), no NULLs | 0.599 | 0.655 |
| DECIMAL(38,9), NULL every eighth row | 0.591 | 0.611 |

### Unicode JSON member paths

`JSON_VALUE_UNICODE_PATH` and `JSON_EXISTS_UNICODE_PATH` select `lax $.用户["姓.名"]`,
each in its own query. Both scenarios use Unicode member names. Documents alternate
between a selected string and a missing member, with a 264-byte padding-string budget;
the nullable scenario also uses Unicode selected values/padding and NULL every eighth row.

JSON_VALUE:

| Input | Flink (s) | Native (s) |
|---|---:|---:|
| ASCII values, no NULLs | 1.718 | 1.224 |
| Unicode values, NULL every eighth row | 1.451 | 1.107 |

JSON_EXISTS:

| Input | Flink (s) | Native (s) |
|---|---:|---:|
| ASCII values, no NULLs | 1.703 | 1.226 |
| Unicode values, NULL every eighth row | 1.445 | 1.097 |

### DECIMAL and VARBINARY scalar UDFs

Measured on Apple M4 Pro with JDK 17 and released Flink 2.2.1 on 2026-09-16. Each identity
UDF has its own source-matched control. The source produces two million rows at parallelism 1,
with NULL every eighth row; decimal input is `DECIMAL(38,9)` and binary input is 264 bytes.
Results are medians of five measured runs after two warmups, alternating engine order. The native
release build uses mimalloc, and the harness verifies both row/Arrow transposes before execution.

```sh
TZ=UTC SF_BENCHMARK=true mvn -pl streamfusion-runtime -am test -Pbench \
  '-Dtest=ScalarFunctionBenchmark#individualFunctions' \
  '-Dscalar.functions=UDF_DECIMAL,UDF_BINARY' \
  -Dscalar.rows=2000000 -Dscalar.warmup=2 -Dscalar.runs=5 \
  -Dscalar.bytes=264 -Dscalar.unicode=false -Dscalar.nullEvery=8 \
  -Dscalar.output=target/udf-exact-types.csv
```

| Expression | Flink (s) | Native (s) | Flink/native ratio |
|---|---:|---:|---:|
| DECIMAL identity control | 0.247 | 0.596 | 0.414x |
| Binary identity control | 0.282 | 0.529 | 0.534x |
| DECIMAL identity UDF | 0.249 | 0.657 | 0.379x |
| Binary identity UDF | 0.275 | 0.601 | 0.457x |

These simple UDFs remain slower than stock Flink. The change extends coverage so a supported
exact-type UDF can stay between native operators; it does not claim a standalone speedup.
The controls show the conversion cost before adding the JVM callback, and larger native islands
require their own measurements before claiming an end-to-end gain.

## FROM_UNIXTIME literal formats

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

## JSON string consumers

The surrogate-identity fix evaluates a JSON string producer and its scalar consumers together
in Flink-generated code within columnar Calc. This preserves correct comparisons before the
final Arrow conversion; it is a correctness change, not a faster JSON parsing kernel.

Release build (`-Pbench`), JDK 17, UTC, parallelism 1, 2m rows, NULL every eighth row, two
warmups and five interleaved measurements per engine. The runtime source alternates ASCII
JSON texts `"\uD800"` and `"?"`; their lengths are fixed at eight and three bytes. The generic
payload-budget flag does not pad this fixture. Both source and blackhole sink are rowwise,
and the benchmark asserts both transpose operators. No other tests ran during measurement.

| Query | Flink seconds | Native Calc with JVM expression seconds | Flink/native |
| --- | ---: | ---: | ---: |
| Source-matched identity control | 0.511876 | 1.061221 | 0.482x |
| `JSON_VALUE(s, '$') = '?'` | 0.771874 | 1.351576 | 0.571x |
| `JSON_UNQUOTE(s) = '?'` | 0.620493 | 1.203020 | 0.516x |

The fused path is slower than Flink in this short pipeline. The previous native comparisons
returned incorrect answers for the surrogate row, so their timings are not a valid performance
baseline for the corrected operation. Direct projections retain the existing Rust kernels.
These measurements do not justify a speed claim for a larger columnar pipeline.

```bash
TZ=UTC SF_BENCHMARK=true mvn -pl streamfusion-runtime -am test -Pbench \
  -Dtest=ScalarFunctionBenchmark -Dsurefire.failIfNoSpecifiedTests=false \
  -Dscalar.functions=JSON_VALUE_IDENTITY,JSON_UNQUOTE_IDENTITY \
  -Dscalar.rows=2000000 -Dscalar.nullEvery=8 -Dscalar.warmup=2 -Dscalar.runs=5 \
  -Dscalar.engine=both
```

## Negative JSON array indexes

`JSON_VALUE_NEGATIVE` and `JSON_EXISTS_NEGATIVE` select `$.a[-1]` from a 32-element
string array. `JSON_VALUE_POSITIVE_CONTROL` selects the same value with `$.a[31]`.
Documents alternate between final values `Alice` and `Bob`, with the usual 264-byte
padding-string budget. Negative indexes previously declined the native JSON kernel.

M4 Pro/JDK 17, release build (`-Pbench`, mimalloc), UTC, parallelism 1, 2m rows,
NULL every eighth row, two warmups and five interleaved measurements per engine:

| Query | Flink seconds | Native seconds | Flink/native |
| --- | ---: | ---: | ---: |
| Source-matched identity control | 0.774887 | 1.496905 | 0.518x |
| `JSON_VALUE(s, '$.a[-1]')` | 2.955556 | 3.951091 | 0.748x |
| `JSON_VALUE(s, '$.a[31]')` | 3.147889 | 3.069012 | 1.026x |
| `JSON_EXISTS(s, '$.a[-1]')` | 2.926447 | 3.831098 | 0.764x |

Both row/Arrow transposes are present and asserted. No other local tests ran during
measurement. The negative-index implementation extends native coverage, but is slower
than Flink in this short pipeline. The scalar reader counts the array in an extra pass;
the SIMD reader uses its existing array length. The positive-index control exposes the
additional cost on the same input. These numbers do not establish a speedup for negative
indexes or for a larger native pipeline.

```bash
TZ=UTC SF_BENCHMARK=true mvn -pl streamfusion-runtime -am test -Pbench \
  -Dtest=ScalarFunctionBenchmark -Dsurefire.failIfNoSpecifiedTests=false \
  -Dscalar.functions=JSON_VALUE_NEGATIVE,JSON_VALUE_POSITIVE_CONTROL,JSON_EXISTS_NEGATIVE \
  -Dscalar.rows=2000000 -Dscalar.nullEvery=8 -Dscalar.warmup=2 -Dscalar.runs=5 \
  -Dscalar.engine=both
```
