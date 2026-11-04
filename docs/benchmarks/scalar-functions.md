# Scalar function benchmarks

| Question | Start here |
| --- | --- |
| Is a function currently admitted? | [Calc coverage](../operators/calc-filter.md) |
| What is the recorded fixture and timing method? | [Inputs](#inputs), [reproduction](#reproduce), [raw data](#raw-data) |
| What was measured for collection and metadata extensions? | [Extended coverage](#extended-function-coverage) |
| Where are the SQL/JSON comparisons? | [SQL/JSON measurements](#sqljson-measurements), [nested boundaries](#nested-sqljson-batch-boundaries-2026-09-19), [string consumers](#json-string-consumers) |
| Which measurements are experiments or diagnostics? | [Evidence index](evidence.md), [array extrema experiment](#array-extrema-admission-experiment-2026-09-28), [BINARY exit experiments](#rejected-manual-binary-exit-projection) |

Each section records its own implementation and environment. Historical timings
and fallback observations remain evidence for that cohort; use the coverage page
for the current supported contract.

The original measurements below were made on 2026-09-09 against the production implementation in `69122d2b`, using Apple M4 Pro,
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

## Extended function coverage

These measurements were recorded on 2026-10-17. The collection and metadata extensions are
enabled by default under the maintainer's explicit acceptance of the measured regressions on
2026-10-17. Native Calc coverage
includes both Rust kernels and complete Flink-generated programs through the JVM batch bridge;
these are separate execution engines. Runtime UUID already uses the ordinary admitted path and
is unchanged by this extension. See [coverage and type gates](../operators/calc-filter.md#collection-expressions).

Measurements use Linux x86-64 on an Intel Core i7-12650H, JDK 17, Flink 2.2.1, release/mimalloc,
parallelism 1 and a 2 GiB test heap. Each case alternates stock Flink and StreamFusion, with two
warmups and five measured trials per engine. Both row/Arrow transposes, planning, row source and
blackhole sink remain in the elapsed time. Independent JVMs measure the candidate and previous
planner `3679d312`. The previous planner rejects these newly supported functions and executes the
host fallback. It shares the candidate's release DSO, but those fallback jobs do not call the new
kernels. No other tests or benchmarks run concurrently.

These are whole-job timings, including startup and planning; subsecond cases do not establish
sustained throughput. Paired stock controls and identity controls are retained without subtraction.
Stock medians can drift between JVMs, so previous/candidate differences are not a precise isolated
optimization measurement. The [raw trials](extended-functions-2026-10-17.csv) retain all five samples
and both paired stock controls.

The metadata workload uses `ScalarFunctionBenchmark`: two million rows, 264-byte payload budget,
`TYPEOF(n)` or `CURRENT_DATABASE()` alone, and `SUM(n)` grouped by the same expression. Pruning
removes the unused input from the standalone metadata constant, retaining row counts. Metadata
aggregates improve substantially, while standalone projections remain below Flink.

| Metadata case | Paired Flink (s) | Candidate (s) | Previous StreamFusion (s) | Flink / candidate |
| --- | ---: | ---: | ---: | ---: |
| Integer identity | 0.235114 | 0.287956 | 0.325156 | 0.816x |
| BIGINT identity | 0.245273 | 0.274623 | 0.291892 | 0.893x |
| TYPEOF | 0.315037 | 0.353870 | 0.280670 | 0.890x |
| CURRENT_DATABASE | 0.305233 | 0.351782 | 0.274232 | 0.868x |
| TYPEOF with aggregate | 0.676733 | 0.349822 | 0.672136 | 1.935x |
| CURRENT_DATABASE with aggregate | 0.689099 | 0.347060 | 0.642492 | 1.986x |

Collection fixtures use `DynamicCollectionBenchmark`: nullable BIGINT arrays at widths 8/64,
NULL containers every eight rows and NULL elements every seven positions. STRING join uses width
8, a 64-byte suffix and 200,000 rows; map fixtures contain three STRING/BIGINT entries. Other
cases use two million rows except width-64 sort at 500,000, plus separate ten-million-row
reverse and ROW-constructor cases. Runtime constructors use the nullable
input index. Inline object update projects its integer field so the sink has an ordinary SQL type.

| Collection case | Paired Flink (s) | Candidate (s) | Previous StreamFusion (s) | Flink / candidate |
| --- | ---: | ---: | ---: | ---: |
| CARDINALITY, array width 64 | 0.900079 | 1.084205 | 0.915184 | 0.830x |
| CARDINALITY, array width 8 | 0.374140 | 0.418935 | 0.362862 | 0.893x |
| CARDINALITY, map width 3 | 0.951833 | 0.923917 | 0.900526 | 1.030x |
| Append, width 8 | 0.488629 | 0.771157 | 0.456011 | 0.634x |
| ARRAY constructor | 0.506142 | 0.611204 | 0.408114 | 0.828x |
| Join strings, width 8 | 0.245785 | 0.406909 | 0.254226 | 0.604x |
| MAP constructor | 0.538187 | 0.782705 | 0.431044 | 0.688x |
| Map entries, width 3 | 1.291837 | 3.225086 | 1.177803 | 0.401x |
| Map keys, width 3 | 0.939283 | 2.051847 | 0.903832 | 0.458x |
| Inline object update, integer field | 0.388066 | 0.476386 | 0.371599 | 0.815x |
| Reverse, width 64 | 1.260770 | 1.646519 | 1.309363 | 0.766x |
| Reverse, width 8 | 0.469198 | 0.510902 | 0.431597 | 0.918x |
| Reverse, width 8, 10M rows | 2.138881 | 2.248478 | 1.820855 | 0.951x |
| ROW constructor | 0.499804 | 0.801584 | 0.423589 | 0.624x |
| ROW constructor, 10M rows | 2.285629 | 3.726311 | 1.829078 | 0.613x |
| Slice last two, width 64 | 0.951815 | 1.097785 | 0.926048 | 0.867x |
| Sort, width 64 | 1.342123 | 1.631030 | 1.349329 | 0.823x |

The ten-million-row cases show that reverse and row construction still lose beyond the shorter
runs. The 1.03x map-cardinality observation is small relative to control drift and does not establish
performance admission. All other standalone collection/constructor cases in this October 17
cohort are slower than stock. The [October 22 comparison](collections-392-2026-10-22.md)
records the subsequent standalone improvement pass for #392.
The maintainer explicitly accepted merging these slower paths enabled by default on 2026-10-17.
Both switches can be set false to restore host execution for these extensions. The [conversion optimization](../optimizations/collection-representation-work.md)
records both the initial overhead reduction and the subsequent measured improvements;
several standalone cases still lose to Flink.

The CSV labels `candidate`, `previous` and `candidate_initial` distinguish final code, previous
production fallback and the five cases rerun after optimization. Profiled runs are excluded.
The release DSO SHA-256 is `991cc2c091cf4e32ea6eb92be5d1114970bda791905ddcddabe09b973c467f26`.

| Case | `collection.expression` |
| --- | --- |
| Reverse | `ARRAY_REVERSE(arr)` |
| Slice | `ARRAY_SLICE(arr,-2,-1)` |
| Append | `ARRAY_APPEND(arr,CAST(idx AS BIGINT))` |
| Sort | `ARRAY_SORT(arr)` |
| Join | `ARRAY_JOIN(arr,'|','?')` |
| Map keys / entries | `MAP_KEYS(m)` / `MAP_ENTRIES(m)` |
| Array constructor | `ARRAY[CAST(idx AS BIGINT),CAST(NULL AS BIGINT)]` |
| Map constructor | `MAP[CAST(idx AS BIGINT),CAST(idx AS BIGINT)]` |
| Row constructor | `ROW(idx,idx+1)` |
| Cardinality | `CARDINALITY(arr)` / `CARDINALITY(m)` |

The object expression is `(OBJECT_UPDATE(OBJECT_OF('tech.streamfusion.FlinkObjectFunctionsSqlHarnessTest$Item',
'a',idx,'b',CAST(idx AS STRING)),'a',idx+1)).a`; the fixture class is included in the test sources.
Use `collection.map=true` for map-source cases, `collection.type=STRING` and `collection.bytes=64`
for join, and the row counts/widths stated above. Other source cases use `collection.map=false`.


Reproduce an individual collection case after building the release library:

The current harness retains operator metrics and requires consumed native Calc input for admitted
runs. It constructs a metrics-enabled MiniCluster before timing each job. Execution-time planning,
deployment, runtime row sources, both row/Arrow transposes, production JNI and the rowwise sink remain
inside timing; cluster construction, preliminary schema/explain checks and teardown are outside.
The October 17 tables above used the earlier harness and are historical observations. Compare fresh
stock and preceding/current StreamFusion cohorts using the same harness rather than subtracting
absolute times across those methods. Per-trial logs retain warmups and native execution counters.

```bash
SF_BENCHMARK=true mvn -Pbench -pl :streamfusion-runtime -am test \
  -Dnative.build.skip=true -Dtest=DynamicCollectionBenchmark \
  -Dsurefire.failIfNoSpecifiedTests=false -Dsf.extraJvmArgs=-Xmx2g \
  -Dstreamfusion.experimental.collectionExpressions=true \
  '-Dcollection.expression=ARRAY_REVERSE(arr)' -Dcollection.map=false \
  -Dcollection.type=BIGINT -Dcollection.width=8 -Dcollection.rows=2000000 \
  -Dcollection.warmup=2 -Dcollection.runs=5
```

For previous-planner controls, use the same harness/library with `-Dcollection.native=false`.
For metadata, use `-Dtest=ScalarFunctionBenchmark#individualFunctions`,
`-Dstreamfusion.experimental.metadataExpressions=true` and
`-Dscalar.functions=BASELINE_integer,BASELINE_bigint,TYPEOF,CURRENT_DATABASE,TYPEOF_GROUP,CURRENT_DATABASE_GROUP`.

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

## Nested SQL/JSON batch boundaries (2026-09-19)

The generated JVM Calc now accepts recursive ARRAY/ROW inputs and results through the
existing batch callback. This extends host-exact coverage inside a columnar plan; it does
not establish a JSON speedup. MAP/MULTISET boundaries still use planner fallback.

On Apple M1 Max, JDK 17 and Flink 2.2.1, each case processes 2,000,000 rows at parallelism 1
with the release/mimalloc native library. The rowwise source provides arrays containing a
264-byte ASCII string, a NULL element and `tail`, with the whole array NULL every seventh
row. The second column is `{"items":[1,2,3]}`. Two warmups and five measured trials per engine
alternate execution order. Plans require NativeCalc and both row/Arrow transposes, with a
rowwise blackhole sink. No other local tests or benchmarks ran concurrently. Whole-job
elapsed medians include planning; the identity control is not subtracted.

| Case | Flink (s) | StreamFusion (s) | Flink / StreamFusion |
|---|---:|---:|---:|
| Array identity control | 1.243185 | 1.968439 | 0.632x |
| `JSON_STRING(a)` | 2.774218 | 3.251184 | 0.853x |
| `ROW(a, JSON_QUERY(s, '$.items[*]'))` | 2.639063 | 4.891670 | 0.539x |

Both JSON cases are slower than their previous stock-Flink fallback in this row-fed
comparison. The benefit is a verified batch boundary that avoids adding another JSON
implementation and can compose with other columnar operators. A representative larger
pipeline comparison remains future work; these results do not establish that such a
pipeline is faster. The existing native JSON fast paths retain their current admission.

Raw trials: [nested JSON batch comparison](sql-json-nested-2026-09-19.csv).
`engine=native` means StreamFusion enabled, with Flink-generated JSON evaluation on the JVM.
Build the release/mimalloc library, then reproduce with:

```bash
SF_BENCHMARK=true mvn -B -ntp -Pbench -pl :streamfusion-runtime -am \
  -Dnative.build.skip=true '-Dtest=ScalarFunctionBenchmark#individualFunctions' \
  -Dsurefire.failIfNoSpecifiedTests=false \
  -Dscalar.functions=JSON_STRING_ARRAY,JSON_QUERY_ARRAY_RESULT \
  -Dscalar.rows=2000000 -Dscalar.bytes=264 -Dscalar.nullEvery=7 \
  -Dscalar.warmup=2 -Dscalar.runs=5 \
  -Dscalar.output=target/sql-json-nested.csv test
```

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

### Integer-Delimiter Split Index

The integer overload has a separate released-Flink contract and measurement. The direct
`SPLIT_INDEX(s, delimiter, idx)` query uses a character code in 1..255 rather than the
decimal rendering of an integer. Verified UTF-8 inputs and equivalent original Java
UTF-16 caches use the [bounded native scan](../optimizations/scalar-function-kernels.md#integer-delimiter-split-index).
Computed operands and non-equivalent original caches retain released Flink evaluation.

Measured on 2026-10-26 on Apple M4 Pro / 24 GiB, JDK 17, released Flink 2.2.1 and 1.18.1,
Rust 1.94.0 release with `--no-default-features`, against canonical main `ffafa92b`.
Each engine/profile starts a fresh JVM with a 256 MiB initial and 512 MiB maximum heap,
eight warmup trials and nine measured trials, processing 131,072 rows per trial. No
other build, test or benchmark runs concurrently. Profiles vary batch rows (16, 1,024,
16,384), payload budget (32/264 UTF-8 bytes), ASCII/CJK/emoji, original Java cache state,
and fixed/mixed field indices. Strings have cardinality 16; delimiters rotate comma,
pipe and U+00E9. Source, delimiter and index are NULL every 7th, 11th and 13th row.
Mixed indices include negative/out-of-range fields and invalid delimiter 256.

The portable `integer_split` release example measures the production compiled Calc,
output allocation and checksum consumption with a timer inside Rust. Stock Flink uses
its unchanged released generated expression on prepared RowData and consumes the same
output bytes with a Java timer. Initial/final JNI entry, C Data import/export and
perimeter transposes are outside this small-functionality boundary. The previous
production Calc retains its intrinsic JVM callback, Arrow round trip and allocation
inside timing. It is a separate binary linked to the unchanged main implementation,
using the same driver and Java reference, with the TYPE_STRING descriptor emitted
by the baseline planner. The earlier TYPE_INTERNAL control remains a separate diagnostic;
only the corrected descriptor is used for final previous-native comparisons. Before timing, the candidate's full output
array must equal that reference; its evaluator ID is invalid so a scalar JVM callback
fails the check. Cold controls use BinaryRowData/Arrow bytes, while cached controls
retain GenericRowData's Java strings and matching Arrow UTF-16 sidecars. Input setup
is outside all timers.

The [raw measurements](results/integer-split-index-2026-10-26.json) retain every measured
trial, medians, ranges, baseline library hashes, and earlier adaptation diagnostics.
The initial 16-row implementation was slower than stock; removing generic argument
adaptation, borrowing array arguments, passing only the source sidecar, reducing ASCII
cache comparisons and using a bounded byte search removed that measured regression.

Values are medians with `[minimum, maximum]` over nine trials:

| Flink | Corpus / payload | Java cache | Batch rows | Indices | Stock ns/row | Previous ns/row | Native ns/row | Stock/native |
| --- | --- | --- | ---: | --- | ---: | ---: | ---: | ---: |
| 2.2.1 | ascii / 32 | cold | 1024 | 1 | 103.43 [91.64, 108.57] | 187.09 [177.73, 193.31] | 13.24 [12.89, 14.37] | 7.81x |
| 2.2.1 | ascii / 264 | cold | 1024 | 1 | 340.09 [329.18, 349.19] | 479.08 [471.51, 490.86] | 17.78 [17.05, 18.98] | 19.13x |
| 2.2.1 | ascii / 264 | present | 1024 | 1 | 233.54 [222.75, 236.77] | 472.54 [462.24, 486.21] | 67.51 [65.10, 71.47] | 3.46x |
| 2.2.1 | cjk / 264 | cold | 1024 | 1 | 286.61 [280.41, 324.78] | 523.59 [513.30, 532.26] | 17.64 [17.09, 18.69] | 16.25x |
| 2.2.1 | cjk / 264 | present | 1024 | 1 | 190.08 [177.75, 192.98] | 506.43 [501.88, 527.52] | 112.34 [109.08, 117.04] | 1.69x |
| 2.2.1 | emoji / 264 | cold | 1024 | 1 | 313.08 [303.38, 352.37] | 583.21 [576.63, 598.73] | 17.64 [17.14, 18.14] | 17.75x |
| 2.2.1 | emoji / 264 | present | 1024 | 1 | 210.15 [198.55, 225.26] | 534.78 [528.17, 554.40] | 106.09 [105.58, 108.88] | 1.98x |
| 2.2.1 | ascii / 32 | present | 16 | 1 | 74.34 [72.21, 84.90] | 856.15 [841.49, 866.31] | 69.33 [66.46, 79.41] | 1.07x |
| 2.2.1 | ascii / 264 | present | 16384 | 1 | 245.38 [238.63, 275.09] | 457.36 [440.51, 476.26] | 67.99 [66.51, 69.83] | 3.61x |
| 2.2.1 | ascii / 264 | present | 1024 | mixed | 194.79 [192.34, 206.07] | 416.41 [405.55, 466.88] | 61.86 [60.93, 66.82] | 3.15x |
| 1.18.1 | ascii / 32 | cold | 1024 | 1 | 125.80 [114.01, 130.97] | 190.10 [176.04, 233.20] | 14.33 [13.18, 15.09] | 8.78x |
| 1.18.1 | ascii / 264 | cold | 1024 | 1 | 333.38 [319.49, 391.33] | 461.74 [454.26, 472.27] | 18.03 [17.94, 18.26] | 18.49x |
| 1.18.1 | ascii / 264 | present | 1024 | 1 | 225.03 [213.01, 236.39] | 453.75 [447.83, 471.43] | 75.49 [69.36, 78.42] | 2.98x |
| 1.18.1 | cjk / 264 | cold | 1024 | 1 | 290.07 [280.27, 313.48] | 496.14 [489.07, 501.57] | 17.68 [17.46, 17.86] | 16.41x |
| 1.18.1 | cjk / 264 | present | 1024 | 1 | 196.86 [187.04, 198.18] | 495.58 [487.61, 505.17] | 114.09 [111.68, 114.76] | 1.73x |
| 1.18.1 | emoji / 264 | cold | 1024 | 1 | 285.90 [284.23, 306.30] | 530.87 [526.82, 535.19] | 18.45 [18.28, 18.71] | 15.50x |
| 1.18.1 | emoji / 264 | present | 1024 | 1 | 220.33 [209.49, 250.26] | 548.73 [542.43, 554.88] | 107.95 [106.14, 109.60] | 2.04x |
| 1.18.1 | ascii / 32 | present | 16 | 1 | 74.04 [72.83, 85.23] | 842.07 [835.36, 884.99] | 74.04 [72.34, 75.01] | 1.00x |
| 1.18.1 | ascii / 264 | present | 16384 | 1 | 251.62 [238.71, 257.85] | 449.26 [434.94, 463.80] | 71.73 [70.45, 72.34] | 3.51x |
| 1.18.1 | ascii / 264 | present | 1024 | mixed | 183.08 [168.34, 191.90] | 405.80 [401.71, 417.17] | 64.10 [60.96, 70.52] | 2.86x |

The 16-row 1.18 control is tied with stock within observed variability (74.04 versus
74.04 ns/row); it does not establish a small-batch speedup. The regular-batch profiles
are 1.69-19.13x stock and at least 4.34x the preceding native callback path.

Reproduce the component candidate and stock controls after compiling test classes with
the selected released Flink profile:

```sh
cargo build --manifest-path native/Cargo.toml --release --no-default-features \
  -p streamfusion --example integer_split
# Set SF_NATIVE_BENCH_CLASSPATH to target/test-classes, target/classes and test dependencies.
export SF_NATIVE_LIB_DIR="$PWD/native/target/release"
# This directory must contain the matching release libstreamfusion DSO for JVM callbacks.
native/target/release/examples/integer_split ascii 264 true 1024 128 false native 8 9
native/target/release/examples/integer_split ascii 264 true 1024 128 false flink 8 9
```

For the previous implementation, compile the same example against main `ffafa92b`,
using its unchanged production Rust library and matching native DSO, and select
`baseline` rather than `native`. `IntegerSplitBenchmarkFunctions` must be on its test
classpath. This is a native-baseline ablation, not a Java helper pretending to be the
old native implementation.

The separate `IntegerSplitPipelineBenchmark` diagnostic retains a row source, both
production transposes and a blackhole row sink. It processes 2,000,000 rows, parallelism
1, a 264-byte ASCII payload, three delimiters and NULL every 7th row, with 2 GiB heap,
two warmups and five measured jobs per engine in fresh JVMs. Plan assertions require
both transposes, and runtime metrics require consumed native Calc rows. Its job timer
includes query planning/submission and startup after environment setup, so it is not
a sustained-throughput measurement. Output equality is established separately by the
runtime-source SQL tests and component array comparison; a blackhole sink alone cannot
establish equality. Run it with `SF_BENCHMARK=true mvn test -Pbench
-Dtest=IntegerSplitPipelineBenchmark`, selecting `-Dsplit.pipeline.engine=stock` or
`native` and building each production baseline in its own checkout.

| Engine | Job median (s) | Minimum-maximum (s) |
| --- | ---: | ---: |
| stock | 1.828223 | 1.795710-1.863590 |
| previous | 2.460203 | 2.427904-2.485643 |
| native | 0.951705 | 0.942732-0.966414 |

The candidate is 1.92x stock and 2.59x the preceding native pipeline in this
startup-inclusive fixture. This result applies to this final STRING-output query;
it does not establish arbitrary connector or steady-state throughput.

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

## Runtime SHA2 Widths

`SHA2(s, bits)` with direct STRING/INT operands uses the same RustCrypto digest and
lowercase hex writer as fixed SHA-2 functions. A batch verifies non-NULL widths and
reserves exactly the output space for its 224/256/384/512 digests. NULL inputs or widths
produce NULL. An invalid runtime width retains released Flink evaluation and the JDK's
`NoSuchAlgorithmException`; computed arguments keep the exact scalar evaluator. No
configuration switch is introduced. Literal NULL widths are admitted, while existing
valid literal fixed kernels and invalid/oversized literal fallback remain unchanged.

The measurements use canonical main `ffafa92b`, Rust 1.94.0 release with
`--no-default-features`, Apple M4 Pro / 24 GiB, JDK 17, and released Flink 2.2.1/1.18.1.
Each engine/profile starts a fresh JVM (256 MiB initial / 512 MiB maximum heap), with
8 warmup and 9 measured trials of 131,072 rows. Profiles vary ASCII/CJK/emoji payloads,
32/264-byte budgets, 16/1,024/16,384-row batches, and cold/cached Java strings. Logical
values, widths and NULLs match: 16 keys, four rotating valid widths, source NULL every
7th row, width NULL every 11th row. Input preparation is outside timing. Engines run
sequentially without concurrent builds/tests/benchmarks.

The portable `runtime_sha2` example runs the production compiled Calc with a timer
inside Rust, on prepared Arrow RecordBatch inputs, including allocation and checksum
consumption. Stock uses unchanged released generated Flink on prepared RowData with a
Java-side timer. Initial/final JNI and perimeter transposes are excluded from this
small-functionality gate; the previous native production binary retains its intrinsic
JVM callback, Arrow export/import and allocations. The same example is linked against
the unchanged baseline Rust library for that control. Before timing, the candidate's
full output array must equal the generated reference; an invalid candidate evaluator
ID makes accidental JVM computation fail. Cached controls affect Java's input handling;
the native digest deliberately reads bytes, just like Flink's BinaryStringData hash.

[Every raw trial, median/range and library hash](results/runtime-sha2-2026-10-27.json)
is retained with the complete-pipeline diagnostic. These profiles establish bounded
gains for the tested direct signature, not arbitrary computed inputs or invalid-width
batches that retain the host evaluator.

Values are medians with `[minimum, maximum]` over nine measured trials:

| Flink | Corpus / payload budget | Java cache | Batch rows | Stock ns/row | Previous ns/row | Native ns/row | Stock/native |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
| 2.2.1 | ascii / 32 | cold | 1024 | 289.83 [281.41, 306.13] | 358.60 [353.95, 380.94] | 84.68 [80.02, 84.82] | 3.42x |
| 2.2.1 | ascii / 264 | cold | 1024 | 738.92 [723.19, 816.93] | 868.92 [860.50, 912.52] | 171.39 [161.22, 177.45] | 4.31x |
| 2.2.1 | ascii / 264 | present | 1024 | 736.94 [731.62, 754.16] | 894.84 [855.14, 909.22] | 163.84 [161.54, 175.59] | 4.50x |
| 2.2.1 | cjk / 264 | cold | 1024 | 733.77 [727.37, 760.30] | 1016.30 [1003.85, 1054.98] | 159.97 [158.49, 163.85] | 4.59x |
| 2.2.1 | cjk / 264 | present | 1024 | 735.07 [730.10, 752.61] | 1004.26 [997.17, 1012.32] | 165.00 [162.68, 172.09] | 4.45x |
| 2.2.1 | emoji / 264 | cold | 1024 | 766.42 [755.58, 774.53] | 1006.83 [991.28, 1023.74] | 169.85 [165.28, 174.57] | 4.51x |
| 2.2.1 | emoji / 264 | present | 1024 | 749.81 [729.67, 776.85] | 996.59 [980.35, 1025.20] | 168.10 [162.06, 171.72] | 4.46x |
| 2.2.1 | ascii / 32 | present | 16 | 262.83 [253.98, 276.40] | 935.46 [921.22, 949.36] | 104.86 [102.90, 106.35] | 2.51x |
| 2.2.1 | ascii / 264 | present | 16384 | 783.93 [768.11, 813.64] | 850.41 [834.82, 860.69] | 178.13 [172.19, 181.99] | 4.40x |
| 1.18.1 | ascii / 32 | cold | 1024 | 285.25 [276.26, 298.58] | 357.37 [350.15, 371.73] | 87.18 [81.01, 89.61] | 3.27x |
| 1.18.1 | ascii / 264 | cold | 1024 | 728.05 [722.41, 746.24] | 881.23 [859.99, 901.85] | 172.42 [170.22, 174.59] | 4.22x |
| 1.18.1 | ascii / 264 | present | 1024 | 756.71 [741.79, 772.90] | 869.09 [864.30, 911.75] | 176.14 [168.87, 182.15] | 4.30x |
| 1.18.1 | cjk / 264 | cold | 1024 | 729.19 [723.14, 756.06] | 1006.41 [998.65, 1026.25] | 161.39 [158.80, 167.09] | 4.52x |
| 1.18.1 | cjk / 264 | present | 1024 | 748.85 [730.16, 770.20] | 1027.83 [1023.35, 1041.85] | 172.24 [158.60, 177.12] | 4.35x |
| 1.18.1 | emoji / 264 | cold | 1024 | 770.58 [748.36, 787.40] | 1000.87 [984.90, 1032.93] | 159.48 [155.76, 163.07] | 4.83x |
| 1.18.1 | emoji / 264 | present | 1024 | 775.41 [742.13, 801.22] | 998.94 [984.40, 1026.74] | 162.12 [159.17, 163.42] | 4.78x |
| 1.18.1 | ascii / 32 | present | 16 | 272.49 [260.93, 282.38] | 954.04 [928.45, 1049.92] | 106.83 [104.47, 107.95] | 2.55x |
| 1.18.1 | ascii / 264 | present | 16384 | 824.35 [781.61, 879.61] | 867.03 [858.87, 876.18] | 173.55 [166.10, 285.19] | 4.75x |

Across these profiles the candidate runs 2.51-4.83x stock and 4.10-8.93x the preceding
native implementation. The raw artifact also retains the earlier borrowed-internal
control separately; final comparisons use the actual baseline TYPE_STRING descriptor.

Reproduce after compiling test classes/dependencies for the selected released profile:

```sh
cargo build --manifest-path native/Cargo.toml --release --no-default-features \
  -p streamfusion --example runtime_sha2
# Set SF_NATIVE_BENCH_CLASSPATH to test classes, core classes and test dependencies.
export SF_NATIVE_LIB_DIR="$PWD/native/target/release"
# Keep the matching release libstreamfusion DSO here for intrinsic JVM callbacks.
native/target/release/examples/runtime_sha2 ascii 264 true 1024 128 native 8 9
native/target/release/examples/runtime_sha2 ascii 264 true 1024 128 flink 8 9
```

Compile the same example against main `ffafa92b` and its unchanged production native
library to run the previous behavior, selecting `baseline` and the matching DSO. The
new `RuntimeSha2BenchmarkFunctions` test helper is required on its test classpath.

`RuntimeSha2PipelineBenchmark` separately retains a row source, both production
RowData/Arrow transposes and a blackhole row sink. Its plan asserts both transposes,
and runtime counters require consumed native Calc rows. It uses 2M rows, parallelism 1,
a 264-byte ASCII payload, four rotating digest widths, the same NULL frequencies, 2 GiB
heap, two warmups and five measured jobs per engine in fresh JVMs. The job timer includes
planning/submission and startup after environment creation; it is a startup-inclusive
fixture rather than sustained throughput. The component and SQL tests establish value
equality separately; the blackhole sink alone is insufficient. Reproduce with
`SF_BENCHMARK=true mvn test -Pbench -Dtest=RuntimeSha2PipelineBenchmark` and
`-Dsha2.pipeline.engine=stock` or `native`, building each baseline in a separate checkout.

| Engine | Job median (s) | Minimum-maximum (s) |
| --- | ---: | ---: |
| stock | 2.301219 | 2.288133-2.320313 |
| previous | 2.483776 | 2.445620-2.545634 |
| native | 1.046509 | 1.017852-1.065204 |

The candidate is 2.20x stock and 2.37x previous native in this
startup-inclusive fixture. It does not establish arbitrary connector or steady-state
throughput.

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

## JSON_VALUE exception parity (2026-09-19)

`JSON_VALUE_ERROR` selects `lax $.user.active` with `ERROR ON EMPTY ERROR ON ERROR`.
`JSON_VALUE_ACTIVE` selects the same field with the default NULL policies. Both use
the unchanged `tt_json` row source, whose non-null documents contain that boolean field.
These are successful-path measurements; the error policies still affect which evaluator runs.

Apple M1 Max, JDK 17, Flink 2.2.1, release build with mimalloc (`-Pbench`), parallelism 1,
one million rows, 264-byte padding budget, NULL every eighth row, two warmups and five
interleaved trials per engine. Each before/after run uses a fresh JVM. No other test suite
or native build runs during measurement. Medians include planning, the row source and sink,
and both row/Arrow transposes, which the harness asserts in the plan.

| Query | Before Flink (s) | Before native (s) | After Flink (s) | After native (s) |
| --- | ---: | ---: | ---: | ---: |
| Source-matched identity | 0.354 | 0.608 | 0.355 | 0.592 |
| Default NULL policies | 0.994 | 0.812 | 1.006 | 0.799 |
| ERROR policies | 1.004 | 0.809 | 0.988 | 1.604 |

The before implementation is `762efc8`, with only the two benchmark queries added.
Routing ERROR policies through Flink's generated batch evaluator costs 1.98× elapsed time
relative to the previous native kernel and takes 1.62× Flink's time in this short pipeline.
This corrects the previous exception-class/message mismatch; it is not a performance improvement.
Non-throwing policies retain the native path. The generated route performs one JVM callback
per Arrow batch, with row iteration inside that callback.

```bash
SF_BENCHMARK=true mvn -pl streamfusion-runtime -am test -Pbench \
  '-Dtest=ScalarFunctionBenchmark#individualFunctions' \
  -Dscalar.functions=JSON_VALUE_ACTIVE,JSON_VALUE_ERROR \
  -Dscalar.rows=1000000 -Dscalar.bytes=264 -Dscalar.nullEvery=8 \
  -Dscalar.warmup=2 -Dscalar.runs=5 -Dscalar.engine=both
```


## Integer ARRAY_DISTINCT (2026-09-28)

Measured against canonical main `665dc10d`, Intel Core i7-12650H, Linux, JDK 17,
Flink 2.2.1, release Rust with mimalloc, parallelism one, 2 GiB heap and 1,024-row
Arrow batches. Each case uses a fresh JVM, five warmups and five alternating
stock/native trials. No other build or test ran concurrently. Times include
planning and the complete job, a rowwise DataStream source, both production
transposes, JNI execution and a rowwise blackhole sink. The harness asserts
NativeCalc and both transposes, completes the job and checks planner substitution
counts; an intentionally disabled admission must have zero substitutions.

The query projects `ARRAY_DISTINCT(arr)` plus a TRUE anchor. Every eighth array is
NULL; every seventh element is NULL; other elements are
`(31 * elementIndex + rowIndex) % domain - domain / 2`. Width counts all elements;
domain is the value generator's modulus, not the resulting distinct count.

| Element type | Rows | Width / domain | Flink median (range), s | Native median (range), s | Flink/native |
| --- | ---: | ---: | --- | --- | ---: |
| BIGINT | 200,000 | 64 / 64 | 1.047 (1.039–1.065) | 0.310 (0.308–0.319) | 3.38x |
| BIGINT | 200,000 | 256 / 256 | 13.388 (13.345–13.403) | 0.924 (0.911–0.942) | 14.50x |
| BIGINT | 200,000 | 64 / 8 | 0.334 (0.328–0.373) | 0.243 (0.238–0.252) | 1.37x |
| INT | 200,000 | 64 / 64 | 0.989 (0.979–1.005) | 0.306 (0.299–0.325) | 3.23x |
| BIGINT | 2,000,000 | 8 / 8 | 0.668 (0.621–0.672) | 0.581 (0.581–0.625) | 1.15x |
| BIGINT | 20,000,000 | 8 / 8 | 5.875 (5.660–5.943) | 5.215 (5.136–5.241) | 1.13x |
| BIGINT, repeat | 20,000,000 | 8 / 8 | 5.430 (5.394–5.492) | 5.413 (5.358–5.468) | 1.00x |

Large-array gains are clear. Small-array gains are sensitive to run-to-run control
movement: the repeat is effectively tied, and neither long run is evidence of a
stable 13% improvement. Short jobs also include appreciable fixed startup costs.
These results do not establish gains for all integer widths or data distributions.

For the previous StreamFusion behavior, an admission ablation removed only the
new ARRAY_DISTINCT Java admission from the same runtime. The harness verified
`unsupported function/operator: ARRAY_DISTINCT` and zero native substitutions.
With five warmups/five trials, BIGINT 200,000 x 64/domain64 took 1.049s
(1.041–1.124), versus its paired stock control 1.093s (1.031–1.154). BIGINT
2,000,000 x 8/domain8 took 0.580s (0.545–0.607), versus stock 0.564s
(0.545–0.572). The candidate's 0.310s large-array result improves this prior
behavior by about 70%; its 0.581s small-array result is essentially unchanged
across separate JVMs. This is an admission ablation, not a rebuilt old release.

[Raw trials](array-distinct-2026-09-28.csv) retain the full investigation, including
unfavorable implementations on baseline `4aae50a1`. Released DataFusion's generic
kernel took 0.724s versus Flink 0.625s for two million eight-element arrays; typed
hash membership reduced native time to 0.659s versus Flink 0.623s. A bounded stack
search took 0.624s versus Flink 0.601s with two warmups, and 0.592s versus Flink
0.607s with five. Its twenty-million-row result remained slower (5.393s versus
5.314s). The final implementation adds a collision-checked fingerprint to that
stack search. Its first long run improved native time to 5.215s, but the Flink
control also moved; the repeat above prevents attributing the entire difference
to the kernel. CSV implementation/run columns distinguish these intermediate
variants from the shipped `typed_fingerprint` measurements.

A diagnostic async-profiler CPU-timer run of the generic kernel collected 6,519
mixed Java/native samples; 683 stacks included NativeCalc/JNI, 296 the generic
array-distinct kernel, 199 hash insertion, and 36 row conversion. These overlapping
counts include both engines and startup. That diagnostic overlapped a native
build, so its elapsed times are excluded from benchmark claims. It motivated
primitive membership without encoded rows; it does not measure isolated kernel
CPU percentages. See the [kernel technique](../optimizations/scalar-function-kernels.md#integer-array_distinct).

Reproduce each table scenario by changing rows, width, domain and `collection.int`
(the default is BIGINT):

```sh
SF_BENCHMARK=true mvn -pl streamfusion-runtime -am test -Pbench \
  -Dtest=DynamicCollectionBenchmark -Dsurefire.failIfNoSpecifiedTests=false \
  -Dsf.extraJvmArgs=-Xmx2g '-Dcollection.expression=ARRAY_DISTINCT(arr)' \
  -Dcollection.map=false -Dcollection.rows=200000 -Dcollection.width=64 \
  -Dcollection.domain=64 -Dcollection.int=false -Dcollection.warmup=5
```

For an admission ablation, remove the Java ARRAY_DISTINCT admission and add
`-Dcollection.native=false` and
`'-Dcollection.fallbackReason=unsupported function/operator: ARRAY_DISTINCT'`.
Restore admission before measuring the candidate. MAP_KEYS/MAP_VALUES were also
investigated but remain fallback after end-to-end regressions; their
[raw trials](map-array-projections-2026-09-28.csv) and
[rejection rationale](https://github.com/datafusion-contrib/StreamFusion/blob/main/.claude/wontdos/234-map-array-projections.md)
are retained.


## Array extrema admission experiment (2026-09-28)

Direct admission of released DataFusion 54 integer ARRAY_MIN/ARRAY_MAX kernels
was tested on baseline `2201e98f`, Intel Core i7-12650H, Linux/JDK 17, Flink 2.2.1,
release/mimalloc, 2 GiB heap, parallelism one and 1,024-row batches. Five warmups
preceded five alternating whole-job trials with a row source, row blackhole sink,
both transposes and verified native Calc routing.

For two million eight-element BIGINT arrays, MIN took 0.502s native versus 0.480s
stock and MAX took 0.496s versus 0.465s. At twenty million rows MIN took 4.549s
(4.537–4.572) versus 3.967s (3.932–4.072), a 14.7% regression. Wider arrays showed
modest gains: 200,000 arrays of width 256 took 0.497s versus 0.525s for MIN and
0.498s versus 0.532s for MAX. Width-64 BIGINT and INT results were approximately
tied or modestly favorable. The default admission was rejected; both functions
continue on Flink.

[Raw trials](array-extrema-2026-09-28.csv) include every candidate scenario and
previous-admission controls. The
[experiment note](https://github.com/datafusion-contrib/StreamFusion/blob/main/.claude/wontdos/234-array-extrema.md)
records all ranges, the input generator, reproduction commands, control drift,
prototype parity results and the separate profile. No timing from the instrumented
profile is included in the raw benchmark series. This investigation does not
remove the remaining collection gaps in [#234](https://github.com/datafusion-contrib/StreamFusion/issues/234).


## DOUBLE TRUNCATE consumer and nullable workloads

`ScalarFunctionBenchmark` adds `DOUBLE_TRUNCATE_CASE`, `DOUBLE_TRUNCATE_COALESCE`
and `DOUBLE_TRUNCATE_NESTED`, all on the existing bounded signed source with
runtime scale -3..3. They respectively select
`CASE WHEN n > 0 THEN TRUNCATE(n,s) ELSE -1E0 END`,
`COALESCE(TRUNCATE(n,s),1E0)`, and `TRUNCATE(TRUNCATE(n,3),s)`.
`DOUBLE_TRUNCATE_LARGE` uses signed `1e12 + 0.12345 + row % 1024`, explicitly
outside the helper's magnitude domain, with the same runtime scales.
`scalar.nullEvery` controls operand NULLs; new `scalar.scaleNullEvery` controls
scale NULLs for these DOUBLE source fixtures (default zero leaves scales non-null).
The configuration is captured outside the source's per-row loop. Each source
has its own matching DOUBLE identity control.

The existing strict native-plan/runtime checks retain JNI, both transposes and
the runtime row source/blackhole sink. Six workload smoke checks at 5,003 rows,
operand NULL every eight rows and scale NULL every seven rows complete on
released Flink 2.2.1. They prove execution/admission, not timing or output parity;
consumer SQL parity is independently checked against released Flink. Whole-job
comparisons use repeated alternating trials and the same fixture-only copies
in the previous-production checkout, with fallback controls explicitly expected.
An initial timing attempt overlapped an own native build and was stopped; it
supplies no accepted full-run timing result. Retain its log rather than treating
partial observations as complete samples.

```sh
SF_BENCHMARK=true mvn -Pbench -pl streamfusion-runtime -am test \
  -Dtest=ScalarFunctionBenchmark -Dsurefire.failIfNoSpecifiedTests=false \
  -Dscalar.functions=DOUBLE_TRUNCATE_CASE,DOUBLE_TRUNCATE_COALESCE,DOUBLE_TRUNCATE_NESTED,DOUBLE_TRUNCATE_LARGE \
  -Dscalar.rows=2000000 -Dscalar.warmup=2 -Dscalar.runs=5 \
  -Dscalar.nullEvery=8 -Dscalar.scaleNullEvery=7
```

### Rejected manual BINARY exit projection

A direct Arrow-segment copy prototype preserved owned Flink row bytes and passed
29 targeted wire, lifetime, ownership and SQL parity checks on Flink 2.2.1. The
manual per-field projection did not improve release whole-job results over the
prior generated projection: ELT medians are stock/manual/prior-native
0.305/0.402/0.387 seconds; string cast medians 0.570/0.615/0.613 seconds. Both native
versions still lose to stock. These use two million rows, two warmups, five
alternating trials, a 2 GB heap, 1,024-row batches and both transposes. All
[manual trials](binary-direct-exit-candidate-2026-10-01.csv) and
[prior-native trials](binary-direct-exit-prior-native-2026-10-01.csv), including
identity controls and changing stock timings, are retained. The prototype was
removed; production still uses the generated projection. The
[scoped rejection](https://github.com/datafusion-contrib/StreamFusion/blob/feat/recovered-goal-followups/.claude/wontdos/binary-manual-exit-projection.md)
records the ownership design, validation and remaining investigation. This does
not close [#235](https://github.com/datafusion-contrib/StreamFusion/issues/235).

### Rejected generated BINARY segment projection

A second prototype kept Flink-generated field execution while copying binary
payload segments directly into owned Flink rows. It passed five wire/lifetime,
fourteen fixed-BINARY SQL and ten existing ownership checks on Flink 2.2.1, but
still lost to stock in the release measurements. At two million rows, stock /
prototype medians are ELT 0.314 / 0.399 seconds and cast 0.567 / 0.606 seconds for
the 264-byte input budget. Widening the string input to 4,096 bytes gives cast
8.496 / 8.682 seconds; the wide identity control is 9.070 / 10.345 seconds.
All measurements keep BINARY(16) output, a 2 GB heap, 1,024-row batches, two
warmups, five alternating trials, JNI and both transposes. The
[small-input trials](binary-generated-segment-candidate-2026-10-01.csv) and
[wide-input trials](binary-generated-segment-wide-candidate-2026-10-01.csv)
retain all observations. The prototype was removed; the original generated
projection remains. The
[scoped rejection](https://github.com/datafusion-contrib/StreamFusion/blob/feat/recovered-goal-followups/.claude/wontdos/binary-generated-segment-projection.md)
records validation and architecture. Wider inputs did not prove the hypothesized
encoding advantage, and no issue is closed from this experiment.

## MAP SQL/JSON batch boundaries (2026-09-19)

The generated Calc bridge also carries MAP values whose keys are declared non-null, and
MULTISET values whose elements are declared non-null. This extends coverage through the same
borrowed-input/owned-output lifetime; it adds no per-row JNI calls and no native JSON parser.

The existing scalar benchmark measured 2M rows on M1 Max/JDK 17, release + mimalloc,
parallelism one, 264-byte ASCII payloads and a NULL outer map every seventh row. Each non-null
map contains three string keys and one NULL value. Both transposes and the rowwise blackhole
sink remain in the timed path. There were two warmups and five interleaved measured trials;
medians below include all conversion and callback work.

| Case | Flink (s) | Native pipeline (s) | Flink / native |
|---|---:|---:|---:|
| MAP identity control | 1.318 | 2.379 | 0.554x |
| `JSON_STRING(m)` | 2.727 | 4.179 | 0.652x |
| `ROW(m, JSON_QUERY(s, '$.items[*]'))` | 2.611 | 5.599 | 0.466x |

These isolated projections are slower than Flink. The purpose is to keep supported MAP-bearing
JSON Calcs inside a larger native pipeline; this measurement does not establish a speedup for
that composition. Existing measured native JSON fast paths remain in use. Nullable keys still
fall back because Arrow's map representation cannot preserve them safely.

[All measured trials](sql-json-map-2026-09-19.csv) are retained. Reproduce with the existing
`ScalarFunctionBenchmark#individualFunctions` under `-Pbench`, `SF_BENCHMARK=true`,
`-Dscalar.functions=JSON_STRING_MAP,JSON_QUERY_MAP_RESULT -Dscalar.nullEvery=7`.

A second benchmark put the same JSON map serialization inside a filter followed by grouped
COUNT over 4,096 keys, retaining both transposes and the rowwise blackhole sink. It used the
same 2M rows, payload sizes, NULL rate, warmups and interleaving. Flink took **3.365s** and
the native pipeline **4.774s** (**0.705x**). Thus this measured composition is also slower;
there is no demonstrated speedup for the MAP extension. The implementation is small because
it reuses the generated host evaluator, but performance needs further work before recommending
it as an optimization. [Composition trials](sql-json-map-composition-2026-09-19.csv) are retained;
reproduce with `JsonMapCompositionBenchmark` under `-Pbench` and `SF_BENCHMARK=true`.


The maintainer accepted shipping the verified MAP/MULTISET boundary extension on
October 16 with the measured regressions above retained and performance work deferred.
The historical measurements are not a claim of improvement against current main.


## MAP scalar callback costs

Measured on October 20, 2026 with released Flink 2.2.1, JDK 17, release native code, parallelism 1,
2 GiB task heap and batches of 1,024 rows. `MapScalarUdfBenchmark` uses a Java identity callback
on nullable `MAP<STRING,INT>` values and a row blackhole sink. Source maps are constructed during
the job. Timings include execution planning/deployment, both perimeter transposes, JNI/C Data,
callbacks and the sink; initial environment/explain setup is excluded. These are complete-job
cost diagnostics under the accepted UDF exception, not small-functionality admission timings.

Two warmups and five measured trials alternate stock/native-enabled order. Candidate and previous-
fallback processes run serially, each with paired stock controls. Native plans require both
transposes and actual NativeCalc input counts; previous fallback requires zero native operators.

| Rows / entries | Candidate's stock control (s) | Native callback (s) | Previous fallback (s) | Previous stock control (s) |
| --- | ---: | ---: | ---: | ---: |
| 16,384 / 4 | 0.076014 | 0.163299 | 0.080867 | 0.080237 |
| 65,536 / 64 | 0.617199 | 1.221389 | 0.614807 | 0.607610 |

The callback path is about twice as slow on these fixtures. Correct external MAP coverage now
defaults on without a new opt-in; this makes no standalone acceleration claim. The blackhole
timings do not validate result values: independent SQL/Table API, mutable/retained Java values,
error/lifecycle, separate-JVM and two-restore checks do that on both released Flink lines.
The [raw evidence](results/map-scalar-udfs-2026-10-20.json) retains all trials and ranges,
paired controls, library identity, test counts, 48 transport fixtures and allocation probes.

Reproduce with `SF_BENCHMARK=true`, `-Pbench`, `-Dtest=MapScalarUdfBenchmark`,
`-Dmapbench.rows=16384 -Dmapbench.width=4` or `65536/64`, and
`-Dsf.extraJvmArgs="-Xmx2g -Dstreamfusion.exchange.zeroCopyLocal=false"`. The old revision uses
the same benchmark source with `-Dmapbench.expectNative=false`.

## Final JSON_VALUE and UNION ALL (2026-10-22)

These are complete-job measurements of two filtered JSON_VALUE branches feeding a final UNION ALL
and row blackhole sink. The source constructs runtime rows; both entry transposes, the exit transpose,
JNI/C Data and Arrow IPC remain in the timed path. Timing starts at `executeSql` and ends after job
completion, including execution planning and deployment. Environment creation and the separate EXPLAIN
are outside timing. Each native trial asserts native Calc/Union plans, both perimeter conversion
directions, positive NativeCalc input counts and no fallback. Correctness uses separate typed collectors
and a sink that captures UTF-16 units before result serialization.

Apple M4 Pro, macOS/aarch64, released Flink 2.2.1, JDK 11.0.27, release/mimalloc, one task manager,
parallelism one, 8 GiB maximum JVM heap and 1,024-row transpose batches. There are two warmups and five
measured trials with alternating engine order. The metrics harness grants the same 48 GiB off-heap
accounting budget to both engines; it is a limit, not allocated or measured resident memory.

The previous behavior is canonical main `47bbc8b080e8cece29540e2f739ddbfdc6bc7111`, using its own rebuilt
release library and the same benchmark source. It declines the complete query with zero substitutions.
Previous and candidate processes run serially with separate stock controls; their differences must not
be attributed entirely to the implementation. Medians in seconds:

| Input | Rows | Candidate stock | Native | Previous stock | Previous fallback |
| --- | ---: | ---: | ---: | ---: | ---: |
| Short ASCII | 2,000,000 | 1.042780 | 0.687971 | 0.967463 | 0.965541 |
| Nine-member ASCII | 2,000,000 | 1.537207 | 1.022508 | 1.574752 | 1.572480 |
| Escaped Unicode/empty/NULL | 2,000,000 | 1.214878 | 0.743581 | 1.132274 | 1.133687 |
| Raw Chinese/supplementary/NULL | 2,000,000 | 0.830042 | 0.652332 | 0.790804 | 0.787202 |
| Unique ASCII document per row | 2,000,000 | 1.001590 | 0.721988 | 0.982541 | 0.964158 |
| Short ASCII | 20,000,000 | 9.497192 | 5.962004 | -- | -- |

The five two-million-row fixtures reduce time by 21.4%-38.8% versus their paired stock controls.
The larger ASCII job reduces time by 37.2%; previous-main performance was not separately measured
at that size. These results establish the tested final row-sink composition, not performance or
semantic admission for grouping, joins, sorting, JSON_QUERY or native columnar sinks.

The same five two-million-row fixtures also execute natively on released Flink 1.18.1/JDK 11 with
the same resources, warmups, interleaving and release library. Their paired medians are:

| Input | Flink 1.18 stock (s) | Native (s) |
| --- | ---: | ---: |
| Short ASCII | 1.050900 | 0.796418 |
| Nine-member ASCII | 1.697440 | 1.355543 |
| Escaped Unicode/empty/NULL | 1.143719 | 0.681589 |
| Raw Chinese/supplementary/NULL | 0.831848 | 0.646050 |
| Unique ASCII document per row | 1.055666 | 0.825663 |

The unique fixture constructs a distinct document and extracted value for every row inside the
timed job. Its gains therefore do not depend on repeated-document encoding reuse.

Previous-main performance was not separately remeasured on 1.18. The JDK 11 correctness selection
passes 85 checks on 2.2 and 74 checks with 11 explicit catalog skips on 1.18. All 23 issue-specific
SQL checks execute on both lines, including duplicate/mixed branches, sink-side code-unit capture,
four changelog kinds, two checkpoint restores at parallelism one/three and explicit fallback cases.
A further JDK 17 selection passes 96 SQL/JNI/Arrow ownership, partial-batch close and failure-cleanup
checks on 2.2. The connector-free Rust core passes all 758 library tests. These are selected JVM
suites rather than a full JVM-suite run.

The initial input-cache prototype was slower than stock on short and wide ASCII. Profiling identified
duplicated UTF-16 documents and entry encoding work. The final transpose omits only redundant caches
for parse-only JSON inputs, retaining malformed UTF-16 and differing cached/byte values. A Unicode
diagnostic with ASCII-only cache omission also regressed; retaining lazy encoding and omitting valid
lazy Unicode caches removes that measured gap. These intermediate results are retained with the
[raw trials, ranges, native library identities and validation counts](results/json-final-union-2026-10-22.json).

Reproduce with `SF_BENCHMARK=true`, `-Pbench`, `-Dtest=JsonStringUnionBenchmark` and
`-Djsonunion.fixture=ascii|wide|unicode|raw-unicode|unique`. Select one fixture per invocation; use
`-Djsonunion.rows=20000000` for the larger job. Previous main uses the identical benchmark source
with `-Djsonunion.expectNative=false`; add `-Pflink-1.18` for the older released line.
See [Union coverage](../operators/union.md#final-json_value-projections)
and [parse-only entry caches](../optimizations/projection-pruning-transpose.md#parse-only-json-document-caches).

## Exact numeric ARRAY casts

The functionality gate compares released Flink 2.2.1's generated Calc with the previous
StreamFusion fallback at `d951a838` and the new Rust ARRAY cast. Measurements use JDK 11,
Apple M4 Pro, release Rust with mimalloc, one active benchmark, 131,072 rows, five warmups
and nine measured trials. The Arrow input and Flink BinaryRowData contain identical integer
or DECIMAL values, NULL containers, empty arrays and NULL elements. Every trial consumes
every output element; all three engines have identical checksums for each shape.

This is the repository's small-functionality boundary: stock/previous start on prepared
RowData in Java, and native starts on prepared Arrow batches of 1,024 rows in Rust. Setup,
input IPC loading, initial/final JNI and perimeter transposes are outside timing. Generated
evaluation, native Calc evaluation, allocation and equivalent output consumption are inside.
These measurements do not establish an end-to-end job speedup. DECIMAL rescale is
DECIMAL(18,2) to DECIMAL(18,1); integer-to-DECIMAL targets DECIMAL(18,2).

Median milliseconds; speedup is stock divided by Rust:

| Conversion / array width | Stock | Previous fallback | Rust | Speedup |
| --- | ---: | ---: | ---: | ---: |
| INT to BIGINT / 4 | 10.003 | 6.681 | 0.992 | 10.08x |
| INT to BIGINT / 32 | 17.828 | 17.958 | 6.629 | 2.69x |
| INT to BIGINT / 128 | 58.470 | 60.420 | 24.851 | 2.35x |
| INT to DECIMAL / 4 | 10.589 | 13.155 | 2.322 | 4.56x |
| INT to DECIMAL / 32 | 50.672 | 37.684 | 16.713 | 3.03x |
| INT to DECIMAL / 128 | 203.839 | 186.003 | 68.721 | 2.97x |
| DECIMAL rescale / 4 | 10.629 | 10.119 | 3.426 | 3.10x |
| DECIMAL rescale / 32 | 49.437 | 45.040 | 24.884 | 1.99x |
| DECIMAL rescale / 128 | 178.626 | 152.197 | 98.925 | 1.81x |

[Raw warmup/measured times, ranges and checksums](results/exact-array-casts-2026-10-22.json)
retain the JVM trial variation; short stock trials are noisier than the Rust batch loop.
The initial sliced-input prototype repeatedly converted the complete shared child buffer.
Restricting conversion to each slice's referenced values removes that amplification, including
at nested array levels.

Run the Java half with:

```sh
SF_BENCHMARK=true mvn -pl streamfusion-runtime -Pbench \
  -Dtest=ArrayCastAdmissionBenchmark -Darray.cast.mode=stock \
  -Darray.cast.shape=int-bigint -Darray.cast.rows=131072 -Darray.cast.width=32 \
  -Darray.cast.warmups=5 -Darray.cast.runs=9 -Darray.cast.export=/tmp/array-cast.arrow test
```

Use `int-decimal` and `decimal-rescale` for the other shapes. Copy the same benchmark test
to a separate checkout of the baseline and use `-Darray.cast.mode=previous` to require the
old planner's whole-Calc ARRAY cast fallback. The new benchmark class is disabled unless
`SF_BENCHMARK=true`. Build and run the native half on the exported input:

```sh
cd native
cargo build -p streamfusion --release --no-default-features --features mimalloc --example array_cast
target/release/examples/array_cast /tmp/array-cast.arrow 5 9
```

The native example uses the production Calc decoder and cast, times inside Rust and verifies
the same output digest. Ordinary SQL regressions separately establish released-Flink result
and type parity and positive consumed native work on both supported Flink lines.
Numeric ARRAY TRY_CAST invokes the identical total conversion kernel; these measurements use
CAST SQL and do not claim a separate TRY_CAST measurement.

## Standalone collection programs

Standalone ARRAY/MAP/ROW casts and dynamic or empty-separator SPLIT can now select the existing
generated collection evaluator without another collection function in the Calc. This is coverage
under the accepted collection-extension tradeoff; these new standalone forms are slower than
stock Flink and previous-main fallback. Existing numeric ARRAY kernels remain unchanged.

Measured on 2026-10-23 with Linux x86-64/WSL2, Intel Core i7-12650H, OpenJDK 17.0.20.1,
Flink 2.2.1 and release/mimalloc. Each query uses 500,000 runtime-source rows, parallelism 1,
an 8 GiB test heap, two warmups and three alternating measurements per engine. Source, blackhole sink, both
row/Arrow transposes, planning and submission remain timed. Initial explain and cluster setup
are outside timing. No other tests or benchmarks ran concurrently. Previous main is `1bd9daa8`,
in a separate JVM using the same native DSO and benchmark source; each new expression must
have zero native Calc substitutions there. Identity controls still require native Calc.

DECIMAL arrays contain two DECIMAL(38,9) values and a NULL element. Every eighth container is
NULL. SPLIT uses a 32-byte Unicode payload budget and every eighth input is NULL; the dynamic
separator is `LEFT(s,1)`. Empty separators produce UTF-16-unit elements, including isolated
surrogate halves, whose Java caches must survive the represented Arrow result.

Median seconds; ranges and both paired stock controls are retained in the
[trial/profile summary](results/standalone-collections-2026-10-23.json).

| Expression | Stock | Generated collection | Previous fallback | Stock / generated |
| --- | ---: | ---: | ---: | ---: |
| DECIMAL ARRAY to STRING ARRAY | 0.397833 | 0.681770 | 0.400672 | 0.584x |
| SPLIT with dynamic separator | 0.304137 | 0.575999 | 0.294347 | 0.528x |
| SPLIT with empty separator | 0.661662 | 1.967336 | 0.650776 | 0.336x |

These complete jobs include startup; the subsecond results do not establish sustained throughput.
The initial DECIMAL-array-to-FLOAT diagnostic was already native on previous main and is retained
as a diagnostic, not evidence of newly admitted coverage or an improvement from this change.

Optimization investigation was limited to two eight-second CPU captures, one per SPLIT form.
The empty-separator capture has 10,331 samples across all JVM threads, including 2,309 inclusive
samples in Flink's array copy and 3,666 in represented array writing; the dynamic capture has
13,333 total samples, with 943 and 1,286 in those paths. Counts overlap, include compilation and
planning, and do not measure allocations or copied bytes. These paths preserve released array
copy categories and UTF-16 caches; no shortcut was enabled without a verified representation
contract. Further standalone optimization remains in [#392](https://github.com/datafusion-contrib/StreamFusion/issues/392).

Reproduce with a released native build:

```sh
SF_BENCHMARK=true mvn -pl streamfusion-runtime test -Pbench \
  '-Dtest=ScalarFunctionBenchmark#individualFunctions' -Dsf.testForks=1 \
  -Dscalar.functions=DECIMAL_ARRAY_TO_STRING,SPLIT_DYNAMIC,SPLIT_EMPTY \
  -Dscalar.rows=500000 -Dscalar.bytes=32 -Dscalar.unicode=true -Dscalar.nullEvery=8 \
  -Dscalar.warmup=2 -Dscalar.runs=3
```

The previous checkout copies only the same benchmark source and adds
`-Dscalar.native.expected=false`. SPLIT is absent from released Flink 1.18.1; standalone cast
parity is checked on both supported lines, with NativeCalc consumption required for successful
runtime queries and ordinary fallback retained at unverified collection boundaries.

These timings and CPU profiles precede the JVM bridge/type extension in [#469](https://github.com/datafusion-contrib/StreamFusion/pull/469). The rebased change passes 250 focused checks on Flink 2.2.1 and 213 passes plus 37 host-function skips on 1.18.1, including a temporary numeric UDF named SPLIT. The historical timings were not repeated after rebasing.


## Collection character casts

Standalone ARRAY/MAP CAST and TRY_CAST to STRING, VARCHAR and CHAR now use the existing
generated collection evaluator; ROW-to-character casts already used whole-Calc JVM evaluation.
This is coverage under the accepted collection-extension tradeoff. These standalone jobs are
slower than stock Flink and previous-main fallback; no acceleration is claimed.

The 2026-10-23 release/mimalloc comparison uses Flink 2.2.1, OpenJDK 17.0.20.1 and Linux
x86-64/WSL2 on an Intel Core i7-12650H. Each engine processes 500,000 runtime-source rows,
parallelism 1, two warmups and three alternating trials with an 8 GiB test heap. Complete-job
timing retains planning, submission, source, rowwise blackhole sink and both row/Arrow transposes;
cluster setup and initial explain are outside timing. No other tests or benchmarks run concurrently.
Previous main `8eb2b004` uses the identical benchmark source and released native DSO in a separate
JVM, with fallback required for the new expressions. Source-matched identity controls still require
native Calc. Subsecond complete-job results include startup and do not establish sustained throughput.

DECIMAL arrays have three elements: two DECIMAL(38,9) values and a NULL. Text arrays have a
32-byte-budget Unicode string, NULL and `tail`; maps have three ordered STRING entries, including
a NULL value. Every eighth container is NULL. Trial values, variability, both paired stock controls,
identity controls and CPU counts are retained in the
[measurement summary](results/collection-character-casts-2026-10-23.json).

Median seconds (minimum–maximum):

| Expression | Stock Flink | Generated collection | Previous fallback |
| --- | ---: | ---: | ---: |
| DECIMAL ARRAY → STRING | 0.406602 (0.397808–0.410599) | 0.547139 (0.546228–0.568279) | 0.401366 (0.395287–0.406225) |
| STRING ARRAY → CHAR(32), TRY_CAST | 0.355477 (0.352671–0.357062) | 0.577292 (0.573435–0.592959) | 0.355399 (0.338167–0.361236) |
| STRING MAP → STRING | 0.561181 (0.553266–0.564460) | 0.899699 (0.887600–0.900220) | 0.559865 (0.557814–0.569516) |

Optimization was limited to two eight-second CPU captures, one each for ARRAY and MAP formatting.
The ARRAY capture has 15,151 samples across all JVM threads, with 3,660 inclusive samples in the
JVM callback and 2,521 in generated row evaluation. The MAP capture has 13,381 total samples, with
3,479 in the callback, 2,278 in generated evaluation and 1,247 in represented-string writing.
Counts overlap and include compilation/planning; they do not measure allocation or copied bytes.
Formatting, materializing strings and preserving Java caches remain visible costs. No obvious
safe shortcut removed those costs within this increment; further work stays in
[#392](https://github.com/datafusion-contrib/StreamFusion/issues/392).

Reproduce with a released native build:

```sh
SF_BENCHMARK=true mvn -pl streamfusion-runtime test -Pbench \
  '-Dtest=ScalarFunctionBenchmark#individualFunctions' -Dsf.testForks=1 \
  -Dscalar.functions=ARRAY_FORMAT_STRING,TEXT_ARRAY_FORMAT_CHAR,MAP_FORMAT_STRING \
  -Dscalar.rows=500000 -Dscalar.bytes=32 -Dscalar.unicode=true -Dscalar.nullEvery=8 \
  -Dscalar.warmup=2 -Dscalar.runs=3
```

The previous checkout copies the same benchmark source and adds `-Dscalar.native.expected=false`.
For each of ARRAY_FORMAT_STRING and MAP_FORMAT_STRING, select `#profileFunction`, set
`SF_PROFILE_SCALAR=true`, `-Dscalar.warmup=1 -Dscalar.seconds=8` and
`-Dprofile.asprof=/path/to/asprof -Dprofile.outputDir=/path/to/output`. Runtime parity on both
supported Flink lines independently requires consumed NativeCalc input, including bounded UTF-16
results, legacy formatting and all four changelog kinds. NOT NULL fixed BINARY MAP leaves remain
guarded on 1.18; the [2.2 admission](#not-null-fixed-binary-map-leaves) has separate measurements.
Unverified state/native-writer boundaries retain fallback. Fixed-BINARY MULTISET elements use the
[separate verified boundary](#fixed-binary-multiset-elements).


## Fixed BINARY map values

Nullable BINARY(n) map values now enter the existing generated collection evaluator. A direct
final MAP_VALUES of an input-reference map with VARCHAR keys reuses the Rust shared-child view;
other consumers retain Flink evaluation. NOT NULL fixed BINARY keys and values remain guarded on
1.18; the [2.2 admission](#not-null-fixed-binary-map-leaves) has separate measurements. Nullable
fixed BINARY keys are covered below. NOT NULL values have an observed NULL-formatting difference
on released Flink 1.18.

Release/mimalloc measurements on 2026-10-24 use Flink 2.2.1, OpenJDK 17.0.20.1 and Linux
x86-64/WSL2 on an Intel Core i7-12650H. Each job has 500,000 runtime-source rows, parallelism 1,
two warmups and three alternating trials per engine, with an 8 GiB test heap. The three-entry
map has fixed ASCII STRING keys and two BINARY values of the selected width plus a NULL value;
every eighth parent is NULL. Timing includes planning, submission, processing, both row/Arrow
transposes and the blackhole row sink; cluster setup and initial explain are outside timing.
No other tests, benchmarks or native compilation run concurrently with these measurements.

Previous main `01b24b62` uses identical benchmark code and the same native DSO in a separate JVM.
The new expressions must fall back there; source-matched identity controls still require NativeCalc.
The initial generated route precedes the bounded optimization; the final route uses Rust for the
direct MAP_VALUES projection. MAP-to-STRING continues to use generated Flink evaluation.
All trial values, both paired stock controls, identity controls and CPU/allocation diagnostics are
in the [measurement summary](results/fixed-binary-map-values-2026-10-24.json).

Median seconds (minimum–maximum), retaining complete-job startup costs:

| Expression / bytes per value | Paired stock, final run | Previous fallback | Initial generated route | Final native route |
| --- | ---: | ---: | ---: | ---: |
| MAP_VALUES / 4 | 0.312171 (0.310226–0.327523) | 0.291862 (0.289075–0.299226) | 0.581776 (0.561526–0.597252) | 0.366429 (0.352071–0.367083) |
| MAP → STRING / 4 | 0.462423 (0.450366–0.471487) | 0.438994 (0.437611–0.451575) | 0.737274 (0.728585–0.741667) | 0.756175 (0.752077–0.835100) |
| MAP_VALUES / 256 | 0.400533 (0.377027–0.426528) | 0.387280 (0.383559–0.407149) | 0.685394 (0.681593–0.699388) | 0.448989 (0.446273–0.480963) |
| MAP → STRING / 256 | 1.167681 (1.161664–1.181050) | 1.173208 (1.138053–1.187564) | 1.651487 (1.616399–1.669336) | 1.763396 (1.744110–1.808813) |

The Rust view reduces MAP_VALUES native medians by 34.5–37.0% versus the generated route. Every final
standalone case remains slower than stock Flink and previous fallback, under the existing accepted
collection-extension tradeoff. These subsecond/second complete jobs do not establish sustained
throughput, and this is not a standalone acceleration claim.

Optimization stopped after two passes. An eight-second CPU capture of generated MAP_VALUES has
12,125 samples, with 2,297 inclusive samples in the JVM callback and 1,122 in generated evaluation.
The bounded change selects the existing Rust child view and avoids that callback for the verified
projection. The second eight-second capture, for MAP-to-STRING at 256 bytes, has 12,044 samples,
including 4,961 in the callback, 2,686 in generated evaluation and 2,056 in represented-string writing.
Inclusive counts overlap and include compilation/planning; they do not measure allocations or copied
bytes. Formatting, byte/string materialization and both perimeter conversions remain costs for
[#392](https://github.com/datafusion-contrib/StreamFusion/issues/392).

Six release Criterion smokes independently assert shared child identity, offsets and parent validity
on sliced maps at 16/1024 rows and 0/4/256-byte payloads with NULL/empty parents and NULL values.
The untimed System-allocation probe makes 104 allocation requests for 6,481 bytes per case and
reports zero newly owned Arrow output-buffer bytes. This includes first schema binding and is
not a steady-state allocation or copied-byte count. The complete output buffers share input storage;
the SQL benchmarks above still retain all perimeter conversion costs.

Reproduce each width with a released native build:

```sh
SF_BENCHMARK=true mvn -pl streamfusion-runtime test -Pbench \
  '-Dtest=ScalarFunctionBenchmark#individualFunctions' -Dsf.testForks=1 \
  -Dscalar.functions=FIXED_BINARY_MAP_VALUES,FIXED_BINARY_MAP_STRING \
  -Dscalar.rows=500000 -Dscalar.binary.width=256 -Dscalar.nullEvery=8 \
  -Dscalar.warmup=2 -Dscalar.runs=3
```

Also run with `-Dscalar.binary.width=4`. The previous checkout copies the same benchmark and input
fixture sources and adds `-Dscalar.native.expected=false`. For CPU captures, select
`#profileFunction`, set `SF_PROFILE_SCALAR=true`, choose one function, and use
`-Dscalar.warmup=1 -Dscalar.seconds=8 -Dprofile.asprof=/path/to/asprof`.
Rust fixtures run with `cargo bench -p streamfusion --bench collection_expressions --features mimalloc
--locked -j2 -- binary_map_values --test` from `native/`.

## Fixed BINARY map keys

Nullable BINARY(n) map keys use the existing length-delimited generated collection bridge.
A direct final MAP_KEYS of an input-reference map with nullable basic values reuses the Rust
shared-child view; other consumers retain generated Flink evaluation. NOT NULL fixed BINARY
keys and values remain guarded on 1.18; see the [2.2 measurements](#not-null-fixed-binary-map-leaves).
External STRUCTURED inputs and unverified state/native-writer boundaries remain guarded. Released Flink 1.18 cannot initialize CASE returning BINARY arrays; tests retain
its exact compiler failure and planning fallback for that form.

Release/mimalloc measurements on 2026-10-24 use Flink 2.2.1, OpenJDK 17.0.20.1 and Linux
x86-64/WSL2 on an Intel Core i7-12650H. Each job has 500,000 runtime-source rows, parallelism 1,
an 8 GiB test heap, two warmups and three alternating trials per engine. The ordered internal
RowData source supplies three-entry maps: two binary keys of width4/256 and a NULL key, with
INT values 1/NULL/-1. Every eighth parent is NULL. Public InternalTypeInfo registration avoids
an external byte-array-key conversion that could reorder entries by object identity.
Timing includes planning, submission, both row/Arrow transposes and a blackhole row sink;
cluster setup and initial explain are outside timing. No other tests, benchmarks or native
compilation run concurrently with these measurements.

Previous main `dbb9cffd` uses identical benchmark/input code and the same release native DSO
in a separate JVM. New expressions must fall back there, while source-matched identity controls
still require NativeCalc. The initial generated route precedes the bounded optimization;
MAP-to-STRING remains generated throughout. [All trials and diagnostics](results/fixed-binary-map-keys-2026-10-24.json)
retain both stock controls, identity controls, allocation probes and CPU profile summaries.

Median seconds (minimum–maximum), retaining complete-job startup costs:

| Expression / bytes per key | Paired stock, final run | Previous fallback | Initial generated route | Final native route |
| --- | ---: | ---: | ---: | ---: |
| MAP_KEYS / 4 | 0.243699 (0.234586–0.247416) | 0.224914 (0.224026–0.225689) | 0.383493 (0.378825–0.386779) | 0.253453 (0.252776–0.257386) |
| MAP → STRING / 4 | 0.340207 (0.336850–0.350045) | 0.339059 (0.338770–0.354235) | 0.487764 (0.482822–0.492686) | 0.482662 (0.478601–0.498011) |
| MAP_KEYS / 256 | 0.274349 (0.263257–0.276442) | 0.275038 (0.265149–0.280884) | 0.490600 (0.485984–0.510844) | 0.304727 (0.298132–0.439454) |
| MAP → STRING / 256 | 0.982406 (0.978934–0.987015) | 0.968631 (0.958710–0.969560) | 1.081767 (1.076390–1.102349) | 1.070686 (1.040013–1.078272) |

The Rust view reduces MAP_KEYS native medians by 33.9–37.9% versus generated evaluation.
All final standalone cases remain slower than paired stock and previous fallback under the
accepted collection-extension tradeoff. The wide-key native range also contains a slower trial.
These short complete jobs do not establish sustained throughput or standalone acceleration.

Optimization stopped after two passes. The first eight-second CPU capture of generated
MAP_KEYS has 14,036 samples, including 2,494 inclusive callback samples, 1,038 in generated
evaluation and 822 in array writing. Reusing the existing Rust view avoids that callback for
the verified projection. The second eight-second capture, for MAP-to-STRING at 256 bytes,
has 11,930 samples, including 4,263 in the callback, 2,907 in generated evaluation and 898 in
represented-string writing. Inclusive counts overlap and include planning and compilation;
they do not measure allocations or copied bytes. Formatting, map serialization and both
perimeter conversions remain costs for [#392](https://github.com/datafusion-contrib/StreamFusion/issues/392).

Six release Criterion smokes assert shared key-child identity, offsets and parent validity on
sliced maps at 16/1024 rows and 0/4/256-byte keys with duplicate key contents, NULL values and NULL/empty parents.
The untimed System-allocation probe reports 104 allocation requests for 6,475 bytes per case
and zero newly owned Arrow output-buffer bytes, including first schema binding. Buffer sharing
and allocation requests do not measure copied bytes or end-to-end speedup.

Reproduce each width with a released native build:

```sh
SF_BENCHMARK=true mvn -pl streamfusion-runtime test -Pbench \
  '-Dtest=ScalarFunctionBenchmark#individualFunctions' -Dsf.testForks=1 \
  -Dscalar.functions=FIXED_BINARY_MAP_KEYS,BINARY_KEY_MAP_STRING \
  -Dscalar.rows=500000 -Dscalar.binary.width=256 -Dscalar.nullEvery=8 \
  -Dscalar.warmup=2 -Dscalar.runs=3
```

Also run with `-Dscalar.binary.width=4`. The previous checkout copies the same benchmark/input
sources and adds `-Dscalar.native.expected=false`. For the two CPU captures, select
`#profileFunction`, set `SF_PROFILE_SCALAR=true`, choose one function, and use
`-Dscalar.warmup=1 -Dscalar.seconds=8 -Dprofile.asprof=/path/to/asprof`.
From `native/`, run `cargo bench -p streamfusion --bench collection_expressions --features mimalloc
--locked -j2 -- binary_map_keys --test` for the Rust fixtures.

## MULTISET casts

Standalone MULTISET-to-MULTISET and MULTISET-to-STRING/VARCHAR/CHAR CAST and TRY_CAST use the
existing generated collection evaluator and map-shaped Arrow transport. Nullable elements,
counts, nested ROW/ARRAY elements, DECIMAL conversion, legacy formatting and whole-container
TRY_CAST failures retain released Flink semantics. Direct fixed BINARY elements, external
STRUCTURED inputs and unverified representation boundaries retain fallback. COLLECT and
ARRAY_AGG are separate remaining coverage; updating retained MAP/MULTISET payload work in
[#437](https://github.com/datafusion-contrib/StreamFusion/issues/437) stays deferred.

Release/mimalloc measurements on 2026-10-24 use Flink 2.2.1, OpenJDK 17.0.20.1 and Linux
x86-64/WSL2 on an Intel Core i7-12650H. Each complete job has 250,000 runtime-source rows,
parallelism 1, an 8 GiB test heap, two warmups and three alternating trials per engine. Inputs
have 4/32 entries: INT keys `i*257` with counts `i%3+1`, followed by a NULL key with count 3.
Every eighth parent is NULL. An ordered internal GenericMapData source shares the prepared,
immutable key/count map across its rows and registers it using public InternalTypeInfo.

Timing includes planning, submission, source, both row/Arrow transposes and a blackhole row sink;
cluster setup and initial explain are outside timing. Measurements run without concurrent tests,
native compilation or other benchmarks. The previous-code checkout is the PR #476 tree measured
at `d48fba98`, with identical benchmark/input code and the same native DSO. Both new expressions
must fall back there; source-matched identity controls still require a native plan.
The [full trial summary](results/multiset-casts-2026-10-24.json) retains both stock controls,
identity controls and CPU diagnostics. CSV `multiset_entries` records the selected cardinality;
text-fixture defaults such as `payload_bytes=264` are unused for this input.

Median seconds (minimum–maximum), retaining complete-job startup costs:

| Expression / entries | Paired stock | Previous fallback | Final native route |
| --- | ---: | ---: | ---: |
| INT → BIGINT elements / 4 | 0.185376 (0.182766–0.187421) | 0.191836 (0.188395–0.199897) | 0.253451 (0.246239–0.261849) |
| MULTISET → STRING / 4 | 0.226101 (0.219468–0.252986) | 0.224224 (0.216410–0.229614) | 0.287859 (0.286316–0.290994) |
| INT → BIGINT elements / 32 | 0.466856 (0.462920–0.470641) | 0.456071 (0.454004–0.466342) | 0.756280 (0.752158–0.756718) |
| MULTISET → STRING / 32 | 0.491673 (0.479175–0.542994) | 0.493186 (0.480225–0.510621) | 0.718291 (0.713210–0.727207) |

All final native cases are slower than stock and previous fallback under the accepted collection
tradeoff. These short complete jobs do not establish sustained throughput or standalone acceleration.
No new performance-only opt-in is added.

Optimization stops after two profiling/optimization passes. The eight-second numeric-cast CPU
capture has 12,872 samples, including 3,793 inclusive callback samples, 2,209 in generated evaluation,
2,812 in map serialization and 2,277 in Arrow map writing. The second eight-second capture, for
formatting, has 15,115 samples, including 4,007 in the callback, 3,303 in generated evaluation, 1,907
in map serialization and 908 in Arrow map writing. Inclusive counts overlap and include planning
and compilation; they do not measure allocation counts or copied bytes. No safe small shortcut
was identified that preserves released cast/copy semantics. Further serialization, formatting
and conversion work stays in [#392](https://github.com/datafusion-contrib/StreamFusion/issues/392).
No production Rust kernel or JNI ownership protocol changes in this increment.

Reproduce both cardinalities with a release native build:

```sh
SF_BENCHMARK=true mvn -pl streamfusion-runtime test -Pbench \
  '-Dtest=ScalarFunctionBenchmark#individualFunctions' -Dsf.testForks=1 \
  -Dscalar.functions=MULTISET_CAST,MULTISET_FORMAT_STRING \
  -Dscalar.rows=250000 -Dscalar.multiset.entries=32 -Dscalar.nullEvery=8 \
  -Dscalar.warmup=2 -Dscalar.runs=3
```

Also run with `-Dscalar.multiset.entries=4`. The previous checkout copies the same benchmark
and input sources and adds `-Dscalar.native.expected=false`. For each of the two CPU captures,
select `#profileFunction`, set `SF_PROFILE_SCALAR=true`, choose one function, and use
`-Dscalar.warmup=1 -Dscalar.seconds=8 -Dprofile.asprof=/path/to/asprof`.

## Fixed BINARY MULTISET elements

The [verified generated boundary](../operators/calc-filter.md#fixed-binary-multiset-elements)
admits nullable fixed-BINARY elements on both released lines and NOT NULL elements on 2.2.
Flink 1.18 NOT NULL elements retain the observed unchecked-NULL getter/formatting fallback.
Fixed-binary MULTISET CASE retains released implicit branch casts, including 1.18 padding.
No state, ownership or Rust operation changes. The local matrix passes 235 checks on 2.2.1
and 191 with 44 version skips on 1.18.1. This combines the broad eight-class matrix and final
24-case multiset reruns after CASE routing and deterministic-control corrections; the other
211 checks are unchanged. Benchmark smokes are excluded from correctness totals.

Release/mimalloc measurements on the Intel Core i7-12650H Linux x86-64/WSL2 host use
Flink 2.2.1, OpenJDK 17.0.20.1, parallelism one and an 8 GiB heap. Runtime internal RowData
contains an ordered four-entry multiset with 4-byte keys at 500,000 rows, then 16 entries
with 256-byte keys at 200,000 rows. Each has one NULL key with count 3; other key counts cycle
through 1/2/3. Every eighth parent is NULL. CAST changes BINARY(width) elements to
VARBINARY(width); the second query formats the original multiset as STRING.

Timing includes planning, submission, runtime source, key conversions, output, both
row/Arrow transposes and the blackhole row sink. Fresh per-job metrics-cluster/schema setup
and plan assertions are outside timing. Each admitted job requires positive consumed Calc
and both transpose counters; stock/prior-fallback jobs require no native operators. Two
warmups precede three alternating trials per engine in each separate cohort JVM. Prior
candidate 7391128b uses identical benchmark/input sources and the same frozen release DSO.
Source-matched identity controls remain native on both checkouts and are retained without
subtraction. Their variability and all independent prior stock controls remain in the
[full evidence](results/fixed-binary-multisets-2026-10-24.json).

A two-second sampler observed no overlapping external Java/Cargo workloads. Two matrix
observations and one profile observation are own Maven launch shells, identified by their
unique CSV/profile-output arguments within 0.1 seconds of phase launch before JVM replacement.
The committed summary retains those classifications; raw local reports retain every observation.

Median seconds (minimum–maximum):

| Expression | Bytes per key / entries | Paired stock | Previous fallback | Native route |
| --- | ---: | ---: | ---: | ---: |
| CAST | 4/4 | 0.211763 (0.204312–0.309692) | 0.205085 (0.200423–0.211794) | 0.422545 (0.412186–0.430073) |
| STRING | 4/4 | 0.337583 (0.331511–0.347192) | 0.357177 (0.339805–0.377967) | 0.490561 (0.481319–0.500198) |
| CAST | 256/16 | 0.382414 (0.379815–0.387833) | 0.379241 (0.377375–0.386449) | 1.460521 (1.444518–1.474352) |
| STRING | 256/16 | 2.529796 (2.512200–2.538575) | 2.769301 (2.761990–2.785375) | 4.928001 (4.909214–5.198685) |

All four native medians exceed both paired stock and prior fallback. These complete-job
costs ship under the accepted collection tradeoff; they establish no acceleration claim.
The shorter jobs retain startup and trial variation rather than implying sustained throughput.

The bounded investigation closes after two eight-second target CPU captures, after one
unprofiled native job each. Both use 200,000 rows, 256-byte keys and 16 entries. CAST has 11,711
samples: 2,929 inclusive in the callback, 822 in generated evaluation, 3,957 in Arrow map
writing and 2,088 in map serialization. STRING has 13,087 samples: 7,623 in the callback,
2,563 in evaluation,895 in map writing and 4,830 in represented-string writing. UTF16-to-UTF8
encoding contributes 1,595 self samples, with 527 in UTF8-to-UTF16 decoding. Counts overlap
and include fresh-cluster setup, planning, compilation and all JVM threads. They are not
allocation or copied-byte counts, nor a causal isolation of the entire slowdown against stock.
Map serialization/writing, callback conversions and represented string encoding remain costs.
No safe small shortcut preserving released key conversion and representation was found in
the two passes; further optimization belongs to
[#392](https://github.com/datafusion-contrib/StreamFusion/issues/392).

Reproduce the small shape after a release build:

```sh
SF_BENCHMARK=true mvn -pl streamfusion-runtime test -Pbench \
  '-Dtest=ScalarFunctionBenchmark#individualFunctions' -Dsf.testForks=1 \
  -Dscalar.functions=BINARY_MULTISET_CAST,BINARY_MULTISET_STRING \
  -Dscalar.rows=500000 -Dscalar.binary.width=4 -Dscalar.multiset.entries=4 \
  -Dscalar.nullEvery=8 -Dscalar.warmup=2 -Dscalar.runs=3
```

Also run rows 200000, width 256, entries 16. The prior checkout copies the same benchmark/input
sources and adds `-Dscalar.native.expected=false`. For each of the two profiles, select
`#profileFunction`, set `SF_BENCHMARK=true SF_PROFILE_SCALAR=true`, choose one function,
and retain the wide shape with `-Dscalar.warmup=1 -Dscalar.seconds=8` plus an async-profiler
path through `profile.asprof`. Profiled timings do not enter the comparison table.

## NOT NULL fixed BINARY map leaves

On Flink 2.2, fixed BINARY(n) NOT NULL map keys and values now enter the existing generated
collection evaluator. The Rust MAP_KEYS/MAP_VALUES child-view whitelist is unchanged: these
new NOT NULL-element projections retain generated evaluation. Flink 1.18 keeps its fallback
because unchecked NULL getters can change released map formatting. No native operation,
transport schema, state serializer or ownership protocol changes.

Release/mimalloc measurements on 2026-10-24 use Flink 2.2.1, OpenJDK 17.0.20.1 and Linux
x86-64/WSL2 on an Intel Core i7-12650H. Every job has 500,000 rows, parallelism one, an 8 GiB
heap, two warmups and three alternating trials per engine. Widths are four and 256 bytes.
The MAP_VALUES fixture has three ASCII STRING keys and three non-null binary values;
the MAP_KEYS fixture has three non-null binary keys with INT values 1/NULL/-1. Every eighth
parent is NULL. Performance inputs respect declared widths and nullability; separate correctness
checks include real NULL leaves and out-of-declaration lengths in both generic and binary maps.

Previous candidate `25fe176a` uses identical benchmark/input sources and the same frozen release
DSO. The current parent's later merge adds only a unit-test fixture repair and suite documentation;
production and measured benchmark sources are unchanged. Each admitted measured job requires
consumed NativeCalc and both row/Arrow transpose counters. Stock and previous-fallback jobs
require no native operators. Same-source identity controls still consume native work on both
checkouts and are retained without subtraction. Timing includes planning/submission, processing,
all intrinsic conversions and the blackhole row sink. Fresh per-job metrics-cluster/schema setup
and plan assertions are outside timing. No other local Java/Cargo work overlaps measurements.
[All trials, both stock controls, identities and profiles](results/not-null-fixed-binary-maps-2026-10-24.json)
retain the unfavorable results.

Median seconds (minimum–maximum), including complete-job startup:

| Expression / bytes per leaf | Paired stock | Previous fallback | Generated native route |
| --- | ---: | ---: | ---: |
| NOT_NULL_BINARY_MAP_VALUES / 4 | 0.295370 (0.288900–0.307176) | 0.301189 (0.289556–0.302601) | 0.620183 (0.619322–0.622486) |
| NOT_NULL_BINARY_MAP_STRING / 4 | 0.458551 (0.455308–0.476524) | 0.454731 (0.442781–0.461344) | 0.796819 (0.796102–0.801216) |
| NOT_NULL_BINARY_MAP_KEYS / 4 | 0.254220 (0.253209–0.262033) | 0.242558 (0.240891–0.245612) | 0.463470 (0.462984–0.464247) |
| NOT_NULL_BINARY_KEYS_STRING / 4 | 0.402373 (0.397256–0.413330) | 0.382396 (0.380785–0.383945) | 0.570521 (0.569917–0.572501) |
| NOT_NULL_BINARY_MAP_VALUES / 256 | 0.381256 (0.375123–0.382389) | 0.379787 (0.377312–0.388174) | 0.846614 (0.836081–0.851432) |
| NOT_NULL_BINARY_MAP_STRING / 256 | 1.484237 (1.480179–1.501203) | 1.464445 (1.449250–1.486186) | 3.074439 (3.047926–3.095101) |
| NOT_NULL_BINARY_MAP_KEYS / 256 | 0.319906 (0.318649–0.339938) | 0.310305 (0.305981–0.321606) | 0.675674 (0.658897–0.679109) |
| NOT_NULL_BINARY_KEYS_STRING / 256 | 1.383292 (1.335081–1.385634) | 1.379093 (1.372284–1.394037) | 2.419619 (2.377253–2.434042) |

Optimization stopped after exactly two eight-second CPU captures at 256-byte width. MAP_VALUES
has 13,987 samples across all JVM threads, including 2,494 inclusive callback samples, 1,120
in generated row evaluation, 1,033 in map serialization and 350 in array serialization.
Binary-key map formatting has 14,113 samples, including 6,115 in the callback, 2,426 in generated
evaluation and 3,329 in represented-string writing. Both retain copying/materialization and
perimeter costs. Counts overlap and include fresh-cluster setup, planning and compilation;
they are not allocation or copied-byte measurements and do not isolate the whole slowdown.
No safe small shortcut preserving released NULL and representation behavior was identified
inside the timebox. No further profiling was performed for this increment.

Every new native median exceeds paired stock and previous fallback. These short complete jobs
ship under the accepted collection-coverage tradeoff and do not establish standalone acceleration
or sustained throughput. Further performance work stays in
[#392](https://github.com/datafusion-contrib/StreamFusion/issues/392).

Reproduce each width with a released native build:

```sh
SF_BENCHMARK=true mvn -pl streamfusion-runtime test -Pbench \
  '-Dtest=ScalarFunctionBenchmark#individualFunctions' -Dsf.testForks=1 \
  -Dscalar.functions=NOT_NULL_BINARY_MAP_VALUES,NOT_NULL_BINARY_MAP_STRING,NOT_NULL_BINARY_MAP_KEYS,NOT_NULL_BINARY_KEYS_STRING \
  -Dscalar.rows=500000 -Dscalar.binary.width=256 -Dscalar.nullEvery=8 \
  -Dscalar.warmup=2 -Dscalar.runs=3
```

Also run with `-Dscalar.binary.width=4`. The previous checkout copies both benchmark/input
sources and adds `-Dscalar.native.expected=false`. For each of NOT_NULL_BINARY_MAP_VALUES and
NOT_NULL_BINARY_KEYS_STRING, select `#profileFunction`, set `SF_PROFILE_SCALAR=true`, and use
`-Dscalar.native=true -Dscalar.warmup=1 -Dscalar.seconds=8`, with
`-Dprofile.asprof=/path/to/asprof -Dprofile.outputDir=/path/to/output`.

## Runtime ARRAY_SORT options (2026-10-27)

The runtime Boolean-option subset now executes in Rust for nullable integer and
DECIMAL arrays already proven shorter than 32 elements or produced by native
ARRAY_DISTINCT. Constant non-NULL options keep the existing DataFusion sort.
Unrestricted long duplicate arrays, representation-sensitive types and fallible
operands retain generated Flink evaluation. ARRAY_SORT is absent from 1.18.1.
This is one increment of [#505](https://github.com/datafusion-contrib/StreamFusion/issues/505).

The small-function comparison uses Flink 2.2.1, HotSpot 17.0.12, Rust 1.94.0
release/mimalloc without RocksDB, macOS 26.5 arm64, an Apple M4 Pro with 14 logical
cores and 24 GiB RAM. The JVM heap is 1 GiB; execution is serial, without CPU
pinning. No task builds or other tests overlap measurements; desktop activity is
uncontrolled. Baseline `eca8f717` precedes this change. The subsequently merged
integer SPLIT_INDEX increment does not change the measured ARRAY_SORT paths.

Every engine evaluates the same query and logical inputs:

```sql
SELECT ARRAY_SORT(ARRAY_SLICE(a,1,31),ascending_order,null_first) FROM inputs
```

Stock starts on prepared GenericRowData or BinaryRowData in Java. Previous
StreamFusion and the candidate start on prepared Arrow RecordBatch input in
Rust. Setup and initial expression compilation are outside timers. Stock times
released generated evaluation and complete typed-array consumption. Both native
timers run inside Rust around production compiled Calc evaluation and equivalent
typed Arrow consumption. Previous StreamFusion retains its whole-Calc kind-38
JVM callback, including the callback's Arrow import/export and conversions.
The candidate uses the production ARRAY_SLICE and typed ARRAY_SORT kernels,
without a JVM callback. Initial native entry, final output export and perimeter
row/Arrow transposes are outside this small-function boundary only.

Arrays contain repeated exact values; BIGINT values exceed 2^53 and decimals are
DECIMAL(38,9). Nullable profiles include NULL parents, elements and independently
NULL option flags. Ascending and NULL-order flags vary by row. Output schema and
complete arrays match the previous generated evaluator before timing; every
stock/previous/native trial also checks matching consumed-value checksums.
Each profile uses eight warmups and nine measured trials, rotating engine order
per trial. Batches of 1,024 rows run 32 iterations; batches of 16 run 1,024.

Median nanoseconds per row (minimum-maximum); the binary/generic column describes
stock's prepared input, and every StreamFusion input is already Arrow:

| Type / width / batch rows / stock input / NULL profile | Stock Flink | Previous StreamFusion | Rust runtime options | Stock/native |
| --- | ---: | ---: | ---: | ---: |
| INT / 8 / 1024 / generic / nullable | 92.65 (90.81-143.65) | 247.91 (240.87-320.22) | 30.79 (29.15-33.61) | 3.01x |
| INT / 31 / 1024 / generic / nullable | 469.91 (457.15-476.93) | 781.34 (735.84-813.48) | 93.28 (90.74-98.95) | 5.04x |
| BIGINT / 8 / 1024 / binary / nullable | 139.24 (126.27-270.01) | 276.10 (255.95-325.14) | 32.58 (29.15-35.66) | 4.27x |
| BIGINT / 31 / 1024 / binary / non-null | 817.17 (799.83-858.47) | 1178.60 (1113.14-1253.36) | 82.44 (76.79-92.76) | 9.91x |
| DECIMAL / 8 / 1024 / binary / nullable | 254.94 (243.62-312.33) | 640.27 (609.27-668.54) | 33.67 (32.30-37.39) | 7.57x |
| DECIMAL / 31 / 1024 / generic / non-null | 1180.24 (1163.51-1233.21) | 2983.79 (2839.01-3053.41) | 127.77 (124.37-133.96) | 9.24x |
| INT / 8 / 16 / binary / nullable | 125.21 (121.63-133.57) | 1647.62 (1455.35-1853.17) | 93.03 (84.16-102.95) | 1.35x |
| DECIMAL / 31 / 16 / generic / nullable | 633.60 (624.04-659.60) | 3119.82 (3051.92-3340.86) | 160.04 (152.57-165.47) | 3.96x |

All eight measured medians improve on stock and previous StreamFusion. The
previous/native ratios range from 8.05x to 23.35x. These measurements establish
the applicable small-function gate; they do not measure complete-job throughput.
[All 408 trials, configuration and fingerprints](results/runtime-array-sort-2026-10-27.json)
retain warmups, ranges and the slower observations.

A separate conversion-inclusive diagnostic uses 500,000 nullable INT rows of
width eight, parallelism one, a 2 GiB heap, one warmup and three measured trials
in a fresh JVM per engine. It retains a row source, both row/Arrow transposes and
a blackhole row sink. Native plans require consumed NativeCalc work; the
candidate must have no bound JVM expression, while the previous planner must
retain its generated evaluator. Planning/submission and execution are timed;
MiniCluster/environment construction and EXPLAIN checks are outside timing.
The same release library and fixture run stock, previous, then candidate.

| Complete job | Median seconds | Minimum-maximum |
| --- | ---: | ---: |
| Stock Flink | 0.224650 | 0.219570-0.225222 |
| Previous StreamFusion | 0.277142 | 0.274509-0.286544 |
| Rust runtime options | 0.197072 | 0.193343-0.204127 |

The candidate takes 12.3% less time than stock and 28.9% less than previous in
this short job. This single profile does not establish sustained performance
for every width/type, nor remove general row/Arrow perimeter costs.

The 27 focused Flink 2.2 SQL tests pass, including positive consumed native-work
checks for all five admitted element types, independent nullable flags, a
66-element distinct array, filtering, empty input and lazy failing branches.
The unchanged released ARRAY_SORT specification also passes all 56 invocations
under stock and the injected native planner. Its 54 executing queries retain
their expected custom-type-inference fallback, with zero native-work credit;
the other two are validation cases. Native sort evidence comes from the focused
SQL contracts and the compiled-Calc test that runs without a JVM. On 1.18,
ARRAY_SORT is N/A; shared array search/distinct regressions pass on both lines.

After normal Java test compilation, construct a test classpath containing
`streamfusion-runtime/target/test-classes`, its production classes and released
dependencies, and supply it as `SF_NATIVE_BENCH_CLASSPATH`. Set
`SF_NATIVE_LIB_DIR` to the matching release library directory and select JDK 17
with `JAVA_HOME`. Run one small profile from the repository root:

```sh
cargo +1.94.0 run --manifest-path native/Cargo.toml --release --no-default-features \
  -p streamfusion --example runtime_array_sort -- int 8 true false 1024 32 8 9
```

Arguments are element (`int`, `bigint`, `decimal`), width, nullable, binary stock
input, batch rows, iterations, warmups and measured trials. The complete-job
diagnostic is optional and excluded from ordinary test runs:

```sh
SF_BENCHMARK=true mvn -pl streamfusion-runtime test -Pbench \
  -Dtest=RuntimeArraySortPipelineBenchmark -Dsf.testForks=1 \
  -Darray-sort.pipeline.engine=native -Darray-sort.pipeline.rows=500000 \
  -Darray-sort.pipeline.warmups=1 -Darray-sort.pipeline.runs=3
```

Use `engine=flink` for stock. For `engine=previous`, compile the same fixture
against the preceding planner and require its bound generated ARRAY_SORT route;
the candidate planner deliberately fails that previous-route assertion.

## Integral SIGN

Measured on 2026-10-27, canonical baseline `7a6a790e`, Apple M4 Pro (14 logical CPUs),
macOS aarch64, Flink 2.2.1, JDK 17.0.12, Rust 1.94.0 release with default features disabled
and the system allocator. This replaces generated computation in an already-admitted Calc;
it adds no operator, type admission or configuration switch.

`integral_sign` prepares equal integer values, minima and NULLs outside timing. Stock Flink
uses its released generated expression over prebuilt GenericRowData or BinaryRowData in Java.
The previous and candidate controls use production native Calc over prebuilt Arrow batches;
the previous generated JVM callback, its conversions and dispatch remain inside the Rust timer.
The candidate uses the typed SIGN kernel. Every output value participates in a checksum,
with full Arrow output/schema equality checked before timing. JNI entry/input import, final
export and perimeter row transposes are outside this prepared-input timer.

Both logical schemas are nullable; the nullable flag controls actual data (every seventh row
is NULL when true, none when false). Each case has 16 warmups and 11 measured trials, 131,072 rows per trial, rotating the three
engine orders. Values are ns/row, median [min, max]; speedups divide stock/previous by candidate.

| Type, input NULLs, stock row format, batch rows | Stock | Previous callback | Candidate | vs stock | vs previous |
| --- | ---: | ---: | ---: | ---: | ---: |
| BIGINT, no NULLs, binary, 1024 | 6.426 [6.258, 6.815] | 42.782 [41.370, 44.467] | 0.620 [0.578, 0.699] | 10.37x | 69.05x |
| BIGINT, with NULLs, generic, 1024 | 4.819 [4.381, 5.007] | 43.542 [41.267, 59.324] | 1.222 [1.035, 1.296] | 3.94x | 35.62x |
| BIGINT, with NULLs, binary, 16 | 6.275 [6.061, 7.140] | 773.391 [755.702, 841.681] | 22.712 [21.311, 26.172] | 0.28x | 34.05x |
| INT, no NULLs, binary, 1024 | 6.701 [6.256, 6.776] | 42.707 [41.813, 44.414] | 0.536 [0.512, 0.636] | 12.50x | 79.68x |
| INT, with NULLs, generic, 1024 | 4.554 [4.367, 4.623] | 42.878 [41.013, 55.608] | 1.428 [1.335, 1.481] | 3.19x | 30.03x |
| INT, with NULLs, binary, 16 | 5.650 [5.397, 5.821] | 749.547 [740.193, 768.904] | 22.887 [22.644, 23.201] | 0.25x | 32.75x |

At 1,024 rows the candidate wins these prepared-input controls. At 16 rows it remains slower
than stock despite removing most of the preceding callback cost. A separate native-only
five-second sample identifies generic DataFusion argument/field setup and destruction,
allocation, batch construction and consumption as remaining small-batch costs. Inclusive
frames overlap; they are not independent percentages or a universal causal estimate.
The implementation bypasses the generic scalar adapter for integer arrays/scalars. A production
Calc allocation probe still requests 20 allocations / 1,255 bytes for a nullable 16-row INT
batch and 20 / 5,287 bytes for 1,024 rows: result values are new, validity is shared.
Further Calc batching/argument overhead remains part of [#491](https://github.com/datafusion-contrib/StreamFusion/issues/491).

A separate complete INT job retains the bounded 1M-row Flink source, both transposes and
rowwise blackhole sink, with parallelism one, two warmups and five measured jobs in separate
sequential JVMs for stock, previous and candidate. The native plan contains both boundary
transposes and consumed NativeCalc work is required. Elapsed seconds are median [min, max]:

| Stock | Previous callback | Candidate |
| ---: | ---: | ---: |
| 0.129245 [0.119284, 0.135270] | 0.168748 [0.159775, 0.187790] | 0.144245 [0.134325, 0.155320] |

The complete candidate job improves the preceding implementation but still loses to stock.
These short local jobs include submission/JIT/startup effects; they do not establish a
sustained end-to-end acceleration. Do not infer a standalone win from the prepared-input
results. Full measured trials, configuration and allocation observations are retained in the
[raw result record](results/integral-sign-2026-10-27.json).

The example is `native/engine/examples/integral_sign.rs`; the stock/previous helper is
`IntegralSignBenchmarkFunctions` and the full job is `IntegralSignPipelineBenchmark`. Build
the Java test classes, prepare their dependency classpath and set `JAVA_HOME`,
`SF_NATIVE_BENCH_CLASSPATH` and `SF_NATIVE_LIB_DIR` to the release library directory. Then run:

```sh
cargo build --manifest-path native/Cargo.toml -p streamfusion --release \
  --no-default-features --lib --example integral_sign
native/target/release/examples/integral_sign INT true false 1024 128 16 11
```

The arguments are type (INT/BIGINT), with NULLs, stock BinaryRowData, rows/batch, loops, warmups
and trials. `SF_NATIVE_BENCH_ENGINE=native` optionally isolates the candidate for profiling.
The full job is opt-in with `SF_BENCHMARK=true` and the `bench` Maven profile; use
`-Dtest=IntegralSignPipelineBenchmark -Dsign.pipeline.engine=flink` or `native`. Run the same
benchmark class with the baseline planner/native library for the preceding implementation.

Correctness checks pass on Flink 2.2.1 and 1.18.1 with JDK 11 and 17: 12 focused SQL cases
and three existing numeric/floating cases per combination. The focused runtime witnesses
consume 5,003 rows for direct INT/BIGINT and explicitly widened narrow inputs, with no JVM
evaluator. Released unchanged `ScalarFunctionsTest.testSign` passes on both lines; it tests
generated expressions and is oracle evidence, while the focused runtime witnesses establish
executed native work. The release Rust core suite passes 813 tests with default features
disabled. Full JVM/upstream suites are left to CI.

## Integral ABS

October 27, 2026; Apple M4 Pro/macOS aarch64, JDK 17, released Flink 2.2.1,
Rust 1.94.0, release build without default features and with the system allocator.
This replaces the JVM evaluator inside an already-admitted Calc for the verified integer
ABS subset; it does not expand the set of admitted SQL islands.

The functionality control starts stock Flink on prepared RowData and the native engines on
matching prepared Arrow RecordBatch inputs. The stock timer is inside Java and the native
timers are inside Rust. The preceding control uses the existing generated whole-Calc callback;
its intrinsic JNI, Arrow handoffs and output writing remain timed. The candidate executes the
compiled production Calc, including result allocation and checksum consumption. Setup,
initial input import, final output export and perimeter transposes are outside this small
functionality timer. Checksums, output types and NULL positions agree before timing.

All four widths include mixed signs and minimum negative values. Nullable inputs insert NULL
every seventh row. Each engine has 16 warmups and 11 measured trials of 131,072 rows, rotating
engine order. The allocation probe runs outside timing; its counting allocator remains inactive
during the measured iterations. Table entries are median nanoseconds per row; all trials and
minimum/maximum variability are in the [raw results](results/integral-abs-2026-10-27.json).

| Type / input profile | Stock Flink | Previous callback | Rust Calc | Stock / Rust |
| --- | ---: | ---: | ---: | ---: |
| TINYINT / nullable GenericRowData, batch 1024 | 4.798 | 43.584 | 0.915 | 5.24x |
| SMALLINT / nullable GenericRowData, batch 1024 | 4.524 | 41.101 | 0.907 | 4.99x |
| INT / nullable GenericRowData, batch 1024 | 4.557 | 40.236 | 0.916 | 4.98x |
| BIGINT / nullable GenericRowData, batch 1024 | 4.797 | 41.975 | 0.960 | 5.00x |
| TINYINT / no-NULL BinaryRowData, batch 1024 | 6.486 | 43.763 | 0.529 | 12.27x |
| SMALLINT / no-NULL BinaryRowData, batch 1024 | 6.508 | 42.487 | 0.529 | 12.30x |
| INT / no-NULL BinaryRowData, batch 1024 | 5.988 | 43.962 | 0.536 | 11.17x |
| BIGINT / no-NULL BinaryRowData, batch 1024 | 6.363 | 43.394 | 0.605 | 10.51x |
| TINYINT / nullable BinaryRowData, batch 16 | 5.836 | 739.484 | 21.764 | 0.27x |
| SMALLINT / nullable BinaryRowData, batch 16 | 5.759 | 745.591 | 23.342 | 0.25x |
| INT / nullable BinaryRowData, batch 16 | 5.613 | 784.946 | 23.013 | 0.24x |
| BIGINT / nullable BinaryRowData, batch 16 | 6.055 | 764.878 | 22.401 | 0.27x |

The default-size batches are 4.98-12.30x faster than stock and 43.72-82.78x faster than the
preceding callback. Sixteen-row batches improve 31.94-34.15x over the old callback but remain
slower than stock. Direct scalar/array dispatch removes the generic scalar adapter's temporary
vectors, but fixed Calc overhead remains: the final INT probe records 20 Rust allocation
requests at both batch sizes, requesting 1,255 bytes for 16 rows and 5,287 bytes for 1,024 rows.
The output value buffer is new and the input validity buffer is shared. The operation still
constructs a projected input and an output batch per call. These numbers do not claim that
Rust beats stock for tiny batches.

A separate standalone INT query uses one million runtime rows, parallelism one, a bounded
row source, a blackhole row sink, default batches, two warmups and five measured runs per
engine in separate sequential JVM processes. Native runs require consumed NativeCalc work
and plans containing both RowDataToArrow and ArrowToRowData. Median complete-job seconds
(minimum-maximum), including submission/startup, intrinsic JNI and both transposes:

| Stock Flink | Previous callback | Rust Calc |
| ---: | ---: | ---: |
| 0.127333 (0.123515-0.129871) | 0.160102 (0.157433-0.180064) | 0.144527 (0.136700-0.155362) |

The candidate improves the old standalone median but remains slower than stock. These short
jobs and sequential process order do not establish a sustained end-to-end throughput win.
Only the default-size prepared-input functionality comparison establishes a speedup here.

Reproduce after compiling test classes and resolving their released Flink classpath:

```sh
export SF_NATIVE_BENCH_CLASSPATH="streamfusion-runtime/target/test-classes:streamfusion-runtime/target/classes:$(cat /path/to/test-classpath.txt)"
export SF_NATIVE_LIB_DIR="$PWD/native/target/release"
cargo build --manifest-path native/Cargo.toml -p streamfusion --release \
  --no-default-features --lib --example integral_abs
native/target/release/examples/integral_abs INT true false 1024 128 16 11
```

The arguments are type, inject-NULLs, use-BinaryRowData, rows per batch, loops, warmups and
trials. Select each of TINYINT/SMALLINT/INT/BIGINT; also run `false true 1024 128 16 11`
and `true true 16 8192 16 11`. The complete-job diagnostic is
`SF_BENCHMARK=true mvn -pl streamfusion-runtime test -Pbench
-Dtest=IntegralAbsPipelineBenchmark -Dabs.pipeline.engine=native`; use `flink` for stock.
For the previous complete-job control, use the parent planner and matching native library
with the same benchmark class and inputs.

## Integral unary minus

Measured on 2026-10-27, canonical baseline `7a6a790e`, Apple M4 Pro (14 logical CPUs),
macOS aarch64, Flink 2.2.1, JDK 17.0.12, Rust 1.94.0 release with default features disabled
and the system allocator. This removes generated computation from an already-admitted Calc;
it introduces no operator/type admission or configuration switch.

The `integral_negate` example prepares identical integer widths, values, minima and NULLs.
Stock executes the released generated expression over prebuilt GenericRowData or BinaryRowData
with a Java timer. Previous/candidate execute production Calc over prebuilt Arrow RecordBatch
with timers inside Rust. The preceding intrinsic JVM callback and its conversions remain in
timing. The candidate uses DataFusion's physical negative expression for the input column.
Full output/schema equality is checked before measurement; every output participates in the
checksum. Setup, JNI entry/input import, final export and perimeter transposes are outside this
prepared-input timer. Both schemas are nullable; the NULL flag controls actual data (every
seventh row is NULL when true, none when false).

Each case has 16 warmups, 11 measured trials and 131,072 rows per trial. The three engine orders
rotate each trial. Values are ns/row, median [min, max]; speedups divide stock/previous by candidate.

| Type, input NULLs, stock row format, batch rows | Stock | Previous callback | Candidate | vs stock | vs previous |
| --- | ---: | ---: | ---: | ---: | ---: |
| BIGINT, no NULLs, binary, 1024 | 6.568 [6.366, 6.880] | 43.653 [41.321, 46.587] | 0.503 [0.476, 0.535] | 13.07x | 86.86x |
| BIGINT, with NULLs, generic, 1024 | 4.648 [4.501, 4.868] | 42.628 [40.340, 55.610] | 0.840 [0.809, 0.865] | 5.53x | 50.75x |
| BIGINT, with NULLs, binary, 16 | 5.954 [5.890, 6.078] | 755.616 [750.959, 773.752] | 18.546 [18.170, 18.993] | 0.32x | 40.74x |
| INT, no NULLs, binary, 1024 | 6.356 [6.096, 6.550] | 41.874 [41.222, 55.153] | 0.456 [0.432, 0.490] | 13.94x | 91.86x |
| INT, with NULLs, generic, 1024 | 4.529 [4.438, 4.743] | 40.935 [39.514, 55.468] | 0.828 [0.788, 0.874] | 5.47x | 49.45x |
| INT, with NULLs, binary, 16 | 5.523 [5.186, 5.776] | 741.295 [730.931, 763.478] | 16.898 [16.172, 17.647] | 0.33x | 43.87x |
| SMALLINT, no NULLs, binary, 1024 | 6.682 [6.150, 6.839] | 39.565 [38.405, 41.207] | 0.413 [0.395, 0.436] | 16.19x | 95.89x |
| SMALLINT, with NULLs, generic, 1024 | 4.593 [4.550, 4.630] | 40.504 [40.053, 60.173] | 0.818 [0.797, 0.836] | 5.62x | 49.52x |
| SMALLINT, with NULLs, binary, 16 | 5.770 [5.651, 5.978] | 771.427 [760.795, 786.748] | 18.223 [17.536, 18.751] | 0.32x | 42.33x |
| TINYINT, no NULLs, binary, 1024 | 6.705 [6.390, 6.922] | 41.736 [40.644, 44.806] | 0.437 [0.421, 0.447] | 15.33x | 95.41x |
| TINYINT, with NULLs, generic, 1024 | 4.732 [4.596, 5.028] | 41.661 [40.280, 60.471] | 0.813 [0.783, 0.882] | 5.82x | 51.27x |
| TINYINT, with NULLs, binary, 16 | 5.821 [5.596, 5.913] | 755.487 [747.510, 766.201] | 17.883 [17.567, 18.380] | 0.33x | 42.25x |

The 1,024-row prepared-input cases beat stock; the 16-row cases still lose to stock despite
removing most of the preceding callback cost. A five-second native-only CPU sample exposed
scalar-function argument/field cloning, dispatch and destruction in the initial adapter. The
final planner reuses DataFusion's physical negative expression for verified input-reference
chains and reserves the wrapping adapter for literal-rooted chains. A nullable 16-row INT
allocation probe falls from 20 requests / 1,255 bytes to 16 / 838 bytes. Output values are new
and validity is shared. These are allocation traffic counts, not peak memory or copied bytes.
The remaining profile includes Calc batch/schema construction, allocation, Arrow numeric output
and checksum consumption; inclusive stacks overlap. Generic small-batch costs remain part of
[#491](https://github.com/datafusion-contrib/StreamFusion/issues/491).

A separate complete 1M-row INT job retains the bounded Flink row source, both transposes and
rowwise blackhole sink. Parallelism is one, with two warmups and five measured jobs per engine
in separate sequential stock/previous/candidate JVMs. Native plans require both boundary
transposes and consumed NativeCalc work. Elapsed seconds are median [min, max]:

| Stock | Previous callback | Candidate |
| ---: | ---: | ---: |
| 0.128052 [0.119442, 0.132488] | 0.162798 [0.161175, 0.181275] | 0.146477 [0.134687, 0.153037] |

The complete candidate job improves the preceding implementation but still loses to stock.
These short jobs include submission, JIT and startup costs; the results do not demonstrate
sustained standalone acceleration. The prepared-input win does not remove the row/Arrow
perimeter cost. Full repeated measurements, configuration and allocation observations are in
the [raw result record](results/integral-negate-2026-10-27.json).

The example is `native/engine/examples/integral_negate.rs`, with Java helper
`IntegralNegateBenchmarkFunctions` and opt-in complete-job fixture `IntegralNegatePipelineBenchmark`.
Build Java test classes and their dependency classpath, set `JAVA_HOME`,
`SF_NATIVE_BENCH_CLASSPATH` and `SF_NATIVE_LIB_DIR` to the matching release library directory:

```sh
cargo build --manifest-path native/Cargo.toml -p streamfusion --release \
  --no-default-features --lib --example integral_negate
native/target/release/examples/integral_negate INT true false 1024 128 16 11
```

Arguments are integer type (TINYINT/SMALLINT/INT/BIGINT), with NULLs, stock BinaryRowData,
rows/batch, loops, warmups and measured trials. `SF_NATIVE_BENCH_ENGINE=native` isolates the
candidate for profiling. The complete job uses `SF_BENCHMARK=true`, Maven `-Pbench`,
`-Dtest=IntegralNegatePipelineBenchmark` and `-Dnegate.pipeline.engine=flink` or `native`.
Run the same fixture against the baseline planner/native library for the preceding control.

Validation passes on Flink 2.2.1/1.18.1 with JDK 11/17: 14 focused SQL cases, one encoding
case and two existing numeric cases per combination. Runtime witnesses consume 5,003 rows
per integer width with a recorded native minus route, no JVM evaluator and no fallback.
Minima, widths, NULLs, filtering, empty batches, lossless widening, generated producer guards
and selected/unselected failure behavior are covered. Unchanged released
`ScalarOperatorsTest.testUnaryPlusMinus` passes on both lines; it exercises generated
expressions and supplies oracle evidence, while the focused witnesses establish executed
native work. All 814 release Rust core tests pass without default features, including a
compiled-Calc literal-minimum test that runs without a JVM. Full JVM/upstream suites are left
to CI.

## Integer FLOOR

Measured on 2026-10-27 against canonical main `1604a3aa`: Apple M4 Pro (14 logical CPUs),
macOS aarch64, Flink 2.2.1, JDK 17.0.12, Rust 1.94.0 release, default features disabled,
system allocator. The change removes generated computation from an already-admitted Calc;
it does not add operator/type admission or a configuration switch.

`integral_rounding` prepares identical typed integer values, minima, BIGINTs above 2^53 and
NULLs outside timing. Stock uses released generated Java over prepared GenericRowData or
BinaryRowData. Previous and candidate use production Calc over prepared Arrow; the previous
JVM callback, its conversion and dispatch remain inside the Rust timer. Candidate integer
FLOOR emits its operand directly. Full Arrow/schema equality is checked before timing;
every output value participates in an equal checksum in its own runtime. JNI entry/import,
final export and perimeter transposes are outside this small-function timer.

All schemas are nullable; the input NULL flag controls actual values (NULL every seventh row
when true). Each case has 16 warmups and 11 measured trials of 131,072 rows, rotating
stock/previous/native order. Values are ns/row, median [min, max].

| Type, input NULLs, stock format, batch rows | Stock | Previous callback | Candidate | vs stock | vs previous |
| --- | ---: | ---: | ---: | ---: | ---: |
| BIGINT, no NULLs, binary, 1024 | 6.403 [6.145, 6.606] | 43.082 [41.289, 44.238] | 0.337 [0.325, 0.340] | 19.00x | 127.85x |
| BIGINT, with NULLs, generic, 1024 | 4.740 [4.649, 17.621] | 40.934 [39.657, 42.302] | 0.711 [0.699, 0.836] | 6.67x | 57.59x |
| BIGINT, with NULLs, binary, 16 | 6.242 [5.819, 6.345] | 766.428 [754.580, 802.290] | 14.398 [13.828, 15.580] | 0.43x | 53.23x |
| INT, no NULLs, binary, 1024 | 6.459 [6.215, 6.724] | 42.038 [40.380, 64.610] | 0.326 [0.295, 0.353] | 19.78x | 128.76x |
| INT, with NULLs, generic, 1024 | 4.638 [4.491, 5.035] | 42.349 [40.091, 59.744] | 0.714 [0.702, 0.789] | 6.50x | 59.34x |
| INT, with NULLs, binary, 16 | 5.905 [5.735, 6.150] | 741.679 [736.464, 775.826] | 14.805 [14.082, 15.093] | 0.40x | 50.10x |
| SMALLINT, no NULLs, binary, 1024 | 6.331 [5.972, 6.491] | 41.370 [41.097, 43.408] | 0.336 [0.294, 0.351] | 18.84x | 123.12x |
| SMALLINT, with NULLs, generic, 1024 | 4.580 [4.271, 4.637] | 41.599 [39.527, 52.664] | 0.714 [0.611, 0.746] | 6.41x | 58.26x |
| SMALLINT, with NULLs, binary, 16 | 5.690 [5.347, 6.019] | 782.506 [765.244, 791.278] | 14.865 [13.527, 15.567] | 0.38x | 52.64x |
| TINYINT, no NULLs, binary, 1024 | 6.172 [5.958, 6.412] | 40.221 [38.532, 41.907] | 0.329 [0.303, 0.407] | 18.76x | 122.25x |
| TINYINT, with NULLs, generic, 1024 | 4.679 [4.580, 4.944] | 41.606 [40.318, 56.980] | 0.690 [0.630, 0.742] | 6.79x | 60.34x |
| TINYINT, with NULLs, binary, 16 | 5.444 [5.244, 5.704] | 752.234 [742.954, 769.962] | 14.164 [13.976, 14.453] | 0.38x | 53.11x |

The 1,024-row candidate wins these prepared-input controls. The 16-row controls still lose to
stock, despite sharing all output value/validity buffers and eliminating the callback. The
production INT Calc allocation probe requests 13 allocations / 606 bytes at either batch
size, including RecordBatch/result bookkeeping; no new output buffer bytes are requested.
A separate five-second native-only sample shows Calc evaluation, RecordBatch projection,
allocation/free, Arc ownership and checksum consumption as remaining costs. The sample is
diagnostic; overlapping frames do not establish independent percentages. This experiment does
not claim that pure Rust or columnar execution always wins.

A separate complete 1M-row INT job retains a bounded Flink row source, both transposes and a
rowwise blackhole sink. Parallelism is one, with two warmups and five measured jobs in
separate sequential stock/previous/native JVMs. Previous planning uses the unchanged main
encoder and requires a FLOOR JVM route; the candidate requires `jvm=[]`. Both native jobs
require NativeCalc, both transposes and positive consumed native work. Seconds are median
[min, max]:

| Stock | Previous callback | Candidate |
| ---: | ---: | ---: |
| 0.131442 [0.125461, 0.149769] | 0.158871 [0.157430, 0.172846] | 0.138411 [0.132027, 0.139610] |

The candidate improves the preceding implementation but remains slower than stock in this
complete job. These short jobs include startup, submission and JIT effects; they do not prove
sustained end-to-end acceleration. Full trials/checksums and allocation observations are in
[the raw result record](results/integral-floor-2026-10-27.json). Broader Calc/perimeter costs
remain in [#491](https://github.com/datafusion-contrib/StreamFusion/issues/491).

The released Java control is `IntegralRoundingBenchmarkFunctions`; the complete job is
`IntegralRoundingPipelineBenchmark`, enabled by `SF_BENCHMARK=true` and the `bench` profile.
After building Java test classes, prepare the dependency classpath and set `JAVA_HOME`,
`SF_NATIVE_BENCH_CLASSPATH` and `SF_NATIVE_LIB_DIR` to the release library directory:

```sh
cargo build --manifest-path native/Cargo.toml -p streamfusion --release \
  --no-default-features --lib --example integral_rounding
SF_INTEGRAL_ROUNDING_FUNCTION=FLOOR \
  native/target/release/examples/integral_rounding INT true false 1024 128 16 11
```

For the complete job, use `-Drounding.pipeline.function=FLOOR` and
`-Drounding.pipeline.engine=flink|previous|native`. The previous control requires the
preceding encoder first on the classpath; merely changing the engine label is insufficient.
`SF_NATIVE_BENCH_ENGINE=native` selects only native trials for a separate profiling diagnostic.

## Integer CEIL / CEILING

Measured separately on 2026-10-27 against canonical main `1604a3aa`, on the same Apple M4
Pro, Flink 2.2.1, JDK 17.0.12 and Rust 1.94.0 release configuration as [Integer FLOOR](#integer-floor).
The input values, all four widths, NULL patterns, stock GenericRowData/BinaryRowData and
Rust-side production Calc timing boundaries are identical. The query calls CEIL; CEILING
resolves to the same admitted operator and is additionally covered by runtime parity tests.
Previous generated JVM computation/conversions remain inside its Rust timer; candidate
integer rounding reuses the verified operand. Setup, JNI entry/import/export and perimeter
transposes are excluded from prepared-input timing. Complete schema/Arrow equality and
checksums verify every output. No operator/type admission or configuration switch is added.

All input schemas are nullable; the flag denotes actual NULL values. Each case rotates
stock/previous/native for 16 warmups and 11 measured trials of 131,072 rows. Values are
ns/row, median [min, max].

| Type, input NULLs, stock format, batch rows | Stock | Previous callback | Candidate | vs stock | vs previous |
| --- | ---: | ---: | ---: | ---: | ---: |
| BIGINT, no NULLs, binary, 1024 | 6.556 [6.260, 6.740] | 43.084 [41.326, 47.007] | 0.344 [0.329, 0.382] | 19.06x | 125.26x |
| BIGINT, with NULLs, generic, 1024 | 4.680 [4.479, 4.859] | 41.885 [39.602, 55.228] | 0.699 [0.676, 0.774] | 6.70x | 59.95x |
| BIGINT, with NULLs, binary, 16 | 6.058 [5.987, 6.602] | 756.261 [746.306, 797.435] | 13.754 [13.474, 14.605] | 0.44x | 54.99x |
| INT, no NULLs, binary, 1024 | 6.361 [6.240, 6.480] | 40.662 [38.870, 41.578] | 0.311 [0.280, 0.325] | 20.48x | 130.92x |
| INT, with NULLs, generic, 1024 | 4.534 [4.463, 4.759] | 41.025 [39.273, 54.750] | 0.692 [0.669, 0.814] | 6.55x | 59.28x |
| INT, with NULLs, binary, 16 | 5.335 [5.174, 5.623] | 753.331 [740.801, 781.752] | 14.322 [13.754, 14.870] | 0.37x | 52.60x |
| SMALLINT, no NULLs, binary, 1024 | 6.587 [6.408, 6.717] | 41.383 [40.316, 43.505] | 0.336 [0.300, 0.353] | 19.60x | 123.16x |
| SMALLINT, with NULLs, generic, 1024 | 4.627 [4.476, 5.068] | 41.518 [39.421, 54.906] | 0.693 [0.678, 0.739] | 6.67x | 59.88x |
| SMALLINT, with NULLs, binary, 16 | 5.530 [5.313, 7.826] | 741.658 [735.667, 765.638] | 15.277 [14.481, 15.898] | 0.36x | 48.55x |
| TINYINT, no NULLs, binary, 1024 | 6.353 [6.011, 6.534] | 39.769 [38.562, 40.987] | 0.367 [0.337, 0.393] | 17.32x | 108.41x |
| TINYINT, with NULLs, generic, 1024 | 4.728 [4.625, 4.823] | 41.698 [39.988, 54.603] | 0.675 [0.666, 0.692] | 7.01x | 61.78x |
| TINYINT, with NULLs, binary, 16 | 5.651 [5.270, 5.855] | 747.532 [735.846, 774.913] | 14.199 [13.643, 15.871] | 0.40x | 52.65x |

The candidate beats both controls at 1,024 rows. At 16 rows it remains slower than stock,
with the same shared-operand Calc path and generic batch/result costs diagnosed for FLOOR.
The INT allocation probe again requests 13 allocations / 606 bytes at both batch sizes;
all output value/validity buffers are shared. This is a measured improvement to the existing
native Calc path, without a universal small-batch acceleration claim.

The independent complete job uses the same 1M-row INT source, parallelism one, rowwise
blackhole sink, two warmups and five measured jobs in sequential stock/previous/native JVMs.
Both transposes stay in native plans, and positive consumed NativeCalc work is required.
The previous unchanged encoder must report a CEIL JVM route; candidate must report `jvm=[]`.
Seconds are median [min, max]:

| Stock | Previous callback | Candidate |
| ---: | ---: | ---: |
| 0.127985 [0.118868, 0.129826] | 0.154719 [0.149769, 0.156016] | 0.139331 [0.128387, 0.145281] |

The candidate improves previous StreamFusion but remains slower than stock. Startup/JIT and
submission are included; this is not evidence of sustained complete-job acceleration.
[All measured trials, checksums, configuration and allocation observations](results/integral-ceil-2026-10-27.json)
are retained. Generic Calc/perimeter costs remain in [#491](https://github.com/datafusion-contrib/StreamFusion/issues/491).

Use the shared `integral_rounding` release example and `IntegralRoundingPipelineBenchmark`
from the FLOOR instructions, selecting `SF_INTEGRAL_ROUNDING_FUNCTION=CEIL` for prepared
controls or `-Drounding.pipeline.function=CEIL` for complete jobs. The explicit previous
encoder and route checks apply to both functions.

## Single-argument integer ROUND

Measured on 2026-10-27 against canonical main `d98dc2f2`, on Apple M4 Pro, JDK 17.0.12,
Rust 1.94.0 release without default features. Both released Flink 1.18.1 and 2.2.1 are
measured independently. Only `ROUND(value)` on verified integer operands is changed;
explicit scale arguments and unverified producers retain generated evaluation.

Equal prepared values, resolved widths and NULL patterns start on RowData for stock Flink
and Arrow RecordBatch for previous/candidate native Calc. Stock executes its released
generated expression on prepared GenericRowData/BinaryRowData. Native timers start and
stop inside Rust around production Calc and equivalent checksum consumption; preceding
JVM computation and its intrinsic conversions remain timed. Setup, initial JNI entry,
Arrow import/export and perimeter transposes are excluded from prepared measurements.
Exact output schema/Arrow equality is checked before timing, and every timed output
contributes to matching checksums. All schemas are nullable; the NULL flag denotes actual
NULL values every seventh row. Stock/previous/native order rotates for 16 warmups and
11 measured trials of 131,072 rows each. Values are median [min, max] ns/row.

| Flink, type, NULL values, stock format, batch rows | Stock ns/row | Previous ns/row | Native ns/row | vs stock | vs previous |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1.18.1, BIGINT, false, binary, 1024 | 8.986 [6.941, 18.408] | 54.478 [47.632, 64.772] | 0.348 [0.306, 0.363] | 25.79x | 156.36x |
| 1.18.1, BIGINT, true, generic, 1024 | 6.400 [5.808, 16.941] | 47.064 [42.686, 50.074] | 0.701 [0.681, 0.875] | 9.13x | 67.11x |
| 1.18.1, BIGINT, true, binary, 16 | 5.628 [5.491, 6.113] | 768.974 [761.583, 784.511] | 14.967 [14.371, 15.448] | 0.38x | 51.38x |
| 1.18.1, INT, false, binary, 1024 | 5.838 [5.432, 5.972] | 42.728 [40.682, 57.784] | 0.305 [0.292, 0.317] | 19.17x | 140.30x |
| 1.18.1, INT, true, generic, 1024 | 4.519 [4.452, 18.006] | 37.391 [35.634, 39.007] | 0.717 [0.693, 0.829] | 6.30x | 52.14x |
| 1.18.1, INT, true, binary, 16 | 5.409 [5.286, 5.889] | 762.968 [757.441, 772.750] | 14.406 [14.010, 14.858] | 0.38x | 52.96x |
| 1.18.1, SMALLINT, false, binary, 1024 | 5.689 [5.467, 5.932] | 41.973 [39.931, 59.374] | 0.328 [0.315, 0.346] | 17.33x | 127.82x |
| 1.18.1, SMALLINT, true, generic, 1024 | 5.853 [5.665, 6.021] | 42.039 [40.251, 62.638] | 0.693 [0.672, 0.741] | 8.44x | 60.63x |
| 1.18.1, SMALLINT, true, binary, 16 | 5.446 [5.280, 5.825] | 742.352 [737.850, 752.496] | 14.085 [13.723, 14.905] | 0.39x | 52.71x |
| 1.18.1, TINYINT, false, binary, 1024 | 7.581 [7.238, 17.433] | 42.084 [41.233, 57.969] | 0.336 [0.313, 0.375] | 22.56x | 125.25x |
| 1.18.1, TINYINT, true, generic, 1024 | 8.117 [5.692, 8.867] | 43.509 [38.539, 50.779] | 0.745 [0.706, 0.932] | 10.90x | 58.41x |
| 1.18.1, TINYINT, true, binary, 16 | 5.629 [5.530, 5.820] | 772.427 [763.950, 783.090] | 14.762 [13.350, 15.756] | 0.38x | 52.33x |
| 2.2.1, BIGINT, false, binary, 1024 | 10.198 [8.557, 20.903] | 50.490 [45.352, 60.357] | 0.346 [0.334, 0.364] | 29.46x | 145.85x |
| 2.2.1, BIGINT, true, generic, 1024 | 6.886 [6.348, 18.278] | 55.496 [48.014, 63.099] | 0.696 [0.677, 0.750] | 9.89x | 79.75x |
| 2.2.1, BIGINT, true, binary, 16 | 6.246 [6.150, 6.558] | 773.582 [756.754, 793.155] | 14.002 [13.630, 16.256] | 0.45x | 55.25x |
| 2.2.1, INT, false, binary, 1024 | 7.436 [6.917, 14.206] | 56.088 [49.785, 72.955] | 0.330 [0.317, 0.359] | 22.53x | 169.98x |
| 2.2.1, INT, true, generic, 1024 | 4.766 [4.670, 4.946] | 48.693 [44.799, 61.810] | 0.692 [0.672, 0.851] | 6.88x | 70.33x |
| 2.2.1, INT, true, binary, 16 | 6.060 [6.009, 6.960] | 781.974 [769.627, 816.086] | 15.437 [13.803, 18.351] | 0.39x | 50.66x |
| 2.2.1, SMALLINT, false, binary, 1024 | 6.265 [6.026, 6.478] | 48.445 [43.458, 62.665] | 0.338 [0.329, 0.347] | 18.52x | 143.23x |
| 2.2.1, SMALLINT, true, generic, 1024 | 4.925 [4.675, 5.293] | 41.861 [40.380, 56.673] | 0.710 [0.676, 0.787] | 6.93x | 58.94x |
| 2.2.1, SMALLINT, true, binary, 16 | 5.575 [5.347, 5.802] | 773.666 [761.840, 789.940] | 14.704 [14.331, 15.127] | 0.38x | 52.62x |
| 2.2.1, TINYINT, false, binary, 1024 | 8.385 [7.735, 19.151] | 44.980 [41.002, 64.401] | 0.343 [0.319, 0.370] | 24.47x | 131.26x |
| 2.2.1, TINYINT, true, generic, 1024 | 6.457 [5.727, 15.747] | 42.013 [39.691, 56.503] | 0.696 [0.676, 0.845] | 9.28x | 60.37x |
| 2.2.1, TINYINT, true, binary, 16 | 6.280 [6.077, 6.752] | 732.857 [728.134, 750.648] | 15.533 [14.994, 16.240] | 0.40x | 47.18x |

At 1,024 rows this shared-operand path improves 6.30-29.46x over stock and 52.14-169.98x
over the preceding callback. Sixteen-row controls remain slower than stock, despite
removing most callback overhead. The candidate has the same input-expression Calc path
as FLOOR/CEIL, without a rounding loop to optimize. Its allocation probe requests
13 allocations / 606 bytes per batch and shares all output value/validity buffers;
generic Calc/RecordBatch ownership, result handling and checksum consumption retain the
fixed costs identified in the FLOOR profile. These are sampled batch improvements,
not a universal small-batch acceleration claim.

The independent complete job uses 1M INT rows, parallelism one, a row source and a rowwise
blackhole sink. Native plans retain both transposes, require consumed NativeCalc work
and distinguish the old JVM route from candidate `jvm=[]`. The previous encoder comes
from `d98dc2f2`; relabeling an unchanged candidate is rejected. Separate sequential JVMs
run two warmups and five measured jobs per engine. Seconds are median [min, max]:

| Stock | Previous callback | Candidate |
| ---: | ---: | ---: |
| 0.138810 [0.133661, 0.148813] | 0.162969 [0.156567, 0.183940] | 0.141323 [0.134961, 0.155040] |

The complete candidate improves previous StreamFusion but remains slightly slower than
stock by median, with overlapping ranges. Submission, startup and JIT remain included;
this does not establish sustained full-job acceleration. All raw trials, checksums,
configuration and allocation observations are in the
[result record](results/integral-round-2026-10-27.json).

Use the shared `integral_rounding` release example from the FLOOR instructions, selecting
`SF_INTEGRAL_ROUNDING_FUNCTION=ROUND`. For the complete job, select
`-Drounding.pipeline.function=ROUND` with `-Drounding.pipeline.engine=flink|previous|native`;
the previous control requires the preceding encoder first on the classpath. Runtime
parity/route tests cover all four integer widths on both supported Flink lines and JDK
11/17, including NULLs, signed minima, nested/widened calls and failing unselected
branches. The unchanged released `testRound` fixtures pass on both lines.

## Single-argument integer TRUNCATE

Measured on 2026-10-27 against canonical main `d98dc2f2`, using the same machine, release
configuration, timing boundaries, NULL patterns and output checks as
[single-argument integer ROUND](#single-argument-integer-round). Both Flink 1.18.1 and
2.2.1 are measured independently. INT and BIGINT single-argument TRUNCATE reuse the
verified operand instead of invoking Flink's DECIMAL(38,18) intermediate. This is exact
at the default zero scale, including signed minima and large BIGINTs. Explicit scales,
unverified producers, DECIMAL/floating contracts and direct TINYINT/SMALLINT compilation
failures retain their preceding paths.

Stock runs the released generated expression on equal prepared RowData; previous and
candidate run production Calc on prepared Arrow inside Rust. Intrinsic callback and
conversion costs stay timed; setup/import/export and perimeter transposes are excluded.
Every output contributes to a checksum, with full schema/Arrow equality checked before
timing. Sixteen warmups and eleven measured 131,072-row trials rotate engine order.
All input schemas are nullable; actual NULL values occur every seventh row when enabled.
Values are median [min, max] ns/row.

| Flink, type, NULL values, stock format, batch rows | Stock ns/row | Previous ns/row | Native ns/row | vs stock | vs previous |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1.18.1, BIGINT, false, binary, 1024 | 114.692 [108.152, 121.879] | 163.985 [155.083, 185.194] | 0.357 [0.305, 0.395] | 321.56x | 459.76x |
| 1.18.1, BIGINT, true, generic, 1024 | 94.695 [88.018, 99.201] | 152.532 [148.545, 156.392] | 0.719 [0.698, 0.833] | 131.75x | 212.22x |
| 1.18.1, BIGINT, true, binary, 16 | 102.172 [94.620, 103.305] | 851.419 [832.244, 868.287] | 14.344 [13.178, 14.791] | 7.12x | 59.36x |
| 1.18.1, INT, false, binary, 1024 | 50.861 [48.534, 54.994] | 100.856 [94.868, 109.976] | 0.345 [0.308, 0.409] | 147.60x | 292.68x |
| 1.18.1, INT, true, generic, 1024 | 46.233 [44.280, 49.321] | 91.235 [87.666, 102.056] | 0.720 [0.684, 0.777] | 64.21x | 126.71x |
| 1.18.1, INT, true, binary, 16 | 42.947 [40.513, 45.516] | 811.769 [806.045, 815.572] | 14.427 [13.648, 14.712] | 2.98x | 56.27x |
| 2.2.1, BIGINT, false, binary, 1024 | 112.933 [103.880, 117.159] | 167.469 [157.077, 173.877] | 0.357 [0.310, 0.408] | 316.07x | 468.70x |
| 2.2.1, BIGINT, true, generic, 1024 | 99.859 [94.567, 104.457] | 147.552 [141.161, 153.150] | 0.703 [0.610, 0.746] | 142.01x | 209.84x |
| 2.2.1, BIGINT, true, binary, 16 | 98.121 [88.774, 101.313] | 839.763 [827.729, 851.937] | 15.164 [14.409, 16.540] | 6.47x | 55.38x |
| 2.2.1, INT, false, binary, 1024 | 64.964 [59.286, 73.217] | 95.684 [94.034, 98.344] | 0.318 [0.294, 0.347] | 204.56x | 301.30x |
| 2.2.1, INT, true, generic, 1024 | 45.872 [44.496, 53.871] | 91.028 [85.339, 94.128] | 0.700 [0.622, 0.741] | 65.53x | 130.04x |
| 2.2.1, INT, true, binary, 16 | 55.065 [51.269, 60.222] | 834.556 [821.474, 875.742] | 14.462 [13.581, 15.038] | 3.81x | 57.70x |

All sampled cases beat stock and the previous callback. The 1,024-row cases improve
64.21-321.56x over stock and 126.71-468.70x over previous StreamFusion. Sixteen-row
controls improve 2.98-7.12x over stock and 55.38-59.36x over previous StreamFusion.
The native path remains a shared-operand Calc, requesting 13 allocations / 606 bytes
per batch with no new output values/validity buffers. Flink's decimal conversion and
rounding work account for the difference from FLOOR/CEIL/ROUND controls; these batch
ratios must not be applied to complete jobs.

A separate Flink 2.2.1 complete job retains a bounded 1M-row INT source, both transposes,
parallelism one and a rowwise blackhole sink. Positive consumed NativeCalc work and the
expected previous/candidate JVM route are required. Previous uses the canonical main
encoder. Each engine runs in a separate sequential JVM with two warmups and five
measured jobs. Seconds are median [min, max]:

| Stock | Previous callback | Candidate |
| ---: | ---: | ---: |
| 0.188721 [0.185241, 0.200471] | 0.261994 [0.256899, 0.264275] | 0.132958 [0.129006, 0.147995] |

The measured candidate reduces elapsed time by about 29.5% versus stock and 49.3% versus
the preceding implementation in this workload. Startup, submission and JIT remain
included; this is not proof of sustained acceleration for arbitrary inputs or jobs.
The [raw result record](results/integral-truncate-2026-10-27.json) retains every trial,
checksum, allocation observation and benchmark configuration.

Use the shared release example with `SF_INTEGRAL_ROUNDING_FUNCTION=TRUNCATE` and INT or
BIGINT. Complete-job controls select `-Drounding.pipeline.function=TRUNCATE`; the prior
encoder must be first on the classpath for the previous control. Runtime parity and
consumed-work witnesses cover both types on Flink 1.18.1/2.2.1 and JDK 11/17. Explicit
widening of TINYINT/SMALLINT is verified, while direct calls retain stock compilation
failures. Unchanged released `testTruncate` fixtures pass on both lines.

## DECIMAL ABS

Measured on 2026-10-28 against canonical main `d89e03b5`, with released Flink 1.18.1
and 2.2.1, JDK 17, Apple M4 Pro, Rust 1.94.0 and a release/no-default-features native
build with the system allocator. All core Java production sources and compatibility
sources are compiled from the candidate with `--release 11`. This replaces generated
JVM computation inside an already-admitted Calc without changing operator/type admission.

Stock executes the released generated expression over prepared GenericRowData or
BinaryRowData. Previous and candidate execute production Calc over equal Arrow values,
types and NULLs, with the timer inside Rust. Setup, Arrow import/export and perimeter
transposes are excluded from this prepared control. The previous control uses the scalar
generated-expression callback emitted by main (kind 17), including decimal argument and
result marshalling; the earlier whole-row callback control is superseded. Intrinsic conversions,
result allocation and equivalent signum consumption remain timed. Full schema/Arrow
output equality is checked before timing, and each measured output contributes to a
checksum. Schemas are nullable; enabled NULL values occur every seventh row. Inputs
alternate signs, include precision maxima, and include values beyond 2^53 for noncompact
types. Sixteen warmups and eleven measured trials of 65,536 rows rotate engine order.
No other test or benchmark runs concurrently. Values are median [min, max] ns/row:

| Flink, type, NULL values, stock format, batch rows | Stock ns/row | Previous ns/row | Native ns/row | vs stock | vs previous |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1.18.1, DECIMAL(18,3), false, binary, 1024 | 6.771 [6.255, 33.129] | 38.748 [31.810, 46.170] | 1.277 [1.254, 1.657] | 5.30x | 30.34x |
| 1.18.1, DECIMAL(18,3), true, generic, 1024 | 6.641 [5.821, 31.708] | 46.051 [41.422, 67.759] | 1.778 [1.691, 1.986] | 3.74x | 25.91x |
| 1.18.1, DECIMAL(18,3), true, binary, 16 | 5.556 [4.824, 8.092] | 596.312 [564.630, 711.404] | 25.133 [23.392, 26.421] | 0.22x | 23.73x |
| 1.18.1, DECIMAL(19,3), false, binary, 1024 | 31.134 [23.839, 47.973] | 66.718 [48.780, 139.852] | 1.256 [1.146, 1.330] | 24.78x | 53.11x |
| 1.18.1, DECIMAL(19,3), true, generic, 1024 | 8.194 [7.320, 9.511] | 96.540 [75.018, 127.890] | 1.782 [1.696, 1.970] | 4.60x | 54.17x |
| 1.18.1, DECIMAL(19,3), true, binary, 16 | 18.159 [17.847, 23.510] | 685.521 [571.056, 760.549] | 23.739 [22.762, 24.487] | 0.76x | 28.88x |
| 1.18.1, DECIMAL(38,18), false, binary, 1024 | 25.438 [22.809, 48.136] | 58.722 [44.867, 91.820] | 1.297 [1.263, 1.371] | 19.61x | 45.27x |
| 1.18.1, DECIMAL(38,18), true, generic, 1024 | 8.318 [7.112, 30.991] | 89.708 [78.290, 132.299] | 1.781 [1.743, 2.115] | 4.67x | 50.37x |
| 1.18.1, DECIMAL(38,18), true, binary, 16 | 18.058 [16.403, 22.636] | 594.976 [570.112, 623.375] | 24.618 [24.240, 25.860] | 0.73x | 24.17x |
| 1.18.1, DECIMAL(38,38), false, binary, 1024 | 27.431 [24.631, 49.947] | 66.619 [46.130, 134.900] | 1.331 [1.214, 1.488] | 20.60x | 50.04x |
| 1.18.1, DECIMAL(38,38), true, generic, 1024 | 7.809 [6.671, 30.333] | 94.542 [83.144, 120.974] | 1.834 [1.743, 4.465] | 4.26x | 51.56x |
| 1.18.1, DECIMAL(38,38), true, binary, 16 | 19.852 [17.552, 23.687] | 624.911 [583.037, 760.024] | 24.169 [23.059, 28.229] | 0.82x | 25.86x |
| 2.2.1, DECIMAL(18,3), false, binary, 1024 | 8.650 [6.227, 34.993] | 53.272 [44.177, 86.080] | 1.297 [1.142, 1.580] | 6.67x | 41.07x |
| 2.2.1, DECIMAL(18,3), true, generic, 1024 | 7.817 [6.538, 14.962] | 64.180 [36.743, 212.652] | 2.127 [1.836, 3.219] | 3.67x | 30.17x |
| 2.2.1, DECIMAL(18,3), true, binary, 16 | 5.786 [4.878, 8.431] | 596.074 [567.899, 718.964] | 22.915 [22.058, 24.011] | 0.25x | 26.01x |
| 2.2.1, DECIMAL(19,3), false, binary, 1024 | 26.063 [24.449, 52.284] | 66.973 [49.121, 126.535] | 1.317 [1.204, 1.621] | 19.79x | 50.86x |
| 2.2.1, DECIMAL(19,3), true, generic, 1024 | 8.820 [7.537, 10.936] | 93.368 [72.261, 150.984] | 1.766 [1.605, 2.389] | 4.99x | 52.86x |
| 2.2.1, DECIMAL(19,3), true, binary, 16 | 19.715 [18.974, 20.285] | 596.653 [570.852, 694.953] | 24.459 [23.104, 25.412] | 0.81x | 24.39x |
| 2.2.1, DECIMAL(38,18), false, binary, 1024 | 25.969 [25.345, 49.030] | 71.037 [49.401, 127.594] | 1.300 [1.154, 1.346] | 19.97x | 54.64x |
| 2.2.1, DECIMAL(38,18), true, generic, 1024 | 7.465 [7.186, 34.662] | 75.359 [47.146, 168.208] | 1.760 [1.705, 2.176] | 4.24x | 42.81x |
| 2.2.1, DECIMAL(38,18), true, binary, 16 | 19.304 [17.955, 27.007] | 581.214 [573.262, 902.620] | 24.179 [23.502, 26.571] | 0.80x | 24.04x |
| 2.2.1, DECIMAL(38,38), false, binary, 1024 | 23.404 [22.887, 24.978] | 66.765 [54.471, 96.620] | 1.295 [1.149, 1.497] | 18.07x | 51.55x |
| 2.2.1, DECIMAL(38,38), true, generic, 1024 | 7.948 [7.013, 10.184] | 66.031 [48.108, 108.056] | 1.802 [1.633, 1.963] | 4.41x | 36.65x |
| 2.2.1, DECIMAL(38,38), true, binary, 16 | 18.920 [17.187, 22.478] | 587.500 [567.618, 703.150] | 26.159 [24.940, 27.522] | 0.72x | 22.46x |

At 1,024 rows the candidate is 3.67-24.78x faster than stock and
25.91-54.64x faster than the preceding callback. Sixteen-row
controls still lose to stock (0.22-0.82x), while improving the previous
native-Calc route by 22.46-28.88x. These are sampled prepared-batch
gains; they do not establish universal small-batch acceleration. The nullable 1,024-row
allocation probe records 20 allocations / 17,575 requested bytes, with 16,384 new value
bytes and 128 shared validity bytes. Generic Calc/batch/result handling retains fixed
costs alongside the allocation-free per-element transform. Remaining small-batch and
row/Arrow perimeter work stays in
[#491](https://github.com/datafusion-contrib/StreamFusion/issues/491).

The independent complete job uses 1M DECIMAL(18,3) rows, parallelism one, a bounded
row source, both transposes and a rowwise blackhole sink. The previous control loads
the actual `d89e03b5` encoder ahead of candidate classes and requires its JVM route;
candidate requires `jvm=[]`. Both require positive consumed NativeCalc work.
Each engine runs two warmups and five measured jobs in separate sequential JVMs.
Seconds are median [min, max]:

| Stock | Previous callback | Candidate |
| ---: | ---: | ---: |
| 0.138626 [0.123898, 0.143039] | 0.183183 [0.176360, 0.212639] | 0.139391 [0.137642, 0.155442] |

The complete candidate improves previous StreamFusion by
23.9% but remains
0.6% slower than stock
by median in this workload. Startup, submission, JIT, source conversion and both
transposes remain included; their individual contributions are not isolated here.
The prepared-batch gain is not a whole-job acceleration claim. The
[result record](results/decimal-abs-2026-10-28.json) retains every trial,
checksum, environment and allocation observation.

After test-compiling with the `bench` Maven profile, set `SF_NATIVE_BENCH_CLASSPATH`
to test classes, production classes and Maven dependencies, and `SF_NATIVE_LIB_DIR`
to the release library directory. Build/run the shared control from `native/`:

```sh
cargo build --release -p streamfusion --no-default-features --example decimal_unary
./target/release/examples/decimal_unary 18 3 true false 1024 64 16 11
```

Arguments are precision, scale, actual NULL values, BinaryRowData input, batch rows,
loops, warmups and measured trials. Complete controls use `DecimalUnaryPipelineBenchmark`
with `SF_BENCHMARK=true` and `-Ddecimal.unary.pipeline.engine=flink|previous|native`;
previous requires the baseline encoder first on the classpath. Runtime tests verify
results, resolved types, consumed native work and no JVM binding on both Flink lines
and JDK 11/17. Rust/encoding checks cover all legal precision/scale pairs. Unchanged
released `testAbs` fixtures pass on both lines.

## DECIMAL unary minus

Measured on 2026-10-28 against canonical main `d89e03b5`, with released Flink 1.18.1
and 2.2.1, JDK 17, Apple M4 Pro, Rust 1.94.0 and a release/no-default-features native
build with the system allocator. All core Java production sources and compatibility
sources are compiled from the candidate with `--release 11`. This replaces generated
JVM computation inside an already-admitted Calc without changing operator/type admission.

Stock executes the released generated expression over prepared GenericRowData or
BinaryRowData. Previous and candidate execute production Calc over equal Arrow values,
types and NULLs, with the timer inside Rust. Setup, Arrow import/export and perimeter
transposes are excluded from this prepared control. The previous control uses the scalar
generated-expression callback emitted by main (kind 17), including decimal argument and
result marshalling; the earlier whole-row callback control is superseded. Intrinsic conversions,
result allocation and equivalent signum consumption remain timed. Full schema/Arrow
output equality is checked before timing, and each measured output contributes to a
checksum. Schemas are nullable; enabled NULL values occur every seventh row. Inputs
alternate signs, include precision maxima, and include values beyond 2^53 for noncompact
types. Sixteen warmups and eleven measured trials of 65,536 rows rotate engine order.
No other test or benchmark runs concurrently. Values are median [min, max] ns/row:

| Flink, type, NULL values, stock format, batch rows | Stock ns/row | Previous ns/row | Native ns/row | vs stock | vs previous |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1.18.1, DECIMAL(18,3), false, binary, 1024 | 5.608 [5.397, 30.964] | 38.780 [36.360, 48.179] | 1.424 [1.360, 1.448] | 3.94x | 27.23x |
| 1.18.1, DECIMAL(18,3), true, generic, 1024 | 5.138 [4.994, 5.221] | 39.243 [30.175, 62.565] | 2.113 [2.067, 2.254] | 2.43x | 18.57x |
| 1.18.1, DECIMAL(18,3), true, binary, 16 | 5.169 [5.025, 9.325] | 589.500 [563.921, 610.549] | 25.894 [24.883, 27.327] | 0.20x | 22.77x |
| 1.18.1, DECIMAL(19,3), false, binary, 1024 | 25.869 [24.083, 45.725] | 60.425 [53.040, 101.635] | 1.488 [1.422, 1.619] | 17.39x | 40.62x |
| 1.18.1, DECIMAL(19,3), true, generic, 1024 | 17.958 [16.544, 35.588] | 120.014 [115.727, 146.039] | 2.097 [2.040, 2.342] | 8.56x | 57.24x |
| 1.18.1, DECIMAL(19,3), true, binary, 16 | 18.345 [17.713, 19.267] | 584.371 [578.275, 610.158] | 25.778 [25.321, 27.777] | 0.71x | 22.67x |
| 1.18.1, DECIMAL(38,18), false, binary, 1024 | 34.512 [21.738, 44.704] | 64.531 [43.767, 72.120] | 1.458 [1.367, 1.554] | 23.67x | 44.26x |
| 1.18.1, DECIMAL(38,18), true, generic, 1024 | 7.675 [7.338, 7.944] | 41.761 [39.220, 59.480] | 2.070 [1.995, 2.111] | 3.71x | 20.17x |
| 1.18.1, DECIMAL(38,18), true, binary, 16 | 18.198 [16.834, 23.837] | 576.080 [568.844, 607.250] | 26.079 [25.583, 26.886] | 0.70x | 22.09x |
| 1.18.1, DECIMAL(38,38), false, binary, 1024 | 30.949 [22.371, 51.205] | 51.428 [45.383, 66.168] | 1.465 [1.390, 1.631] | 21.13x | 35.11x |
| 1.18.1, DECIMAL(38,38), true, generic, 1024 | 7.643 [7.118, 8.465] | 42.615 [39.193, 86.474] | 2.089 [2.033, 2.347] | 3.66x | 20.40x |
| 1.18.1, DECIMAL(38,38), true, binary, 16 | 18.907 [17.902, 19.407] | 581.264 [573.115, 596.482] | 27.077 [26.356, 28.397] | 0.70x | 21.47x |
| 2.2.1, DECIMAL(18,3), false, binary, 1024 | 8.586 [7.102, 8.956] | 90.221 [35.983, 179.781] | 1.551 [1.473, 1.753] | 5.54x | 58.18x |
| 2.2.1, DECIMAL(18,3), true, generic, 1024 | 6.987 [6.547, 7.708] | 51.341 [36.867, 91.749] | 2.126 [2.028, 2.342] | 3.29x | 24.15x |
| 2.2.1, DECIMAL(18,3), true, binary, 16 | 6.000 [5.145, 6.451] | 620.616 [588.384, 971.621] | 25.933 [25.349, 34.006] | 0.23x | 23.93x |
| 2.2.1, DECIMAL(19,3), false, binary, 1024 | 23.132 [21.831, 42.864] | 46.033 [42.233, 72.575] | 1.542 [1.408, 1.836] | 15.00x | 29.85x |
| 2.2.1, DECIMAL(19,3), true, generic, 1024 | 9.666 [8.183, 11.278] | 44.669 [40.233, 67.673] | 2.143 [2.030, 2.563] | 4.51x | 20.84x |
| 2.2.1, DECIMAL(19,3), true, binary, 16 | 19.288 [18.641, 25.727] | 585.044 [571.290, 591.088] | 27.367 [26.751, 28.982] | 0.70x | 21.38x |
| 2.2.1, DECIMAL(38,18), false, binary, 1024 | 23.328 [22.225, 42.695] | 48.661 [43.836, 67.923] | 1.458 [1.418, 1.911] | 15.99x | 33.36x |
| 2.2.1, DECIMAL(38,18), true, generic, 1024 | 8.416 [7.359, 14.885] | 51.033 [42.275, 80.820] | 2.165 [2.050, 2.892] | 3.89x | 23.57x |
| 2.2.1, DECIMAL(38,18), true, binary, 16 | 24.755 [18.713, 33.524] | 585.119 [570.680, 1058.025] | 27.826 [26.473, 78.137] | 0.89x | 21.03x |
| 2.2.1, DECIMAL(38,38), false, binary, 1024 | 23.077 [21.238, 45.155] | 47.891 [43.574, 75.425] | 1.476 [1.308, 1.616] | 15.63x | 32.44x |
| 2.2.1, DECIMAL(38,38), true, generic, 1024 | 8.402 [7.579, 13.434] | 57.577 [43.598, 75.661] | 2.181 [2.028, 2.316] | 3.85x | 26.39x |
| 2.2.1, DECIMAL(38,38), true, binary, 16 | 19.016 [18.543, 19.412] | 599.046 [577.167, 607.373] | 25.196 [24.239, 26.623] | 0.75x | 23.78x |

At 1,024 rows the candidate is 2.43-23.67x faster than stock and
18.57-58.18x faster than the preceding callback. Sixteen-row
controls still lose to stock (0.20-0.89x), while improving the previous
native-Calc route by 21.03-23.93x. These are sampled prepared-batch
gains; they do not establish universal small-batch acceleration. The nullable 1,024-row
allocation probe records 20 allocations / 17,575 requested bytes, with 16,384 new value
bytes and 128 shared validity bytes. Generic Calc/batch/result handling retains fixed
costs alongside the allocation-free per-element transform. Remaining small-batch and
row/Arrow perimeter work stays in
[#491](https://github.com/datafusion-contrib/StreamFusion/issues/491).

The independent complete job uses 1M DECIMAL(18,3) rows, parallelism one, a bounded
row source, both transposes and a rowwise blackhole sink. The previous control loads
the actual `d89e03b5` encoder ahead of candidate classes and requires its JVM route;
candidate requires `jvm=[]`. Both require positive consumed NativeCalc work.
Each engine runs two warmups and five measured jobs in separate sequential JVMs.
Seconds are median [min, max]:

| Stock | Previous callback | Candidate |
| ---: | ---: | ---: |
| 0.131800 [0.119555, 0.133355] | 0.174760 [0.167390, 0.180106] | 0.150979 [0.145502, 0.164335] |

The complete candidate improves previous StreamFusion by
13.6% but remains
14.6% slower than stock
by median in this workload. Startup, submission, JIT, source conversion and both
transposes remain included; their individual contributions are not isolated here.
The prepared-batch gain is not a whole-job acceleration claim. The
[result record](results/decimal-negate-2026-10-28.json) retains every trial,
checksum, environment and allocation observation.

Use the shared release example with `SF_DECIMAL_UNARY_FUNCTION=NEGATE`. Complete-job
controls select `-Ddecimal.unary.pipeline.function=NEGATE` and the engine property above.
Previous requires the baseline encoder first on the classpath; relabeling an unchanged
candidate is rejected. Runtime regressions cover precision/scale endpoints, extrema,
NULL/non-NULL data, widening/nesting, string/Boolean consumers, unselected failures and
empty filtered batches, with positive consumed NativeCalc work on both Flink lines and
JDK 11/17. Rust/encoding tests cover every legal decimal precision/scale pair. Unchanged
released `ScalarOperatorsTest.testUnaryPlusMinus` and `DecimalTypeTest.testUnaryPlusMinus`
fixtures pass on both lines.


## External STRUCTURED collection inputs

Source STRUCTURED values now reuse the existing generated collection evaluator and represented
ROW boundary, including nested array/map/multiset containers and ROW/STRUCTURED casts. NativeCalc
consumption establishes this route; these Java casts are not described as Rust kernels.
[Coverage](../operators/calc-filter.md#external-structured-collection-inputs) gives the exact
built-in-program, callback, state/order and writer limits.

The 2026-10-28 release/mimalloc comparison uses Flink 2.2.1/1.18.1, OpenJDK 17.0.20.1, Linux
x86-64/WSL2, parallelism one and an 8 GiB heap. Every job has 50,000 rowwise source records and a
rowwise blackhole sink, with two warmups and three alternating trials per engine. The source
array elements remain declared STRUCTURED with nullable INT/string fields. Widths are 3/16,
parent NULLs occur every eighth row, element NULLs every seventh position, and INT/string field
NULLs every eleventh/thirteenth row. Strings use 16 seeds plus a 32-character suffix. The single
projection constructs a ROW containing both an ARRAY<ROW<a BIGINT,b STRING>> cast and character
formatting of the original array; it measures their combined production boundary.

Candidate and actual previous `86c9d244` use identical fixture sources and the **same frozen native
release library**. The previous planner must fall back without NativeCalc. Each candidate native
job requires 50,000 consumed NativeCalc inputs and `jvm=[ROW, CAST]` descriptors. Both perimeter
transposes, JNI, generated evaluation and planning/submission remain timed; environment setup
and the initial explain probe remain outside timing. Local exchange caching stays disabled.
No competing Java/native build, test or benchmark process was observed by the process monitor.

[All 80 jobs, 20 native Calc witnesses and paired controls](results/structured-collection-inputs-2026-10-28.json)
retain source/library hashes, raw trials, commands and ranges. Median seconds (min–max):

| Flink | Array width | Previous paired stock | Previous fallback | Candidate paired stock | NativeCalc |
| --- | ---: | ---: | ---: | ---: | ---: |
| 2.2 | 3 | 0.194330 (0.170898–0.262270) | 0.191025 (0.175968–0.222784) | 0.177382 (0.175108–0.182358) | 0.384402 (0.365160–0.412428) |
| 2.2 | 16 | 0.370867 (0.356476–0.390086) | 0.372421 (0.365365–0.372573) | 0.383198 (0.380443–0.386313) | 0.735325 (0.727280–0.855846) |
| 1.18 | 3 | 0.178257 (0.174778–0.197603) | 0.183170 (0.182411–0.217055) | 0.184813 (0.178814–0.220786) | 0.382043 (0.360289–0.391352) |
| 1.18 | 16 | 0.375509 (0.362542–0.478923) | 0.378216 (0.367642–0.418500) | 0.383667 (0.374440–0.396060) | 0.779504 (0.709350–0.798988) |

Every native median loses to both stock and the preceding fallback. The new route is roughly
twice paired stock on these workloads. This ships as correctness-verified collection coverage
under the existing accepted collection-extension tradeoff, without an acceleration claim or
an additional performance-only switch. Standalone optimization remains in
[#392](https://github.com/datafusion-contrib/StreamFusion/issues/392).

Exactly two eight-second CPU captures on Flink 2.2, one at each width, close the bounded
investigation. Each has one unprofiled native warmup followed by native-only complete jobs;
profiled timings are excluded from the table. Width 3/16 captures have 20,683/15,380 execution
samples. Row serialization appears in 349/800 samples, Arrow field writing in 476/789,
generated evaluation in 385/513 and the existing JVM batch callback in 309/374. Categories
are inclusive and overlap. They include JIT, job setup and all process threads, and do not
measure allocations, copied bytes or isolated kernel costs. No small safe change to this
existing serialization/representation boundary was selected. The report retains capture
hashes, scope and the remaining conversion/serialization costs; no third tuning pass follows.

Reproduce the complete-job comparison after building the release library:

```sh
SF_BENCHMARK=true mvn -pl streamfusion-runtime test -Pbench \
  '-Dtest=DynamicCollectionBenchmark#compareExpressions' \
  '-Dcollection.expression=ROW(CAST(arr AS ARRAY<ROW<a BIGINT,b STRING>>),CAST(arr AS STRING))' \
  -Dcollection.map=false -Dcollection.type=STRUCTURED -Dcollection.width=3 \
  -Dcollection.rows=50000 -Dcollection.bytes=32 -Dcollection.domain=16 \
  -Dcollection.warmup=2 -Dcollection.runs=3 -Dcollection.native=true -Dsf.testForks=1
```

Repeat at width 16 and add `flink-1.18` to the profile list for the second released line. The
previous checkout copies the identical benchmark fixture, sets `collection.native=false` and
loads the same release library. For the CPU diagnostic, additionally set
`SF_PROFILE_COLLECTION=true`, select `DynamicCollectionBenchmark#profileNativeExpressions`
and set `collection.profileSeconds=12`. After its `collection-profile-ready` message, attach
`asprof -d 8 -e cpu -i 1ms -f structured-width3.jfr PID`; repeat once at width 16. The diagnostic
requires positive native work in every job and does not supply timing results for comparison.

## DECIMAL SIGN

The typed Decimal128 kernel replaces the scalar generated callback for verified direct/literal,
same-scale lossless-widening and nested SIGN inputs. Ordinary `p > s` returns scaled -1/0/1;
noncompact `p = s` returns NULL for nonzero values and scaled zero otherwise. Compact `p = s`
remains generated because its out-of-precision internal value has consumer-specific behavior.
DECIMAL CASE/IF containing that compact intermediate retains the released branch conversion.
This bounded implementation does not complete [#491](https://github.com/datafusion-contrib/StreamFusion/issues/491).

### Prepared operation

Apple M4 Pro, macOS aarch64, JDK 17.0.12, Rust 1.94.0, release/no-default-features,
system allocator with the counting wrapper; released Flink 1.18.1 and 2.2.1. Each trial consumes
65,536 logical rows: 64 batches of 1,024 or 4,096 batches of 16. Each configuration has 16
warmups and 11 measured trials, rotating stock/previous/native order. Data include signed
extremes, values beyond 2^53, actual zeros every eleventh row, and NULL every seventh row
in the nullable-value configurations. All logical schemas are nullable; the NULL-values column
below describes actual values. GenericRowData and BinaryRowData stock formats are both retained.

Stock starts in Java on prepared RowData, evaluates the released generated projection and
consumes every decimal result. Previous and candidate start in Rust on equivalent prepared
Arrow batches, run the production Calc and consume each output through an equivalent checksum.
Their timers start and stop inside Rust. Setup, Arrow import/export and perimeter transposes
are outside this small-operation timer. The previous control uses the actual main `d89e03b5`
kind-17 scalar evaluator with one decimal argument and the matching precision/scale result code;
its JVM callback and both intrinsic decimal marshalling boundaries stay inside timing.
The baseline encoding is independently checked on both released lines. The candidate asserts
opcode 65 and empty JVM descriptors. Before timing, every candidate Arrow output and schema
is compared exactly with the preceding evaluator; each timed engine also checks its checksum.

Medians and observed min/max are ns/row. Ratios above 1 favor native.

| Flink, type, NULL values, stock format, batch rows | Stock ns/row | Previous ns/row | Native ns/row | vs stock | vs previous |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1.18.1, DECIMAL(18,3), false, binary, 1024 | 8.788 [7.560, 32.969] | 31.644 [29.802, 35.288] | 1.726 [1.422, 1.775] | 5.09x | 18.34x |
| 1.18.1, DECIMAL(18,3), true, generic, 1024 | 5.188 [5.105, 5.262] | 39.490 [37.286, 64.526] | 2.113 [2.028, 2.317] | 2.45x | 18.69x |
| 1.18.1, DECIMAL(18,3), true, binary, 16 | 5.563 [4.834, 6.092] | 573.643 [558.766, 590.951] | 25.682 [23.939, 28.564] | 0.22x | 22.34x |
| 1.18.1, DECIMAL(19,19), false, binary, 1024 | 36.277 [33.051, 60.411] | 49.828 [45.473, 70.459] | 3.698 [3.549, 3.969] | 9.81x | 13.47x |
| 1.18.1, DECIMAL(19,19), true, generic, 1024 | 17.546 [16.832, 29.591] | 50.177 [47.459, 73.354] | 3.915 [3.748, 3.991] | 4.48x | 12.82x |
| 1.18.1, DECIMAL(19,19), true, binary, 16 | 27.656 [27.335, 34.155] | 588.833 [575.139, 716.778] | 29.204 [28.835, 30.251] | 0.95x | 20.16x |
| 1.18.1, DECIMAL(19,3), false, binary, 1024 | 31.000 [28.632, 49.302] | 53.205 [44.348, 117.357] | 1.711 [1.563, 1.806] | 18.12x | 31.10x |
| 1.18.1, DECIMAL(19,3), true, generic, 1024 | 9.205 [8.858, 10.038] | 44.694 [41.683, 68.454] | 2.144 [1.930, 2.423] | 4.29x | 20.85x |
| 1.18.1, DECIMAL(19,3), true, binary, 16 | 19.688 [19.352, 20.637] | 592.555 [583.083, 611.048] | 25.354 [24.656, 26.271] | 0.78x | 23.37x |
| 1.18.1, DECIMAL(38,18), false, binary, 1024 | 28.429 [27.130, 49.131] | 46.403 [43.915, 85.672] | 1.654 [1.457, 1.750] | 17.19x | 28.06x |
| 1.18.1, DECIMAL(38,18), true, generic, 1024 | 8.948 [8.448, 26.579] | 42.668 [40.415, 60.392] | 2.248 [2.197, 2.358] | 3.98x | 18.98x |
| 1.18.1, DECIMAL(38,18), true, binary, 16 | 20.622 [19.639, 25.847] | 585.923 [563.824, 598.157] | 23.598 [23.005, 24.824] | 0.87x | 24.83x |
| 1.18.1, DECIMAL(38,38), false, binary, 1024 | 43.238 [34.046, 66.534] | 52.330 [45.084, 97.438] | 3.705 [3.527, 3.743] | 11.67x | 14.12x |
| 1.18.1, DECIMAL(38,38), true, generic, 1024 | 19.147 [18.496, 42.937] | 72.809 [54.204, 98.392] | 3.871 [3.808, 4.006] | 4.95x | 18.81x |
| 1.18.1, DECIMAL(38,38), true, binary, 16 | 29.189 [28.351, 33.110] | 576.757 [569.712, 624.387] | 31.391 [30.214, 32.533] | 0.93x | 18.37x |
| 2.2.1, DECIMAL(18,3), false, binary, 1024 | 5.978 [5.406, 34.654] | 38.769 [30.835, 45.469] | 1.563 [1.343, 1.754] | 3.82x | 24.80x |
| 2.2.1, DECIMAL(18,3), true, generic, 1024 | 7.204 [6.912, 7.796] | 32.211 [29.266, 59.492] | 2.220 [1.803, 2.257] | 3.24x | 14.51x |
| 2.2.1, DECIMAL(18,3), true, binary, 16 | 5.570 [5.396, 6.739] | 557.027 [543.966, 583.133] | 25.658 [24.828, 26.042] | 0.22x | 21.71x |
| 2.2.1, DECIMAL(19,19), false, binary, 1024 | 35.250 [34.651, 74.614] | 76.201 [48.690, 124.938] | 3.799 [3.780, 3.902] | 9.28x | 20.06x |
| 2.2.1, DECIMAL(19,19), true, generic, 1024 | 15.188 [14.617, 34.014] | 71.803 [50.053, 157.637] | 3.953 [3.872, 4.104] | 3.84x | 18.17x |
| 2.2.1, DECIMAL(19,19), true, binary, 16 | 28.546 [27.047, 32.573] | 590.797 [561.872, 627.825] | 29.456 [28.050, 30.531] | 0.97x | 20.06x |
| 2.2.1, DECIMAL(19,3), false, binary, 1024 | 27.291 [26.423, 46.593] | 45.663 [42.999, 72.019] | 1.730 [1.412, 1.883] | 15.78x | 26.40x |
| 2.2.1, DECIMAL(19,3), true, generic, 1024 | 8.733 [7.748, 11.492] | 46.698 [41.372, 63.026] | 2.172 [1.893, 2.428] | 4.02x | 21.50x |
| 2.2.1, DECIMAL(19,3), true, binary, 16 | 20.555 [19.496, 23.326] | 574.708 [558.461, 636.387] | 24.882 [23.509, 30.155] | 0.83x | 23.10x |
| 2.2.1, DECIMAL(38,18), false, binary, 1024 | 28.486 [27.484, 49.393] | 51.687 [46.701, 74.498] | 1.746 [1.603, 1.935] | 16.32x | 29.61x |
| 2.2.1, DECIMAL(38,18), true, generic, 1024 | 9.229 [8.596, 14.255] | 49.003 [42.676, 66.046] | 2.261 [2.113, 2.501] | 4.08x | 21.67x |
| 2.2.1, DECIMAL(38,18), true, binary, 16 | 20.540 [19.787, 26.115] | 606.761 [588.124, 715.747] | 24.800 [23.598, 25.590] | 0.83x | 24.47x |
| 2.2.1, DECIMAL(38,38), false, binary, 1024 | 34.427 [32.860, 68.352] | 52.185 [46.946, 90.398] | 3.644 [3.595, 3.922] | 9.45x | 14.32x |
| 2.2.1, DECIMAL(38,38), true, generic, 1024 | 15.423 [14.765, 34.475] | 46.836 [43.168, 89.345] | 3.973 [3.814, 4.130] | 3.88x | 11.79x |
| 2.2.1, DECIMAL(38,38), true, binary, 16 | 30.945 [28.578, 46.801] | 596.071 [584.038, 614.315] | 30.892 [29.867, 31.805] | 1.00x | 19.30x |

At 1,024 rows the candidate is **2.45–18.12x stock** and **11.79–31.10x previous** across
these configurations. At 16 rows it is **0.22–1.00x stock**, although **18.37–24.83x previous**.
Small-batch Calc/result overhead remains in #491; these controls do not establish a stock
speedup. The allocation probe records 20 calls for an ordinary SIGN Calc at either batch size,
with 256 new output value-buffer bytes at 16 rows versus 16,384 at 1,024. Noncompact p=s
also derives a new validity bitmap (22 recorded allocation calls). These are per-invocation
observations, not a complete causal CPU profile or an allocation-free execution claim.

### Complete row-source job

Flink 2.2.1, parallelism one, one million nullable DECIMAL(18,3) values with mixed signs,
zero and extremes, rowwise blackhole sink. Each engine has two warmups and five measured jobs
in separate sequential JVMs. Previous uses the unchanged main encoder; native plans require
both RowDataToArrow and ArrowToRowData and positive consumed NativeCalc work. Timings include
submission, full source/sink processing and both perimeter transposes.

| Engine | Median seconds | Observed min/max seconds |
| --- | ---: | ---: |
| Stock Flink | 0.135526792 | 0.125703833–0.148966750 |
| Previous scalar callback | 0.174364125 | 0.168311458–0.186801792 |
| Typed native SIGN | 0.151711958 | 0.141338042–0.159535541 |

The candidate is **0.89x stock** and **1.15x previous** by these medians. It still loses to
stock in this standalone job, with overlapping stock/native ranges; the prepared-kernel gain
is not an end-to-end acceleration claim. Both transposes remain a material pipeline cost.

### Validation and reproduction

The focused matrix passes 324 JVM test invocations across both Flink lines and JDK 11/17,
including NULL/NOT NULL results, released sink failures, consumed native batches, compact
character and DECIMAL conditional consumers, integer/floating SIGN controls, rounding metadata,
unselected failures and all-filtered batches. A released-method oracle separately checks 2,337
values and precision/scale pairs per Flink line, including compact and noncompact p=s.
The unchanged released `ScalarFunctionsTest.testSign` passes on both lines as a result oracle;
separate runtime-source witnesses establish native execution over 5,003 rows per admitted
profile. Rust tests cover every supported precision/scale, scalar/array results, slices,
empty/all-NULL inputs and input ownership; the release core suite passes 834 tests.

The helper is `DecimalSignBenchmarkFunctions`, the full job is `DecimalSignPipelineBenchmark`,
and all measured trials/allocation probes are in the [raw results](results/decimal-sign-2026-10-28.json).
With the ordinary Maven test classpath in `SF_NATIVE_BENCH_CLASSPATH`, JDK 17 in `JAVA_HOME`,
and `SF_NATIVE_LIB_DIR` pointing at the release library directory:

```sh
cargo build --manifest-path native/Cargo.toml -p streamfusion --release \
  --no-default-features --lib --example decimal_sign
native/target/release/examples/decimal_sign 18 3 true false 1024 64 16 11
```

Use `19 19` or `38 38` for noncompact zero-integer-width controls; `16 4096` as the batch/loop
arguments retains the same row count for the small control. The pipeline is opt-in:
`SF_BENCHMARK=true mvn test -Pbench -pl streamfusion-core -Dtest=DecimalSignPipelineBenchmark`
with `-Ddecimal.sign.pipeline.engine=flink` or `native`; `previous` requires the independently
compiled baseline encoder at the front of the classpath. Defaults retain one million rows,
two warmups and five measured runs. Do not compare a debug library with these release results.

## Fixed binary Base64 encoding

Measured on 2026-10-28 against canonical main `d89e03b5`, with released Flink 1.18.1
and 2.2.1, JDK 17, Apple M4 Pro, Rust 1.94.0 and release/no-default-features native code
using the system allocator. The fixed-BINARY overload now uses the same byte encoder as
VARBINARY. Both Arrow Binary and FixedSizeBinary are consumed directly without a preliminary
byte copy. Legacy actual byte lengths remain unchanged; casts own padding/truncation.

Stock runs the released generated expression over prepared GenericRowData or BinaryRowData.
Previous and candidate production Calc start on identical prepared Arrow values/types/NULLs,
with timers inside Rust. The previous control retains the actual main scalar callback
(kind 17, BINARY argument and STRING result), including its intrinsic marshalling. Setup,
initial JNI import/export and perimeter transposes are excluded from this small-function
control. Full schema/Arrow output equality is checked before timing; each trial consumes
equal length/first/last-byte checksums. All schemas are nullable; enabled NULL values occur
every seventh row. Sixteen warmups and eleven measured 65,536-row trials rotate engine order.
Configurations execute serially, without concurrent tests or builds. Values are median
[min, max] ns/row:

| Flink, bytes, NULL values, stock format, Arrow type, batch rows | Stock ns/row | Previous ns/row | Native ns/row | vs stock | vs previous |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1.18.1, 16, false, binary, FixedSizeBinary, 1024 | 25.670 [24.779, 47.560] | 84.452 [78.779, 115.376] | 7.149 [7.073, 8.103] | 3.59x | 11.81x |
| 1.18.1, 16, true, generic, Binary, 1024 | 27.252 [16.367, 42.290] | 97.043 [76.092, 127.300] | 6.220 [5.834, 6.392] | 4.38x | 15.60x |
| 1.18.1, 16, true, binary, FixedSizeBinary, 16 | 26.383 [23.383, 36.641] | 612.534 [604.555, 641.434] | 35.286 [34.123, 36.072] | 0.75x | 17.36x |
| 1.18.1, 2, false, binary, FixedSizeBinary, 1024 | 18.431 [17.228, 34.025] | 59.009 [51.335, 86.955] | 6.527 [6.195, 6.877] | 2.82x | 9.04x |
| 1.18.1, 2, true, generic, Binary, 1024 | 12.836 [10.766, 26.017] | 57.096 [48.562, 76.233] | 5.573 [4.941, 6.486] | 2.30x | 10.25x |
| 1.18.1, 2, true, binary, FixedSizeBinary, 16 | 14.723 [13.955, 18.323] | 566.812 [554.206, 580.385] | 32.871 [32.306, 34.672] | 0.45x | 17.24x |
| 1.18.1, 256, false, binary, FixedSizeBinary, 1024 | 45.441 [43.418, 72.034] | 182.394 [172.059, 195.187] | 30.001 [29.427, 30.876] | 1.51x | 6.08x |
| 1.18.1, 256, true, generic, Binary, 1024 | 29.374 [28.728, 54.389] | 164.123 [153.769, 187.140] | 25.521 [25.085, 26.051] | 1.15x | 6.43x |
| 1.18.1, 256, true, binary, FixedSizeBinary, 16 | 37.827 [35.375, 40.659] | 735.647 [717.014, 813.629] | 47.815 [47.017, 50.003] | 0.79x | 15.39x |
| 2.2.1, 16, false, binary, FixedSizeBinary, 1024 | 26.119 [24.993, 43.158] | 91.498 [83.882, 115.524] | 7.357 [7.231, 7.710] | 3.55x | 12.44x |
| 2.2.1, 16, true, generic, Binary, 1024 | 16.402 [15.439, 34.462] | 77.600 [71.830, 98.326] | 6.156 [5.679, 6.345] | 2.66x | 12.61x |
| 2.2.1, 16, true, binary, FixedSizeBinary, 16 | 24.954 [22.902, 29.020] | 610.266 [605.935, 625.162] | 36.786 [35.926, 37.446] | 0.68x | 16.59x |
| 2.2.1, 2, false, binary, FixedSizeBinary, 1024 | 18.450 [17.279, 34.189] | 52.354 [50.027, 75.937] | 6.508 [6.390, 6.891] | 2.83x | 8.04x |
| 2.2.1, 2, true, generic, Binary, 1024 | 10.519 [10.345, 13.292] | 53.988 [48.670, 71.146] | 5.329 [4.779, 5.461] | 1.97x | 10.13x |
| 2.2.1, 2, true, binary, FixedSizeBinary, 16 | 14.802 [13.870, 20.783] | 556.320 [547.763, 559.575] | 32.548 [31.918, 32.867] | 0.45x | 17.09x |
| 2.2.1, 256, false, binary, FixedSizeBinary, 1024 | 43.749 [42.463, 53.862] | 179.748 [170.487, 216.596] | 29.687 [27.475, 30.474] | 1.47x | 6.05x |
| 2.2.1, 256, true, generic, Binary, 1024 | 30.261 [28.229, 40.974] | 158.586 [156.895, 174.400] | 26.091 [25.161, 26.950] | 1.16x | 6.08x |
| 2.2.1, 256, true, binary, FixedSizeBinary, 16 | 36.395 [35.843, 41.303] | 742.716 [732.299, 774.394] | 47.683 [46.579, 48.219] | 0.76x | 15.58x |

The 1,024-row configurations improve 1.15-4.38x over stock and 6.05-15.60x over the
preceding callback. Sixteen-row controls remain slower than stock (0.45-0.79x) while
improving 15.39-17.36x over the preceding callback: batch setup/allocation is significant
for tiny inputs. These controls are retained and do not establish universal acceleration.

The initial scalar codec lost to stock on 256-byte inputs. A native CPU sample placed
1,864 of 2,442 main-thread samples in that codec. The released SIMD encoder removed much
of that cost, after which redundant UTF-8 validation still accounted for 413 of 2,433
samples. Checked offsets, exact-size initialized row buffers and the encoder's ASCII-only
output permit omitting that rescan. Runtime CPU dispatch retains a portable fallback;
regression tests compare against the independent standard codec and fully validate Arrow
outputs at vector/padding boundaries, slices, NULLs and empty batches. See the
[allocation technique](../optimizations/allocation-discipline.md#to_base64).

The separate Flink 2.2.1 complete job uses a bounded 1M-row BINARY(16) source, parallelism
one, both row/Arrow transposes and a rowwise blackhole sink. Candidate requires `jvm=[]`
and positive consumed NativeCalc work. The previous control loads main's actual encoder
first on the classpath and requires its JVM evaluator. Each engine runs two warmups and
five measured jobs in separate sequential JVMs. Seconds are median [min, max]:

| Stock | Previous callback | Candidate |
| ---: | ---: | ---: |
| 0.186338 [0.175739, 0.196376] | 0.270293 [0.265321, 0.280235] | 0.170568 [0.162968, 0.171469] |

This workload reduces elapsed time by about 8.5% versus stock and 36.9% versus previous
StreamFusion. Startup, submission, JIT and both transposes remain included; these subsecond
jobs do not establish sustained throughput for arbitrary pipelines. The
[result record](results/fixed-binary-base64-2026-10-28.json) retains all trials, checksums,
allocation observations and the measured intermediate codec controls.

After compiling test helpers, set `SF_NATIVE_BENCH_CLASSPATH` to test/production classes
and released Maven dependencies, and `SF_NATIVE_LIB_DIR` to the release library directory.
Build/run the prepared control from `native/`:

```sh
cargo build --release -p streamfusion --no-default-features --example fixed_binary_base64
./target/release/examples/fixed_binary_base64 16 true false false 1024 64 16 11
```

Arguments are byte width, actual NULL values, BinaryRowData stock input, FixedSizeBinary
native input, batch rows, loops, warmups and measured trials. Complete jobs use
`FixedBinaryBase64PipelineBenchmark`, `SF_BENCHMARK=true` and
`-Dfixed.binary.base64.pipeline.engine=flink|previous|native`; previous requires the baseline
encoder first on the classpath. Both released lines and JDK 11/17 retain runtime parity
and consumed native-work witnesses. The subsequent FROM_BASE64 decoder is measured separately.

## FROM_BASE64

Measured on 2026-10-28 with canonical main `5dce7f04`, released Flink 1.18.1/2.2.1,
JDK 17, Apple M4 Pro, Rust 1.94.0 and release/no-default-features native code using the
system allocator. The previous implementation is PR #549's preceding commit `ea0268e5`.
Only direct final projections enter the native decoder; the sensitive-string consumer
and relational restrictions in the [Calc coverage page](../operators/calc-filter.md#from_base64)
remain unchanged.

Stock generated evaluation starts on prebuilt GenericRowData or BinaryRowData. The native
and preceding production Calc start on equivalent prepared Arrow buffers, with their timers
inside Rust. Initial import/export and perimeter transposes are outside this small-function
measurement. The preceding encoder is compiled independently and loaded first on the
classpath: STRING asserts its actual kind-44 original-string evaluator, BYTES asserts kind 17,
and both return raw Arrow Binary. The candidate asserts kind 6/opcode 230. Intrinsic callbacks
and argument/result marshalling remain timed in the preceding implementation.

Each encoder runs in its own sequential JVM with a stock control and rotating two-engine
order. Both stock controls are retained: their JIT/GC variability is visible, and the speedup
column conservatively uses the **smaller** of their medians. Each process uses sixteen warmups
and eleven measured trials of 65,536 consumed rows. Output length and first/last-byte checksums
must match. Values contain deterministic arbitrary bytes before encoding; nullable schemas
include actual NULLs every seventh row when enabled. No tests or builds run concurrently with
measurements. Width is decoded byte length; reported values are median [min, max] ns/row:

| Flink, bytes, NULLs, SQL argument, stock format, batch rows | Stock/current process | Stock/previous process | Previous | Native | vs stock, conservative | vs previous |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1.18.1, 16, false, BYTES, binary, 1024 | 26.901 [26.489, 40.339] | 31.277 [27.017, 49.859] | 62.787 [56.459, 83.304] | 8.784 [8.691, 9.063] | 3.06x | 7.15x |
| 1.18.1, 16, true, STRING, generic, 1024 | 30.517 [28.866, 61.686] | 36.214 [34.405, 64.553] | 74.523 [66.104, 90.952] | 8.684 [8.558, 9.497] | 3.51x | 8.58x |
| 1.18.1, 16, true, BYTES, binary, 16 | 22.761 [22.219, 36.091] | 23.008 [22.319, 28.255] | 601.892 [596.954, 617.264] | 34.293 [33.745, 34.853] | 0.66x | 17.55x |
| 1.18.1, 2, false, BYTES, binary, 1024 | 24.943 [22.899, 37.237] | 23.944 [22.483, 46.245] | 48.325 [43.325, 71.232] | 6.770 [5.990, 7.114] | 3.54x | 7.14x |
| 1.18.1, 2, true, STRING, generic, 1024 | 41.164 [27.367, 57.690] | 27.501 [27.240, 56.787] | 66.313 [60.933, 105.553] | 6.821 [6.140, 7.190] | 4.03x | 9.72x |
| 1.18.1, 2, true, BYTES, binary, 16 | 20.885 [19.041, 32.747] | 19.783 [19.346, 23.593] | 560.979 [554.251, 586.925] | 32.129 [31.499, 32.871] | 0.62x | 17.46x |
| 1.18.1, 256, false, BYTES, binary, 1024 | 65.030 [63.217, 75.474] | 77.693 [68.219, 116.290] | 104.519 [98.938, 123.337] | 33.895 [33.569, 34.896] | 1.92x | 3.08x |
| 1.18.1, 256, true, STRING, generic, 1024 | 73.407 [68.149, 111.362] | 75.641 [73.624, 103.769] | 126.862 [118.484, 181.338] | 31.077 [29.801, 33.537] | 2.36x | 4.08x |
| 1.18.1, 256, true, BYTES, binary, 16 | 52.860 [51.834, 65.730] | 52.813 [52.046, 57.954] | 647.031 [641.312, 659.692] | 53.922 [53.396, 54.974] | 0.98x | 12.00x |
| 2.2.1, 16, false, BYTES, binary, 1024 | 35.700 [27.991, 54.179] | 29.100 [27.502, 46.365] | 57.854 [51.655, 78.010] | 8.886 [8.677, 9.521] | 3.27x | 6.51x |
| 2.2.1, 16, true, STRING, generic, 1024 | 39.433 [29.179, 43.844] | 33.691 [31.814, 55.526] | 71.623 [66.146, 91.021] | 8.807 [8.603, 9.258] | 3.83x | 8.13x |
| 2.2.1, 16, true, BYTES, binary, 16 | 29.550 [22.308, 40.774] | 23.141 [21.571, 27.645] | 600.082 [579.973, 628.577] | 34.034 [33.515, 35.280] | 0.68x | 17.63x |
| 2.2.1, 2, false, BYTES, binary, 1024 | 43.046 [24.864, 45.284] | 24.703 [23.446, 43.276] | 46.487 [43.537, 66.571] | 6.715 [5.974, 6.949] | 3.68x | 6.92x |
| 2.2.1, 2, true, STRING, generic, 1024 | 29.752 [26.725, 40.999] | 28.487 [26.877, 39.600] | 64.948 [60.902, 84.585] | 6.152 [6.048, 6.826] | 4.63x | 10.56x |
| 2.2.1, 2, true, BYTES, binary, 16 | 36.005 [18.298, 41.290] | 19.275 [18.974, 24.094] | 557.683 [550.521, 575.581] | 34.298 [31.134, 36.540] | 0.56x | 16.26x |
| 2.2.1, 256, false, BYTES, binary, 1024 | 75.551 [63.354, 113.574] | 68.417 [64.597, 81.800] | 102.013 [99.084, 115.237] | 34.513 [33.751, 35.754] | 1.98x | 2.96x |
| 2.2.1, 256, true, STRING, generic, 1024 | 81.366 [67.916, 121.744] | 73.105 [67.038, 89.697] | 121.209 [114.834, 145.936] | 30.738 [29.545, 31.277] | 2.38x | 3.94x |
| 2.2.1, 256, true, BYTES, binary, 16 | 52.263 [51.912, 64.269] | 58.423 [52.698, 60.040] | 665.132 [654.268, 689.308] | 57.742 [57.051, 60.036] | 0.91x | 11.52x |

The 1,024-row configurations improve 1.92-4.63x over stock and 2.96-10.56x over the
preceding scalar path. Sixteen-row controls improve over the preceding callback but remain
slower than stock (0.56-0.98x using the conservative stock median). Calc/batch allocation
and dispatch are significant at tiny sizes; these results do not establish a universal
speedup. The implementation sizes checked offsets, writes directly into final Binary
buffers and decodes complete groups with runtime SIMD dispatch. A short scalar tail preserves
the JDK's accepted nonzero unused bits; invalid-input diagnostics stay in Rust. See the
[allocation technique](../optimizations/allocation-discipline.md#from_base64).

The separate Flink 2.2.1 complete job uses one million encoded STRING rows, representing
16 arbitrary decoded bytes, parallelism one, a bounded row source and a rowwise blackhole
sink. Source encoding, submission, JIT, JNI and both row/Arrow transposes remain included.
Candidate requires `jvm=[]`, both transposes and positive consumed NativeCalc work. The
previous control loads the actual preceding encoder and requires the FROM_BASE64 evaluator.
Each engine runs two warmups and five measured jobs in a separate sequential JVM. Seconds
are median [min, max]:

| Stock | Previous callback | Native |
| ---: | ---: | ---: |
| 0.243187 [0.235530, 0.244118] | 0.344742 [0.341944, 0.353287] | 0.210599 [0.201220, 0.212980] |

Elapsed time falls about 13.4% versus stock and 38.9% versus previous StreamFusion in this
bounded workload. These subsecond jobs include startup costs and do not establish sustained
throughput for other pipelines. The [result record](results/from-base64-2026-10-28.json)
retains both stock controls, every timing trial, checksums and untimed allocation observations.

Validation includes 542 focused JVM invocations across Flink 1.18.1/2.2.1 and JDK 11/17,
841 release Rust core tests, and the unchanged released `testFromBase64`/`testToBase64`
fixtures on both lines. A deterministic 46,443-case oracle covers short encodings, missing
and malformed padding, unused bits, nonalphabet/signed bytes and mutated longer encodings.
Both Flink overloads on both JDKs/versions agree, and the production Rust scalar matches every
case. SQL tests require consumed native work and no JVM evaluator for STRING, CHAR, BYTES
and fixed-BINARY inputs. Raw decoded bytes, nullable slices, vector boundaries, legacy casts,
all-filtered batches, lazy branches and retained consumer/sink restrictions remain covered.

After compiling `FromBase64BenchmarkFunctions`, set `SF_NATIVE_BENCH_CLASSPATH` to test and
production classes plus released dependencies, and `SF_NATIVE_LIB_DIR` to the release library
directory. Build and run from `native/`:

```sh
cargo build --release --locked -p streamfusion --no-default-features --example from_base64
./target/release/examples/from_base64 16 true false false 1024 64 16 11
java -cp "$SF_NATIVE_BENCH_CLASSPATH" tech.streamfusion.planner.FromBase64BenchmarkFunctions > oracle.tsv
./target/release/examples/from_base64 --oracle oracle.tsv
```

Prepared arguments are decoded width, NULL values, binary SQL argument, BinaryRowData stock
input, batch rows, loops, warmups and measured trials. The previous control sets
`SF_FROM_BASE64_PREVIOUS=true` and loads the independently compiled preceding encoder first
on the classpath. Complete jobs use `FromBase64PipelineBenchmark`, `SF_BENCHMARK=true` and
`-Dfrom.base64.pipeline.engine=flink|previous|native`, with the same previous-classpath rule.

## Runtime SQL TRIM

The `sql_trim` release driver compares SQL `TRIM` in all three directions using identical
nullable values on prebuilt Java RowData and Arrow batches. Stock Flink and the previous
StreamFusion route are the same generated Flink operation: runtime SQL trim previously
caused planner fallback. A third generated batch-callback measurement is diagnostic only;
it is not labelled as the previous route. All three consume output byte lengths.

The native timer runs inside Rust around production compiled Calc, including the ASCII/cache
certificate, set membership, output allocation and consumption. Setup, initial JNI/Arrow
import and final export are outside this prepared-function timer. Java times the released
generated expression and equivalent consumption on its prepared rows. Both retain nullable
strings and the original decoded Java caches. The native driver uses an invalid evaluator
id for admitted ASCII controls, so an unexpected callback fails instead of silently being
counted as native computation. Unicode controls retain the exact evaluator.

Measurements on Apple M4 Pro/24 GiB, JDK 17.0.12 and Rust 1.95.0 use released Flink
1.18.1/2.2.1, release Rust with mimalloc, Arrow 58.4.1 and DataFusion 54.0.0. Each
profile runs eight warmups and nine measured trials of 128 batches, sequentially
without concurrent tests or builds. Fresh JVMs use 256/512 MiB initial/maximum heaps.
Four runtime trim sets (` ab`, `ab`, tab-plus-space and `[]+`) provide cardinality four.
Source NULLs occur every seventh row, trim-set NULLs every eleventh row. Width is the
middle payload in bytes; each source also contains the trim set on both ends.

Median [minimum-maximum] ns/row:

| Flink | Payload bytes / batch rows | Direction | Stock / previous fallback | Native | Stock/native |
| --- | --- | --- | ---: | ---: | ---: |
| 1.18.1 | 8 / 16 | BOTH | 184.31 [162.33-334.27] | 56.97 [54.38-58.59] | 3.24x |
| 1.18.1 | 8 / 16 | LEADING | 102.86 [96.42-157.27] | 56.82 [53.79-57.64] | 1.81x |
| 1.18.1 | 8 / 16 | TRAILING | 112.35 [104.13-180.09] | 56.54 [50.05-59.65] | 1.99x |
| 1.18.1 | 8 / 1024 | BOTH | 147.08 [143.19-152.81] | 20.96 [19.73-21.70] | 7.02x |
| 1.18.1 | 8 / 1024 | LEADING | 84.72 [82.31-88.87] | 19.66 [18.52-20.77] | 4.31x |
| 1.18.1 | 8 / 1024 | TRAILING | 103.82 [101.80-104.36] | 20.19 [19.19-20.80] | 5.14x |
| 1.18.1 | 264 / 16 | BOTH | 301.21 [248.94-554.30] | 96.92 [89.56-100.54] | 3.11x |
| 1.18.1 | 264 / 16 | LEADING | 159.28 [113.87-240.48] | 90.50 [85.53-95.24] | 1.76x |
| 1.18.1 | 264 / 16 | TRAILING | 237.90 [196.82-1063.74] | 92.88 [86.26-96.46] | 2.56x |
| 1.18.1 | 264 / 1024 | BOTH | 260.56 [252.13-263.20] | 58.98 [56.48-61.01] | 4.42x |
| 1.18.1 | 264 / 1024 | LEADING | 109.44 [107.03-110.84] | 55.22 [53.59-59.03] | 1.98x |
| 1.18.1 | 264 / 1024 | TRAILING | 209.08 [204.29-214.20] | 55.34 [54.65-57.76] | 3.78x |
| 2.2.1 | 8 / 16 | BOTH | 163.17 [148.21-313.42] | 57.96 [52.06-67.89] | 2.82x |
| 2.2.1 | 8 / 16 | LEADING | 174.99 [97.17-218.97] | 54.85 [49.82-59.86] | 3.19x |
| 2.2.1 | 8 / 16 | TRAILING | 202.90 [110.41-1007.57] | 57.58 [56.80-58.92] | 3.52x |
| 2.2.1 | 8 / 1024 | BOTH | 152.31 [150.98-156.82] | 21.17 [20.34-21.66] | 7.19x |
| 2.2.1 | 8 / 1024 | LEADING | 86.67 [83.49-94.38] | 20.29 [19.06-20.98] | 4.27x |
| 2.2.1 | 8 / 1024 | TRAILING | 119.16 [115.61-120.89] | 19.76 [18.67-20.59] | 6.03x |
| 2.2.1 | 264 / 16 | BOTH | 321.49 [250.08-1077.96] | 92.20 [90.82-104.19] | 3.49x |
| 2.2.1 | 264 / 16 | LEADING | 130.45 [110.03-1226.44] | 90.86 [87.22-92.90] | 1.44x |
| 2.2.1 | 264 / 16 | TRAILING | 212.24 [201.80-260.23] | 90.17 [82.52-92.26] | 2.35x |
| 2.2.1 | 264 / 1024 | BOTH | 269.05 [266.16-272.72] | 57.91 [57.15-60.14] | 4.65x |
| 2.2.1 | 264 / 1024 | LEADING | 111.92 [108.80-113.16] | 56.65 [55.57-58.46] | 1.98x |
| 2.2.1 | 264 / 1024 | TRAILING | 215.36 [212.93-218.58] | 55.40 [54.48-57.55] | 3.89x |

The 24 measured medians improve 1.44-7.19x over stock, which is
also the previous fallback route. This does not establish a win for every input,
computed expression, Unicode evaluator batch or downstream pipeline. The
[result record](results/runtime-sql-trim-2026-10-29.json) retains every warmup and
measurement, checksums, ranges, callback diagnostics and initial implementation.
The [raw CSV](results/runtime-sql-trim-2026-10-29.csv) contains all prepared trials.
The callback is deliberately a separate diagnostic and cannot serve as the
previous-production baseline.

The initial cache comparison checked one UTF-16 unit at a time. On 264-byte,
1,024-row LEADING profiles it lost to stock: 121.10 versus 106.99 ns/row on 1.18.1,
and 123.13 versus 109.35 on 2.2.1. Profiling localized the expensive certificate
frame. Comparing eight widened byte/unit lanes together preserves the complete
check and removes that measured regression. Both the unfavorable initial samples
and final ranges are retained; their source delta is the cache-comparison loop.

A separate complete job uses one million nullable rows, 64-byte payloads, BOTH,
parallelism one and a rowwise blackhole sink. It retains the row source, both
perimeter transposes, native JNI and planning/submission through completion.
Each Flink version runs two warmups and five measured trials per engine in one
JVM, alternating engine order, with the same 1 GiB maximum heap. Native plans and
positive consumed Calc work are required. Seconds are median [minimum-maximum]:

| Flink | Stock / previous fallback | Native |
| --- | ---: | ---: |
| 1.18.1 | 0.427211 [0.423715-0.438213] | 0.325314 [0.318184-0.330477] |
| 2.2.1 | 0.531298 [0.524301-0.537697] | 0.319176 [0.317447-0.327937] |

These short jobs include startup and do not establish sustained connector
throughput. Blackhole timing does not verify output values; separate SQL tests
compare values, logical types, NULLs, lazy branches, empty filtered batches,
Unicode and isolated surrogates. Validation passes 94 focused JVM tests with
14 explicit version skips across both Flink lines and JDK 11/17, 857 release
Rust core tests, and unchanged released `ScalarFunctionsTest.testTrim` on each
Flink line. Rust tests use an invalid evaluator ID to prove each admitted trim
direction can execute without a JVM callback.

Reproduction after compiling runtime production/test classes and obtaining their test
classpath:

```sh
cd native
cargo build --locked --release -p streamfusion --no-default-features --features mimalloc --example sql_trim
SF_NATIVE_BENCH_CLASSPATH="$TEST_CLASSPATH" SF_NATIVE_LIB_DIR="$NATIVE_LIB_DIR" \
  target/release/examples/sql_trim ascii 264 true 1024 128 0 all 8 9
```

Arguments are corpus (`ascii` or `unicode`), payload bytes, decoded-cache presence,
batch rows, repeated batches, direction (0 BOTH, 1 LEADING, 2 TRAILING), engine filter,
warmup trials and measured trials. These prepared measurements do not establish complete-job
throughput; a row-source/row-sink job retains both perimeter transposes.

Run the separate bounded job from the repository root, with the release library
directory in `NATIVE_LIB_DIR`:

```sh
SF_BENCHMARK=true mvn -B -ntp -pl streamfusion-runtime -am test -Pbench \
  -Dnative.build.skip=true -Dsurefire.failIfNoSpecifiedTests=false \
  -Dsf.testForks=1 -Dtest=SqlTrimPipelineBenchmark \
  "-Dsf.extraJvmArgs=-Xmx1g -Djava.library.path=$NATIVE_LIB_DIR" \
  -Dtrim.pipeline.rows=1000000 -Dtrim.pipeline.width=64 \
  -Dtrim.pipeline.direction=BOTH -Dtrim.pipeline.warmups=2 -Dtrim.pipeline.runs=5
```

Add `flink-1.18` to the Maven profiles for the other supported line. Complete-job
trials and their engine order are retained in the JSON result record.

## Integer ROUND with a literal position

Measured on 2026-10-28 against canonical main `d89e03b5`: Apple M4 Pro, macOS aarch64,
JDK 17.0.12, Rust 1.94.0 release without default features, system allocator with counting
allocation wrapper. Released Flink 1.18.1 and 2.2.1 are measured independently. The query
is `ROUND(v, literal_position)` on each signed integer width. No option is required for
admitted positions in -38..38 or typed NULL; runtime/extreme positions stay generated.

The shared `integral_rounding` example compares equal prepared logical values, widths,
NULLs, signed minima and BIGINTs above 2^53. Stock uses the released generated expression
over GenericRowData/BinaryRowData. Main and candidate start on Arrow in production Calc,
with timers inside Rust. Main's actual encoding was independently checked on both lines:
kind 17, one integer input, a matching primitive result code and the literal embedded in
the generated expression. Its scalar JVM callback and intrinsic conversions remain timed;
a whole-row evaluator is not substituted for that baseline. Setup, initial JNI entry,
Arrow import/export and perimeter transposes are excluded. Full Arrow/schema equality
precedes timing; every measured output contributes to matching wrapping checksums.

All schemas are nullable; the NULL flag means actual NULLs every seventh row. Profiles
include a native-width divisor (-1), a widened divisor (-19), shared-buffer identity (+2),
1,024-row bulk and 16-row controls. Stock/main/candidate order rotates over 16 warmups
and 11 measured trials of 65,536 rows per trial. Runs are sequential without concurrent
builds or tests. Values are median [min, max] ns/row.

| Flink / type / NULL values / stock format / rows / position | Stock ns/row | Main ns/row | Native ns/row | Stock/native | Main/native |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1.18.1 / BIGINT / false / binary / 1024 / -1 | 20.295 [16.401, 34.218] | 63.417 [42.317, 97.714] | 1.054 [0.970, 1.186] | 19.25x | 60.16x |
| 1.18.1 / BIGINT / true / generic / 1024 / -1 | 25.187 [15.237, 44.253] | 57.227 [40.222, 115.171] | 1.401 [1.374, 1.504] | 17.98x | 40.86x |
| 1.18.1 / BIGINT / true / generic / 1024 / 2 | 21.570 [19.807, 38.154] | 48.349 [42.130, 79.212] | 0.776 [0.706, 0.835] | 27.79x | 62.28x |
| 1.18.1 / BIGINT / true / binary / 1024 / -19 | 21.341 [19.769, 45.342] | 64.600 [48.355, 104.913] | 1.433 [1.362, 1.626] | 14.89x | 45.08x |
| 1.18.1 / BIGINT / true / binary / 16 / -1 | 8.309 [8.127, 8.578] | 522.553 [509.412, 531.985] | 26.217 [25.069, 27.737] | 0.32x | 19.93x |
| 1.18.1 / INT / false / binary / 1024 / -1 | 14.713 [11.123, 16.694] | 56.767 [51.779, 101.699] | 1.043 [0.915, 1.096] | 14.11x | 54.44x |
| 1.18.1 / INT / true / generic / 1024 / -1 | 9.023 [8.751, 10.166] | 32.519 [29.354, 98.593] | 1.403 [1.362, 1.588] | 6.43x | 23.18x |
| 1.18.1 / INT / true / generic / 1024 / 2 | 9.085 [8.963, 12.779] | 39.531 [34.158, 76.625] | 0.813 [0.800, 0.884] | 11.17x | 48.61x |
| 1.18.1 / INT / true / binary / 1024 / -19 | 10.394 [9.523, 29.420] | 44.711 [41.842, 96.408] | 1.404 [1.354, 1.561] | 7.40x | 31.85x |
| 1.18.1 / INT / true / binary / 16 / -1 | 8.261 [8.142, 12.412] | 506.146 [501.726, 549.752] | 23.167 [22.579, 23.507] | 0.36x | 21.85x |
| 1.18.1 / SMALLINT / false / binary / 1024 / -1 | 12.840 [12.487, 34.390] | 50.510 [40.183, 100.056] | 1.024 [0.963, 1.071] | 12.54x | 49.31x |
| 1.18.1 / SMALLINT / true / generic / 1024 / -1 | 10.525 [9.774, 28.827] | 32.763 [31.300, 39.398] | 1.408 [1.360, 1.470] | 7.48x | 23.28x |
| 1.18.1 / SMALLINT / true / generic / 1024 / 2 | 14.768 [11.401, 28.476] | 47.838 [35.615, 119.448] | 0.830 [0.793, 0.968] | 17.79x | 57.61x |
| 1.18.1 / SMALLINT / true / binary / 1024 / -19 | 19.641 [14.444, 36.503] | 37.951 [33.329, 54.068] | 1.392 [1.289, 1.594] | 14.11x | 27.27x |
| 1.18.1 / SMALLINT / true / binary / 16 / -1 | 6.919 [6.827, 27.100] | 523.215 [515.179, 527.838] | 23.293 [22.640, 23.792] | 0.30x | 22.46x |
| 1.18.1 / TINYINT / false / binary / 1024 / -1 | 11.786 [11.625, 12.552] | 51.721 [46.646, 98.059] | 1.037 [1.020, 1.140] | 11.37x | 49.88x |
| 1.18.1 / TINYINT / true / generic / 1024 / -1 | 10.710 [9.556, 29.348] | 41.841 [31.691, 72.752] | 1.429 [1.249, 1.666] | 7.49x | 29.27x |
| 1.18.1 / TINYINT / true / generic / 1024 / 2 | 16.310 [14.906, 31.041] | 38.218 [35.713, 56.314] | 0.826 [0.773, 0.844] | 19.75x | 46.27x |
| 1.18.1 / TINYINT / true / binary / 1024 / -19 | 21.159 [15.428, 37.910] | 58.338 [39.677, 113.485] | 1.396 [1.350, 1.724] | 15.15x | 41.78x |
| 1.18.1 / TINYINT / true / binary / 16 / -1 | 9.501 [8.812, 10.990] | 569.044 [549.186, 615.807] | 24.003 [23.064, 25.495] | 0.40x | 23.71x |
| 2.2.1 / BIGINT / false / binary / 1024 / -1 | 17.445 [16.772, 31.464] | 38.129 [36.368, 54.123] | 1.046 [1.013, 1.104] | 16.68x | 36.46x |
| 2.2.1 / BIGINT / true / generic / 1024 / -1 | 18.519 [18.015, 36.332] | 34.995 [32.736, 55.193] | 1.427 [1.368, 1.521] | 12.97x | 24.52x |
| 2.2.1 / BIGINT / true / generic / 1024 / 2 | 34.648 [19.582, 43.114] | 65.889 [49.659, 140.938] | 0.837 [0.698, 1.000] | 41.38x | 78.69x |
| 2.2.1 / BIGINT / true / binary / 1024 / -19 | 20.863 [20.085, 64.760] | 65.118 [54.057, 106.282] | 1.440 [1.410, 1.745] | 14.49x | 45.22x |
| 2.2.1 / BIGINT / true / binary / 16 / -1 | 8.451 [7.376, 13.141] | 571.144 [539.469, 584.906] | 26.101 [22.622, 28.975] | 0.32x | 21.88x |
| 2.2.1 / INT / false / binary / 1024 / -1 | 12.973 [10.591, 17.623] | 54.145 [47.759, 98.277] | 1.066 [0.959, 1.113] | 12.17x | 50.81x |
| 2.2.1 / INT / true / generic / 1024 / -1 | 12.401 [9.130, 14.297] | 49.821 [34.920, 102.674] | 1.415 [1.298, 1.521] | 8.77x | 35.22x |
| 2.2.1 / INT / true / generic / 1024 / 2 | 12.624 [10.127, 27.711] | 66.395 [42.700, 91.185] | 0.837 [0.818, 0.850] | 15.09x | 79.35x |
| 2.2.1 / INT / true / binary / 1024 / -19 | 15.711 [9.849, 37.292] | 105.385 [38.283, 164.042] | 1.377 [1.354, 1.432] | 11.41x | 76.53x |
| 2.2.1 / INT / true / binary / 16 / -1 | 8.342 [7.921, 8.502] | 516.578 [501.617, 538.036] | 23.314 [21.689, 25.815] | 0.36x | 22.16x |
| 2.2.1 / SMALLINT / false / binary / 1024 / -1 | 14.891 [12.455, 33.859] | 60.584 [44.174, 101.692] | 1.100 [1.064, 1.170] | 13.54x | 55.08x |
| 2.2.1 / SMALLINT / true / generic / 1024 / -1 | 13.615 [10.628, 30.334] | 55.721 [51.214, 106.354] | 1.415 [1.271, 1.623] | 9.62x | 39.39x |
| 2.2.1 / SMALLINT / true / generic / 1024 / 2 | 13.747 [11.222, 32.217] | 49.999 [41.403, 97.153] | 0.832 [0.741, 0.855] | 16.53x | 60.12x |
| 2.2.1 / SMALLINT / true / binary / 1024 / -19 | 14.741 [14.492, 31.559] | 35.318 [33.274, 53.660] | 1.404 [1.383, 1.635] | 10.50x | 25.16x |
| 2.2.1 / SMALLINT / true / binary / 16 / -1 | 7.176 [6.625, 8.462] | 557.781 [533.161, 611.802] | 24.355 [22.543, 28.458] | 0.29x | 22.90x |
| 2.2.1 / TINYINT / false / binary / 1024 / -1 | 24.363 [11.980, 32.704] | 57.568 [33.740, 84.532] | 1.095 [0.927, 1.188] | 22.25x | 52.58x |
| 2.2.1 / TINYINT / true / generic / 1024 / -1 | 11.126 [9.336, 39.951] | 43.566 [31.790, 70.793] | 1.387 [1.241, 1.590] | 8.02x | 31.40x |
| 2.2.1 / TINYINT / true / generic / 1024 / 2 | 18.447 [15.104, 29.708] | 68.672 [44.312, 97.975] | 0.820 [0.750, 0.860] | 22.51x | 83.80x |
| 2.2.1 / TINYINT / true / binary / 1024 / -19 | 16.088 [15.526, 35.040] | 54.562 [38.261, 114.259] | 1.413 [1.254, 1.724] | 11.38x | 38.61x |
| 2.2.1 / TINYINT / true / binary / 16 / -1 | 9.165 [8.950, 9.549] | 498.255 [490.307, 538.081] | 22.830 [22.391, 24.350] | 0.40x | 21.82x |

The 1,024-row candidate improves 6.43-41.38x over stock and 23.18-83.80x over main.
The 16-row candidate remains slower than stock (0.29-0.40x), while improving
19.93-23.71x over main. Native-width remainder avoids per-element i128 division;
large powers use widened comparisons and nonnegative positions share the input.
A three-second native-only sample on the slower INT/16-row profile observes
scalar-function argument/result setup and destruction, primitive-array allocation,
Calc projection and checksum consumption. Overlapping sampled frames do not establish
independent CPU percentages. The raw allocation probes distinguish shared output buffers
from newly requested values buffers. These are bounded batch gains, not a universal
small-batch or end-to-end claim.

An independent complete job processes 1M INT rows using `ROUND(v,-1)`, parallelism one,
a bounded Flink row source and a rowwise blackhole sink. Native plans retain both
transposes and require positive consumed NativeCalc work. Main uses its actual encoder
first on the classpath and requires the ROUND JVM route; candidate requires `jvm=[]`.
Two warmups precede five measured jobs in each sequential engine JVM. Seconds are
median [min, max]:

| Stock | Main scalar callback | Candidate |
| ---: | ---: | ---: |
| 0.143764 [0.139465, 0.157385] | 0.164713 [0.157161, 0.173648] | 0.139733 [0.130734, 0.143993] |

The candidate improves main on this job, with overlapping stock/candidate ranges.
Startup, submission and JIT remain included; these short jobs do not establish sustained
complete-job acceleration. [All raw trials, checksums, configuration and allocation probes](results/integral-round-positions-2026-10-28.json)
remain available. Generic small-batch and perimeter work stays in
[#491](https://github.com/datafusion-contrib/StreamFusion/issues/491).

After building current Java test classes, set `JAVA_HOME`, `SF_NATIVE_BENCH_CLASSPATH`
and `SF_NATIVE_LIB_DIR` to the release library directory. Build and run:

```sh
cargo build --manifest-path native/Cargo.toml -p streamfusion --release \
  --no-default-features --lib --example integral_rounding
SF_INTEGRAL_ROUNDING_FUNCTION=ROUND SF_INTEGRAL_ROUNDING_POSITION=-1 \
  native/target/release/examples/integral_rounding INT true false 1024 64 16 11
```

Arguments are type, actual NULL flag, stock BinaryRowData flag, batch rows, loops,
warmups and measured trials. Without the position environment variable the shared
example retains its historical single-argument mode. Complete-job properties are
`rounding.pipeline.function=ROUND`, `rounding.pipeline.position=-1` and
`rounding.pipeline.engine=flink|previous|native`, enabled by `SF_BENCHMARK=true` and
the `bench` profile. The previous mode requires the preceding encoder first on the
classpath; relabeling the unchanged candidate is insufficient.

Runtime parity covers every admitted position and all widths on both supported Flink
lines and JDK 11/17. Witnesses consume 5,003 rows per width with no ROUND JVM route,
check exact expected values/NULLs, and require positive native work and no fallback.
Nested/widened expressions, scalar consumers, filters, empty batches, selected/unselected
failures and extreme-position host errors are covered. The unchanged released upstream
`ScalarFunctionsTest.testRound` passes on both lines; it supplies oracle evidence,
while the runtime witnesses prove consumed native work. Full JVM/upstream suites remain CI checks.

## Integer TRUNCATE with a literal position

Measured independently on 2026-10-28 against canonical main `d89e03b5`, using the same
Apple M4 Pro, JDK 17.0.12, Rust 1.94.0 release/no-default-features configuration,
system/counting allocator and boundaries as [literal-position ROUND](#integer-round-with-a-literal-position).
The query is `TRUNCATE(v, literal_position)` on INT/BIGINT. Direct narrow-integer
signatures preserve released Flink's generated-compilation failure; explicit widening
is supported. Admission covers INT literal positions -38..38 or typed NULL and verified
producers only; runtime/extreme positions and other producers stay generated.

Stock executes the released generated projection over prepared GenericRowData/BinaryRowData.
Main's actual kind-17 scalar expression has one integer argument/result and embeds the
literal position; its callback and intrinsic conversion stay inside the Rust timer.
Candidate production Calc invokes the Arrow toward-zero kernel. Values, widths, NULLs,
minima and BIGINTs above 2^53 match. Full output/schema equality precedes timing and all
measured outputs contribute to matching checksums in their own runtime. Input/operator
setup, JNI import/export and perimeter transposes are excluded only from this prepared
functionality measurement. No concurrent tests/builds run during measurement.

Every schema is nullable; actual NULLs appear every seventh row when the flag is true.
Profiles cover -1, -19 and +2 positions, native-width/large divisors, shared-buffer
identity and 1,024-/16-row batches. Each case rotates stock/main/candidate order for
16 warmups and 11 measured trials of 65,536 rows. Values are median [min, max] ns/row.

| Flink / type / NULL values / stock format / rows / position | Stock ns/row | Main ns/row | Native ns/row | Stock/native | Main/native |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1.18.1 / BIGINT / false / binary / 1024 / -1 | 82.793 [80.556, 96.215] | 105.433 [99.404, 160.713] | 1.088 [1.046, 1.149] | 76.06x | 96.86x |
| 1.18.1 / BIGINT / true / generic / 1024 / -1 | 73.506 [70.045, 119.251] | 99.348 [92.960, 154.620] | 1.449 [1.300, 1.569] | 50.73x | 68.57x |
| 1.18.1 / BIGINT / true / generic / 1024 / 2 | 79.189 [66.412, 109.428] | 96.375 [88.223, 119.227] | 0.942 [0.796, 0.998] | 84.04x | 102.28x |
| 1.18.1 / BIGINT / true / binary / 1024 / -19 | 29.352 [28.196, 60.632] | 49.988 [47.063, 72.699] | 0.969 [0.942, 1.118] | 30.29x | 51.59x |
| 1.18.1 / BIGINT / true / binary / 16 / -1 | 66.630 [63.771, 71.818] | 622.942 [606.855, 636.044] | 24.017 [22.496, 30.481] | 2.77x | 25.94x |
| 1.18.1 / INT / false / binary / 1024 / -1 | 63.423 [61.859, 95.424] | 88.022 [80.719, 137.247] | 1.005 [0.931, 1.125] | 63.10x | 87.57x |
| 1.18.1 / INT / true / generic / 1024 / -1 | 56.305 [54.621, 109.278] | 81.130 [74.706, 99.076] | 1.493 [1.376, 1.634] | 37.70x | 54.32x |
| 1.18.1 / INT / true / generic / 1024 / 2 | 54.448 [52.867, 97.235] | 96.239 [73.837, 135.696] | 0.849 [0.821, 0.868] | 64.10x | 113.30x |
| 1.18.1 / INT / true / binary / 1024 / -19 | 26.170 [24.514, 49.325] | 62.735 [46.422, 148.438] | 1.012 [0.946, 1.991] | 25.86x | 61.98x |
| 1.18.1 / INT / true / binary / 16 / -1 | 52.738 [50.178, 59.952] | 595.444 [586.147, 614.468] | 24.014 [22.436, 26.831] | 2.20x | 24.80x |
| 2.2.1 / BIGINT / false / binary / 1024 / -1 | 81.130 [77.929, 119.018] | 106.023 [100.971, 168.556] | 1.126 [1.043, 1.234] | 72.05x | 94.16x |
| 2.2.1 / BIGINT / true / generic / 1024 / -1 | 71.206 [68.321, 89.049] | 93.903 [89.711, 114.658] | 1.430 [1.390, 1.520] | 49.80x | 65.67x |
| 2.2.1 / BIGINT / true / generic / 1024 / 2 | 74.601 [61.907, 107.140] | 93.374 [87.193, 154.513] | 0.860 [0.745, 0.956] | 86.72x | 108.55x |
| 2.2.1 / BIGINT / true / binary / 1024 / -19 | 29.249 [27.757, 61.291] | 66.074 [50.031, 106.862] | 1.003 [0.959, 1.086] | 29.17x | 65.90x |
| 2.2.1 / BIGINT / true / binary / 16 / -1 | 66.425 [64.204, 75.006] | 623.986 [616.370, 649.518] | 24.160 [23.481, 25.192] | 2.75x | 25.83x |
| 2.2.1 / INT / false / binary / 1024 / -1 | 63.797 [60.988, 116.205] | 88.085 [79.557, 121.353] | 1.036 [0.984, 1.175] | 61.60x | 85.05x |
| 2.2.1 / INT / true / generic / 1024 / -1 | 55.975 [53.712, 95.767] | 82.354 [73.753, 118.947] | 1.420 [1.291, 1.466] | 39.41x | 57.98x |
| 2.2.1 / INT / true / generic / 1024 / 2 | 65.570 [53.518, 104.253] | 85.697 [76.602, 140.537] | 0.852 [0.741, 0.964] | 76.96x | 100.59x |
| 2.2.1 / INT / true / binary / 1024 / -19 | 25.875 [24.856, 50.336] | 51.790 [44.918, 96.118] | 0.957 [0.909, 1.131] | 27.04x | 54.13x |
| 2.2.1 / INT / true / binary / 16 / -1 | 52.284 [50.070, 56.420] | 586.700 [566.703, 602.521] | 28.638 [27.205, 29.861] | 1.83x | 20.49x |

At 1,024 rows the recorded candidate improves 25.86-86.72x over stock and 51.59-113.30x
over main. At 16 rows it improves 1.83-2.77x over stock and 20.49-25.94x over main.
The kernel avoids released Flink's DECIMAL(38,18) intermediate, uses native integer
remainder for negative positions, writes zero when the divisor exceeds the input width,
and shares buffers for nonnegative positions. There is no HALF_UP increment in TRUNCATE.
Allocation probes in the raw record distinguish shared validity/identity buffers from
new result values. These measurements do not imply that every Calc or workload accelerates.

The independent complete job retains the bounded row source, both transposes and a
rowwise blackhole sink: Flink 2.2.1, 1M INT rows, `TRUNCATE(v,-1)`, parallelism one,
two warmups and five measured jobs per engine in sequential JVMs. The actual main
encoder is loaded first for the previous control; route checks require its TRUNCATE
JVM evaluator and candidate `jvm=[]`. Both native modes require consumed NativeCalc
work and both transposes. Seconds are median [min, max]:

| Stock | Main scalar callback | Candidate |
| ---: | ---: | ---: |
| 0.210510 [0.200087, 0.216174] | 0.215636 [0.205634, 0.227845] | 0.131335 [0.127314, 0.152458] |

The recorded complete workload improves 1.60x over stock and 1.64x over main by median.
Submission, startup, JIT and both transposes remain included; this does not establish
sustained throughput or a universal gain. [All raw trials, checksums and allocation probes](results/integral-truncate-positions-2026-10-28.json)
retain variability and the exact configuration. Generic Calc/perimeter costs remain in
[#491](https://github.com/datafusion-contrib/StreamFusion/issues/491).

Use the shared release example and complete-job fixture from ROUND, selecting
`SF_INTEGRAL_ROUNDING_FUNCTION=TRUNCATE` / `rounding.pipeline.function=TRUNCATE`,
with the corresponding position setting. Without a position setting the historical
single-argument mode is unchanged. The previous mode requires its actual encoder
first on the classpath.

The final combined regression passes 396 JVM test invocations across 13 classes,
Flink 1.18.1/2.2.1 and JDK 11/17, plus all 835 release Rust core tests. Fresh all-core
production compilation targets Java 11 on both lines. TRUNCATE parity covers all 77
positions, declared nullability, typed NULL positions, signed extrema, exact BIGINTs,
scalar consumers, filters and ROUND/TRUNCATE nesting. INT/BIGINT witnesses verify exact
outputs and consumed native work over 5,003 rows per type with no evaluator or fallback.
Selected/unselected failures, empty batches, extreme-position Underflow and narrow-input
compilation behavior remain checked. Unchanged released upstream `testRound` and
`testTruncate` pass on both lines; those supply oracle evidence, while the focused
witnesses establish execution of the native kernels. Full JVM/upstream suites remain CI checks.

After sharing the ROUND/TRUNCATE input handling, independent ROUND INT/-1 and BIGINT/-19
1,024-row controls recheck output/schema/checksums and timing against stock and actual main.
Their full measured trials are included as `shared_round_kernel_rechecks` in the TRUNCATE
raw record; the original 40-case ROUND matrix records the preceding ROUND-only implementation.

## DECIMAL to STRING

The direct-input `CAST(d AS STRING)` comparison uses released Flink 1.18.1 and 2.2.1,
JDK 17, one thread and a release/mimalloc native build. Stock Flink starts with prepared
GenericRowData or BinaryRowData; StreamFusion starts with the same logical decimals in
prepared Arrow Decimal128 buffers. Setup and input preparation are outside both timers.
Stock uses the released generated expression and consumes UTF-8 result lengths; native
times the production Calc and the same length checksum inside Rust. Output allocation and
the previous implementation's JVM callbacks remain inside the timer. Perimeter JNI entry,
export and row/Arrow transposes are excluded from this small-functionality comparison.

Inputs cover DECIMAL(18,3), DECIMAL(38,0) and DECIMAL(38,38), with signed varying coefficients,
maxima, zero, coefficients above 2^53 and optional NULL every seven rows. Batch sizes are
16 and 1,024 rows. Each independent JVM runs 16 warmups and nine measured trials, alternating
engine order. Each timed trial processes 131,072 rows. The previous control loads the actual
preceding encoder from `2441db79` first on the classpath and verifies its JVM expression
binding; it uses the unchanged callback implementation in the same release engine build.
Candidate and previous controls each retain their paired stock trials. Benchmarks run serially.

On Apple M4 Pro with JDK 17.0.12, the measured median ranges are:

| Flink line | Batch rows | Flink / native | Previous StreamFusion / native |
| --- | ---: | ---: | ---: |
| 1.18.1 | 16 | 1.008–2.362x | 19.776–23.101x |
| 1.18.1 | 1,024 | 2.861–6.438x | 5.725–8.238x |
| 2.2.1 | 16 | 1.087–1.964x | 20.088–22.412x |
| 2.2.1 | 1,024 | 2.816–6.363x | 5.743–8.234x |

The near-1x small-batch observation is within timing variability, not evidence of a reliable
speedup. Separate-JVM stock controls drift between 0.786x and 1.380x, so previous/candidate
ratios do not isolate only the implementation change. The [prepared raw trials](results/decimal-string-2026-10-29.csv)
retain every warmup/sample, paired stock control and checksum. The
[result record](results/decimal-string-2026-10-29.json) retains min/median/max for every
configuration, the release DSO hash and all complete-job trials.

The separate bounded job compares one million nullable DECIMAL(18,3) rows at parallelism 1,
with a rowwise sequence/map source and blackhole STRING sink. Two warmups and five measured
trials alternate engine order. Planning, startup, production JNI and both perimeter transposes
remain included. Each native job must consume NativeCalc rows, and its plan and evaluator
descriptors must match the expected native or previous JVM route.

| Flink line | Candidate's stock (s) | Native (s) | Previous's stock (s) | Previous StreamFusion (s) |
| --- | ---: | ---: | ---: | ---: |
| 1.18.1 | 0.205514 | 0.174687 | 0.206497 | 0.237319 |
| 2.2.1 | 0.203710 | 0.176602 | 0.198950 | 0.232253 |

Native job medians improve by 15.0% and 13.3% versus their paired stock controls, and 26.4%
and 24.0% versus previous StreamFusion. These subsecond bounded jobs include startup and
do not establish sustained throughput for other pipelines or configurations.

The released DecimalData oracle checks 28,823 boundary and deterministic random values across
every precision 1–38 and every valid scale against released DecimalData's plain formatting.
The focused SQL matrix additionally checks resolved types, legacy/modern mode, consumers,
nullable runtime inputs, unselected failures, empty filtered results and retained bounded and
computed-DECIMAL paths. The unchanged released DecimalTypeTest literal, field-access and
CASE fixtures supplement that oracle; their local expression tests are not native-work evidence.
The runtime SQL tests separately require native execution and empty JVM evaluator descriptors.
All 352 focused JVM invocations pass across Flink 1.18.1/2.2.1 and JDK 11/17 with no skips;
855 release Rust core tests also pass. Each of the four version/JDK oracle runs matches all
28,823 values. This does not replace the full upstream integration suite or CI.

After compiling `DecimalStringBenchmarkFunctions`, use the released test/runtime classpath as
`SF_NATIVE_BENCH_CLASSPATH` and the release library directory as `SF_NATIVE_LIB_DIR`:

```sh
cargo build --manifest-path native/Cargo.toml --release --locked -p streamfusion \
  --no-default-features --features mimalloc --example decimal_string
native/target/release/examples/decimal_string 18 3 true false 1024 128 16 9
java -cp "$SF_NATIVE_BENCH_CLASSPATH" tech.streamfusion.planner.DecimalStringBenchmarkFunctions > oracle.csv
native/target/release/examples/decimal_string --oracle oracle.csv
```

Arguments are precision, scale, NULL values, BinaryRowData stock input, batch rows, loops,
warmups and measured trials. The previous control also sets `SF_DECIMAL_STRING_PREVIOUS=true`
and prepends the independently compiled preceding encoder to the classpath. Complete jobs use
`DecimalStringPipelineBenchmark`, `SF_BENCHMARK=true` and the corresponding
`sf.decimal.string.previous` property/classpath pair.

## Binary STARTSWITH

Released Flink 2.2.1, Oracle JDK 17.0.12 and a release/mimalloc engine are compared on an
Apple M4 Pro, at one thread. Stock executes the released generated expression over prepared
GenericRowData or BinaryRowData. Native executes the production Calc over equivalent prepared
Arrow Binary or FixedSizeBinary inputs. Both consume a Boolean checksum including NULLs.
Inputs, compilation and setup are outside the timers; native timing starts and stops inside
Rust. Output allocation, validity work and the previous implementation's JVM callback remain
inside timing. Initial JNI entry, Arrow export and row/Arrow transposes are excluded only from
this prepared-function gate.

The 40 configurations cover 16/1,024-row batches, widths 8/64/4,096, fixed/variable bytes,
Generic/BinaryRowData and NULL/non-NULL inputs. Needles include matching and non-matching bytes,
empty needles and needles longer than the input. Fixed-width needles contain four bytes;
padding is retained. Each independent JVM alternates engine order for 24 warmups and nine
measured trials, processing 131,072 rows per trial. Previous controls prepend the independently
compiled encoder from canonical main `3af56f9a`, assert its JVM binding and retain that callback
inside the same production Calc. They each retain their own stock control. All runs are serial.

Ranges of medians across the matching configurations, in nanoseconds per row:

| Stock input | Batch rows | Stock Flink | Previous StreamFusion | Native |
| --- | ---: | ---: | ---: | ---: |
| GenericRowData | 16 | 1.521–2.047 | 525.025–776.607 | 19.858–29.297 |
| GenericRowData | 1,024 | 2.043–3.137 | 36.469–220.812 | 3.028–4.568 |
| BinaryRowData | 16 | 10.491–183.693 | 554.351–839.167 | 19.851–29.036 |
| BinaryRowData | 1,024 | 15.052–190.540 | 37.113–213.782 | 3.033–4.622 |

For 1,024-row BinaryRowData inputs, matching stock/native ratios are 3.878–62.816x and
previous/native ratios are 8.338–70.478x. The stock generated binary getter copies bytes from
BinaryRowData; native compares existing Arrow buffers. GenericRowData already holds byte
arrays, so its cheap getter avoids that copying: those controls still beat native, whose
stock/native ratios are 0.474–0.829x at 1,024 rows. Sixteen-row native batches also lose to
stock for all GenericRowData and several BinaryRowData cases. This is a qualified bulk binary
comparison improvement, not a universal scalar or complete-job acceleration claim.

The [raw trials](results/binary-starts-2026-11-01.csv) distinguish each control and include all
warmups, samples and checksums. The [record](results/binary-starts-2026-11-01.json) includes
per-case min/median/max and the release library SHA-256.

The separate million-row job uses nullable 64-byte inputs, parallelism 1, a rowwise sequence/map
source and a blackhole BOOLEAN sink. Two warmups and five measured trials alternate engine
order. JNI, planning/startup and both transposes remain included. Native operators must consume
rows, and evaluator descriptors must match the intended native or previous JVM route.

| Job control | Stock median (min–max), seconds | StreamFusion median (min–max), seconds |
| --- | ---: | ---: |
| Candidate | 0.170384 (0.162449–0.181901) | 0.187063 (0.185032–0.198267) |
| Previous | 0.166628 (0.157360–0.177254) | 0.223773 (0.215104–0.239078) |

Native improves 16.4% over previous StreamFusion but takes 9.8% longer than its paired stock
control. Separate-JVM stock medians drift 2.3%; these short bounded jobs do not establish
sustained throughput. [All complete-job trials](results/binary-starts-pipeline-2026-11-01.json)
retain the unfavorable results as well as the gains over the callback.

After compiling the test helper, set `JAVA_HOME` to the supported JDK and provide the matching
runtime/test classpath through `SF_NATIVE_BENCH_CLASSPATH`, plus `SF_NATIVE_LIB_DIR`:

```sh
cargo build --manifest-path native/Cargo.toml --release --locked -p streamfusion \
  --features mimalloc --lib --example binary_boundary
native/target/release/examples/binary_boundary 64 false true true 1024 128 24 9
```

Arguments are width, fixed-width input, nullable input, BinaryRowData stock input, batch rows,
loops, warmups and measured trials. Previous controls additionally set
`SF_BINARY_BOUNDARY_PREVIOUS=true` and prepend the actual preceding encoder's compiled classes.
An optional final `ends` argument selects ENDSWITH; the default remains STARTSWITH.
Complete jobs use `BinaryBoundaryPipelineBenchmark` with `SF_BENCHMARK=true`; its test-only
`sf.binary.boundary.previous` property must agree with that classpath. Set
`sf.binary.boundary.suffix=true` for the suffix workload.

## Binary ENDSWITH

The suffix comparison uses the same released Flink 2.2.1/JDK 17.0.12, release/mimalloc,
single-thread machine, input matrix, checksums and timer boundaries as the binary STARTSWITH
comparison. A separate 40-configuration matrix compares the shared suffix kernel with the
released generated expression and the actual preceding encoder from `3af56f9a`. Matching
needles are drawn from the tail of the input. Arbitrary non-text bytes, NULLs, mismatches,
empty/long needles and fixed padding remain represented. Each trial processes 131,072 rows;
24 warmups and nine measured trials alternate engine order. Benchmarks run serially.

Ranges of medians, in nanoseconds per row:

| Stock input | Batch rows | Stock Flink | Previous StreamFusion | Native |
| --- | ---: | ---: | ---: | ---: |
| GenericRowData | 16 | 1.971–2.325 | 534.839–813.729 | 19.512–27.349 |
| GenericRowData | 1,024 | 2.515–4.505 | 35.695–215.287 | 3.015–4.722 |
| BinaryRowData | 16 | 11.218–194.367 | 535.303–804.324 | 19.411–26.987 |
| BinaryRowData | 1,024 | 14.347–196.462 | 35.084–214.879 | 3.100–4.623 |

At 1,024 rows with BinaryRowData stock inputs, native improves 3.677–62.521x over stock
and 8.218–68.381x over previous StreamFusion. GenericRowData stock/native ratios range
from 0.571 to 1.223x: its existing byte arrays avoid the generated BinaryRowData getter's
copying, and most of those cheap controls still beat native. Sixteen-row controls also
retain Calc setup and output-allocation costs. These results establish a bulk binary
comparison improvement, not a universal speedup across input representations or batch sizes.

[Raw prepared trials](results/binary-ends-2026-11-01.csv) retain every control, warmup,
sample and checksum. The [result record](results/binary-ends-2026-11-01.json) includes
all min/median/max values and the release library SHA-256. Use the shared `binary_boundary`
example with a final `ends` argument to reproduce the suffix comparison.

The corresponding million-row job retains the same nullable 64-byte source, blackhole sink,
parallelism 1, both transposes and JNI as the prefix diagnostic. Two warmups and five measured
trials alternate engines; consumed native work and the exact evaluator route are required.

| Job control | Stock median (min–max), seconds | StreamFusion median (min–max), seconds |
| --- | ---: | ---: |
| Candidate | 0.177847 (0.163777–0.195478) | 0.198512 (0.193328–0.208405) |
| Previous | 0.180443 (0.172684–0.201166) | 0.237657 (0.226938–0.247604) |

Native takes 16.5% less time than previous StreamFusion, but 11.6% more than its paired stock
control. Separate-JVM stock medians drift 1.5%; the diagnostic includes startup and does not
establish sustained throughput. [All complete-job trials](results/binary-ends-pipeline-2026-11-01.json)
retain these limits. The shared-kernel change also rechecks representative STARTSWITH prepared
configurations; those trials are included in the suffix result record.

On Flink 2.2.1, the focused prefix/suffix and adjacent STRING/BOOLEAN runtime suites pass
42 tests each on JDK 11 and 17. They include arbitrary bytes, fixed casts, filters, conditional
branches and empty filtered batches. Separate 5,004-row witnesses consume native Calc output
without JVM evaluator bindings or fallback. The Rust core suite without default features
passes 865 tests, including sliced value/validity buffers, all 256 byte values, empty arrays,
scalar NULLs and byte-for-byte comparison oracles. The default-feature release suite also
passes 1,004 tests with two existing ignores. CI supplies the broader version matrix.

## Binary to STRING

The 2026-11-01 comparison uses Apple M4 Pro / 24 GiB, released Flink 1.18.1 and
2.2.1, JDK 17.0.12 and a Rust 1.95.0 release/mimalloc build with default features.
JDK 11 correctness is checked separately. Stock executes the released generated
cast over prepared GenericRowData or BinaryRowData. Native executes the complete
production Calc over equivalent prepared Arrow Binary or FixedSizeBinary batches;
the timer starts and stops inside Rust. Setup, initial JNI entry, final export and
perimeter transposes are outside this small-functionality timer. Allocation and
all conversions intrinsic to the expression remain inside it.

Both timers consume NULL as -1, empty as zero, otherwise the first UTF-16 unit plus
one. The cast still computes its complete result. Complete outputs are separately
verified against released generated casts over 90,404 deterministic inputs per
Flink/JDK pair. Earlier full-length consumption added a UTF-16 scan on Arrow that
Java String.length does not perform; those diagnostics are retained separately,
not pooled with the final results.

The actual preceding encoder from main `3af56f9a` is independently compiled and
loaded first for the previous control. It must reject the binary cast, so its
prepared operation is the resulting generated Flink fallback, rather than a
simulated JVM callback. Each candidate JVM alternates stock/native order; the
previous control runs in a separate JVM with the same values and resources.
All runs are serial, with no concurrent builds or tests, 256/512 MiB initial/max
heap, 16 warmups, nine measured trials and 65,536 processed rows per trial.

The 71 configurations include widths 8/64/4096, batches 16/1024, fixed/variable
bytes, both stock RowData forms, NULL/non-NULL controls, ASCII, supplementary/CJK
text, malformed surrogate tails, mixed tails and dense deterministic arbitrary
bytes. NULL appears every seventh row when enabled. Flink 2.2 has the full
55-configuration matrix; Flink 1.18 has 16 representative controls including
dense invalid input. At 1,024 rows with BinaryRowData:

| Flink | Stock / native median | Previous fallback / native median |
| --- | ---: | ---: |
| 1.18.1 | 1.225-27.153x | 1.206-26.926x |
| 2.2.1 | 1.220-27.143x | 1.181-26.864x |

Selected variable-width nullable inputs, ns/row, median [minimum-maximum]:

| Flink | Bytes / content | Stock | Previous fallback | Native |
| --- | --- | ---: | ---: | ---: |
| 2.2.1 | 8 / ASCII | 18.90 [18.69-19.16] | 17.46 [16.40-18.01] | 1.98 [1.92-2.08] |
| 2.2.1 | 64 / supplementary+CJK | 55.18 [50.66-91.12] | 54.81 [53.80-84.05] | 21.44 [21.19-27.80] |
| 2.2.1 | 4096 / malformed tail | 3541.72 [3488.84-3632.33] | 3505.34 [3437.55-3646.49] | 130.49 [126.02-131.81] |
| 2.2.1 | 4096 / dense arbitrary | 3187.90 [3090.52-3303.64] | 3085.71 [3070.70-3224.00] | 2612.54 [2536.02-2682.08] |
| 1.18.1 | 4096 / supplementary+CJK | 2202.27 [2169.05-2249.56] | 2182.91 [2153.31-2273.44] | 1264.74 [1201.06-1324.22] |

Small batches remain a limit. Across the Flink 2.2 16-row controls, stock/native
median ratios range from 0.504x to 20.828x, and previous/native from 0.528x to
20.673x. One 1,024-row GenericRowData case also loses to its independent previous
fallback control despite beating paired stock. Independent stock/fallback control
medians drift by factors 0.625-1.736. Retain both controls and their variability;
these measurements do not isolate a universal throughput improvement.

The [complete record](results/binary-character-2026-11-01.json) includes all
configuration ranges, the production library hash, validation and full-job trials.
[All prepared trials](results/binary-character-2026-11-01.csv.gz) retain warmups
and checksums. The implementation shares valid buffers, copies validated prefixes,
writes directly into Arrow and batches independent invalid-byte replacements;
see the [decoder technique](../optimizations/jdk-utf8-decoding.md).

### Complete row-source jobs

Each Flink line also runs a one-million-row job at parallelism one, with 64-byte
inputs, a malformed tail every seventh row and independent NULL every eleventh
row. A rowwise sequence/map source feeds the cast and a blackhole STRING sink.
Planning, startup, production JNI and both RowData/Arrow transposes stay timed.
The candidate must show both transposes, no evaluator/fallback and consumed
NativeCalc work; the old encoder must show stock fallback and zero native work.
Each independent JVM uses the same 2 GiB maximum heap, two warmups and five
measured jobs with alternating engine order. Seconds, median [minimum-maximum]:

| Flink | Candidate's stock | Native | Previous's stock | Previous fallback |
| --- | ---: | ---: | ---: | ---: |
| 2.2.1 | 0.228064 [0.225555-0.241543] | 0.210187 [0.209547-0.220651] | 0.213727 [0.206077-0.250102] | 0.212407 [0.206951-0.222358] |
| 1.18.1 | 0.239151 [0.219020-0.254718] | 0.204987 [0.202946-0.213079] | 0.212720 [0.205804-0.217317] | 0.213624 [0.204449-0.216186] |

Native is 7.8%/14.3% lower than paired stock medians, but the independent stock
controls drift and ranges overlap. Gains versus the actual preceding fallback
are only 1.0%/4.0%, also with overlapping ranges. These short jobs do not establish
a reliable whole-job speedup or sustained connector throughput.

### Profiling and validation

Earlier experiments are retained as diagnostics with their original consumption
method. The [initial partial matrix](results/binary-character-before-opt-2026-11-01.csv.gz),
[prefix-validation experiment](results/binary-character-opt-diag-2026-11-01.csv.gz)
and [UTF-8-chunks experiment](results/binary-character-chunks-diag-2026-11-01.csv.gz)
consume full UTF-16 length. Profiling first located most long-ASCII samples in
that extra result scan. After changing to constant output consumption, the
[prefix/manual decoder](results/binary-character-prefix-copy-diagnostic-2026-11-01.csv.gz)
still lost on dense invalid bytes. The
[direct-builder version](results/binary-character-direct-builder-diagnostic-2026-11-01.csv.gz)
measured 3412 ns/row versus stock 3164 for 4096-byte dense inputs. Batching
independent replacement writes reduces that profile to 2596 versus stock 3129
in the [final preflight](results/binary-character-final-diagnostic-2026-11-01.csv.gz).
Only the subsequent frozen 71-configuration matrix supplies the reported ranges.

Final validation passes 142 focused JVM test invocations across both released
lines and JDK 11/17, without skips, and 1004 release Rust core tests with two
existing ignores. Each of the four generated-expression oracles matches all
90,404 inputs, including every single/two-byte input, surrogate continuation
boundaries and deterministic longer arbitrary bytes. SQL coverage adds modern
and legacy modes, fixed padding, composed consumers, TRY_CAST operand failures,
lazy CASE, all-filtered batches, multi-batch ownership and retained fallbacks.
Separate fixed/variable witnesses consume 5,003 rows and require `jvm=[]` plus
positive native work. Full CI and unchanged upstream suites remain required.

### Reproduce

Compile the test helper with the released runtime/test classpath, then use JDK 17:

```sh
mvn -Pbench -pl streamfusion-runtime -am -Dnative.build.skip=true -DskipTests test-compile
mvn -pl streamfusion-runtime dependency:build-classpath -Dmdep.outputFile=target/binary-character.classpath
export SF_NATIVE_BENCH_CLASSPATH="$PWD/streamfusion-runtime/target/test-classes:$PWD/streamfusion-runtime/target/classes:$(cat streamfusion-runtime/target/binary-character.classpath)"
export SF_NATIVE_LIB_DIR="$PWD/native/target/release"
cargo build --manifest-path native/Cargo.toml --release --locked -p streamfusion \
  --features mimalloc --lib --example binary_character
native/target/release/examples/binary_character 64 4 false true true 1024 64 16 9
java -cp "$SF_NATIVE_BENCH_CLASSPATH" tech.streamfusion.planner.BinaryCharacterBenchmarkFunctions > oracle.csv
native/target/release/examples/binary_character --oracle oracle.csv
```

Arguments are byte width, content mode 0-4, fixed-width input, nullable values,
BinaryRowData stock input, batch rows, loops, warmups and measured trials. The
previous control prepends the independently compiled main encoder and sets
`SF_BINARY_CHARACTER_PREVIOUS=true`; it fails if that encoder admits the cast.
Use the `flink-1.18` Maven profile for the other released line. Complete jobs use
`BinaryCharacterPipelineBenchmark`, `SF_BENCHMARK=true` and the matching
`sf.binary.character.previous` property/classpath pair.

### Modern bounded CHAR/VARCHAR targets

Modern `CAST`/`TRY_CAST(binary AS VARCHAR(n)/CHAR(n))` extends the same kernel with
Flink's UTF-16 truncation and CHAR padding. The preceding encoder at `c0d1a509` rejects
these targets, so its control verifies that rejection and measures released Flink's
generated fallback. The prepared comparison uses released Flink 1.18.1/2.2.1 on JDK
17.0.12, Apple M4 Pro, one thread and a release/mimalloc build, with the binary-character
harness above: nullable inputs, GenericRowData/BinaryRowData, 16/1,024-row batches,
131,072 rows per trial, 16 warmups and nine measurements per engine. Targets are
VARCHAR(4), VARCHAR(64) and CHAR(16) over 8/64-byte ASCII, supplementary+CJK and
malformed-tail content, plus 16-byte fixed BINARY. The stock consumer maps the lone
high surrogate of a split pair to `?`, the character Flink writes for it.

Selected variable-width nullable BinaryRowData inputs at 1,024 rows, ns/row median:

| Flink | Target | Bytes / content | Stock | Previous fallback | Native |
| --- | --- | --- | ---: | ---: | ---: |
| 2.2.1 | VARCHAR(4) | 8 / ASCII | 18.42 | 18.56 | 5.10 |
| 2.2.1 | VARCHAR(4) | 64 / supplementary+CJK | 58.09 | 62.03 | 29.79 |
| 2.2.1 | VARCHAR(64) | 64 / ASCII | 15.02 | 14.09 | 3.48 |
| 2.2.1 | CHAR(16) | 8 / ASCII | 23.91 | 24.79 | 8.69 |
| 2.2.1 | CHAR(16) | 64 / supplementary+CJK | 59.35 | 57.26 | 38.37 |
| 1.18.1 | VARCHAR(4) | 8 / supplementary+CJK | 27.20 | 27.09 | 11.94 |
| 1.18.1 | CHAR(16) | 64 / ASCII | 25.19 | 23.85 | 5.96 |
| 1.18.1 | CHAR(16) | 64 / supplementary+CJK | 50.34 | 56.44 | 37.36 |

Across all 42 1,024-row configurations per line, stock/native median ratios are
1.357-8.168x on Flink 2.2.1 and 1.331-6.179x on 1.18.1; previous/native ratios are
1.370-8.457x and 1.198-7.167x. Supplementary and CJK text gains least. Sixteen-row
batches remain a limit: 30 of 36 such configurations on Flink 2.2.1 and 32 of 36 on
1.18.1 are slower than stock, with stock/native ratios down to 0.404x and 0.394x.
These results do not establish acceleration for small batches.

An [initial Flink 2.2.1 matrix](results/binary-character-bounded-before-opt-2026-11-02.csv.gz)
built every bounded row through a string builder and scanned whole rows for ASCII. Seven
of its 42 1,024-row configurations lost to stock (down to 0.818x), all truncating short
ASCII outputs. The measured version writes values and offsets directly, reuses input
validity and checks only the truncated prefix for ASCII. [All final trials](results/binary-character-bounded-2026-11-02.csv.gz)
retain warmups, checksums and the target column. Before timing, the generated-expression
oracle matches all 90,404 inputs on both lines for STRING, VARCHAR(1), VARCHAR(2),
VARCHAR(5), CHAR(1), CHAR(3) and CHAR(8).

Complete one-million-row jobs reuse the row-source pipeline above, with a bounded
blackhole column of the same type. Candidate runs require both transposes, consumed
NativeCalc work and no evaluator or fallback. Two warmups and five measured jobs
alternate engine order in one JVM per target. Seconds, median [minimum-maximum]:

| Flink | Target | Stock | Native |
| --- | --- | ---: | ---: |
| 2.2.1 | VARCHAR(16) | 0.231831 [0.229030-0.237508] | 0.207224 [0.200973-0.220357] |
| 2.2.1 | CHAR(16) | 0.239948 [0.232398-0.260705] | 0.212151 [0.203249-0.215048] |
| 1.18.1 | VARCHAR(16) | 0.235405 [0.223152-0.248219] | 0.211334 [0.201674-0.211897] |
| 1.18.1 | CHAR(16) | 0.237634 [0.235295-0.243123] | 0.202265 [0.198970-0.212588] |

Native medians are 10.2-14.9% lower. The previous encoder falls back to the same
generated stock path, so these jobs have no separate previous column. They are short and
use one stock control per target, so they do not establish sustained connector
throughput.

Reproduce with the commands above, setting `SF_BINARY_CHARACTER_TARGET`, for example
`VARCHAR(4)` or `CHAR(16)`, for the prepared runs and oracle. The Java oracle also reads
`-Dsf.binary.character.target`. Complete jobs pass the target with
`-Dsf.extraJvmArgs="-Dsf.binary.character.target=CHAR(16) -Xmx2g"`.

### Latest-main bounded-cast requalification (2026-11-03)

The [fresh record](results/binary-character-bounded-2026-11-03.json) repeats the
prepared comparison on canonical main `5d628b6a` with the same final kernel and
released lines, using release/mimalloc for both the executable and JNI DSO
(verified from their allocator symbols). The actual preceding main encoder is
compiled independently against the current runtime and fixture. This isolates
admission/kernel changes, not an entire historical library replay.

Twelve 1,024-row configurations per line cover VARCHAR(4), VARCHAR(64), CHAR(16),
ASCII, supplementary/CJK, malformed tails, dense arbitrary bytes, NULLs and a
non-null fixed-BINARY GenericRowData control. Stock/native median ratios are
1.107-5.306x on 2.2 and 1.105-6.429x on 1.18; previous/native ratios are
1.111-4.950x and 1.194-5.524x. All 24 bulk cells improve against both controls.
Four of eight 16-row controls lose to stock; the lowest stock/native ratio is
0.705x. [All trials](results/binary-character-bounded-2026-11-03.csv.gz) retain
warmups, checksums and those slower cells.

Complete-job stock/native medians, seconds, with five measured jobs per engine:

| Flink | Target | Paired stock | Native | Independent stock | Previous fallback |
| --- | --- | ---: | ---: | ---: | ---: |
| 2.2.1 | VARCHAR(16) | 0.234476 | 0.211839 | 0.211336 | 0.213190 |
| 2.2.1 | CHAR(16) | 0.231994 | 0.209202 | 0.216906 | 0.214053 |
| 1.18.1 | VARCHAR(16) | 0.247202 | 0.210752 | 0.211955 | 0.209260 |
| 1.18.1 | CHAR(16) | 0.233629 | 0.208619 | 0.205848 | 0.209841 |

All four candidate medians beat paired stock, but independent stock/fallback
controls drift. One native median loses to previous fallback and measured ranges
overlap. These short jobs do not establish a reliable full-job throughput win.
The linked JSON retains all complete-job trials and configuration ranges.

Fresh validation passes 292 focused JVM invocations across Flink 1.18.1/2.2.1
and JDK 11/17, targeting Java release 11. Twelve existing fixed-BINARY controls
abort on Flink 1.18 under their version assumptions; the bounded-cast tests all
execute. Twenty-eight generated-expression oracle runs each match 90,404 complete
outputs, covering seven targets across all four release/JDK pairs. The Rust core
suite passes 892 tests without ignores. Runtime witnesses consume 5,003 rows and
require empty evaluator descriptors plus consumed NativeCalc work; lazy branches,
all-filtered failures, operand exceptions and represented consumers have explicit
SQL regressions. Full hosted CI remains a separate check.

## DECIMAL to STRING TRY_CAST

Direct-input `TRY_CAST(d AS STRING)` reuses the qualified native formatter above. Its
previous encoder at `3af56f9a` rejects this signature, so the previous control verifies that
rejection and measures released Flink's generated fallback; it does not substitute a JVM
callback that the old query would never execute. Modern and legacy modes share the same
plain formatting. Bounded targets and computed DECIMAL operands retain their existing rules.

The prepared comparison uses released Flink 1.18.1/2.2.1 on JDK 17.0.12, Apple M4 Pro,
one thread, and a release/default-feature/mimalloc build. It retains the preceding comparison's
RowData and Arrow timer boundaries, UTF-8 length checksum, three DECIMAL types, signed
coefficients, optional NULLs, GenericRowData/BinaryRowData and 16/1,024-row batches. Each
of the 48 configurations processes 131,072 rows per trial, with 16 warmups and nine
measurements per engine. Separate candidate/previous processes retain both stock controls;
all timings run serially. Median ratio ranges are:

| Flink line | Batch rows | Stock / native | Previous fallback / native |
| --- | ---: | ---: | ---: |
| 1.18.1 | 16 | 1.084–2.549x | 1.215–3.082x |
| 1.18.1 | 1,024 | 3.103–7.309x | 3.451–7.837x |
| 2.2.1 | 16 | 1.089–2.716x | 1.082–3.022x |
| 2.2.1 | 1,024 | 2.830–7.505x | 3.104–7.949x |

The small-batch results closest to 1x do not establish a reliable speedup. Independent
stock controls drift by 0.994–1.491x, so the previous/candidate ratios also contain run
variation. The [raw trials](results/decimal-try-string-2026-11-01.csv.gz) keep every warmup,
measurement and checksum; the [result record](results/decimal-try-string-2026-11-01.json)
includes per-configuration min/median/max, build hash and complete-job samples.

The separate complete job processes one million nullable DECIMAL(18,3) rows with a rowwise
sequence/map source and blackhole STRING sink at parallelism one. Two warmups and five
measurements alternate paired engines. Planning, startup, production JNI and both candidate
perimeter transposes remain included. Candidate plans require consumed NativeCalc work and
`jvm=[]`; previous plans require the actual unsupported-TRY_CAST fallback and no native work.

| Flink line | Candidate stock median (range), s | Native median (range), s | Previous stock median (range), s | Previous fallback median (range), s |
| --- | ---: | ---: | ---: | ---: |
| 1.18.1 | 0.210033 (0.198174–0.341704) | 0.178884 (0.175655–0.193307) | 0.173271 (0.171626–0.178455) | 0.176192 (0.171641–0.181437) |
| 2.2.1 | 0.200191 (0.189743–0.214374) | 0.176218 (0.172520–0.180682) | 0.180489 (0.175176–0.203107) | 0.171967 (0.163684–0.176673) |

Candidate medians improve over their paired stock controls, but the previous fallback is
1.5%/2.5% faster than native. The independent stock drift, overlapping ranges and subsecond
startup cost prevent a reliable complete-job improvement claim. These observations do not
establish sustained throughput for other jobs.

The focused suite passes 91 tests on each released line/JDK 11/17 pair, 364 invocations
with no skips. It retains the CAST regressions and adds TRY_CAST to the all-precision/scale
encoding checks, runtime multi-batch witnesses, consumers, NULLs, lazy branches and empty
filtered results. Computing a failing operand still fails; bounded TRY_CAST remains fallback.
The unchanged formatting kernel keeps its preceding released-DecimalData oracle coverage;
this admission change does not replace the full upstream integration suite or CI.

Use the preceding `decimal_string` commands with `SF_DECIMAL_STRING_TRY_CAST=true`.
For the old control also set `SF_DECIMAL_STRING_PREVIOUS=true` and prepend an independently
compiled `3af56f9a` encoder. Complete jobs set `SF_BENCHMARK=true` and
`-Dsf.decimal.string.tryCast=true`, plus the matching previous property/classpath when required.

## STRING IS_ALPHA

The native classifier uses the actual worker JDK's immutable BMP letter bitmap. It
copies 8 KiB once during expression compilation, outside the prepared-operation timer;
batch and row execution do not call Java. ASCII letters take a byte fast path. The
UTF-16-char contract rejects supplementary letters, combining marks and isolated
surrogates, and returns nonnullable FALSE for NULL or empty strings.

The original alpha matrix uses nullable generated Java/encoder signatures for both data
populations; its nullable flag selects whether rows contain NULL. It does not establish
the cost of a declared NOT NULL signature. The separate
[matching NOT NULL controls](#declared-not-null-classification-controls) below qualify that
signature explicitly. Original trials remain available with their source-commit metadata.

Prepared controls use released Flink 1.18.1/2.2.1, JDK 17.0.12, Apple M4 Pro, one thread
and a release/default-feature/mimalloc native build. Flink starts with prepared
GenericRowData or BinaryRowData and times its released generated expression in Java.
StreamFusion starts with equivalent Arrow strings and times the production Calc and a
TRUE-count checksum inside Rust. Both engines consume the same logical Boolean results.
Perimeter JNI/import/export and row/Arrow transposes are outside this small-operation
timer; output allocation and previous behavior's intrinsic JVM callback remain inside.

The 96 configurations cover ASCII letter bodies of 16/128 characters, mixed ASCII,
BMP letters, mixed Unicode and supplementary-letter rejection with 16-character bodies,
varying prefixes and optional NULL every seven rows. Each independently prepared JVM
runs 16 warmups and nine measurements, 131,072 rows per trial, with alternating paired
engines. The previous control prepends the actual `3af56f9a` encoder and requires its
generated evaluator binding. Candidate and previous processes retain separate stock
controls. All measurements run serially.

| Flink line | Batch rows | Stock / native median range | Previous / native median range |
| --- | ---: | ---: | ---: |
| 1.18.1 | 16 | 0.612–2.389x | 9.915–31.988x |
| 1.18.1 | 1,024 | 1.321–13.424x | 3.189–25.839x |
| 2.2.1 | 16 | 0.628–2.359x | 10.343–33.583x |
| 2.2.1 | 1,024 | 1.312–12.425x | 3.058–26.485x |

All 1,024-row cells improve over their paired stock controls, including cached
GenericRowData. Twenty-three 16-row cells lose to stock: small batches and early-rejecting
strings expose fixed Calc/output costs. These losses remain in the
[raw trials](results/string-alpha-2026-11-01.csv.gz) and
[result record](results/string-alpha-2026-11-01.json). Independent stock controls drift
by 0.761–1.774x; previous/candidate ratios include that variability. The strongest
supplementary-string ratios do not describe the cost of scanning an entire letter string.

The separate complete job processes one million nullable mixed-Unicode strings with a
rowwise sequence/map source and BOOLEAN NOT NULL blackhole sink at parallelism one.
Two warmups and five measurements alternate paired engines. The measured job retains
planning, startup, production JNI and both perimeter transposes. Candidate and previous
plans both require consumed NativeCalc work, with empty versus present JVM evaluator
descriptors respectively.

| Flink line | Candidate stock median (range), s | Native median (range), s | Previous stock median (range), s | Previous median (range), s |
| --- | ---: | ---: | ---: | ---: |
| 1.18.1 | 0.209614 (0.204382–0.218415) | 0.213378 (0.205885–0.222676) | 0.218010 (0.205615–0.228056) | 0.334182 (0.320680–0.358193) |
| 2.2.1 | 0.233453 (0.226726–0.262184) | 0.213483 (0.209738–0.218223) | 0.220989 (0.211911–0.226113) | 0.315859 (0.307981–0.323943) |

The 2.2 native median improves 8.6% over its paired stock and 32.4% over previous
StreamFusion. The 1.18 native median is 1.8% slower than paired stock, with overlapping
ranges, while improving 36.2% over previous StreamFusion. These startup-heavy bounded
jobs do not establish a universal or sustained whole-job speedup.

Focused SQL checks every BMP unit, supplementary letters, NULL/empty inputs, CHAR casts,
consumers, filters, CASE, grouping, selected failures and empty filtered batches on each
released Flink/JDK 11/17 pair. The multi-batch witness checks every input ID exactly once,
positive native work and `jvm=[]`. Released upstream `ScalarFunctionsTest.testIsAlpha`
assertions also run unchanged on all four pairs; those local expression tests are not
native-work evidence. Noncharacter and sensitive-expression evaluator rules remain separate.

After compiling `StringClassificationBenchmarkFunctions`, supply the corresponding released
runtime/test classpath as `SF_NATIVE_BENCH_CLASSPATH` and release library directory as
`SF_NATIVE_LIB_DIR`:

```sh
cargo build --manifest-path native/Cargo.toml --release --locked -p streamfusion \
  --features mimalloc --lib --example string_classification
native/target/release/examples/string_classification unicode 16 true true 1024 128 16 9
```

Arguments are corpus, body characters, NULL inputs, BinaryRowData stock input, batch rows,
loops, warmups and measured trials. Available corpora are `ascii`, `mixed`, `bmp`, `unicode`
and `supplementary`. The old control also sets `SF_CLASSIFICATION_PREVIOUS=true` and prepends
an independently compiled `3af56f9a` encoder. Complete jobs use
`StringClassificationPipelineBenchmark`, `SF_BENCHMARK=true` and the matching
`sf.classification.previous` property/classpath pair.

## STRING IS_DIGIT

The digit classifier uses the same worker-JDK snapshot lifecycle and Arrow/RowData timing
boundaries as IS_ALPHA. A separate 8 KiB BMP digit bitmap preserves `Character.isDigit(char)`;
NULL/empty strings, supplementary digits, signs, decimal points, superscripts and Roman
numerals return nonnullable FALSE. ASCII decimal digits have a specialized byte loop.
There are no batch/row JVM calls in the classifier.

As in the original alpha matrix, generated Java/encoder signatures are nullable in all
original digit cells; the flag selects data NULLs. See the separate
[declared NOT NULL controls](#declared-not-null-classification-controls) for that signature.

The prepared controls cover ASCII digit bodies of 16/128 characters, mixed ASCII, BMP
digits, mixed Unicode digits and supplementary-digit rejection with 16-character bodies,
varying prefixes, NULL every seven rows, GenericRowData/BinaryRowData and 16/1,024-row
batches. Flink 1.18.1/2.2.1, JDK 17.0.12, Apple M4 Pro, one thread and a release/default-feature/
mimalloc build match the alpha controls. Every trial consumes 131,072 Boolean results,
with 16 warmups and nine measurements, alternating paired engines and retaining both
independent stock controls. The previous process uses the actual `3af56f9a` encoder and
its generated JVM callback.

| Flink line | Batch rows | Stock / native median range | Previous / native median range |
| --- | ---: | ---: | ---: |
| 1.18.1 | 16 | 0.527–3.017x | 10.051–32.780x |
| 1.18.1 | 1,024 | 1.160–13.203x | 2.605–25.935x |
| 2.2.1 | 16 | 0.530–2.556x | 9.431–31.182x |
| 2.2.1 | 1,024 | 1.172–12.236x | 2.471–25.662x |

Every 1,024-row configuration improves over paired stock, including cached GenericRowData.
Twenty-three 16-row cells lose to stock: fixed Calc/output costs remain visible on tiny
batches. Independent stock controls drift by 0.710–1.553x. Supplementary-digit rejection
does not measure the cost of scanning a full digit string. The
[raw trials](results/string-digit-2026-11-01.csv.gz) and
[result record](results/string-digit-2026-11-01.json) retain these limitations and all losses.

The separate one-million-row jobs use nullable mixed Unicode digits, a rowwise source and
BOOLEAN NOT NULL blackhole, parallelism one, two warmups and five measurements. Production
JNI, planning/startup and both transposes stay in the job path. Both candidates require
consumed NativeCalc work; only the candidate requires empty JVM evaluator descriptors.

| Flink line | Candidate stock median (range), s | Native median (range), s | Previous stock median (range), s | Previous median (range), s |
| --- | ---: | ---: | ---: | ---: |
| 1.18.1 | 0.208241 (0.206801–0.219251) | 0.214863 (0.209152–0.225335) | 0.207409 (0.202933–0.213170) | 0.331346 (0.322421–0.333293) |
| 2.2.1 | 0.220444 (0.216505–0.229146) | 0.217703 (0.213208–0.220951) | 0.214789 (0.209489–0.223645) | 0.315420 (0.306975–0.349680) |

The 2.2 native median is 1.2% faster than paired stock and 31.0% faster than previous
StreamFusion. The 1.18 median is 3.2% slower than paired stock and 35.2% faster than previous
StreamFusion. Stock/native ranges overlap on both lines. These startup-heavy jobs do not
establish a reliable whole-job win over stock or a universal sustained throughput gain.

Focused classification checks pass 56 JVM invocations across released Flink 1.18.1/2.2.1
and JDK 11/17, including both alpha/digit kernels after shared initialization was factored.
Every BMP unit, supplementary characters, NULL/empty inputs, CHAR casts, consumers, grouping,
lazy and selected failures, and multi-batch IDs are checked against stock. The unchanged
released `ScalarFunctionsTest.testIsDigit` also passes on all four pairs; these local
expression tests do not establish native work. Default-feature release/mimalloc Rust core:
1,004 pass, two pre-existing ignores.

Eight 1,024-row alpha controls after sharing initialization cover ASCII 16/128, nullable
mixed Unicode BinaryRowData and BMP GenericRowData on both lines. All remain faster than
paired stock, by 1.350–4.314x. Their summaries are in the digit result record and
[raw trials](results/string-alpha-shared-2026-11-01.csv.gz) retain the separate controls.

Reproduce with the IS_ALPHA commands and arguments, additionally setting
`SF_CLASSIFICATION_DIGIT=true` for the native example. For the complete-job launcher set
`-Dsf.classification.digit=true` instead. Both are benchmark controls, not production switches.
The old control keeps its matching `SF_CLASSIFICATION_PREVIOUS=true` or
`-Dsf.classification.previous=true` and independently compiled encoder classpath.

## STRING IS_DECIMAL

Flink's predicate tries Java integer, long and double parsing. Native ASCII lexical
classification therefore accepts signed integers/fractions, decimal/hexadecimal exponents,
floating suffixes, signed NaN/Infinity and magnitude overflow. It trims only U+0000..U+0020
on the double path. A separate worker-JDK snapshot supplies packed BMP radix-10 digit
values for Unicode integers, which require signed-long bounds and no trimming. No row/batch
Java parser, exception allocation or floating conversion remains in the classifier.

Controls use released Flink 1.18.1/2.2.1, JDK 17.0.12, Apple M4 Pro, one thread and a
release/default-feature/mimalloc build. The generated released RowData evaluator and
production Arrow Calc consume matching Boolean TRUE counts in their own runtimes. Native
timers are inside Rust after Arrow preparation and before export. Perimeter JNI and both
transposes are outside this small-operation timer; intrinsic previous JVM callbacks remain
inside. Candidate and actual `3af56f9a` encoder processes retain independent stock controls.

The 160 prepared configurations cover ASCII digit bodies of 1/6/16/128 characters, fractions,
decimal/hexadecimal exponents, mixed invalid/empty strings, BMP/Unicode digits, Unicode
integer overflow, special values and supplementary-digit rejection. Each has GenericRowData/
BinaryRowData and nullable/nonnullable controls at 1,024 rows. Six representative corpora also
have 16-row controls. Trials process 32,768 rows with 16 warmups and nine measurements,
alternating paired engines, with all timing jobs run serially.

| Flink line | Batch rows | Stock / native median range | Previous / native median range |
| --- | ---: | ---: | ---: |
| 1.18.1 | 16 | 0.653–54.114x | 35.620–81.912x |
| 1.18.1 | 1,024 | 2.310–229.729x | 10.439–256.494x |
| 2.2.1 | 16 | 1.369–59.370x | 28.012–84.348x |
| 2.2.1 | 1,024 | 3.261–203.293x | 9.438–215.964x |

All 1,024-row cells improve over paired stock, including cached GenericRowData and actual
NOT NULL declarations. Four 16-row 1.18 cells lose to stock; fixed Calc/output cost remains
visible on short integer strings. The strongest gains avoid released Flink's caught parser
exceptions for fractional, invalid or non-int numeric syntax; they do not describe arbitrary
numeric arithmetic or every string input. Independent stock controls drift by 0.423–2.257x,
and previous/native ratios include that variability. The
[result record](results/string-is-decimal-2026-11-01.json) and
[raw trials](results/string-is-decimal-2026-11-01.csv.gz) retain every qualified cell and trial.

Separate one-million-row jobs retain production JNI, startup/planning, a rowwise source,
BOOLEAN NOT NULL blackhole and both transposes. They use nullable ASCII bodies of six
characters or mixed Unicode bodies of sixteen, parallelism one, two warmups and five
measurements. Both native modes require consumed Calc work; the candidate additionally
requires empty JVM evaluator descriptors. The independent previous process keeps its own
stock control.

| Flink line / corpus | Candidate stock median (range), s | Native median (range), s | Previous stock median (range), s | Previous median (range), s |
| --- | ---: | ---: | ---: | ---: |
| 1.18.1 / ASCII 6 | 0.204180 (0.188094–0.212755) | 0.192536 (0.189526–0.208814) | 0.190818 (0.186239–0.199758) | 0.252382 (0.245369–0.265800) |
| 2.2.1 / ASCII 6 | 0.199403 (0.196239–0.213949) | 0.183029 (0.177659–0.187342) | 0.211075 (0.209459–0.220351) | 0.259388 (0.247681–0.320371) |
| 1.18.1 / Unicode 16 | 1.459045 (1.447974–1.466384) | 0.250724 (0.242249–0.255112) | 1.468166 (1.455251–1.487260) | 1.802646 (1.780039–1.815647) |
| 2.2.1 / Unicode 16 | 1.311750 (1.308548–1.336238) | 0.237144 (0.225340–0.255713) | 1.340659 (1.302556–1.365580) | 1.691473 (1.621389–1.739532) |

The ASCII native medians improve 5.7%/8.2% over paired stock and 23.7%/29.4% over
previous StreamFusion on 1.18/2.2 respectively. Stock/native ranges overlap on 1.18;
independent stock controls also vary. Unicode medians improve 5.82x/5.53x over paired
stock on these exact jobs: longer Unicode integer strings cause the released predicate
to allocate and catch integer-parser failures before trying long parsing. These bounded
jobs do not establish a universal or sustained whole-job gain, and neither the source
string construction nor the sink/transpose costs are removed.

Focused classification checks pass 76 JVM invocations across released Flink 1.18.1/2.2.1
and JDK 11/17, including the three classifiers after shared initialization changes. The
decimal witnesses compare all 65,536 BMP single units and 82,556 embedded-unit,
grammar-combination and seeded random strings, plus positive/negative signed-long bounds
for every BMP digit, against the released
parser and stock runtime SQL. Unique source IDs, complete output counts, consumed native
work, no fallback and empty JVM descriptors are required. Unchanged released upstream
`ScalarFunctionsTest.testIsDecimal` passes on all four pairs; local expression assertions
are separate from the native witnesses. Default-feature release/mimalloc Rust core:
1,007 pass, two pre-existing ignores.

Reproduce with the classification example and `SF_CLASSIFICATION_DECIMAL=true` instead of
the digit flag. The complete-job launcher takes `-Dsf.classification.decimal=true`, plus
`-Dclassification.pipeline.corpus=ascii -Dclassification.pipeline.width=6` or
`-Dclassification.pipeline.corpus=unicode -Dclassification.pipeline.width=16`.
These are benchmark-only controls. The example additionally accepts `fraction`, `exponent`,
`hex`, `special` and `unicode-overflow` corpora. Previous controls prepend the independently
compiled main encoder and enable the existing previous-mode flag.

## Declared NOT NULL classification controls

These additional controls declare STRING NOT NULL in the actual encoder and generated
released Java input signature and use a matching nonnullable Arrow field with no NULL data.
They separate that signature from the original alpha/digit matrices, whose no-NULL data
populations still used nullable Java signatures. `StringClassificationBenchmarkFunctions`
requires the fixture's data setting to match its signature. The native example passes that
setting before creating the JVM; it is not a production option.

The 176 configurations cover 48 alpha, 48 digit and 80 decimal cells on both releases,
with the same corpora, prepared timing boundaries, one thread, release/default/mimalloc,
16 warmups and nine measurements. Alpha/digit trials process 131,072 rows and decimal
trials 32,768 rows. Every 1,024-row cell beats its paired stock control. Twelve alpha,
eleven digit and two decimal 16-row cells lose to stock; no small-batch win is assumed.
The decimal NOT NULL cells are included in its 160-cell qualified matrix, not extra
independent evidence for that total.

The [result record](results/classification-not-null-2026-11-01.json) and
[raw trials](results/classification-not-null-2026-11-01.csv.gz) retain both stock controls,
all variability and each slower case. Existing nullable-signature data remains in the
original alpha/digit records with its scope and source commits explicitly identified.

## Integer ABS over arithmetic

The admitted producer tree contains only integer inputs, typed literals, recursive lossless
widening, nested ABS, addition, subtraction and multiplication. Every arithmetic node wraps
to its resolved Java width. Division, remainder, narrowing and other unverified producers
retain the complete generated outer expression.

`IntegralExpressionBenchmarkFunctions` parses real SQL through each released planner and
uses the actual encoder, including the independently compiled preceding main encoder.
The candidate must have no JVM descriptors; the previous control must bind one. Stock uses
the released generated evaluator on prepared GenericRowData/BinaryRowData. The native
example prepares matching Arrow input and times production Calc plus equal output checksum
consumption inside Rust. Perimeter JNI/import/export and both row/Arrow transposes are
outside this small-operation timer; previous intrinsic callbacks stay inside.

The release/default-feature/mimalloc controls use JDK 17.0.12, Apple M4 Pro, one thread,
16 warmups, nine measurements and 131,072 rows per trial. Input declarations and Arrow
metadata match the selected nullable/NOT NULL data. The matrix covers all four integer
widths, varying values including signed minima, optional NULL every seven rows and both
RowData representations. Both `ABS(v+1)` and `ABS((v+1)*(v-1))` use typed literals in
1,024-row batches; INT/BIGINT addition also has 16-row controls. Independent paired-stock
processes, checksums and all unfavorable timings are retained.

The initial correct native implementation still lost to stock in twelve bulk cells per
release. Result casts after every narrow arithmetic node widened already-correct arrays
to BIGINT and reconstructed their original width. Sharing only actual same-width integer
arrays removes that work without changing true narrowing or scalar materialization.

| Flink line | Batch rows | Initial stock/native median range | Qualified stock/native median range | Qualified previous/native median range |
| --- | ---: | ---: | ---: | ---: |
| 1.18.1 | 16 | 0.054–0.124x | 0.067–0.128x | 9.650–10.806x |
| 1.18.1 | 1,024 | 0.369–4.570x | 1.477–7.808x | 11.416–41.864x |
| 2.2.1 | 16 | 0.057–0.126x | 0.070–0.129x | 10.010–11.365x |
| 2.2.1 | 1,024 | 0.369–4.637x | 1.498–8.087x | 10.858–50.934x |

All 64 qualified bulk cells beat their paired stock control. All sixteen small-batch
cells remain slower than stock: fixed Calc/result costs dominate this tiny computation.
Independent stock controls drift by 0.611–1.638x. Initial/qualified processes are separate
measurements, so their timing ratios do not isolate a causal effect from that variability.
The [result record](results/integral-arithmetic-abs-2026-11-01.json) and
[raw trials](results/integral-arithmetic-abs-2026-11-01.csv.gz) retain both matrices,
including the rejected initial bulk path and every small-batch loss.

Single-batch allocation probes use the same production operation after compilation, with
1,024 nullable GenericRowData-equivalent inputs on 2.2.1. They count requests/bytes, not
retained memory or copied bytes. The accompanying prepared timing cells are reported
separately; allocation probes themselves are not elapsed-time evidence.

| Input / expression | Initial allocation requests / bytes | Qualified allocation requests / bytes | Initial / qualified median ns per row |
| --- | ---: | ---: | ---: |
| INT / addition | 52 / 23,876 | 41 / 10,820 | 4.151 / 1.259 |
| INT / nested | 129 / 61,704 | 96 / 22,536 | 11.249 / 2.711 |
| BIGINT / addition | 39 / 18,728 | 39 / 18,728 | 1.320 / 1.306 |
| BIGINT / nested | 76 / 37,399 | 76 / 37,399 | 2.471 / 2.473 |

The BIGINT controls already lack the narrow result casts and retain their allocation
counts. Rust tests require pointer-identical same-width slices/NULLs/empty arrays,
unchanged scalar materialization and real narrowing. The default-feature release/mimalloc
Rust suite passes 1,003 tests with two pre-existing ignores. Eighty focused JVM invocations
pass across released 1.18.1/2.2.1 and JDK 11/17, including width/overflow/NULL parity,
multi-batch IDs, consumed native work and empty evaluator descriptors. The independently
compiled preceding encoder fails the new route regression. Released upstream `testAbs`
passes unchanged on all four pairs; those local assertions are not native-work evidence.

The allocation executable retains its counting wrapper with counts disabled during timing;
link-symbol inspection confirms its System calls resolve through the enabled engine aliases
(`malloc=mi_malloc`, `free=sf_free`). JVM/C++/other-thread allocations are outside the probe.

Separate million-row INT source/blackhole jobs retain production JNI, planning/startup
and both transposes. They use nullable inputs, parallelism one, two warmups and five
measurements. Both native modes require consumed Calc work; the candidate additionally
requires empty evaluator descriptors. Each previous process has its own paired stock.

| Flink line / expression | Candidate stock median (range), s | Native median (range), s | Previous stock median (range), s | Previous median (range), s |
| --- | ---: | ---: | ---: | ---: |
| 1.18.1 / addition | 0.132738 (0.125002–0.135948) | 0.148356 (0.143165–0.164692) | 0.125945 (0.121515–0.144187) | 0.164539 (0.153559–0.179181) |
| 1.18.1 / nested | 0.134089 (0.123711–0.144396) | 0.147029 (0.144632–0.161026) | 0.124107 (0.120619–0.128548) | 0.164153 (0.154318–0.174387) |
| 2.2.1 / addition | 0.134425 (0.128763–0.179637) | 0.147232 (0.142800–0.159292) | 0.128637 (0.124836–0.164790) | 0.168241 (0.153037–0.189463) |
| 2.2.1 / nested | 0.129607 (0.121807–0.147824) | 0.142487 (0.139940–0.152310) | 0.128585 (0.121173–0.152629) | 0.171499 (0.160844–0.192563) |

Native medians improve 9.8–16.9% over previous StreamFusion, but remain 9.5–11.8%
slower than paired stock. The 2.2 stock/native ranges overlap; independent stock
controls also drift. Source/sink and perimeter conversion costs remain in these
startup-heavy bounded jobs. This qualifies the bulk small-operation gate, not a
standalone-job or universal sustained throughput win over Flink. Generic small-batch
and perimeter costs remain in [the numeric follow-up](https://github.com/datafusion-contrib/StreamFusion/issues/491).

Reproduce after compiling the Java fixture and providing its matching released runtime/test
classpath as `SF_NATIVE_BENCH_CLASSPATH` and the release library directory as
`SF_NATIVE_LIB_DIR`:

```sh
cargo build --manifest-path native/Cargo.toml --release --locked -p streamfusion \
  --features mimalloc --lib --example integral_expression
SF_INTEGRAL_EXPRESSION='ABS((v+CAST(1 AS $TYPE))*(v-CAST(1 AS $TYPE)))' \
  native/target/release/examples/integral_expression INT true false 1024 128 16 9
```

The example arguments are SQL integer type, NULL inputs, BinaryRowData input, batch rows,
loops, warmups and measurements. The previous control sets `SF_INTEGRAL_PREVIOUS=true`
and prepends the actual preceding encoder to the classpath. Complete jobs use
`IntegralExpressionPipelineBenchmark`, `SF_BENCHMARK=true` and the corresponding
`sf.integral.expression` / `sf.integral.previous` properties. These are benchmark controls,
not production switches.

## Integer unary minus over arithmetic

Unary minus now admits the same recursively verified integer addition, subtraction and
multiplication producers as ABS. Every intermediate retains its resolved integer width
and wrapping overflow. An input-containing tree uses the physical negative expression;
a constant-only tree uses the wrapping scalar kernel because DataFusion's ordinary scalar
negative checks overflow. Division, remainder, narrowing and unverified producers retain
the complete generated expression.

The prepared controls use the actual preceding main encoder and the production Calc,
with the same timing boundaries, release/default-feature/mimalloc build and matching
nullable/NOT NULL declarations as the ABS arithmetic matrix. Stock runs released generated
Java on prepared GenericRowData/BinaryRowData; native runs matching Arrow with both timing
and equal checksum consumption inside Rust. Intrinsic previous JVM work remains timed;
entry JNI/import/export and both perimeter transposes remain outside this small-operation
timer. The candidate requires an intended native function and empty JVM descriptors;
the previous control must bind a JVM evaluator.

JDK 17.0.12 on Apple M4 Pro, one thread, 16 warmups, nine measurements and 131,072 rows
per trial cover all four integer widths, NULL/NOT NULL, both row representations, typed
addition and nested multiplication. INT/BIGINT addition also has 16-row controls.

| Flink line | Batch rows | Stock/native median range | Previous/native median range |
| --- | ---: | ---: | ---: |
| 1.18.1 | 16 | 0.073-0.323x | 10.525-29.251x |
| 1.18.1 | 1,024 | 1.483-8.502x | 12.874-57.345x |
| 2.2.1 | 16 | 0.073-0.337x | 10.747-30.518x |
| 2.2.1 | 1,024 | 1.507-9.339x | 13.313-74.468x |

All 64 bulk cells beat paired stock; all sixteen tiny-batch cells remain slower. Fixed
Calc/result costs dominate tiny computations. Independent paired-stock controls drift
by 0.663-1.353x. The [result record](results/integral-arithmetic-negate-2026-11-01.json)
and [raw trials](results/integral-arithmetic-negate-2026-11-01.csv.gz) retain every
measurement, including slower controls and allocation probes.

Separate million-row nullable INT source/blackhole jobs keep production JNI, startup,
planning and both transposes, with parallelism one, two warmups and five measurements.
Both native controls require consumed Calc work; the candidate requires empty evaluators.

| Flink line / expression | Candidate stock median (range), s | Native median (range), s | Previous stock median (range), s | Previous median (range), s |
| --- | ---: | ---: | ---: | ---: |
| 1.18.1 / addition | 0.126943 (0.123006-0.136017) | 0.145248 (0.141566-0.159738) | 0.130385 (0.120768-0.146977) | 0.166026 (0.154087-0.170880) |
| 1.18.1 / nested | 0.127378 (0.119838-0.136126) | 0.142236 (0.138390-0.162559) | 0.129773 (0.123038-0.134026) | 0.168336 (0.155652-0.171366) |
| 2.2.1 / addition | 0.137106 (0.130255-0.147236) | 0.149406 (0.140301-0.164516) | 0.138002 (0.126575-0.175749) | 0.174674 (0.159422-0.202962) |
| 2.2.1 / nested | 0.127712 (0.123361-0.134710) | 0.145771 (0.142729-0.152221) | 0.125265 (0.122946-0.132947) | 0.163662 (0.158853-0.182695) |

Native medians improve 10.9-15.5% over previous StreamFusion, but remain 9.0-14.4%
slower than paired stock. The 2.2 addition ranges overlap. This qualifies prepared bulk
computation, not whole-job acceleration: fixed Calc, source/sink and perimeter costs
remain in these bounded jobs. Tiny-batch/perimeter optimization stays in the
[numeric follow-up](https://github.com/datafusion-contrib/StreamFusion/issues/491).

Correctness checks pass 160 focused JVM invocations across released Flink 1.18.1/2.2.1
and JDK 11/17, plus eight strengthened encoder checks. Consumed runtime jobs compare every
ID, value, type and NULL across batches and require empty JVM descriptors and no fallback.
Sixteen dedicated JVM-started JNI invocations additionally test four signed widths,
both constant operand orders and zero/one/1,023 rows: all 96 batches preserve cardinality,
types and wrapping signed minima after the input closes. The unchanged released upstream
unary-plus/minus method passes on all four pairs. These local upstream assertions are
separate from native-work evidence. Java compiles for release 11. No new Rust production
code is added beyond the separately tested ABS identity-cast optimization.

Reproduce with the same `integral_expression` fixture and preceding-encoder controls as
the ABS matrix, using `SF_INTEGRAL_EXPRESSION='-((v+CAST(1 AS $TYPE))*(v-CAST(1 AS $TYPE)))'`.
Complete jobs use the corresponding `sf.integral.expression` property.

## Integer SIGN over arithmetic

The existing native SIGN implementation now admits recursively verified integer addition,
subtraction and multiplication over its input/literal, lossless-widening and nested-function
whitelist. Each arithmetic node keeps its resolved signed width and Java overflow wrapping.
Division, remainder, narrowing, DECIMAL arithmetic and other unverified producers retain
the complete generated expression. No configuration switch is added.

The [result record](results/integral-arithmetic-sign-2026-11-01.json) and
[raw trials](results/integral-arithmetic-sign-2026-11-01.csv.gz) retain all paired controls, warmups,
measurements, allocation probes and complete-job samples. Release/default-feature/mimalloc,
JDK 17.0.12, Apple M4 Pro, one thread, 16 warmups, nine measurements and 131,072 rows per
trial compare released generated Java on prepared GenericRowData/BinaryRowData with
production Calc on matching Arrow. Native timers and equivalent checksum consumption are
inside Rust; initial JNI/import/export and both perimeter transposes are outside this
small-operation timer. Intrinsic previous JVM computation remains timed. The actual
independently compiled preceding main encoder is required to bind an evaluator; the
candidate requires the intended native function and empty JVM descriptors. Nullable/NOT NULL
declarations match both inputs, including actual NULLs every seven rows where selected.

Typed addition and nested multiplication cover every supported integer width and both row
representations at 1,024 rows; INT/BIGINT addition also has 16-row controls. SIGN and TRUNCATE
retain released Flink's direct narrow-integer compilation failures and use INT/BIGINT here.

| Flink line | Batch rows | Stock/native median range | Previous/native median range | Slower than stock |
| --- | ---: | ---: | ---: | ---: |
| 1.18.1 | 16 | 0.071-0.128x | 9.673-11.315x | 8 / 8 |
| 1.18.1 | 1,024 | 1.540-5.768x | 10.535-34.869x | 0 / 16 |
| 2.2.1 | 16 | 0.072-0.127x | 10.139-11.226x | 8 / 8 |
| 2.2.1 | 1,024 | 1.500-5.587x | 10.989-41.784x | 0 / 16 |

The bulk matrix has 0 slower cells out of 32;
the tiny-batch matrix has 16 slower cells out of 16.
Fixed Calc/result costs remain visible on small computations. Independent stock-control
drift ranges from 0.443 to 1.453x; cross-process ratios do not isolate variability.

Separate million-row nullable INT source/blackhole jobs retain production JNI, planning,
startup and both transposes. Parallelism is one, with two warmups and five measurements.
Both native controls require consumed Calc work; the candidate requires empty descriptors.

| Flink line / expression | Candidate stock median (range), s | Native median (range), s | Previous stock median (range), s | Previous median (range), s |
| --- | ---: | ---: | ---: | ---: |
| 1.18.1 / add | 0.130256 (0.124880-0.139337) | 0.143118 (0.137992-0.154661) | 0.136756 (0.134128-0.150880) | 0.169804 (0.165546-0.210110) |
| 1.18.1 / nested | 0.136361 (0.123144-0.192691) | 0.154559 (0.147487-0.170228) | 0.122213 (0.119685-0.153817) | 0.165679 (0.156633-0.174462) |
| 2.2.1 / add | 0.135250 (0.126187-0.152305) | 0.147599 (0.141288-0.175990) | 0.131027 (0.121526-0.137569) | 0.164077 (0.151389-0.210137) |
| 2.2.1 / nested | 0.129468 (0.122160-0.140752) | 0.156528 (0.143648-0.181162) | 0.136065 (0.122717-0.150573) | 0.159781 (0.159538-0.207609) |

Native/paired-stock elapsed-time ratios range from 1.091 to
1.209; native/previous ratios range from
0.843 to 0.980. Ratios above one are slower.
Retain overlapping ranges, independent stock drift and unfavorable complete jobs. Prepared
bulk qualification does not establish a universal or complete-job throughput improvement.
Remaining small-batch and perimeter costs stay in [the numeric follow-up](https://github.com/datafusion-contrib/StreamFusion/issues/491).

All 100 focused JVM invocations pass across released Flink 1.18.1/2.2.1 and JDK 11/17.
Runtime witnesses compare complete values, types, NULLs, cardinality and unique IDs across
multiple batches, and require consumed native work, no fallback and empty JVM descriptors.
Existing selected/unselected failures, empty filtered batches and unverified-producer guards
remain covered. The unchanged released upstream method also passes on all four pairs; its
local expression assertions supplement the independent runtime native-work witnesses.
Both Java lines compile for release 11. No new Rust production code is introduced by this
producer-admission change.

Reproduce with `integral_expression` and `IntegralExpressionPipelineBenchmark`, setting
`SF_INTEGRAL_EXPRESSION='SIGN((v+CAST(1 AS $TYPE))*(v-CAST(1 AS $TYPE)))'` or the matching
`sf.integral.expression` property. As in the ABS controls, the actual previous encoder is
prepended to its own classpath and qualified with `SF_INTEGRAL_PREVIOUS=true`.

## Integer FLOOR over arithmetic

The existing native FLOOR implementation now admits recursively verified integer addition,
subtraction and multiplication over its input/literal, lossless-widening and nested-function
whitelist. Each arithmetic node keeps its resolved signed width and Java overflow wrapping.
Division, remainder, narrowing, DECIMAL arithmetic and other unverified producers retain
the complete generated expression. No configuration switch is added.

The [result record](results/integral-arithmetic-floor-2026-11-01.json) and
[raw trials](results/integral-arithmetic-floor-2026-11-01.csv.gz) retain all paired controls, warmups,
measurements, allocation probes and complete-job samples. Release/default-feature/mimalloc,
JDK 17.0.12, Apple M4 Pro, one thread, 16 warmups, nine measurements and 131,072 rows per
trial compare released generated Java on prepared GenericRowData/BinaryRowData with
production Calc on matching Arrow. Native timers and equivalent checksum consumption are
inside Rust; initial JNI/import/export and both perimeter transposes are outside this
small-operation timer. Intrinsic previous JVM computation remains timed. The actual
independently compiled preceding main encoder is required to bind an evaluator; the
candidate requires the intended native function and empty JVM descriptors. Nullable/NOT NULL
declarations match both inputs, including actual NULLs every seven rows where selected.

Typed addition and nested multiplication cover every supported integer width and both row
representations at 1,024 rows; INT/BIGINT addition also has 16-row controls. SIGN and TRUNCATE
retain released Flink's direct narrow-integer compilation failures and use INT/BIGINT here.

| Flink line | Batch rows | Stock/native median range | Previous/native median range | Slower than stock |
| --- | ---: | ---: | ---: | ---: |
| 1.18.1 | 16 | 0.073-0.385x | 10.848-33.897x | 8 / 8 |
| 1.18.1 | 1,024 | 1.526-11.036x | 11.725-85.888x | 0 / 32 |
| 2.2.1 | 16 | 0.078-0.377x | 11.285-34.749x | 8 / 8 |
| 2.2.1 | 1,024 | 1.568-12.272x | 10.653-90.189x | 0 / 32 |

The bulk matrix has 0 slower cells out of 64;
the tiny-batch matrix has 16 slower cells out of 16.
Fixed Calc/result costs remain visible on small computations. Independent stock-control
drift ranges from 0.592 to 1.601x; cross-process ratios do not isolate variability.

Separate million-row nullable INT source/blackhole jobs retain production JNI, planning,
startup and both transposes. Parallelism is one, with two warmups and five measurements.
Both native controls require consumed Calc work; the candidate requires empty descriptors.

| Flink line / expression | Candidate stock median (range), s | Native median (range), s | Previous stock median (range), s | Previous median (range), s |
| --- | ---: | ---: | ---: | ---: |
| 1.18.1 / add | 0.123600 (0.122369-0.129598) | 0.151265 (0.137397-0.159724) | 0.126829 (0.118631-0.138255) | 0.164821 (0.157293-0.189417) |
| 1.18.1 / nested | 0.125440 (0.118526-0.154963) | 0.140254 (0.138623-0.154078) | 0.127278 (0.122852-0.136469) | 0.163857 (0.157304-0.179982) |
| 2.2.1 / add | 0.143825 (0.125135-0.170857) | 0.149311 (0.146698-0.163415) | 0.132833 (0.129114-0.141480) | 0.167962 (0.162157-0.214726) |
| 2.2.1 / nested | 0.128320 (0.122758-0.130398) | 0.145575 (0.143623-0.154209) | 0.137148 (0.122207-0.147096) | 0.166432 (0.160759-0.177441) |

Native/paired-stock elapsed-time ratios range from 1.038 to
1.224; native/previous ratios range from
0.856 to 0.918. Ratios above one are slower.
Retain overlapping ranges, independent stock drift and unfavorable complete jobs. Prepared
bulk qualification does not establish a universal or complete-job throughput improvement.
Remaining small-batch and perimeter costs stay in [the numeric follow-up](https://github.com/datafusion-contrib/StreamFusion/issues/491).

All 132 focused JVM invocations pass across released Flink 1.18.1/2.2.1 and JDK 11/17.
Runtime witnesses compare complete values, types, NULLs, cardinality and unique IDs across
multiple batches, and require consumed native work, no fallback and empty JVM descriptors.
Existing selected/unselected failures, empty filtered batches and unverified-producer guards
remain covered. The unchanged released upstream method also passes on all four pairs; its
local expression assertions supplement the independent runtime native-work witnesses.
Both Java lines compile for release 11. No new Rust production code is introduced by this
producer-admission change.

Reproduce with `integral_expression` and `IntegralExpressionPipelineBenchmark`, setting
`SF_INTEGRAL_EXPRESSION='FLOOR((v+CAST(1 AS $TYPE))*(v-CAST(1 AS $TYPE)))'` or the matching
`sf.integral.expression` property. As in the ABS controls, the actual previous encoder is
prepended to its own classpath and qualified with `SF_INTEGRAL_PREVIOUS=true`.

## Integer CEIL over arithmetic

The existing native CEIL implementation now admits recursively verified integer addition,
subtraction and multiplication over its input/literal, lossless-widening and nested-function
whitelist. Each arithmetic node keeps its resolved signed width and Java overflow wrapping.
Division, remainder, narrowing, DECIMAL arithmetic and other unverified producers retain
the complete generated expression. No configuration switch is added.

The [result record](results/integral-arithmetic-ceil-2026-11-01.json) and
[raw trials](results/integral-arithmetic-ceil-2026-11-01.csv.gz) retain all paired controls, warmups,
measurements, allocation probes and complete-job samples. Release/default-feature/mimalloc,
JDK 17.0.12, Apple M4 Pro, one thread, 16 warmups, nine measurements and 131,072 rows per
trial compare released generated Java on prepared GenericRowData/BinaryRowData with
production Calc on matching Arrow. Native timers and equivalent checksum consumption are
inside Rust; initial JNI/import/export and both perimeter transposes are outside this
small-operation timer. Intrinsic previous JVM computation remains timed. The actual
independently compiled preceding main encoder is required to bind an evaluator; the
candidate requires the intended native function and empty JVM descriptors. Nullable/NOT NULL
declarations match both inputs, including actual NULLs every seven rows where selected.

Typed addition and nested multiplication cover every supported integer width and both row
representations at 1,024 rows; INT/BIGINT addition also has 16-row controls. SIGN and TRUNCATE
retain released Flink's direct narrow-integer compilation failures and use INT/BIGINT here.

| Flink line | Batch rows | Stock/native median range | Previous/native median range | Slower than stock |
| --- | ---: | ---: | ---: | ---: |
| 1.18.1 | 16 | 0.077-0.375x | 10.891-34.160x | 8 / 8 |
| 1.18.1 | 1,024 | 1.555-11.353x | 11.566-82.999x | 0 / 32 |
| 2.2.1 | 16 | 0.078-0.363x | 11.241-34.910x | 8 / 8 |
| 2.2.1 | 1,024 | 1.553-12.186x | 11.711-112.828x | 0 / 32 |

The bulk matrix has 0 slower cells out of 64;
the tiny-batch matrix has 16 slower cells out of 16.
Fixed Calc/result costs remain visible on small computations. Independent stock-control
drift ranges from 0.417 to 1.414x; cross-process ratios do not isolate variability.

Separate million-row nullable INT source/blackhole jobs retain production JNI, planning,
startup and both transposes. Parallelism is one, with two warmups and five measurements.
Both native controls require consumed Calc work; the candidate requires empty descriptors.

| Flink line / expression | Candidate stock median (range), s | Native median (range), s | Previous stock median (range), s | Previous median (range), s |
| --- | ---: | ---: | ---: | ---: |
| 1.18.1 / add | 0.130198 (0.122790-0.137458) | 0.157354 (0.138733-0.168121) | 0.121527 (0.119802-0.135765) | 0.163856 (0.155947-0.169186) |
| 1.18.1 / nested | 0.129851 (0.120823-0.152799) | 0.141774 (0.138176-0.156186) | 0.128809 (0.127996-0.153699) | 0.165188 (0.161602-0.178558) |
| 2.2.1 / add | 0.126569 (0.122766-0.131987) | 0.148518 (0.143178-0.160139) | 0.126695 (0.121754-0.140802) | 0.162624 (0.157716-0.181336) |
| 2.2.1 / nested | 0.126593 (0.121725-0.168759) | 0.147910 (0.141753-0.154536) | 0.126083 (0.119509-0.155248) | 0.166177 (0.162577-0.186416) |

Native/paired-stock elapsed-time ratios range from 1.092 to
1.209; native/previous ratios range from
0.858 to 0.960. Ratios above one are slower.
Retain overlapping ranges, independent stock drift and unfavorable complete jobs. Prepared
bulk qualification does not establish a universal or complete-job throughput improvement.
Remaining small-batch and perimeter costs stay in [the numeric follow-up](https://github.com/datafusion-contrib/StreamFusion/issues/491).

All 152 focused JVM invocations pass across released Flink 1.18.1/2.2.1 and JDK 11/17.
Runtime witnesses compare complete values, types, NULLs, cardinality and unique IDs across
multiple batches, and require consumed native work, no fallback and empty JVM descriptors.
Existing selected/unselected failures, empty filtered batches and unverified-producer guards
remain covered. The unchanged released upstream method also passes on all four pairs; its
local expression assertions supplement the independent runtime native-work witnesses.
Both Java lines compile for release 11. No new Rust production code is introduced by this
producer-admission change.

Reproduce with `integral_expression` and `IntegralExpressionPipelineBenchmark`, setting
`SF_INTEGRAL_EXPRESSION='CEIL((v+CAST(1 AS $TYPE))*(v-CAST(1 AS $TYPE)))'` or the matching
`sf.integral.expression` property. As in the ABS controls, the actual previous encoder is
prepended to its own classpath and qualified with `SF_INTEGRAL_PREVIOUS=true`.

## Integer ROUND over arithmetic

The existing native ROUND implementation now admits recursively verified integer addition,
subtraction and multiplication over its input/literal, lossless-widening and nested-function
whitelist. Each arithmetic node keeps its resolved signed width and Java overflow wrapping.
Division, remainder, narrowing, DECIMAL arithmetic and other unverified producers retain
the complete generated expression. No configuration switch is added.

The [result record](results/integral-arithmetic-round-2026-11-01.json) and
[raw trials](results/integral-arithmetic-round-2026-11-01.csv.gz) retain all paired controls, warmups,
measurements, allocation probes and complete-job samples. Release/default-feature/mimalloc,
JDK 17.0.12, Apple M4 Pro, one thread, 16 warmups, nine measurements and 131,072 rows per
trial compare released generated Java on prepared GenericRowData/BinaryRowData with
production Calc on matching Arrow. Native timers and equivalent checksum consumption are
inside Rust; initial JNI/import/export and both perimeter transposes are outside this
small-operation timer. Intrinsic previous JVM computation remains timed. The actual
independently compiled preceding main encoder is required to bind an evaluator; the
candidate requires the intended native function and empty JVM descriptors. Nullable/NOT NULL
declarations match both inputs, including actual NULLs every seven rows where selected.

Typed addition and nested multiplication cover every supported integer width and both row
representations at 1,024 rows; INT/BIGINT addition also has 16-row controls. ROUND covers
all four integer widths.

Additional nullable Generic/Binary cells apply literal INT positions -1, -38 and 38 to the
nested arithmetic expression over every admitted width. NULL-position cells are retained
separately as constant-result controls and excluded from function-execution qualification.

| Flink line | Batch rows | Stock/native median range | Previous/native median range | Slower than stock |
| --- | ---: | ---: | ---: | ---: |
| 1.18.1 | 16 | 0.084-0.384x | 11.029-35.381x | 8 / 8 |
| 1.18.1 | 1,024 | 1.805-35.430x | 10.527-98.619x | 0 / 56 |
| 2.2.1 | 16 | 0.091-0.397x | 11.018-35.726x | 8 / 8 |
| 2.2.1 | 1,024 | 1.832-35.412x | 11.169-105.774x | 0 / 56 |

The bulk matrix has 0 slower cells out of 112;
the tiny-batch matrix has 16 slower cells out of 16.
Fixed Calc/result costs remain visible on small computations. Independent stock-control
drift ranges from 0.284 to 6.389x; cross-process ratios do not isolate variability.

Separate million-row nullable INT source/blackhole jobs retain production JNI, planning,
startup and both transposes. Parallelism is one, with two warmups and five measurements.
Both native controls require consumed Calc work; the candidate requires empty descriptors.
The position-add/position-nested job rows apply the explicit literal position -1.

| Flink line / expression | Candidate stock median (range), s | Native median (range), s | Previous stock median (range), s | Previous median (range), s |
| --- | ---: | ---: | ---: | ---: |
| 1.18.1 / add | 0.138509 (0.128871-0.141295) | 0.151582 (0.149946-0.187205) | 0.135664 (0.129138-0.147661) | 0.179961 (0.172285-0.213695) |
| 1.18.1 / nested | 0.131959 (0.126554-0.138073) | 0.148946 (0.141745-0.190673) | 0.133393 (0.127564-0.136626) | 0.175208 (0.171869-0.184764) |
| 1.18.1 / position-add | 0.143669 (0.135566-0.165214) | 0.146206 (0.144089-0.168085) | 0.141263 (0.139604-0.155608) | 0.175963 (0.164515-0.233102) |
| 1.18.1 / position-nested | 0.155569 (0.146503-0.156909) | 0.157583 (0.143202-0.174439) | 0.158029 (0.141994-0.164224) | 0.193834 (0.177136-0.201926) |
| 2.2.1 / add | 0.138633 (0.135453-0.148245) | 0.149119 (0.143823-0.155219) | 0.136070 (0.132937-0.150031) | 0.178963 (0.170773-0.199273) |
| 2.2.1 / nested | 0.132262 (0.128172-0.159622) | 0.143343 (0.136777-0.161370) | 0.136466 (0.126677-0.165135) | 0.172319 (0.167299-0.197420) |
| 2.2.1 / position-add | 0.146133 (0.142373-0.166690) | 0.147639 (0.145069-0.161272) | 0.147190 (0.132771-0.152431) | 0.170306 (0.165477-0.186386) |
| 2.2.1 / position-nested | 0.145609 (0.142876-0.159408) | 0.143521 (0.142514-0.155381) | 0.146946 (0.140844-0.160657) | 0.187572 (0.177320-0.211928) |

Native/paired-stock elapsed-time ratios range from 0.986 to
1.129; native/previous ratios range from
0.765 to 0.867. Ratios above one are slower.
Retain overlapping ranges, independent stock drift and unfavorable complete jobs. Prepared
bulk qualification does not establish a universal or complete-job throughput improvement.
Remaining small-batch and perimeter costs stay in [the numeric follow-up](https://github.com/datafusion-contrib/StreamFusion/issues/491).

All 240 focused JVM invocations pass across released Flink 1.18.1/2.2.1 and JDK 11/17.
Runtime witnesses compare complete values, types, NULLs, cardinality and unique IDs across
multiple batches, and require consumed native work, no fallback and empty JVM descriptors.
Existing selected/unselected failures, empty filtered batches and unverified-producer guards
remain covered. The unchanged released upstream method also passes on all four pairs; its
local expression assertions supplement the independent runtime native-work witnesses.
Both Java lines compile for release 11. No new Rust production code is introduced by this
producer-admission change.

Reproduce with `integral_expression` and `IntegralExpressionPipelineBenchmark`, setting
`SF_INTEGRAL_EXPRESSION='ROUND((v+CAST(1 AS $TYPE))*(v-CAST(1 AS $TYPE)))'` or the matching
`sf.integral.expression` property. As in the ABS controls, the actual previous encoder is
prepended to its own classpath and qualified with `SF_INTEGRAL_PREVIOUS=true`.

The 16 constant-NULL fold controls have paired stock/native median ratios
from 2.425 to 23.015x. They measure production Calc result
materialization and consumption after a bounded proof excludes operand failures; they
do not establish executed function-kernel work. Division, remainder, narrowing and other
unverified producers retain evaluation even when the position is NULL.

The initial eager NULL-position implementation lost to stock in all eight diagnostic
cells (0.65-0.71x stock/native). Eliminating the verified failure-free operand tree moves
matched diagnostic medians from 2.50-2.72 to 0.62-0.72 ns/row and the paired ratios to
2.48-2.80x, with independent process/stock variation retained. Untimed Rust allocation
requests drop from 79-99 to 16 per batch; these exclude JVM/other-thread allocations and
are not retained memory. The [initial trials](results/integral-arithmetic-round-null-before-fold-2026-11-01.csv.gz)
and [folded trials](results/integral-arithmetic-round-null-after-fold-2026-11-01.csv.gz) supplement the main
[result record](results/integral-arithmetic-round-2026-11-01.json).

All 116 additional shared typed-NULL encoding/SQL invocations pass across the same four
version/JDK pairs, covering precision, nested fields, temporal types, unknown-type guards
and partial final batches.

## Integer TRUNCATE over arithmetic

The existing native TRUNCATE implementation now admits recursively verified integer addition,
subtraction and multiplication over its input/literal, lossless-widening and nested-function
whitelist. Each arithmetic node keeps its resolved signed width and Java overflow wrapping.
Division, remainder, narrowing, DECIMAL arithmetic and other unverified producers retain
the complete generated expression. No configuration switch is added.

The [result record](results/integral-arithmetic-truncate-2026-11-01.json) and
[raw trials](results/integral-arithmetic-truncate-2026-11-01.csv.gz) retain all paired controls, warmups,
measurements, allocation probes and complete-job samples. Release/default-feature/mimalloc,
JDK 17.0.12, Apple M4 Pro, one thread, 16 warmups, nine measurements and 131,072 rows per
trial compare released generated Java on prepared GenericRowData/BinaryRowData with
production Calc on matching Arrow. Native timers and equivalent checksum consumption are
inside Rust; initial JNI/import/export and both perimeter transposes are outside this
small-operation timer. Intrinsic previous JVM computation remains timed. The actual
independently compiled preceding main encoder is required to bind an evaluator; the
candidate requires the intended native function and empty JVM descriptors. Nullable/NOT NULL
declarations match both inputs, including actual NULLs every seven rows where selected.

Typed addition and nested multiplication cover every supported integer width and both row
representations at 1,024 rows; INT/BIGINT addition also has 16-row controls. TRUNCATE
retains released Flink's direct narrow-integer compilation failures and uses INT/BIGINT here.

Additional nullable Generic/Binary cells apply literal INT positions -1, -38 and 38 to the
nested arithmetic expression over every admitted width. NULL-position cells are retained
separately as constant-result controls and excluded from function-execution qualification.

| Flink line | Batch rows | Stock/native median range | Previous/native median range | Slower than stock |
| --- | ---: | ---: | ---: | ---: |
| 1.18.1 | 16 | 0.978-8.364x | 12.298-41.801x | 1 / 8 |
| 1.18.1 | 1,024 | 10.545-277.690x | 19.304-317.218x | 0 / 28 |
| 2.2.1 | 16 | 0.970-7.944x | 12.495-40.313x | 1 / 8 |
| 2.2.1 | 1,024 | 10.417-281.861x | 19.904-333.712x | 0 / 28 |

The bulk matrix has 0 slower cells out of 56;
the tiny-batch matrix has 2 slower cells out of 16.
Fixed Calc/result costs remain visible on small computations. Independent stock-control
drift ranges from 0.271 to 1.448x; cross-process ratios do not isolate variability.

Separate million-row nullable INT source/blackhole jobs retain production JNI, planning,
startup and both transposes. Parallelism is one, with two warmups and five measurements.
Both native controls require consumed Calc work; the candidate requires empty descriptors.
The position-add/position-nested job rows apply the explicit literal position -1.

| Flink line / expression | Candidate stock median (range), s | Native median (range), s | Previous stock median (range), s | Previous median (range), s |
| --- | ---: | ---: | ---: | ---: |
| 1.18.1 / add | 0.193199 (0.183309-0.200168) | 0.148608 (0.138675-0.165526) | 0.190651 (0.188763-0.206507) | 0.261375 (0.241094-0.283575) |
| 1.18.1 / nested | 0.200089 (0.190741-0.203405) | 0.151694 (0.145246-0.160465) | 0.200865 (0.188948-0.208953) | 0.251374 (0.248945-0.270095) |
| 1.18.1 / position-add | 0.196940 (0.188253-0.201866) | 0.151558 (0.143435-0.181935) | 0.195267 (0.192303-0.203604) | 0.219569 (0.216203-0.231150) |
| 1.18.1 / position-nested | 0.200262 (0.191257-0.219927) | 0.153747 (0.147628-0.165354) | 0.207347 (0.203120-0.215704) | 0.236552 (0.231091-0.247184) |
| 2.2.1 / add | 0.192395 (0.188942-0.201869) | 0.152104 (0.145143-0.156501) | 0.189003 (0.181838-0.199292) | 0.268802 (0.263646-0.283940) |
| 2.2.1 / nested | 0.206748 (0.196520-0.222838) | 0.144713 (0.142744-0.166039) | 0.205656 (0.198518-0.212245) | 0.250286 (0.236342-0.257092) |
| 2.2.1 / position-add | 0.209153 (0.200628-0.220100) | 0.147064 (0.143148-0.172021) | 0.206633 (0.199655-0.210218) | 0.222729 (0.217287-0.230073) |
| 2.2.1 / position-nested | 0.215521 (0.212657-0.222801) | 0.149474 (0.144635-0.154722) | 0.213070 (0.206850-0.220141) | 0.244035 (0.233602-0.270349) |

Native/paired-stock elapsed-time ratios range from 0.694 to
0.791; native/previous ratios range from
0.566 to 0.690. Ratios above one are slower.
These eight complete-job medians improve by 20.9-30.6% over paired stock and 31.0-43.4%
over previous StreamFusion. Retain overlapping ranges and independent stock drift;
the measured workloads do not establish a universal complete-job throughput improvement.
Remaining small-batch and perimeter costs stay in [the numeric follow-up](https://github.com/datafusion-contrib/StreamFusion/issues/491).

All 236 focused JVM invocations pass across released Flink 1.18.1/2.2.1 and JDK 11/17.
Runtime witnesses compare complete values, types, NULLs, cardinality and unique IDs across
multiple batches, and require consumed native work, no fallback and empty JVM descriptors.
Existing selected/unselected failures, empty filtered batches and unverified-producer guards
remain covered. The unchanged released upstream method also passes on all four pairs; its
local expression assertions supplement the independent runtime native-work witnesses.
Both Java lines compile for release 11. No new Rust production code is introduced by this
producer-admission change.

Reproduce with `integral_expression` and `IntegralExpressionPipelineBenchmark`, setting
`SF_INTEGRAL_EXPRESSION='TRUNCATE((v+CAST(1 AS $TYPE))*(v-CAST(1 AS $TYPE)))'` or the matching
`sf.integral.expression` property. As in the ABS controls, the actual previous encoder is
prepended to its own classpath and qualified with `SF_INTEGRAL_PREVIOUS=true`.

The 8 constant-NULL fold controls have paired stock/native median ratios
from 2.392 to 7.203x. They measure production Calc result
materialization and consumption after a bounded proof excludes operand failures; they
do not establish executed function-kernel work. Division, remainder, narrowing and other
unverified producers retain evaluation even when the position is NULL.

## Numeric IS_ALPHA

Released Flink receives boxed primitive integers or internal `DecimalData` in this overload.
All admitted numeric arguments return FALSE, including runtime/literal NULLs.
Direct inputs/literals keep a native type-based predicate. Only the recursively verified
failure-free integer producer scope folds away unused arithmetic; division, remainder,
narrowing, UDFs and other unverified producers retain the complete generated call, including
failures and state changes. Floating signatures remain generated. This preserves the released
behavior rather than inferring it from Java's public BigDecimal API.

The final admission guard additionally keeps unverified producers generated. An untimed
comparison on each Flink line confirms that all 28 distinct measured direct/add/nested
signatures retain identical encoded programs and constant pools; the result record keeps
both measured/final source hashes and final-guard correctness validation.

The [result record](results/numeric-classification-alpha-2026-11-01.json) and
[raw trials](results/numeric-classification-alpha-2026-11-01.csv.gz) retain 80 configurations and 8,000 main
trials. Direct runtime arguments retain the native call; arithmetic fold controls are
explicitly excluded from predicate-kernel qualification. The direct matrix covers four
integer widths and DECIMAL(18,2)/(38,18), nullable/NOT NULL and GenericRowData/BinaryRowData
at 1,024 rows. Tiny INT/BIGINT controls use 16 rows. Typed addition/nested multiplication
provide separate fold diagnostics. No input values are converted to strings.

Release/default-feature/mimalloc, JDK 17.0.12, Apple M4 Pro, one thread, 16 warmups/nine
measurements and 131,072 rows/trial compare actual released generated Java on prepared
RowData with production Calc on matching Arrow. Native operation and equivalent Boolean
result consumption are timed inside Rust; initial JNI/import/export and perimeter
transposes are outside this small-operation timer. Intrinsic previous JVM computation
stays inside. The actual independently compiled preceding-main encoder must bind an
evaluator; the candidate requires empty descriptors and the intended function before the
bounded fold. Both sides retain exact types, widths, signed boundaries and matching NULLs.

| Flink line | Rows / measured work | Stock/native median range | Previous/native median range | Slower than stock |
| --- | --- | ---: | ---: | ---: |
| 1.18.1 | 1,024 / predicate | 4.338-75.846x | 57.267-83.709x | 0 / 24 |
| 1.18.1 | 16 / predicate | 0.132-0.177x | 29.727-32.708x | 8 / 8 |
| 1.18.1 | 1,024 / fold control | 5.214-14.863x | 80.878-156.067x | 0 / 8 |
| 2.2.1 | 1,024 / predicate | 4.378-74.153x | 56.493-174.781x | 0 / 24 |
| 2.2.1 | 16 / predicate | 0.132-0.167x | 29.850-33.778x | 8 / 8 |
| 2.2.1 | 1,024 / fold control | 5.669-10.663x | 82.630-211.918x | 0 / 8 |

Ratios above one favor native. The type-based predicate returns a Boolean scalar which
production Calc materializes to the batch length; the measurements do not claim a
per-row character scan. Small-batch fixed Calc/result costs and every unfavorable sample
remain in the record. Independent paired-stock drift ranges from
0.474 to 4.622x; cross-process
ratios are not causal isolation.

Separate million-row nullable INT source/blackhole jobs retain production JNI, planning,
startup and both transposes, with parallelism one and two warmups/five measurements.
Direct jobs retain the predicate. Add/nested jobs exercise the bounded fold and must not
be cited as predicate-kernel work. All require consumed native Calc work, no fallback and
empty candidate evaluator descriptors.

| Flink line / input | Candidate stock median (range), s | Native median (range), s | Previous stock median (range), s | Previous median (range), s |
| --- | ---: | ---: | ---: | ---: |
| 1.18.1 / direct | 0.144208 (0.139951-0.185509) | 0.158264 (0.155906-0.180063) | 0.150093 (0.140484-0.155497) | 0.190045 (0.184045-0.214745) |
| 1.18.1 / add | 0.158886 (0.138947-0.184109) | 0.153646 (0.148057-0.187208) | 0.144336 (0.143366-0.162540) | 0.180045 (0.173658-0.223902) |
| 1.18.1 / nested | 0.147116 (0.143477-0.157420) | 0.153543 (0.149247-0.156138) | 0.148057 (0.140475-0.163178) | 0.179666 (0.174594-0.195992) |
| 2.2.1 / direct | 0.171778 (0.156760-0.184056) | 0.163535 (0.162832-0.170514) | 0.155110 (0.150512-0.163980) | 0.192114 (0.179358-0.216947) |
| 2.2.1 / add | 0.159357 (0.151531-0.164622) | 0.151333 (0.150413-0.174848) | 0.152845 (0.150406-0.165735) | 0.190004 (0.185241-0.206299) |
| 2.2.1 / nested | 0.155059 (0.149082-0.160268) | 0.150045 (0.143815-0.165674) | 0.170340 (0.152518-0.174406) | 0.191185 (0.188699-0.243760) |

Across these jobs, native/stock elapsed-time ratios range from
0.950 to 1.097; native/previous ratios range
from 0.785 to 0.855. Ratios above one are
slower. Retain overlapping ranges and independent stock drift; prepared bulk qualification
does not establish universal whole-job acceleration.

All 56 focused JVM invocations and unchanged released `IS_ALPHA` expression tests
pass on Flink 1.18.1/2.2.1 with JDK 11/17. Eight numeric declarations, 5,003 rows, nullable
and NOT NULL inputs, literal NULLs and Boolean consumers compare complete values, resolved
types, cardinality and unique IDs across multiple batches. Direct runtime witnesses require
consumed work, no fallback and empty descriptors. Separate failure/empty-batch tests and
four additional shared-state invocations prove that arguments are not discarded. Java
compiles for release 11; the release/default/mimalloc Rust core suite passes 1,017 tests
with two pre-existing ignores.

Reproduce with `integral_expression`, for example
`SF_INTEGRAL_EXPRESSION='IS_ALPHA(v)'`, and `IntegralExpressionPipelineBenchmark` with
`sf.integral.expression=IS_ALPHA(v)`. Prepended independently compiled previous-encoder
classes and `SF_INTEGRAL_PREVIOUS=true` select the preceding callback control. Remaining
coverage stays in [the classification follow-up](https://github.com/datafusion-contrib/StreamFusion/issues/494).

The initial nullable INT arithmetic probe loses to stock
(0.882x stock/native).
The bounded fold moves the matched native median from
2.352 to 0.381 ns/row,
with a paired stock/native ratio of
5.815x after the change.
The [initial](results/numeric-classification-alpha-probe-2026-11-01.csv.gz) and
[folded](results/numeric-classification-alpha-fold-probe-2026-11-01.csv.gz) probe trials retain direct/tiny
controls, allocation probes and independent process variation. They are optimizer
diagnostics, separate from the direct predicate qualification.

## Numeric IS_DIGIT

Released Flink receives boxed primitive integers or internal `DecimalData` in this overload.
Integral arguments return TRUE regardless of sign or runtime/literal NULL; internal DecimalData arguments return FALSE.
Direct inputs/literals keep a native type-based predicate. Only the recursively verified
failure-free integer producer scope folds away unused arithmetic; division, remainder,
narrowing, UDFs and other unverified producers retain the complete generated call, including
failures and state changes. Floating signatures remain generated. This preserves the released
behavior rather than inferring it from Java's public BigDecimal API.

The [result record](results/numeric-classification-digit-2026-11-01.json) and
[raw trials](results/numeric-classification-digit-2026-11-01.csv.gz) retain 80 configurations and 8,000 main
trials. Direct runtime arguments retain the native call; arithmetic fold controls are
explicitly excluded from predicate-kernel qualification. The direct matrix covers four
integer widths and DECIMAL(18,2)/(38,18), nullable/NOT NULL and GenericRowData/BinaryRowData
at 1,024 rows. Tiny INT/BIGINT controls use 16 rows. Typed addition/nested multiplication
provide separate fold diagnostics. No input values are converted to strings.

Release/default-feature/mimalloc, JDK 17.0.12, Apple M4 Pro, one thread, 16 warmups/nine
measurements and 131,072 rows/trial compare actual released generated Java on prepared
RowData with production Calc on matching Arrow. Native operation and equivalent Boolean
result consumption are timed inside Rust; initial JNI/import/export and perimeter
transposes are outside this small-operation timer. Intrinsic previous JVM computation
stays inside. The actual independently compiled preceding-main encoder must bind an
evaluator; the candidate requires empty descriptors and the intended function before the
bounded fold. Both sides retain exact types, widths, signed boundaries and matching NULLs.

| Flink line | Rows / measured work | Stock/native median range | Previous/native median range | Slower than stock |
| --- | --- | ---: | ---: | ---: |
| 1.18.1 | 1,024 / predicate | 4.327-76.960x | 56.537-83.467x | 0 / 24 |
| 1.18.1 | 16 / predicate | 0.146-0.207x | 29.999-32.504x | 8 / 8 |
| 1.18.1 | 1,024 / fold control | 5.655-9.284x | 86.747-124.336x | 0 / 8 |
| 2.2.1 | 1,024 / predicate | 4.403-52.638x | 48.010-73.082x | 0 / 24 |
| 2.2.1 | 16 / predicate | 0.138-0.187x | 30.394-32.411x | 8 / 8 |
| 2.2.1 | 1,024 / fold control | 5.822-9.055x | 68.054-78.744x | 0 / 8 |

Ratios above one favor native. The type-based predicate returns a Boolean scalar which
production Calc materializes to the batch length; the measurements do not claim a
per-row character scan. Small-batch fixed Calc/result costs and every unfavorable sample
remain in the record. Independent paired-stock drift ranges from
0.585 to 9.993x; cross-process
ratios are not causal isolation.

Separate million-row nullable INT source/blackhole jobs retain production JNI, planning,
startup and both transposes, with parallelism one and two warmups/five measurements.
Direct jobs retain the predicate. Add/nested jobs exercise the bounded fold and must not
be cited as predicate-kernel work. All require consumed native Calc work, no fallback and
empty candidate evaluator descriptors.

| Flink line / input | Candidate stock median (range), s | Native median (range), s | Previous stock median (range), s | Previous median (range), s |
| --- | ---: | ---: | ---: | ---: |
| 1.18.1 / direct | 0.147362 (0.143816-0.187192) | 0.165473 (0.163363-0.191847) | 0.155385 (0.145714-0.159703) | 0.188236 (0.181853-0.326542) |
| 1.18.1 / add | 0.152047 (0.144916-0.158812) | 0.159139 (0.153293-0.174958) | 0.150465 (0.142756-0.192243) | 0.186863 (0.182414-0.205804) |
| 1.18.1 / nested | 0.152921 (0.146317-0.182007) | 0.155172 (0.151606-0.171286) | 0.148784 (0.140122-0.150412) | 0.181575 (0.178331-0.194795) |
| 2.2.1 / direct | 0.152754 (0.144340-0.183355) | 0.167731 (0.157047-0.171134) | 0.154388 (0.149800-0.160568) | 0.177077 (0.176169-0.203367) |
| 2.2.1 / add | 0.159082 (0.157465-0.169012) | 0.158240 (0.151098-0.166858) | 0.158511 (0.149533-0.166617) | 0.180761 (0.176107-0.188447) |
| 2.2.1 / nested | 0.154696 (0.144169-0.162562) | 0.152258 (0.149800-0.169870) | 0.155161 (0.145043-0.165499) | 0.185097 (0.180521-0.209116) |

Across these jobs, native/stock elapsed-time ratios range from
0.984 to 1.123; native/previous ratios range
from 0.823 to 0.947. Ratios above one are
slower. Retain overlapping ranges and independent stock drift; prepared bulk qualification
does not establish universal whole-job acceleration.

All 96 focused JVM invocations and unchanged released `IS_DIGIT` expression tests
pass on Flink 1.18.1/2.2.1 with JDK 11/17. Eight numeric declarations, 5,003 rows, nullable
and NOT NULL inputs, literal NULLs and Boolean consumers compare complete values, resolved
types, cardinality and unique IDs across multiple batches. Direct runtime witnesses require
consumed work, no fallback and empty descriptors. Separate failure/empty-batch tests and
four additional shared-state invocations prove that arguments are not discarded. Java
compiles for release 11; the release/default/mimalloc Rust core suite passes 1,017 tests
with two pre-existing ignores.

Reproduce with `integral_expression`, for example
`SF_INTEGRAL_EXPRESSION='IS_DIGIT(v)'`, and `IntegralExpressionPipelineBenchmark` with
`sf.integral.expression=IS_DIGIT(v)`. Prepended independently compiled previous-encoder
classes and `SF_INTEGRAL_PREVIOUS=true` select the preceding callback control. Remaining
coverage stays in [the classification follow-up](https://github.com/datafusion-contrib/StreamFusion/issues/494).

## Numeric IS_DECIMAL

Released Flink receives boxed primitive integers or internal `DecimalData` in this overload.
Integral arguments return TRUE regardless of sign or runtime/literal NULL; internal DecimalData arguments return FALSE.
Direct inputs/literals keep a native type-based predicate. Only the recursively verified
failure-free integer producer scope folds away unused arithmetic; division, remainder,
narrowing, UDFs and other unverified producers retain the complete generated call, including
failures and state changes. Floating signatures remain generated. This preserves the released
behavior rather than inferring it from Java's public BigDecimal API.

The [result record](results/numeric-classification-decimal-2026-11-01.json) and
[raw trials](results/numeric-classification-decimal-2026-11-01.csv.gz) retain 80 configurations and 8,000 main
trials. Direct runtime arguments retain the native call; arithmetic fold controls are
explicitly excluded from predicate-kernel qualification. The direct matrix covers four
integer widths and DECIMAL(18,2)/(38,18), nullable/NOT NULL and GenericRowData/BinaryRowData
at 1,024 rows. Tiny INT/BIGINT controls use 16 rows. Typed addition/nested multiplication
provide separate fold diagnostics. No input values are converted to strings.

Release/default-feature/mimalloc, JDK 17.0.12, Apple M4 Pro, one thread, 16 warmups/nine
measurements and 131,072 rows/trial compare actual released generated Java on prepared
RowData with production Calc on matching Arrow. Native operation and equivalent Boolean
result consumption are timed inside Rust; initial JNI/import/export and perimeter
transposes are outside this small-operation timer. Intrinsic previous JVM computation
stays inside. The actual independently compiled preceding-main encoder must bind an
evaluator; the candidate requires empty descriptors and the intended function before the
bounded fold. Both sides retain exact types, widths, signed boundaries and matching NULLs.

| Flink line | Rows / measured work | Stock/native median range | Previous/native median range | Slower than stock |
| --- | --- | ---: | ---: | ---: |
| 1.18.1 | 1,024 / predicate | 4.444-64.321x | 55.610-91.331x | 0 / 24 |
| 1.18.1 | 16 / predicate | 0.155-0.190x | 29.598-32.049x | 8 / 8 |
| 1.18.1 | 1,024 / fold control | 5.805-14.961x | 80.176-100.769x | 0 / 8 |
| 2.2.1 | 1,024 / predicate | 4.555-58.821x | 59.944-78.244x | 0 / 24 |
| 2.2.1 | 16 / predicate | 0.143-0.187x | 30.265-32.526x | 8 / 8 |
| 2.2.1 | 1,024 / fold control | 6.007-11.253x | 74.172-127.370x | 0 / 8 |

Ratios above one favor native. The type-based predicate returns a Boolean scalar which
production Calc materializes to the batch length; the measurements do not claim a
per-row character scan. Small-batch fixed Calc/result costs and every unfavorable sample
remain in the record. Independent paired-stock drift ranges from
0.558 to 1.749x; cross-process
ratios are not causal isolation.

Separate million-row nullable INT source/blackhole jobs retain production JNI, planning,
startup and both transposes, with parallelism one and two warmups/five measurements.
Direct jobs retain the predicate. Add/nested jobs exercise the bounded fold and must not
be cited as predicate-kernel work. All require consumed native Calc work, no fallback and
empty candidate evaluator descriptors.

| Flink line / input | Candidate stock median (range), s | Native median (range), s | Previous stock median (range), s | Previous median (range), s |
| --- | ---: | ---: | ---: | ---: |
| 1.18.1 / direct | 0.152290 (0.143413-0.157737) | 0.164787 (0.151287-0.177813) | 0.143699 (0.140306-0.158073) | 0.182686 (0.176609-0.207314) |
| 1.18.1 / add | 0.150613 (0.146701-0.159775) | 0.158213 (0.153216-0.160860) | 0.146302 (0.140714-0.157664) | 0.187217 (0.173755-0.193327) |
| 1.18.1 / nested | 0.143415 (0.139697-0.151992) | 0.147550 (0.145604-0.167166) | 0.143714 (0.141247-0.143864) | 0.183689 (0.171484-0.196876) |
| 2.2.1 / direct | 0.163562 (0.152313-0.169804) | 0.164748 (0.160193-0.185271) | 0.165385 (0.151227-0.184964) | 0.193048 (0.190476-0.220503) |
| 2.2.1 / add | 0.162638 (0.155080-0.175627) | 0.148933 (0.144973-0.164875) | 0.160004 (0.145057-0.171511) | 0.189043 (0.184488-0.221272) |
| 2.2.1 / nested | 0.156228 (0.154461-0.169524) | 0.158591 (0.154876-0.165304) | 0.148495 (0.145414-0.173559) | 0.187562 (0.184164-0.199794) |

Across these jobs, native/stock elapsed-time ratios range from
0.916 to 1.082; native/previous ratios range
from 0.788 to 0.902. Ratios above one are
slower. Retain overlapping ranges and independent stock drift; prepared bulk qualification
does not establish universal whole-job acceleration.

All 136 focused JVM invocations and unchanged released `IS_DECIMAL` expression tests
pass on Flink 1.18.1/2.2.1 with JDK 11/17. Eight numeric declarations, 5,003 rows, nullable
and NOT NULL inputs, literal NULLs and Boolean consumers compare complete values, resolved
types, cardinality and unique IDs across multiple batches. Direct runtime witnesses require
consumed work, no fallback and empty descriptors. Separate failure/empty-batch tests and
four additional shared-state invocations prove that arguments are not discarded. Java
compiles for release 11; the release/default/mimalloc Rust core suite passes 1,017 tests
with two pre-existing ignores.

Reproduce with `integral_expression`, for example
`SF_INTEGRAL_EXPRESSION='IS_DECIMAL(v)'`, and `IntegralExpressionPipelineBenchmark` with
`sf.integral.expression=IS_DECIMAL(v)`. Prepended independently compiled previous-encoder
classes and `SF_INTEGRAL_PREVIOUS=true` select the preceding callback control. Remaining
coverage stays in [the classification follow-up](https://github.com/datafusion-contrib/StreamFusion/issues/494).

## BOOLEAN CAST to character

Direct BOOLEAN inputs/literals use a native formatter for STRING and bounded VARCHAR/CHAR.
Modern mode emits uppercase text, truncates overlength values and pads bounded CHAR;
legacy mode emits lowercase text without target-length enforcement. NULL remains NULL.
Unverified computed operands retain generated Flink evaluation; independent TRY_CAST and logical-operand qualification follow below.
No production switch is added. Both modes use Flink's existing cast configuration.

The [result record](results/boolean-string-cast-2026-11-01.json) and
[raw trials](results/boolean-string-cast-2026-11-01.csv.gz) retain 152 configurations and
15,200 main trials. Modern bulk cells cover STRING, VARCHAR(1/3/4/5/8) and
CHAR(1/3/4/5/8/32); legacy cells cover STRING, VARCHAR(3) and CHAR(8), including their
ignored target lengths. Both released lines use matching nullable/NOT NULL declarations,
alternating true/false values and actual NULLs every seven rows where selected, with
GenericRowData and BinaryRowData controls. Tiny STRING/CHAR(8) cells retain both modes.

Release/default-feature/mimalloc, JDK 17.0.12, Apple M4 Pro, one thread, 16 warmups/nine
measurements and 131,072 rows/trial compare released generated Java on prebuilt RowData
with production Calc on matching Arrow buffers. Both consume UTF-8 length and boundary
bytes; Java uses BinaryStringData access without copying to a byte array. The Rust timer
covers the native operation and equivalent consumption, excluding initial JNI/import/export
and both perimeter transposes. Intrinsic previous JVM computation stays inside timing.
The actual independently compiled preceding encoder must bind a JVM evaluator; the
candidate requires the intended function and empty descriptors. Every checksum agrees.

| Flink line | Mode | Batch rows | Cells | Stock/native median range | Previous/native median range | Slower than stock |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| 1.18.1 | modern | 1024 | 48 | 1.626-11.810x | 9.879-19.183x | 0 |
| 1.18.1 | modern | 16 | 8 | 0.190-1.836x | 22.304-24.939x | 4 |
| 1.18.1 | legacy | 1024 | 12 | 6.949-17.607x | 12.056-20.007x | 0 |
| 1.18.1 | legacy | 16 | 8 | 0.825-1.385x | 20.465-24.113x | 4 |
| 2.2.1 | modern | 1024 | 48 | 1.551-10.159x | 10.185-19.556x | 0 |
| 2.2.1 | modern | 16 | 8 | 0.198-1.764x | 21.865-23.653x | 4 |
| 2.2.1 | legacy | 1024 | 12 | 8.237-11.477x | 12.273-17.424x | 0 |
| 2.2.1 | legacy | 16 | 8 | 0.838-1.031x | 22.531-25.358x | 6 |

Ratios above one favor native. All 120 bulk cells pass the prepared-operation gate;
18 of 32 tiny cells remain slower than stock. Paired stock-control drift ranges from
0.597 to 1.844x. Keep every range and unfavorable case rather than treating
cross-process ratios as causal isolation.

The initial row writer loses the modern cached-STRING probe. The
[bitmap block technique](../optimizations/boolean-word-blocks.md) reduces the matched
native median from 6.162 to 2.446 ns/row; paired stock/native moves from 0.657x to 1.601x.
The [initial](results/boolean-string-cast-probe-2026-11-01.csv.gz) and
[optimized](results/boolean-string-cast-blocks-probe-2026-11-01.csv.gz) diagnostics retain
eight cells each, their stock drift and the untimed allocation/buffer observations.
They remain separate from the main matrix. The output validity is shared; masked NULL
positions can carry payload, so fewer requested bytes do not imply smaller output buffers.

Separate million-row nullable BOOLEAN source/blackhole jobs retain production JNI,
planning/startup and both transposes, parallelism one, two warmups and five measurements.
They require consumed NativeCalc work, no fallback and empty candidate descriptors.

| Flink line / input | Candidate stock median (range), s | Native median (range), s | Previous stock median (range), s | Previous median (range), s |
| --- | ---: | ---: | ---: | ---: |
| 1.18.1 / modern-string | 0.158820 (0.157408-0.167913) | 0.159093 (0.154768-0.173915) | 0.157794 (0.144974-0.175266) | 0.191205 (0.188041-0.197415) |
| 1.18.1 / modern-varchar3 | 0.173374 (0.170471-0.180415) | 0.158684 (0.156780-0.167511) | 0.173464 (0.172478-0.178372) | 0.219246 (0.216342-0.222373) |
| 1.18.1 / modern-char8 | 0.198373 (0.190325-0.201441) | 0.166334 (0.157919-0.168544) | 0.190850 (0.184823-0.195628) | 0.230782 (0.227496-0.245033) |
| 1.18.1 / legacy-string | 0.183357 (0.172087-0.197666) | 0.169239 (0.158804-0.179719) | 0.186869 (0.176384-0.253488) | 0.190473 (0.182322-0.195269) |
| 1.18.1 / legacy-char8 | 0.175425 (0.173670-0.219901) | 0.167376 (0.157756-0.171659) | 0.178774 (0.171553-0.242362) | 0.200614 (0.194548-0.215857) |
| 2.2.1 / modern-string | 0.161098 (0.151663-0.162549) | 0.162494 (0.156651-0.167074) | 0.155657 (0.150932-0.157231) | 0.193431 (0.183526-0.199987) |
| 2.2.1 / modern-varchar3 | 0.181900 (0.180263-0.197655) | 0.161911 (0.158735-0.177116) | 0.181335 (0.174057-0.184622) | 0.220254 (0.214121-0.229879) |
| 2.2.1 / modern-char8 | 0.194581 (0.188651-0.204043) | 0.162995 (0.158961-0.171878) | 0.193474 (0.183651-0.200218) | 0.227816 (0.222634-0.248081) |
| 2.2.1 / legacy-string | 0.170059 (0.169012-0.170231) | 0.161957 (0.159721-0.165669) | 0.178505 (0.172566-0.190951) | 0.189525 (0.187156-0.201872) |
| 2.2.1 / legacy-char8 | 0.179689 (0.173855-0.195317) | 0.160113 (0.159534-0.168144) | 0.171615 (0.170076-0.182689) | 0.182692 (0.178134-0.195459) |

Across these jobs, native/stock elapsed ratios range from 0.838
to 1.009, and native/previous ratios range from
0.715 to 0.889. Ratios above one are slower.
These short full jobs retain startup and overlapping ranges; prepared bulk qualification
does not establish universal complete-job acceleration.

The initial 172 focused JVM invocations pass, including existing numeric and decimal-cast
regressions. After the bitmap optimization, all 40 focused invocations pass on released
Flink 1.18.1/2.2.1 with JDK 11/17, including added string consumers. Runtime-source
witnesses compare complete 5,003-row outputs, resolved types, NULLs, cardinality and unique
IDs across batches, and require consumed native work, no fallback and empty descriptors.
The unchanged released CastRulesTest supplies 535 passing stock-oracle cases on each
Flink/JDK pair; the separate runtime witnesses establish native execution. Four additional
shared-state invocations pass. Twelve existing integer/predicate fixture smokes retain
their types and checksums. Java compiles for release 11; release/default/mimalloc Rust core
passes 1,019 tests with two pre-existing ignores, including every bitmap word and unaligned
tail. Strict docs, formatting and source/privacy checks pass.

Reproduce the prepared path with `integral_expression`, BOOLEAN input and
`SF_INTEGRAL_EXPRESSION='CAST(v AS STRING)'`; set `SF_INTEGRAL_LEGACY=true` for the legacy
control. Complete jobs use `IntegralExpressionPipelineBenchmark`, `SF_BENCHMARK=true`,
`sf.integral.booleanInput=true`, `sf.integral.expression=CAST(v AS CHAR(8))` and
`sf.integral.legacy=true` where selected. These are benchmark controls, not production
options. Remaining cast coverage stays in [#492](https://github.com/datafusion-contrib/StreamFusion/issues/492).

## BOOLEAN TRY_CAST to character

Direct BOOLEAN inputs/literals now use the same native STRING/VARCHAR/CHAR formatter for
TRY_CAST as for CAST. Formatting cannot produce a conversion error. Modern and legacy
spelling, truncation/padding and NULL behavior match the released generated code.
Unverified computed operands keep the complete generated expression; TRY_CAST must not swallow
failures while evaluating an operand or drop shared stateful calls. An unconfigured
encoder also keeps generated evaluation. No production option is added.

The [independent result record](results/boolean-try-string-cast-2026-11-01.json) and
[raw trials](results/boolean-try-string-cast-2026-11-01.csv.gz) retain 152 configurations
and 15,200 trials for TRY_CAST itself. The baseline is the actual preceding CAST encoder,
which still binds a JVM evaluator for TRY_CAST; the candidate requires empty descriptors
and the intended native function. Both configure the same released cast mode. The Rust
kernel, release DSO and benchmark executable are unchanged from CAST qualification;
source and DSO hashes are checked rather than rerunning the identical Rust suite.

The prepared boundary and matching BOOLEAN/NULL/cached-or-binary RowData inputs are the
same as CAST above: release/default/mimalloc, JDK 17.0.12, Apple M4 Pro, one thread,
16 warmups/nine measurements and 131,072 rows/trial. Rust times the production operation
and equivalent UTF-8 boundary consumption. Initial JNI/import/export and both perimeter
transposes are outside this timer; intrinsic previous JVM calls remain timed. All
checksums agree. Tiny controls and independent stock drift remain separate from bulk
qualification.

| Flink line | Mode | Batch rows | Cells | Stock/native median range | Previous/native median range | Slower than stock |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| 1.18.1 | modern | 1024 | 48 | 1.646-8.972x | 8.592-18.641x | 0 |
| 1.18.1 | modern | 16 | 8 | 0.194-2.023x | 22.171-25.598x | 4 |
| 1.18.1 | legacy | 1024 | 12 | 7.273-11.839x | 16.625-24.167x | 0 |
| 1.18.1 | legacy | 16 | 8 | 0.807-1.134x | 23.742-26.719x | 4 |
| 2.2.1 | modern | 1024 | 48 | 1.604-13.670x | 8.472-18.768x | 0 |
| 2.2.1 | modern | 16 | 8 | 0.198-1.783x | 22.358-26.250x | 4 |
| 2.2.1 | legacy | 1024 | 12 | 8.882-12.173x | 15.719-21.836x | 0 |
| 2.2.1 | legacy | 16 | 8 | 0.846-1.052x | 23.919-27.109x | 6 |

All 120 bulk cells pass the prepared-operation gate;
18 of 32 tiny cells are slower
than stock. Ratios above one favor native. Paired stock-control drift spans
0.674-1.940x; cross-process ratios are not causal isolation.

Ten million-row nullable BOOLEAN source/blackhole configurations retain production JNI,
planning/startup and both transposes, parallelism one, two warmups/five measurements.
They require consumed NativeCalc work, no fallback and empty candidate descriptors.

| Flink line / input | Candidate stock median (range), s | Native median (range), s | Previous stock median (range), s | Previous median (range), s |
| --- | ---: | ---: | ---: | ---: |
| 1.18.1 / modern-string | 0.164568 (0.152968-0.178889) | 0.155482 (0.154097-0.169737) | 0.163534 (0.150881-0.166770) | 0.178076 (0.175590-0.190067) |
| 1.18.1 / modern-varchar3 | 0.172382 (0.168989-0.187415) | 0.163684 (0.158259-0.172744) | 0.182536 (0.172397-0.188274) | 0.205375 (0.204545-0.218700) |
| 1.18.1 / modern-char8 | 0.199662 (0.192069-0.280178) | 0.159020 (0.156592-0.169004) | 0.193263 (0.188125-0.200050) | 0.224147 (0.213284-0.226749) |
| 1.18.1 / legacy-string | 0.181813 (0.172095-0.202011) | 0.161006 (0.159682-0.172754) | 0.173408 (0.164714-0.192122) | 0.208225 (0.194695-0.216245) |
| 1.18.1 / legacy-char8 | 0.179340 (0.176745-0.217252) | 0.166424 (0.164905-0.172886) | 0.183752 (0.171472-0.191367) | 0.205981 (0.200621-0.224427) |
| 2.2.1 / modern-string | 0.167586 (0.155133-0.190176) | 0.164438 (0.157190-0.166998) | 0.155230 (0.150007-0.166039) | 0.181543 (0.179630-0.195766) |
| 2.2.1 / modern-varchar3 | 0.176160 (0.168682-0.181995) | 0.163558 (0.160969-0.169159) | 0.174333 (0.167268-0.188351) | 0.207104 (0.199274-0.218739) |
| 2.2.1 / modern-char8 | 0.195855 (0.191255-0.209562) | 0.158579 (0.157120-0.160678) | 0.192746 (0.189712-0.197713) | 0.216276 (0.211593-0.218344) |
| 2.2.1 / legacy-string | 0.171966 (0.168650-0.194207) | 0.160860 (0.154903-0.172927) | 0.169731 (0.160435-0.179396) | 0.206011 (0.197393-0.216290) |
| 2.2.1 / legacy-char8 | 0.175236 (0.169376-0.186207) | 0.159021 (0.156173-0.166410) | 0.174426 (0.166370-0.183576) | 0.193287 (0.187956-0.213551) |

Native/stock elapsed ratios range from 0.796 to
0.981; native/previous ratios range from
0.709 to 0.906. Ratios above one are slower.
Keep overlapping ranges and unfavorable outcomes; prepared qualification does not prove
universal full-job acceleration.

All 68 focused JVM invocations pass across Flink 1.18.1/2.2.1 and JDK 11/17, including
CAST regression controls. Complete 5,003-row outputs, resolved types, cardinality, unique
IDs and NULLs match stock across batches; direct runtime witnesses require consumed native
work with no fallback or evaluator descriptor. Selected operand division errors, lazy
branches, empty filtered batches, string consumers and unconfigured encoder behavior are
covered. Four additional shared-state invocations pass. The unchanged released CastRules
stock oracle (535 cases per pair) and 1,019 Rust core passes/two pre-existing ignores are
reused from the identical kernel qualification; they are separate from the new native
TRY_CAST witnesses. Java compiles for release 11. Strict docs, diff and privacy checks pass.

Reproduce with `SF_INTEGRAL_EXPRESSION='TRY_CAST(v AS STRING)'` and BOOLEAN input for
`integral_expression`; complete jobs use `sf.integral.booleanInput=true` and
`sf.integral.expression=TRY_CAST(v AS CHAR(8))`. The existing benchmark-only legacy control
selects the corresponding mode. Remaining casts stay in
[#492](https://github.com/datafusion-contrib/StreamFusion/issues/492).

## BOOLEAN CAST over logical operands

The formatter accepts recursively verified BOOLEAN-only NOT, AND, OR, IS NULL,
IS NOT NULL, IS TRUE, IS NOT TRUE, IS FALSE and IS NOT FALSE over BOOLEAN references
and literals. These operations cannot fail or mutate shared state. At this logical-only qualification, numeric comparisons, division, UDFs and CASE kept their generated
or short-circuit fallback routes. The proof never discards a computed operand.

The [result record](results/boolean-logical-cast-2026-11-01.json) and
[raw trials](results/boolean-logical-cast-2026-11-01.csv.gz) retain 76 configurations
and 7,600 trials. Representative producers are NOT(v) and nullable
v IS TRUE. The initial Flink 2.2 NOT NULL v IS TRUE control simplifies to v before encoding and
is already native in the actual preceding encoder. NOT NULL truth-test cells are
excluded from new-feature performance coverage on both lines.
The initial strict previous-route assertion exposed that simplification rather than
allowing it to count as callback removal. Its independent candidate-only
[diagnostic](results/boolean-logical-cast-simplified-control-2026-11-01.csv.gz) retains
all 50 samples separately. Correctness witnesses still cover both declarations.

Release/default-feature/mimalloc, JDK 17.0.12, Apple M4 Pro, one thread, 16 warmups/nine
measurements and 131,072 rows/trial use the prepared Rust/Arrow versus released
Java/RowData boundary described above. Modern targets are STRING/VARCHAR(3)/CHAR(8);
legacy targets are STRING/CHAR(8). Cached and binary RowData controls match logical
values and NULLs. The actual preceding encoder must bind a JVM evaluator; candidate
encoding must have none. Equivalent UTF-8 boundary consumption and every checksum agree.
The identical production Rust kernel/DSO is reused, with source and DSO hashes checked.

| Flink | Workload | Cells | Stock/native median range | Previous/native median range | Slower than stock |
| --- | --- | ---: | ---: | ---: | ---: |
| 1.18.1 | logical bulk | 30 | 1.456-11.667x | 11.497-20.919x | 0 |
| 1.18.1 | tiny NOT control | 8 | 0.175-0.918x | 19.655-21.435x | 8 |
| 2.2.1 | logical bulk | 30 | 1.460-10.751x | 12.686-17.764x | 0 |
| 2.2.1 | tiny NOT control | 8 | 0.180-0.896x | 19.915-22.033x | 8 |

All 60 bulk cells pass the prepared-operation gate;
16 of 16 tiny cells are slower
than stock. Ratios above one favor native. Independent stock-control drift spans
0.573-1.499x. These measurements qualify the representative logical
producers; they do not assert a speedup for every expression tree.

Six million-row source/blackhole configurations retain planning/startup, production JNI
and both transposes. Modern NOT-to-STRING, modern truth-test-to-CHAR(8) and legacy
NOT-to-STRING run at parallelism one with two warmups/five measurements. Runtime witnesses
require consumed native Calc work, no fallback and empty candidate evaluator descriptors.

| Flink / input | Candidate stock median (range), s | Native median (range), s | Previous stock median (range), s | Previous median (range), s |
| --- | ---: | ---: | ---: | ---: |
| 1.18.1 / modern-not | 0.155199 (0.152203-0.180638) | 0.157988 (0.153672-0.165963) | 0.156686 (0.155055-0.184734) | 0.187435 (0.182388-0.190953) |
| 1.18.1 / modern-truth | 0.239740 (0.234626-0.249306) | 0.193730 (0.190997-0.201901) | 0.240761 (0.235625-0.248413) | 0.275991 (0.274064-0.291803) |
| 1.18.1 / legacy-not | 0.172859 (0.170668-0.216929) | 0.165043 (0.159584-0.165952) | 0.193202 (0.175767-0.262754) | 0.191391 (0.189054-0.218440) |
| 2.2.1 / modern-not | 0.162671 (0.154038-0.169849) | 0.155008 (0.153929-0.162233) | 0.159649 (0.156157-0.168375) | 0.196254 (0.186756-0.199247) |
| 2.2.1 / modern-truth | 0.251202 (0.245248-0.265937) | 0.206193 (0.203454-0.212622) | 0.245309 (0.244001-0.258694) | 0.269516 (0.266690-0.276784) |
| 2.2.1 / legacy-not | 0.177856 (0.172548-0.199941) | 0.163611 (0.160215-0.168126) | 0.176808 (0.172573-0.188163) | 0.191048 (0.186868-0.204655) |

Native/stock elapsed ratios span 0.808-1.018;
native/previous ratios span 0.702-0.862.
Ratios above one are slower. Retain overlapping ranges and all unfavorable cases;
bulk qualification does not establish universal full-job acceleration.

All 84 focused JVM invocations and four additional shared-state invocations pass on
released Flink 1.18.1/2.2.1 with JDK 11/17. Runtime witnesses compare complete 5,003-row
outputs, resolved types, NULLs, cardinality and unique IDs across batches; independent
BOOLEAN columns cover AND/OR combinations and nesting. They require consumed native work,
no fallback and empty descriptors for every admitted logical query. Failing comparisons
under logical wrappers, lazy/empty branches and stateful producers retain their exact
prior route and successful/exception behavior. The unchanged released
ScalarFunctionsTest.testIsTrueIsFalse and ScalarOperatorsTest.testFunctionWithBooleanExpression
also pass on all four pairs; they are separate from the new native execution witnesses.
Java targets release 11. The unchanged kernel retains its prior 1,019 Rust core passes,
two pre-existing ignores and released CastRules stock oracle. Strict docs, diff, source
and privacy checks pass.

Reproduce prepared qualification with `integral_expression`, BOOLEAN input and
`SF_INTEGRAL_EXPRESSION='CAST(NOT v AS STRING)'`. Complete jobs use
`sf.integral.booleanInput=true` and `sf.integral.expression=CAST(v IS TRUE AS CHAR(8))`.
No production option is introduced. Remaining conversions are tracked in
[#492](https://github.com/datafusion-contrib/StreamFusion/issues/492).

## BOOLEAN TRY_CAST over logical operands

The formatter accepts recursively verified BOOLEAN-only NOT, AND, OR, IS NULL,
IS NOT NULL, IS TRUE, IS NOT TRUE, IS FALSE and IS NOT FALSE over BOOLEAN references
and literals. These operations cannot fail or mutate shared state. At this logical-only qualification, numeric comparisons, division, UDFs and CASE kept their generated
or short-circuit fallback routes. The proof never discards a computed operand.

The [result record](results/boolean-logical-try-cast-2026-11-01.json) and
[raw trials](results/boolean-logical-try-cast-2026-11-01.csv.gz) retain 76 configurations
and 7,600 trials. Representative producers are NOT(v) and nullable
v IS TRUE. The initial Flink 2.2 NOT NULL v IS TRUE control simplifies to v before encoding and
is already native in the actual preceding encoder. NOT NULL truth-test cells are
excluded from new-feature performance coverage on both lines.
The preceding CAST strict previous-route assertion exposed that simplification rather than
allowing it to count as callback removal. Its independent candidate-only
[diagnostic](results/boolean-logical-cast-simplified-control-2026-11-01.csv.gz) retains
all 50 samples separately. Correctness witnesses still cover both declarations.

Release/default-feature/mimalloc, JDK 17.0.12, Apple M4 Pro, one thread, 16 warmups/nine
measurements and 131,072 rows/trial use the prepared Rust/Arrow versus released
Java/RowData boundary described above. Modern targets are STRING/VARCHAR(3)/CHAR(8);
legacy targets are STRING/CHAR(8). Cached and binary RowData controls match logical
values and NULLs. The actual preceding encoder must bind a JVM evaluator; candidate
encoding must have none. Equivalent UTF-8 boundary consumption and every checksum agree.
The identical production Rust kernel/DSO is reused, with source and DSO hashes checked.

| Flink | Workload | Cells | Stock/native median range | Previous/native median range | Slower than stock |
| --- | --- | ---: | ---: | ---: | ---: |
| 1.18.1 | logical bulk | 30 | 1.429-18.184x | 8.887-21.064x | 0 |
| 1.18.1 | tiny NOT control | 8 | 0.175-0.939x | 19.482-25.024x | 8 |
| 2.2.1 | logical bulk | 30 | 1.453-12.490x | 9.313-21.843x | 0 |
| 2.2.1 | tiny NOT control | 8 | 0.175-1.104x | 19.154-27.388x | 7 |

All 60 bulk cells pass the prepared-operation gate;
15 of 16 tiny cells are slower
than stock. Ratios above one favor native. Independent stock-control drift spans
0.632-1.992x. These measurements qualify the representative logical
producers; they do not assert a speedup for every expression tree.

Six million-row source/blackhole configurations retain planning/startup, production JNI
and both transposes. Modern NOT-to-STRING, modern truth-test-to-CHAR(8) and legacy
NOT-to-STRING run at parallelism one with two warmups/five measurements. Runtime witnesses
require consumed native Calc work, no fallback and empty candidate evaluator descriptors.

| Flink / input | Candidate stock median (range), s | Native median (range), s | Previous stock median (range), s | Previous median (range), s |
| --- | ---: | ---: | ---: | ---: |
| 1.18.1 / modern-not | 0.192990 (0.170395-0.289754) | 0.188779 (0.175542-0.276829) | 0.168986 (0.155901-0.175384) | 0.190369 (0.185188-0.193414) |
| 1.18.1 / modern-truth | 0.240194 (0.235374-0.249937) | 0.189541 (0.177280-0.190945) | 0.218437 (0.205282-0.244150) | 0.235512 (0.227074-0.263892) |
| 1.18.1 / legacy-not | 0.211747 (0.189535-0.228928) | 0.179852 (0.169331-0.181138) | 0.185579 (0.179722-0.194138) | 0.224362 (0.216911-0.230450) |
| 2.2.1 / modern-not | 0.164041 (0.156972-0.166769) | 0.162113 (0.158810-0.163989) | 0.178893 (0.172315-0.193451) | 0.206872 (0.204361-0.213012) |
| 2.2.1 / modern-truth | 0.234089 (0.214258-0.237513) | 0.168890 (0.167765-0.186218) | 0.234878 (0.214046-0.243389) | 0.250744 (0.240538-0.263417) |
| 2.2.1 / legacy-not | 0.226982 (0.200094-0.244595) | 0.205476 (0.184333-0.210453) | 0.198807 (0.191128-0.219500) | 0.244790 (0.222760-0.250576) |

Native/stock elapsed ratios span 0.721-0.988;
native/previous ratios span 0.674-0.992.
Ratios above one are slower. Retain overlapping ranges and all unfavorable cases;
bulk qualification does not establish universal full-job acceleration.

All 92 focused JVM invocations and four additional shared-state invocations pass on
released Flink 1.18.1/2.2.1 with JDK 11/17. Runtime witnesses compare complete 5,003-row
outputs, resolved types, NULLs, cardinality and unique IDs across batches; independent
BOOLEAN columns cover AND/OR combinations and nesting. They require consumed native work,
no fallback and empty descriptors for every admitted logical query. Failing comparisons
under logical wrappers, lazy/empty branches and stateful producers retain their exact
prior route and successful/exception behavior. The unchanged released
ScalarFunctionsTest.testIsTrueIsFalse and ScalarOperatorsTest.testFunctionWithBooleanExpression
retain their unchanged preceding CAST qualification on all four pairs; the new TRY_CAST runtime witnesses establish the changed native route.
Java targets release 11. The unchanged kernel retains its prior 1,019 Rust core passes,
two pre-existing ignores and released CastRules stock oracle. Strict docs, diff, source
and privacy checks pass.

Reproduce prepared qualification with `integral_expression`, BOOLEAN input and
`SF_INTEGRAL_EXPRESSION='TRY_CAST(NOT v AS STRING)'`. Complete jobs use
`sf.integral.booleanInput=true` and `sf.integral.expression=TRY_CAST(v IS TRUE AS CHAR(8))`.
No production option is introduced. Remaining conversions are tracked in
[#492](https://github.com/datafusion-contrib/StreamFusion/issues/492).

## Bounded DECIMAL CAST to character

Direct DECIMAL inputs/literals reuse the exact native plain-text formatter. Modern
VARCHAR truncates through LEFT; modern CHAR truncates or space-pads through RPAD.
DECIMAL text is ASCII, so these existing kernels preserve released Flink character
widths without a Unicode approximation. Legacy mode ignores target length; maximum-INT
CHAR does not pad. This direct-input qualification does not measure computed decimals;
the later unary-operand qualification is recorded separately below. Other computed
decimals and their metadata guards retain existing admission.
Bounded TRY_CAST remains a separate qualification.

The [result record](results/decimal-bounded-cast-2026-11-01.json) and
[raw trials](results/decimal-bounded-cast-2026-11-01.csv.gz) retain 96 configurations/9600 trials.
Release/default-feature/mimalloc, Oracle JDK 17.0.12, Apple M4 Pro, one thread,
16 warmups/nine measurements and 131,072 rows/trial use the prepared Rust/Arrow
versus released Java/RowData boundary. DECIMAL(18,2)/(38,18), signed extrema and
large signed coefficients match across GenericRowData/BinaryRowData and nullable/NOT NULL
schemas. Modern targets are VARCHAR(3)/CHAR(8)/CHAR(44); legacy targets VARCHAR(3)/CHAR(8).
All output checksums match. Candidate descriptors must be empty; the independently
compiled preceding encoder must retain its actual JVM route. Tiny 16-row CHAR(8)
controls remain separate from 1,024-row bulk qualification.

| Flink | Workload | Cells | Stock/native median range | Previous/native median range | Slower than stock |
| --- | --- | ---: | ---: | ---: | ---: |
| 1.18.1 | bulk | 40 | 3.076-9.964x | 4.247-11.757x | 0 |
| 1.18.1 | tiny control | 8 | 1.421-3.349x | 10.676-13.234x | 0 |
| 2.2.1 | bulk | 40 | 3.073-10.201x | 4.239-11.687x | 0 |
| 2.2.1 | tiny control | 8 | 1.427-3.294x | 10.489-12.713x | 0 |

All 80 bulk cells beat stock. 0 of 16 tiny controls are slower.
Independent stock-control drift spans 0.731-1.130x; all ranges remain in the record.

Six complete million-row jobs retain source generation, planning/startup, production JNI
and both perimeter transposes, using a rowwise source and blackhole at parallelism one.
Each has two warmups/five measurements. Compact modern VARCHAR(3), wide modern CHAR(44)
and wide legacy CHAR(8) require consumed NativeCalc, no fallback and no candidate evaluator.

| Flink / input | Candidate stock median (range), s | Native median (range), s | Previous stock median (range), s | Previous median (range), s |
| --- | ---: | ---: | ---: | ---: |
| 1.18.1 / modern-compact | 0.310117 (0.303876-0.311701) | 0.215852 (0.211178-0.227059) | 0.315230 (0.311484-0.321128) | 0.375995 (0.373839-0.383962) |
| 1.18.1 / modern-wide | 0.566128 (0.550808-0.568562) | 0.270693 (0.266628-0.276478) | 0.554017 (0.540817-0.565504) | 0.621521 (0.619297-0.633610) |
| 1.18.1 / legacy-wide | 0.278262 (0.263808-0.285093) | 0.218404 (0.214724-0.225595) | 0.272596 (0.267901-0.296278) | 0.310646 (0.304137-0.321228) |
| 2.2.1 / modern-compact | 0.316695 (0.315670-0.330976) | 0.215533 (0.210773-0.216891) | 0.315654 (0.311846-0.325644) | 0.355857 (0.346658-0.382724) |
| 2.2.1 / modern-wide | 0.565624 (0.549676-0.577944) | 0.263300 (0.249329-0.267397) | 0.555790 (0.545149-0.560500) | 0.622109 (0.613828-0.638969) |
| 2.2.1 / legacy-wide | 0.277581 (0.270206-0.302031) | 0.216288 (0.210198-0.222025) | 0.280246 (0.269209-0.293993) | 0.308279 (0.306989-0.325054) |

Native/stock elapsed ratios span 0.466-0.785;
native/previous ratios span 0.423-0.703. Ratios above one are slower.
Overlapping ranges and unfavorable whole-job results remain; prepared qualification
does not establish universal end-to-end acceleration.

All 132 focused encoding/runtime invocations (including existing unbounded/failure controls) pass on released Flink 1.18.1/2.2.1 with JDK 11/17
(Java release 11). Complete 5,003-row outputs verify declared types, NULLs, unique IDs
and cardinality across batches with consumed native work and empty evaluator descriptors.
Additional consumers, truncation/padding, lazy and empty-batch cases, selected operand
failures and materialized aggregate results match stock. Flink 1.18 does not support
MAX(CHAR); that boundary witness first widens the padded CHAR to STRING before MAX.
Twelve existing integer/predicate fixture controls pass. The unchanged released
CastRulesTest oracle retains its prior 535 stock cases per pair; the unchanged Rust
DSO retains 1,019 core passes/two pre-existing ignores. Neither reused oracle substitutes
for the new native execution witnesses. Source/DSO hashes and independently recompiled
preceding encoder classes are retained.

Reproduce prepared qualification with `integral_expression`, DECIMAL input and
`SF_INTEGRAL_EXPRESSION='CAST(v AS CHAR(8))'`. Complete jobs use
`sf.integral.decimalPrecision=38`, `sf.integral.decimalScale=18` and the same expression.
These are benchmark-only controls; no production option is introduced. Remaining work
is tracked in [#492](https://github.com/datafusion-contrib/StreamFusion/issues/492).

## Bounded DECIMAL TRY_CAST to character

Direct DECIMAL inputs/literals reuse the exact native plain-text formatter. Modern
VARCHAR truncates through LEFT; modern CHAR truncates or space-pads through RPAD.
DECIMAL text is ASCII, so these existing kernels preserve released Flink character
widths without a Unicode approximation. Legacy mode ignores target length; maximum-INT
CHAR does not pad. This direct-input TRY_CAST qualification does not measure computed
decimals; the later unary-operand qualification is recorded separately below. Other
computed decimals and their metadata guards retain existing admission.
TRY_CAST uses the same total formatting operation and does not catch operand failures.

The [result record](results/decimal-bounded-try-cast-2026-11-01.json) and
[raw trials](results/decimal-bounded-try-cast-2026-11-01.csv.gz) retain 96 configurations/9600 trials.
Release/default-feature/mimalloc, Oracle JDK 17.0.12, Apple M4 Pro, one thread,
16 warmups/nine measurements and 131,072 rows/trial use the prepared Rust/Arrow
versus released Java/RowData boundary. DECIMAL(18,2)/(38,18), signed extrema and
large signed coefficients match across GenericRowData/BinaryRowData and nullable/NOT NULL
schemas. Modern targets are VARCHAR(3)/CHAR(8)/CHAR(44); legacy targets VARCHAR(3)/CHAR(8).
All output checksums match. Candidate descriptors must be empty; the independently
compiled preceding encoder must reject bounded DECIMAL TRY_CAST. Its prepared
control runs the released evaluator in Java on matching RowData; operation and
equivalent consumption are timed in Java. Complete previous jobs require the real
planner fallback reason, zero native substitutions and zero consumed native work.
The candidate uses Rust-side timers and empty evaluator descriptors. Tiny 16-row CHAR(8)
controls remain separate from 1,024-row bulk qualification.

| Flink | Workload | Cells | Stock/native median range | Previous/native median range | Slower than stock |
| --- | --- | ---: | ---: | ---: | ---: |
| 1.18.1 | bulk | 40 | 3.251-9.827x | 3.488-10.748x | 0 |
| 1.18.1 | tiny control | 8 | 1.412-3.401x | 1.544-3.745x | 0 |
| 2.2.1 | bulk | 40 | 3.104-10.154x | 3.422-10.276x | 0 |
| 2.2.1 | tiny control | 8 | 1.428-3.434x | 1.746-3.500x | 0 |

All 80 bulk cells beat stock. 0 of 16 tiny controls are slower.
Independent stock-control drift spans 0.759-0.999x; all ranges remain in the record.

Six complete million-row configurations use a rowwise source and blackhole at
parallelism one, with two warmups/five measurements. Candidate native jobs retain source
generation, planning/startup, production JNI and both perimeter transposes; the preceding
stock fallback retains its actual path. Compact modern VARCHAR(3), wide modern CHAR(44)
and wide legacy CHAR(8) require consumed NativeCalc, no fallback and no candidate evaluator.

| Flink / input | Candidate stock median (range), s | Native median (range), s | Previous stock median (range), s | Previous median (range), s |
| --- | ---: | ---: | ---: | ---: |
| 1.18.1 / modern-compact | 0.323108 (0.317246-0.325959) | 0.215176 (0.214526-0.218355) | 0.272784 (0.264374-0.280154) | 0.272559 (0.259251-0.276117) |
| 1.18.1 / modern-wide | 0.554577 (0.544595-0.577801) | 0.249330 (0.245439-0.250792) | 0.521098 (0.515807-0.523225) | 0.520737 (0.517170-0.526215) |
| 1.18.1 / legacy-wide | 0.288225 (0.274643-0.316488) | 0.218552 (0.208366-0.222127) | 0.240750 (0.233640-0.246038) | 0.249669 (0.236538-0.253221) |
| 2.2.1 / modern-compact | 0.317752 (0.315881-0.329789) | 0.216294 (0.213674-0.223117) | 0.268792 (0.265014-0.273145) | 0.281972 (0.268092-0.286282) |
| 2.2.1 / modern-wide | 0.555897 (0.533242-0.559481) | 0.252509 (0.245032-0.255392) | 0.520144 (0.518186-0.537838) | 0.520204 (0.515485-0.531938) |
| 2.2.1 / legacy-wide | 0.285559 (0.268036-0.291094) | 0.218255 (0.212956-0.227082) | 0.244965 (0.238019-0.249634) | 0.237997 (0.235115-0.245426) |

Native/stock elapsed ratios span 0.450-0.764;
native/previous ratios span 0.479-0.917. Ratios above one are slower.
Overlapping ranges and unfavorable whole-job results remain; prepared qualification
does not establish universal end-to-end acceleration.

All 184 focused encoding/runtime invocations (including existing unbounded/failure controls) pass on released Flink 1.18.1/2.2.1 with JDK 11/17
(Java release 11). Complete 5,003-row outputs verify declared types, NULLs, unique IDs
and cardinality across batches with consumed native work and empty evaluator descriptors.
Additional consumers, truncation/padding, lazy and empty-batch cases, selected operand
failures and materialized aggregate results match stock. Flink 1.18 does not support
MAX(CHAR); that boundary witness first widens the padded CHAR to STRING before MAX.
Twelve existing integer/predicate fixture controls pass. The unchanged released
CastRulesTest oracle retains its prior 535 stock cases per pair; the unchanged Rust
DSO retains 1,019 core passes/two pre-existing ignores. Neither reused oracle substitutes
for the new native execution witnesses. Source/DSO hashes and independently recompiled
preceding encoder classes are retained.

Reproduce prepared qualification with `integral_expression`, DECIMAL input and
`SF_INTEGRAL_EXPRESSION='TRY_CAST(v AS CHAR(8))'`. Complete jobs use
`sf.integral.decimalPrecision=38`, `sf.integral.decimalScale=18` and the same expression.
These are benchmark-only controls; no production option is introduced. Remaining work
is tracked in [#492](https://github.com/datafusion-contrib/StreamFusion/issues/492).

The preceding fallback control launches `IntegralExpressionBenchmarkFunctions` with
`sf.integral.previous=true` and `sf.integral.fallbackControl=true`, using the same
expression, legacy mode, row format and trial counts. Complete previous jobs use
the same fallback control. These controls never replace the actual rejected encoding
with a synthetic native or callback program.

## BOOLEAN CAST over integer comparisons

Integer =, <>, <, <=, > and >= comparisons reuse the native BOOLEAN formatter.
The recursive proof admits verified integer references/literals, wrapping addition,
subtraction and multiplication, lossless widening and the existing integral rounding
forms. BOOLEAN logical/null/truth wrappers remain native; integer IS NULL/IS NOT NULL
also cover the optimizer's expansion of nullable comparisons. The proof excludes
failing and stateful producers. DECIMAL, character, floating and temporal comparisons,
division, remainder and other unverified producers keep their existing routes.
Modern and legacy text/width rules are unchanged. No production switch is introduced.

The [result record](results/boolean-integer-comparison-cast-2026-11-01.json) and
[raw trials](results/boolean-integer-comparison-cast-2026-11-01.csv.gz) retain 64 configurations/6,400 trials.
Release/default-feature/mimalloc, JDK 17.0.12, Apple M4 Pro, one thread,
16 warmups/nine measurements and 131,072 rows/trial use the prepared Rust/Arrow
versus released Java/RowData boundary. INT/BIGINT extrema and values beyond 2^53
match across GenericRowData/BinaryRowData and nullable/NOT NULL declarations.
Representative bulk expressions are modern v>=0 to STRING, modern wrapping v+1<0
to CHAR(8), and legacy v<0 to CHAR(8). Sixteen-row modern comparison-to-STRING
controls remain separate. All checksums agree. The independently recompiled actual
preceding encoder must bind a JVM evaluator; the candidate must have no evaluator.
The unchanged Rust formatter, Calc and comparison kernels retain their exact DSO.

| Flink | Workload | Cells | Stock/native median range | Previous/native median range | Slower than stock |
| --- | --- | ---: | ---: | ---: | ---: |
| 1.18.1 | bulk | 24 | 1.571-9.760x | 11.270-14.952x | 0 |
| 1.18.1 | tiny control | 8 | 0.123-0.162x | 14.109-15.029x | 8 |
| 2.2.1 | bulk | 24 | 1.581-9.791x | 11.132-15.929x | 0 |
| 2.2.1 | tiny control | 8 | 0.123-0.163x | 14.003-15.089x | 8 |

All 48 bulk cells beat stock; 16 of 16 tiny controls are slower.
Independent stock-control drift spans 0.650-1.393x. All ranges are retained.
For the Flink 2.2 NOT NULL INT/GenericRowData comparison, untimed native allocation
probes report 29 requests for both batch sizes: 10,534 requested bytes at 1,024 rows
and 1,816 at 16 rows. Native medians rise from 2.315 to 33.023 ns/row while stock
remains near 4.7 ns/row. Fixed per-batch work remains a small-batch limitation;
removing the preceding JVM evaluator does not eliminate that cost.

Six complete million-row source/blackhole configurations retain source generation,
planning/startup, production JNI and both transposes. Modern INT comparison-to-STRING,
modern BIGINT arithmetic comparison-to-CHAR(8), and legacy INT comparison-to-CHAR(8)
run at parallelism one with two warmups/five measurements. Each native query requires
consumed NativeCalc, no fallback and empty candidate evaluator descriptors.

| Flink / input | Candidate stock median (range), s | Native median (range), s | Previous stock median (range), s | Previous median (range), s |
| --- | ---: | ---: | ---: | ---: |
| 1.18.1 / modern-comparison | 0.188399 (0.186934-0.194068) | 0.191475 (0.190601-0.197079) | 0.192767 (0.183027-0.197884) | 0.217869 (0.215422-0.238049) |
| 1.18.1 / modern-arithmetic | 0.235702 (0.231248-0.249417) | 0.199672 (0.196615-0.223385) | 0.242474 (0.237011-0.247843) | 0.273772 (0.270471-0.284878) |
| 1.18.1 / legacy-comparison | 0.208412 (0.200292-0.227803) | 0.188325 (0.185272-0.198546) | 0.212331 (0.208584-0.216080) | 0.223269 (0.220450-0.231086) |
| 2.2.1 / modern-comparison | 0.185857 (0.181895-0.190989) | 0.187063 (0.184722-0.196945) | 0.187830 (0.180328-0.192330) | 0.219429 (0.215679-0.226802) |
| 2.2.1 / modern-arithmetic | 0.225332 (0.219251-0.228684) | 0.190797 (0.190280-0.194885) | 0.229180 (0.218205-0.244467) | 0.258519 (0.251868-0.277756) |
| 2.2.1 / legacy-comparison | 0.205362 (0.204444-0.214410) | 0.192941 (0.189956-0.201685) | 0.204743 (0.194486-0.218907) | 0.212190 (0.207976-0.218663) |

Native/stock elapsed ratios span 0.847-1.016;
native/previous ratios span 0.729-0.909. Ratios above one are slower.
All unfavorable results and overlapping ranges remain. Prepared qualification does
not establish universal end-to-end acceleration.

All 268 focused JVM invocations, four additional shared-state invocations and
12 existing fixture smokes pass on released Flink 1.18.1/2.2.1 with JDK 11/17
(Java release 11). Complete 5,003-row witnesses compare all values, schema, NULLs,
IDs and cardinality across batches for TINYINT/SMALLINT/INT/BIGINT, independent input
columns, nullable/NOT NULL declarations, modern/legacy mode, signed extrema and BIGINT
values beyond 2^53. They require consumed native work, no fallback and no evaluator.
String consumers, grouping, lazy branches and empty filtered batches match stock.
Failing and stateful inputs keep their released behavior and exact previous routes.
Unchanged released boolean-expression/truth tests, 535 CastRules stock-oracle cases
per pair, and the identical DSO's 1,019 core passes/two pre-existing ignores retain
their preceding qualification; new runtime witnesses establish the changed route.
Source/DSO hashes and independently recompiled baseline classes are retained.

Reproduce prepared qualification with `integral_expression`, INT/BIGINT input and
`SF_INTEGRAL_EXPRESSION='CAST(v>=CAST(0 AS $TYPE) AS STRING)'`. Complete jobs use
`sf.integral.expression=CAST(v>=0 AS STRING)`; BIGINT controls additionally use
`sf.integral.bigintInput=true`. Remaining conversions are tracked in
[#492](https://github.com/datafusion-contrib/StreamFusion/issues/492).

## BOOLEAN TRY_CAST over integer comparisons

Integer =, <>, <, <=, > and >= comparisons reuse the native BOOLEAN formatter.
The recursive proof admits verified integer references/literals, wrapping addition,
subtraction and multiplication, lossless widening and the existing integral rounding
forms. BOOLEAN logical/null/truth wrappers remain native; integer IS NULL/IS NOT NULL
also cover the optimizer's expansion of nullable comparisons. The proof excludes
failing and stateful producers. DECIMAL, character, floating and temporal comparisons,
division, remainder and other unverified producers keep their existing routes.
Modern and legacy text/width rules are unchanged. No production switch is introduced.

The [result record](results/boolean-integer-comparison-try-cast-2026-11-01.json) and
[raw trials](results/boolean-integer-comparison-try-cast-2026-11-01.csv.gz) retain 64 configurations/6,400 trials.
Release/default-feature/mimalloc, JDK 17.0.12, Apple M4 Pro, one thread,
16 warmups/nine measurements and 131,072 rows/trial use the prepared Rust/Arrow
versus released Java/RowData boundary. INT/BIGINT extrema and values beyond 2^53
match across GenericRowData/BinaryRowData and nullable/NOT NULL declarations.
Representative bulk expressions are modern v>=0 to STRING, modern wrapping v+1<0
to CHAR(8), and legacy v<0 to CHAR(8). Sixteen-row modern comparison-to-STRING
controls remain separate. All checksums agree. The independently recompiled actual
preceding encoder must bind a JVM evaluator; the candidate must have no evaluator.
The unchanged Rust formatter, Calc and comparison kernels retain their exact DSO.

| Flink | Workload | Cells | Stock/native median range | Previous/native median range | Slower than stock |
| --- | --- | ---: | ---: | ---: | ---: |
| 1.18.1 | bulk | 24 | 1.502-16.474x | 10.559-18.565x | 0 |
| 1.18.1 | tiny control | 8 | 0.124-0.162x | 14.250-15.230x | 8 |
| 2.2.1 | bulk | 24 | 1.531-10.600x | 10.074-19.601x | 0 |
| 2.2.1 | tiny control | 8 | 0.122-0.163x | 14.110-14.918x | 8 |

All 48 bulk cells beat stock; 16 of 16 tiny controls are slower.
Independent stock-control drift spans 0.756-1.905x. All ranges are retained.

Six complete million-row source/blackhole configurations retain source generation,
planning/startup, production JNI and both transposes. Modern INT comparison-to-STRING,
modern BIGINT arithmetic comparison-to-CHAR(8), and legacy INT comparison-to-CHAR(8)
run at parallelism one with two warmups/five measurements. Each native query requires
consumed NativeCalc, no fallback and empty candidate evaluator descriptors.

| Flink / input | Candidate stock median (range), s | Native median (range), s | Previous stock median (range), s | Previous median (range), s |
| --- | ---: | ---: | ---: | ---: |
| 1.18.1 / modern-comparison | 0.188606 (0.183451-0.213412) | 0.194802 (0.194257-0.200845) | 0.193022 (0.181021-0.197639) | 0.212879 (0.212570-0.221866) |
| 1.18.1 / modern-arithmetic | 0.240461 (0.234930-0.242738) | 0.202671 (0.191075-0.207119) | 0.240431 (0.227648-0.241450) | 0.244998 (0.240404-0.252949) |
| 1.18.1 / legacy-comparison | 0.210634 (0.209291-0.225512) | 0.193515 (0.189962-0.200454) | 0.215743 (0.197997-0.227196) | 0.245962 (0.234360-0.359305) |
| 2.2.1 / modern-comparison | 0.189071 (0.184989-0.199581) | 0.189496 (0.185077-0.194949) | 0.192146 (0.185846-0.196981) | 0.207776 (0.204363-0.213585) |
| 2.2.1 / modern-arithmetic | 0.229806 (0.221967-0.238040) | 0.198334 (0.195886-0.204989) | 0.224424 (0.220635-0.238883) | 0.247177 (0.244628-0.259436) |
| 2.2.1 / legacy-comparison | 0.195756 (0.192557-0.200052) | 0.185488 (0.183502-0.196643) | 0.202816 (0.201091-0.213579) | 0.228695 (0.224209-0.232692) |

Native/stock elapsed ratios span 0.843-1.033;
native/previous ratios span 0.787-0.915. Ratios above one are slower.
All unfavorable results and overlapping ranges remain. Prepared qualification does
not establish universal end-to-end acceleration.

All 292 focused JVM invocations, four additional shared-state invocations and
12 existing fixture smokes pass on released Flink 1.18.1/2.2.1 with JDK 11/17
(Java release 11). Complete 5,003-row witnesses compare all values, schema, NULLs,
IDs and cardinality across batches for TINYINT/SMALLINT/INT/BIGINT, independent input
columns, nullable/NOT NULL declarations, modern/legacy mode, signed extrema and BIGINT
values beyond 2^53. They require consumed native work, no fallback and no evaluator.
String consumers, grouping, lazy branches and empty filtered batches match stock.
Failing and stateful inputs keep their released behavior and exact previous routes.
Unchanged released boolean-expression/truth tests, 535 CastRules stock-oracle cases
per pair, and the identical DSO's 1,019 core passes/two pre-existing ignores retain
their preceding qualification; new runtime witnesses establish the changed route.
Source/DSO hashes and independently recompiled baseline classes are retained.

Reproduce prepared qualification with `integral_expression`, INT/BIGINT input and
`SF_INTEGRAL_EXPRESSION='TRY_CAST(v>=CAST(0 AS $TYPE) AS STRING)'`. Complete jobs use
`sf.integral.expression=TRY_CAST(v>=0 AS STRING)`; BIGINT controls additionally use
`sf.integral.bigintInput=true`. Remaining conversions are tracked in
[#492](https://github.com/datafusion-contrib/StreamFusion/issues/492).

## DECIMAL unary CAST to character

Verified DECIMAL ABS and unary minus, same-operator nesting, and same-scale
precision widening reuse the native plain-text formatter and existing ASCII-safe
LEFT/RPAD width kernels. Released Flink retains precision/scale in these unary
operations. Modern/legacy spelling, trailing zeros, truncation/padding and NULLs
remain unchanged. Arithmetic CAST/TRY_CAST is qualified separately below. Other
arithmetic, rounding metadata, SIGN, mixed unary trees, stateful/failing producers
and unknown configuration retain their existing routes.
No native kernel, JNI boundary, ownership contract or production switch changes.

The [result record](results/decimal-unary-cast-2026-11-01.json) and
[raw trials](results/decimal-unary-cast-2026-11-01.csv.gz) retain 64 configurations/6,400 trials.
Release/default-feature/mimalloc, JDK 17.0.12, Apple M4 Pro, one thread,
16 warmups/nine measurements and 131,072 rows/trial use the prepared Rust/Arrow
versus released Java/RowData boundary. DECIMAL(18,2)/(38,18) extrema and large signed coefficients
match across GenericRowData/BinaryRowData and nullable/NOT NULL declarations.
Representative bulk expressions are modern ABS(v) to STRING, modern -v
to CHAR(8), and legacy ABS(v) to CHAR(44). Sixteen-row modern ABS-to-STRING
controls remain separate. All checksums agree. The independently recompiled actual
preceding encoder must bind a JVM evaluator; the candidate must have no evaluator.
The unchanged Rust decimal formatter, Calc and unary kernels retain their exact DSO.

| Flink | Workload | Cells | Stock/native median range | Previous/native median range | Slower than stock |
| --- | --- | ---: | ---: | ---: | ---: |
| 1.18.1 | bulk | 24 | 3.527-8.304x | 5.151-8.657x | 0 |
| 1.18.1 | tiny control | 8 | 1.174-3.390x | 16.101-19.433x | 0 |
| 2.2.1 | bulk | 24 | 3.242-8.637x | 4.963-8.754x | 0 |
| 2.2.1 | tiny control | 8 | 1.221-3.017x | 17.392-19.115x | 0 |

All 48 bulk cells beat stock; 0 of 16 tiny controls are slower.
Independent stock-control drift spans 0.792-1.170x. All ranges are retained.

Six complete million-row source/blackhole configurations retain source generation,
planning/startup, production JNI and both transposes. Modern DECIMAL(18,2) ABS-to-STRING,
modern DECIMAL(38,18) negate-to-CHAR(8), and legacy DECIMAL(38,18) ABS-to-CHAR(44)
run at parallelism one with two warmups/five measurements. Each native query requires
consumed NativeCalc, no fallback and empty candidate evaluator descriptors.

| Flink / input | Candidate stock median (range), s | Native median (range), s | Previous stock median (range), s | Previous median (range), s |
| --- | ---: | ---: | ---: | ---: |
| 1.18.1 / modern-abs | 0.259279 (0.245537-0.298334) | 0.212230 (0.207713-0.220957) | 0.257366 (0.249812-0.266154) | 0.271891 (0.264303-0.274019) |
| 1.18.1 / modern-negate | 0.388762 (0.378809-0.394126) | 0.231802 (0.224440-0.236891) | 0.382977 (0.378330-0.387905) | 0.448462 (0.438670-0.452394) |
| 1.18.1 / legacy-abs | 0.290965 (0.277537-0.297425) | 0.219611 (0.217127-0.231349) | 0.282531 (0.277297-0.308506) | 0.303356 (0.296926-0.312935) |
| 2.2.1 / modern-abs | 0.260823 (0.248016-0.278068) | 0.210224 (0.202756-0.216913) | 0.260424 (0.255985-0.281087) | 0.271364 (0.267967-0.273161) |
| 2.2.1 / modern-negate | 0.365805 (0.361973-0.375923) | 0.224516 (0.218071-0.230758) | 0.367469 (0.361109-0.375109) | 0.420946 (0.414307-0.428079) |
| 2.2.1 / legacy-abs | 0.294697 (0.279771-0.298122) | 0.227533 (0.225032-0.228816) | 0.287236 (0.281952-0.329673) | 0.293164 (0.290418-0.302692) |

Native/stock elapsed ratios span 0.596-0.819;
native/previous ratios span 0.517-0.781. Ratios above one are slower.
All unfavorable results and overlapping ranges remain. Prepared qualification does
not establish universal end-to-end acceleration.

All 248 focused JVM invocations, four additional shared-state invocations and
12 existing fixture smokes pass on released Flink 1.18.1/2.2.1 with JDK 11/17
(Java release 11). Complete 5,003-row witnesses compare all values, schema, NULLs,
IDs and cardinality across batches for eight DECIMAL precision/scale pairs from (1,1)
through (38,38), nullable/NOT NULL declarations, modern/legacy mode, signed extrema,
zeros and unit coefficients. They require consumed native work, no fallback and no evaluator.
String consumers, grouping, lazy branches and empty filtered batches match stock.
Failing and stateful inputs keep their released behavior and exact previous routes.
The unchanged released DecimalData formatting oracle, 535 CastRules stock-oracle cases
per pair, and the identical DSO's 1,019 core passes/two pre-existing ignores retain
their preceding qualification; new runtime witnesses establish the changed route.
Source/DSO hashes and independently recompiled baseline classes are retained.

Reproduce prepared qualification with `integral_expression`, DECIMAL input and
`SF_INTEGRAL_EXPRESSION='CAST(ABS(v) AS STRING)'`. Complete jobs use
`sf.integral.expression=CAST(ABS(v) AS STRING)` and
`sf.integral.decimalPrecision=18`, `sf.integral.decimalScale=2`. Remaining conversions
are tracked in [#492](https://github.com/datafusion-contrib/StreamFusion/issues/492).

## DECIMAL unary TRY_CAST to character

Verified DECIMAL ABS and unary minus, same-operator nesting, and same-scale
precision widening reuse the native plain-text formatter and existing ASCII-safe
LEFT/RPAD width kernels. Released Flink retains precision/scale in these unary
operations. Modern/legacy spelling, trailing zeros, truncation/padding and NULLs
remain unchanged. Arithmetic CAST/TRY_CAST is qualified separately below. Other
arithmetic, rounding metadata, SIGN, mixed unary trees, stateful/failing producers
and unknown configuration retain their existing routes.
No native kernel, JNI boundary, ownership contract or production switch changes.

The [result record](results/decimal-unary-try-cast-2026-11-01.json) and
[raw trials](results/decimal-unary-try-cast-2026-11-01.csv.gz) retain 64 configurations/6,400 trials.
Release/default-feature/mimalloc, JDK 17.0.12, Apple M4 Pro, one thread,
16 warmups/nine measurements and 131,072 rows/trial use the prepared Rust/Arrow
versus released Java/RowData boundary. DECIMAL(18,2)/(38,18) extrema and large signed coefficients
match across GenericRowData/BinaryRowData and nullable/NOT NULL declarations.
Representative bulk expressions are modern ABS(v) to STRING, modern -v
to CHAR(8), and legacy ABS(v) to CHAR(44). Sixteen-row modern ABS-to-STRING
controls remain separate. All checksums agree. The independently recompiled actual
preceding encoder rejects this signature. Its prepared controls use released Java
on RowData with Java-side timing, preserving the actual fallback reason. No JVM
callback baseline is fabricated; the candidate must have no evaluator.
The unchanged Rust decimal formatter, Calc and unary kernels retain their exact DSO.

| Flink | Workload | Cells | Stock/native median range | Previous/native median range | Slower than stock |
| --- | --- | ---: | ---: | ---: | ---: |
| 1.18.1 | bulk | 24 | 3.215-8.503x | 3.728-8.758x | 0 |
| 1.18.1 | tiny control | 8 | 1.302-3.462x | 1.356-3.734x | 0 |
| 2.2.1 | bulk | 24 | 3.215-8.064x | 3.849-9.161x | 0 |
| 2.2.1 | tiny control | 8 | 1.157-3.159x | 1.475-3.526x | 0 |

All 48 bulk cells beat stock; 0 of 16 tiny controls are slower.
Independent stock-control drift spans 0.721-1.218x. All ranges are retained.

Six complete million-row source/blackhole configurations retain source generation,
planning/startup, production JNI and both transposes. Modern DECIMAL(18,2) ABS-to-STRING,
modern DECIMAL(38,18) negate-to-CHAR(8), and legacy DECIMAL(38,18) ABS-to-CHAR(44)
run at parallelism one with two warmups/five measurements. Each native query requires
consumed NativeCalc, no fallback and empty candidate evaluator descriptors. Previous
jobs require actual planner fallback with zero native substitutions or work.

| Flink / input | Candidate stock median (range), s | Native median (range), s | Previous stock median (range), s | Previous median (range), s |
| --- | ---: | ---: | ---: | ---: |
| 1.18.1 / modern-abs | 0.264686 (0.250927-0.269670) | 0.209733 (0.206909-0.211031) | 0.209105 (0.203733-0.212599) | 0.210178 (0.207587-0.217686) |
| 1.18.1 / modern-negate | 0.378330 (0.370410-0.386081) | 0.224428 (0.222141-0.229540) | 0.342058 (0.334649-0.342707) | 0.344431 (0.338319-0.346211) |
| 1.18.1 / legacy-abs | 0.291547 (0.280389-0.335874) | 0.216920 (0.214796-0.218376) | 0.239804 (0.237483-0.253204) | 0.244747 (0.235303-0.253585) |
| 2.2.1 / modern-abs | 0.259245 (0.252922-0.268575) | 0.206518 (0.201711-0.211899) | 0.204024 (0.195745-0.214377) | 0.215092 (0.194208-0.217394) |
| 2.2.1 / modern-negate | 0.376024 (0.372521-0.391604) | 0.227104 (0.225205-0.233658) | 0.344694 (0.340141-0.350055) | 0.354460 (0.345592-0.365853) |
| 2.2.1 / legacy-abs | 0.279563 (0.275328-0.306825) | 0.217126 (0.211098-0.219148) | 0.235848 (0.231293-0.247688) | 0.236468 (0.234912-0.251507) |

Native/stock elapsed ratios span 0.593-0.797;
native/previous ratios span 0.641-0.998. Ratios above one are slower.
All unfavorable results and overlapping ranges remain. Prepared qualification does
not establish universal end-to-end acceleration.

The modern ABS-to-STRING jobs overlap the previous fallback control: native/previous
elapsed ratios are 0.998 on Flink 1.18 and 0.960 on Flink 2.2. Do not infer a robust
whole-job improvement from these subsecond differences. Independent stock-process
controls also differ; all paired samples remain available instead of selecting the
more favorable control.

All 300 focused JVM invocations, four additional shared-state invocations and
12 existing fixture smokes pass on released Flink 1.18.1/2.2.1 with JDK 11/17
(Java release 11). Complete 5,003-row witnesses compare all values, schema, NULLs,
IDs and cardinality across batches for eight DECIMAL precision/scale pairs from (1,1)
through (38,38), nullable/NOT NULL declarations, modern/legacy mode, signed extrema,
zeros and unit coefficients. They require consumed native work, no fallback and no evaluator.
String consumers, grouping, lazy branches and empty filtered batches match stock.
Failing and stateful inputs keep their released behavior and exact previous routes.
The unchanged released DecimalData formatting oracle, 535 CastRules stock-oracle cases
per pair, and the identical DSO's 1,019 core passes/two pre-existing ignores retain
their preceding qualification; new runtime witnesses establish the changed route.
Source/DSO hashes and independently recompiled baseline classes are retained.

Reproduce prepared qualification with `integral_expression`, DECIMAL input and
`SF_INTEGRAL_EXPRESSION='TRY_CAST(ABS(v) AS STRING)'`. Complete jobs use
`sf.integral.expression=TRY_CAST(ABS(v) AS STRING)` and
`sf.integral.decimalPrecision=18`, `sf.integral.decimalScale=2`. Remaining conversions
are tracked in [#492](https://github.com/datafusion-contrib/StreamFusion/issues/492).

## Nullable DECIMAL arithmetic CAST to character

Nullable DECIMAL-only addition, subtraction and multiplication trees reuse the
existing typed arithmetic kernels and exact character formatter. Each node retains
its resolved precision/scale and overflow NULL. The proof also admits previously
verified ABS/negate inputs; it excludes division, remainder, mixed numeric types,
stateful/failing producers and unverified conversions. Both the computed source
and the character result must be nullable. NOT NULL CAST retains the exact
generated value/default/null-flag contract; arithmetic TRY_CAST is qualified
separately below. No Rust kernel, JNI/ownership boundary or production option changes.

The [result record](results/decimal-arithmetic-cast-2026-11-01.json) and
[raw trials](results/decimal-arithmetic-cast-2026-11-01.csv.gz) retain 32 configurations/3,200 trials.
Release/default-feature/mimalloc, JDK 17.0.12, Apple M4 Pro, one thread,
16 warmups/nine measurements and 131,072 rows/trial use the prepared Rust/Arrow
versus released Java/RowData boundary. DECIMAL(18,2)/(38,18) extrema and large signed coefficients
match across GenericRowData/BinaryRowData and nullable declarations.
Representative bulk expressions are modern v+v to STRING, modern v*v
to CHAR(8), and legacy (v+v)*v to CHAR(44). Sixteen-row modern v*v-to-STRING
controls remain separate. All checksums agree. The independently recompiled actual
preceding encoder must bind a JVM evaluator; the candidate must have no evaluator.
The unchanged Rust decimal formatter, Calc and typed arithmetic kernels retain their exact DSO.

| Flink | Workload | Cells | Stock/native median range | Previous/native median range | Slower than stock |
| --- | --- | ---: | ---: | ---: | ---: |
| 1.18.1 | bulk | 12 | 1.832-8.545x | 1.819-10.077x | 0 |
| 1.18.1 | tiny control | 4 | 1.430-2.923x | 9.921-13.647x | 0 |
| 2.2.1 | bulk | 12 | 1.799-8.606x | 1.772-10.408x | 0 |
| 2.2.1 | tiny control | 4 | 1.459-2.968x | 9.659-13.076x | 0 |

All 24 bulk cells beat stock; 0 of 8 tiny controls are slower.
Independent stock-control drift spans 0.899-1.085x. All ranges are retained.

Six complete million-row source/blackhole configurations retain source generation,
planning/startup, production JNI and both transposes. Modern DECIMAL(18,2) v+v-to-STRING,
modern DECIMAL(38,18) v*v-to-CHAR(8), and legacy DECIMAL(38,18) (v+v)*v-to-CHAR(44)
run at parallelism one with two warmups/five measurements. Each native query requires
consumed NativeCalc, no fallback and empty candidate evaluator descriptors.

| Flink / input | Candidate stock median (range), s | Native median (range), s | Previous stock median (range), s | Previous median (range), s |
| --- | ---: | ---: | ---: | ---: |
| 1.18.1 / modern-add | 0.256403 (0.240670-0.266953) | 0.219047 (0.215546-0.224134) | 0.254911 (0.244312-0.263866) | 0.285142 (0.278247-0.293991) |
| 1.18.1 / modern-multiply | 0.378419 (0.360666-0.380856) | 0.244122 (0.239227-0.254304) | 0.366048 (0.359645-0.377322) | 0.346962 (0.340675-0.351597) |
| 1.18.1 / legacy-nested | 0.331110 (0.329966-0.337205) | 0.261276 (0.254386-0.275251) | 0.327974 (0.316690-0.341677) | 0.308758 (0.299839-0.322865) |
| 2.2.1 / modern-add | 0.247919 (0.246311-0.257852) | 0.217974 (0.212238-0.226258) | 0.260530 (0.251712-0.287248) | 0.285710 (0.282034-0.291648) |
| 2.2.1 / modern-multiply | 0.374700 (0.372850-0.383431) | 0.250588 (0.242618-0.255825) | 0.385305 (0.381401-0.389040) | 0.326326 (0.320013-0.336114) |
| 2.2.1 / legacy-nested | 0.337211 (0.328946-0.361897) | 0.252416 (0.249894-0.256505) | 0.344956 (0.331128-0.350719) | 0.303112 (0.294999-0.315260) |

Native/stock elapsed ratios span 0.645-0.879;
native/previous ratios span 0.704-0.846. Ratios above one are slower.
All unfavorable results and overlapping ranges remain. Prepared qualification does
not establish universal end-to-end acceleration.

All 640 focused JVM invocations (including the shared-state suite) and
12 existing fixture smokes pass on released Flink 1.18.1/2.2.1 with JDK 11/17
(Java release 11). Complete 5,003-row witnesses compare all values, schema, NULLs,
IDs and cardinality across batches for eight DECIMAL precision/scale pairs from (1,1)
through (38,38), nullable declarations, modern/legacy mode, signed extrema,
zeros and unit coefficients. They require consumed native work, no fallback and no evaluator.
String consumers, grouping, lazy branches and empty filtered batches match stock.
NOT NULL CAST and failing/stateful producers retain their preceding generated/fallback
routes, with the overflow repair also revalidated. Arithmetic TRY_CAST has a
separate qualification below.
The unchanged released DecimalData formatting oracle, 535 CastRules stock-oracle cases
per pair, and the identical DSO's 1,019 core passes/two pre-existing ignores retain
their preceding qualification; new runtime witnesses establish the changed route.
Source/DSO hashes and independently recompiled baseline classes are retained.

Reproduce prepared qualification with `integral_expression`, DECIMAL input and
`SF_INTEGRAL_EXPRESSION='CAST(v+v AS STRING)'`. Complete jobs use
`sf.integral.expression=CAST(v+v AS STRING)` and
`sf.integral.decimalPrecision=18`, `sf.integral.decimalScale=2`. Remaining conversions
are tracked in [#492](https://github.com/datafusion-contrib/StreamFusion/issues/492).

## DECIMAL arithmetic TRY_CAST to character

DECIMAL-only addition, subtraction and multiplication trees reuse the
existing typed arithmetic kernels and exact character formatter. Each node retains
its resolved precision/scale and overflow NULL. The proof also admits previously
verified ABS/negate inputs; it excludes division, remainder, mixed numeric types,
stateful/failing producers and unverified conversions. TRY_CAST keeps a nullable character result with nullable
or NOT NULL arithmetic sources, retaining overflow NULL instead of a non-nullable
CAST projection default. NOT NULL CAST keeps its generated value/default/null-flag
contract. No Rust kernel, JNI/ownership boundary or production option changes.

The [result record](results/decimal-arithmetic-try-cast-2026-11-01.json) and
[raw trials](results/decimal-arithmetic-try-cast-2026-11-01.csv.gz) retain 64 configurations/6,400 trials.
Release/default-feature/mimalloc, JDK 17.0.12, Apple M4 Pro, one thread,
16 warmups/nine measurements and 131,072 rows/trial use the prepared Rust/Arrow
versus released Java/RowData boundary. DECIMAL(18,2)/(38,18) extrema and large signed coefficients
match across GenericRowData/BinaryRowData and nullable/NOT NULL declarations.
Representative bulk expressions are modern v+v to STRING, modern v*v
to CHAR(8), and legacy (v+v)*v to CHAR(44). Sixteen-row modern v*v-to-STRING
controls remain separate. All checksums agree. The independently recompiled actual
preceding encoder rejects this signature. Its prepared controls use released Java
on RowData with Java-side timing, preserving the actual fallback reason. No JVM
callback baseline is fabricated; the candidate must have no evaluator.
The unchanged Rust decimal formatter, Calc and typed arithmetic kernels retain their exact DSO.

| Flink | Workload | Cells | Stock/native median range | Previous/native median range | Slower than stock |
| --- | --- | ---: | ---: | ---: | ---: |
| 1.18.1 | bulk | 24 | 1.787-10.422x | 2.035-11.123x | 0 |
| 1.18.1 | tiny control | 8 | 1.416-3.495x | 1.482-3.738x | 0 |
| 2.2.1 | bulk | 24 | 1.890-10.314x | 1.983-10.945x | 0 |
| 2.2.1 | tiny control | 8 | 1.417-3.648x | 1.464-3.773x | 0 |

All 48 bulk cells beat stock; 0 of 16 tiny controls are slower.
Independent stock-control drift spans 0.824-1.030x. All ranges are retained.

Six complete million-row source/blackhole configurations retain source generation,
planning/startup, production JNI and both transposes. Modern DECIMAL(18,2) v+v-to-STRING,
modern DECIMAL(38,18) v*v-to-CHAR(8), and legacy DECIMAL(38,18) (v+v)*v-to-CHAR(44)
run at parallelism one with two warmups/five measurements. Each native query requires
consumed NativeCalc, no fallback and empty candidate evaluator descriptors. Previous
jobs require actual planner fallback with zero native substitutions or work.

| Flink / input | Candidate stock median (range), s | Native median (range), s | Previous stock median (range), s | Previous median (range), s |
| --- | ---: | ---: | ---: | ---: |
| 1.18.1 / modern-add | 0.261276 (0.245271-0.299600) | 0.221004 (0.217863-0.234701) | 0.221582 (0.212394-0.230538) | 0.222617 (0.218642-0.236744) |
| 1.18.1 / modern-multiply | 0.370796 (0.351150-0.375298) | 0.250625 (0.241326-0.258752) | 0.328880 (0.314737-0.335114) | 0.323991 (0.320824-0.335213) |
| 1.18.1 / legacy-nested | 0.337627 (0.323339-0.343045) | 0.270040 (0.264440-0.275955) | 0.302010 (0.300327-0.305214) | 0.299326 (0.290850-0.303809) |
| 2.2.1 / modern-add | 0.247882 (0.239912-0.266768) | 0.220313 (0.214473-0.228999) | 0.222404 (0.210382-0.229336) | 0.214866 (0.207088-0.232275) |
| 2.2.1 / modern-multiply | 0.382778 (0.373021-0.391164) | 0.235763 (0.231195-0.237817) | 0.339623 (0.332366-0.347701) | 0.338182 (0.324840-0.350138) |
| 2.2.1 / legacy-nested | 0.332702 (0.329976-0.352564) | 0.253855 (0.247578-0.268084) | 0.296810 (0.288384-0.300062) | 0.296049 (0.287876-0.301089) |

Native/stock elapsed ratios span 0.616-0.889;
native/previous ratios span 0.697-1.025. Ratios above one are slower.
All unfavorable results and overlapping ranges remain. Prepared qualification does
not establish universal end-to-end acceleration.
The simple-addition jobs overlap their previous fallback controls: native/previous
is 0.993 on Flink 1.18 and 1.025 on 2.2, with the latter median 2.5% slower.
Neither simple-addition comparison establishes a stable complete-job improvement.

All 680 focused JVM invocations (including the shared-state suite) and
12 existing fixture smokes pass on released Flink 1.18.1/2.2.1 with JDK 11/17
(Java release 11). Complete 5,003-row witnesses compare all values, schema, NULLs,
IDs and cardinality across batches for eight DECIMAL precision/scale pairs from (1,1)
through (38,38), nullable/NOT NULL declarations, modern/legacy mode, signed extrema,
zeros and unit coefficients. They require consumed native work, no fallback and no evaluator.
String consumers, grouping, lazy branches and empty filtered batches match stock.
NOT NULL CAST and failing/stateful producers retain their preceding generated/fallback
routes, with the overflow repair also revalidated. Mixed numeric arithmetic TRY_CAST
retains an explicit fallback witness.
The unchanged released DecimalData formatting oracle, 535 CastRules stock-oracle cases
per pair, and the identical DSO's 1,019 core passes/two pre-existing ignores retain
their preceding qualification; new runtime witnesses establish the changed route.
Source/DSO hashes and independently recompiled baseline classes are retained.

Reproduce prepared qualification with `integral_expression`, DECIMAL input and
`SF_INTEGRAL_EXPRESSION='TRY_CAST(v+v AS STRING)'`. Complete jobs use
`sf.integral.expression=TRY_CAST(v+v AS STRING)` and
`sf.integral.decimalPrecision=18`, `sf.integral.decimalScale=2`. Remaining conversions
are tracked in [#492](https://github.com/datafusion-contrib/StreamFusion/issues/492).

## DECIMAL ABS over arithmetic

Verified DECIMAL-only addition, subtraction and multiplication now stay in Arrow
through ABS, including nested ABS and same-scale precision widening. Each arithmetic
node retains its resolved precision/scale and overflow NULL. Division, remainder,
mixed numeric operands, rescaling, narrowing, mixed ABS/negation and unverified
producers keep generated evaluation. This extends producer coverage using the existing
typed arithmetic and ABS rules. A typed DECIMAL-array loop removes per-row input-type
and legacy integer-error dispatch, retaining exact iterator capacity for the output.
There is no JNI/ownership or option change.
The separate NOT NULL projection-default repairs remain active for affected basic-type
consumers; they are not claimed as JVM-free acceleration.
Character CAST/TRY_CAST keep their own producer whitelist in
[#492](https://github.com/datafusion-contrib/StreamFusion/issues/492); this ABS extension
does not claim every enclosing character conversion is JVM-free.

The [result record](results/decimal-arithmetic-abs-2026-11-02.json) and
[raw trials](results/decimal-arithmetic-abs-2026-11-02.csv.gz) retain 64 configurations
and 6,400 trials. Release/default-feature/mimalloc, JDK 17.0.12, Apple M4 Pro,
one thread, 16 warmups/nine measurements and 131,072 rows/trial compare released
generated Java on prepared GenericRowData/BinaryRowData with production Calc on Arrow.
Native timing stays inside Rust; initial JNI/C Data and perimeter transposes are
excluded, while intrinsic previous callbacks remain inside. The independently
recompiled actual preceding encoder includes the correctness repair; its retained
preceding executable and DSO bind the real generated evaluator. The candidate requires empty evaluator descriptors and
the intended native function work. Checksums match.

DECIMAL(18,2)/(38,18), nullable/NOT NULL inputs, signed extrema, large coefficients,
addition, multiplication and nested producers cover 1,024-row bulk batches;
16-row multiplication controls remain separate. Every expression consumes ABS through
`>0 OR CAST(NULL AS BOOLEAN)`, retaining overflow NULL even for NOT NULL sources.
These timings measure arithmetic ABS through a BOOLEAN consumer, not final DECIMAL
output. Untimed allocation probes retain requested bytes and output buffers separately.

| Flink | Workload | Cells | Stock/native median range | Previous/native median range | Slower than stock |
| --- | --- | ---: | ---: | ---: | ---: |
| 1.18.1 | bulk | 24 | 1.086-4.339x | 3.732-8.435x | 0 |
| 1.18.1 | tiny control | 8 | 0.193-1.198x | 7.920-12.712x | 6 |
| 2.2.1 | bulk | 24 | 1.065-3.979x | 3.684-7.591x | 0 |
| 2.2.1 | tiny control | 8 | 0.171-1.171x | 8.148-12.954x | 6 |

The initial 64-cell investigation retained 11 slower bulk cells;
the [initial raw trials](results/decimal-arithmetic-abs-initial-2026-11-02.csv.gz)
and full initial results remain in the record. A ten-second native stack sample
located repeated arithmetic input/error dispatch and output-buffer growth.
The typed-array loop removes those costs without changing arithmetic rules.
Final native bulk timings improve by
1.111-1.982x
over the initial native attempt, with independent stock-process drift retained.

All 48 final bulk cells beat stock and the actual preceding path; 12 of 16 tiny
controls are slower than stock. Independent stock-process drift spans
0.643-2.211x; every raw trial is retained.

Six million-row row-source/blackhole configurations retain source generation,
planning/startup, production JNI and both transposes. Nullable DECIMAL(18,2) addition
and DECIMAL(38,18) multiplication/nested producers use the same BOOLEAN consumers,
parallelism one, two warmups/five measurements. Both versions require consumed
NativeCalc work and no fallback; the candidate requires no evaluator and the actual
preceding version requires its generated route. Seconds are median (min-max):

| Flink / producer | Candidate stock | Native | Previous stock | Previous |
| --- | ---: | ---: | ---: | ---: |
| 1.18.1 / add | 0.156515 (0.149577-0.168454) | 0.181894 (0.177573-0.183620) | 0.172555 (0.170005-0.179224) | 0.206288 (0.201754-0.210439) |
| 1.18.1 / multiply | 0.226769 (0.220124-0.239322) | 0.214279 (0.211862-0.224900) | 0.229178 (0.220161-0.240174) | 0.278883 (0.271642-0.300167) |
| 1.18.1 / nested | 0.252536 (0.245212-0.256080) | 0.213737 (0.210284-0.232305) | 0.250923 (0.233308-0.260183) | 0.319663 (0.302313-0.334127) |
| 2.2.1 / add | 0.156260 (0.149138-0.169494) | 0.179437 (0.175440-0.200563) | 0.163660 (0.155227-0.168385) | 0.211092 (0.204884-0.224814) |
| 2.2.1 / multiply | 0.221233 (0.216625-0.238267) | 0.202235 (0.196252-0.205395) | 0.221499 (0.215712-0.227944) | 0.283277 (0.277864-0.290121) |
| 2.2.1 / nested | 0.248131 (0.242918-0.252392) | 0.221560 (0.216796-0.230291) | 0.246680 (0.242554-0.248168) | 0.311119 (0.309095-0.319681) |

Native/stock elapsed-time ratios span 0.846-1.162;
native/previous ratios span 0.669-0.882. Ratios above one are slower.
Unfavorable results, overlapping ranges and independent stock controls remain visible;
prepared qualification does not establish universal complete-job acceleration.

All 856 focused JVM invocations and 12 existing fixture smokes pass across released
Flink 1.18.1/2.2.1 with JDK 11/17, compiled for Java release 11. Complete 5,003-row
witnesses compare values, schema, NULLs, IDs and cardinality over 11 precision/scale
pairs, including nullable extrema/overflow and safe NOT NULL values. They require
consumed native work, no fallback and empty evaluators for admitted arithmetic ABS.
Filters, grouping, lazy CASE branches, empty batches, selected failures, shared state
and the #577/#581 projection-default regressions pass. Direct overflowing DECIMAL
outputs retain stock ERROR/DROP behavior. The new release DSO passes a fresh
1,019 core tests with two pre-existing ignores; source/DSO hashes, allocator aliases
and independently recompiled baseline classes are recorded.

Reproduce prepared qualification using `integral_expression`, DECIMAL inputs and
`SF_INTEGRAL_EXPRESSION='ABS(v*v)>0 OR CAST(NULL AS BOOLEAN)'`. Complete jobs use
the same `sf.integral.expression` with `sf.integral.decimalPrecision=38` and
`sf.integral.decimalScale=18`. Remaining unary/rounding producers are tracked in
[#491](https://github.com/datafusion-contrib/StreamFusion/issues/491).

## DECIMAL negation over arithmetic

DECIMAL-only addition, subtraction and multiplication now retain native unary
minus, including same-operator nesting and same-scale precision widening. Each
arithmetic node keeps its resolved precision/scale and overflow NULL. Negation
uses the existing Arrow numeric kernel for arrays and exact i128 scalar dispatch;
all legal DECIMAL coefficients have a representable negation. The batching
structure follows Comet's Arrow negation; released Flink supplies the semantic
rules. Rust, the release DSO, JNI/ownership and options are unchanged.

Division, remainder, mixed numeric inputs, rescaling, narrowing, mixed ABS/negation
and unverified producers retain generated evaluation. The existing projection-default
repairs remain active for NOT NULL basic-type consumers and are not claimed as
JVM-free acceleration. Direct overflowing DECIMAL outputs retain ERROR/DROP
enforcement. Character CAST/TRY_CAST retain their separate producer whitelist;
admitting arithmetic negation does not extend every enclosing conversion.

The [result record](results/decimal-arithmetic-negate-2026-11-02.json) and
[raw trials](results/decimal-arithmetic-negate-2026-11-02.csv.gz) retain 64 configurations
and 6,400 trials. Release/default-feature/mimalloc, JDK 17.0.12, Apple M4 Pro,
one thread, 16 warmups/nine measurements and 131,072 rows/trial compare released
generated Java on matching prepared GenericRowData/BinaryRowData with production
Calc on Arrow. Native timing stays inside Rust; initial JNI/C Data import/export
and perimeter transposes are excluded, while intrinsic previous JVM callbacks
remain inside. The independently recompiled actual preceding encoder includes
the explicit CAST correctness repair and binds its real generated evaluator.
Both versions use the identical, byte-verified preceding executable and DSO.
The candidate requires empty descriptors and intended native work; checksums match.

DECIMAL(18,2)/(38,18), nullable/NOT NULL sources, signed extrema and large coefficients
cover `-(v+v)`, `-(v*v)` and `-((v+v)*v)` through
`>0 OR CAST(NULL AS BOOLEAN)`. This nullable consumer retains overflow NULL even
for NOT NULL inputs. Timings measure composed arithmetic negation through a
BOOLEAN consumer, not final DECIMAL output. Bulk batches contain 1,024 rows;
16-row multiplication controls remain separate. Untimed allocation probes retain
requested bytes and output buffers separately.

| Flink | Workload | Cells | Stock/native median range | Previous/native median range | Slower than stock |
| --- | --- | ---: | ---: | ---: | ---: |
| 1.18.1 | bulk | 24 | 1.132-3.867x | 3.654-9.141x | 0 |
| 1.18.1 | tiny control | 8 | 0.224-1.174x | 7.753-10.926x | 6 |
| 2.2.1 | bulk | 24 | 1.089-4.076x | 3.694-8.399x | 0 |
| 2.2.1 | tiny control | 8 | 0.228-1.135x | 7.319-10.605x | 6 |

All 48 bulk cells beat stock and actual previous behavior; 12/16 tiny
controls remain slower than stock. Independent stock-process drift spans
0.565-2.730x; every trial is retained.

Six million-row row-source/blackhole controls retain planning/startup, source
generation, production JNI and both transposes. Nullable DECIMAL(18,2) addition
and DECIMAL(38,18) multiplication/nested producers use the same BOOLEAN consumers,
parallelism one, two warmups/five measurements. Both versions require consumed
NativeCalc and no fallback; the candidate requires empty evaluators while the
actual preceding control retains its generated route. Seconds are median (min-max):

| Flink / producer | Candidate stock | Native | Previous stock | Previous |
| --- | ---: | ---: | ---: | ---: |
| 1.18.1 / add | 0.159210 (0.154452-0.162179) | 0.183171 (0.182496-0.189683) | 0.165552 (0.158069-0.180885) | 0.211707 (0.201569-0.221333) |
| 1.18.1 / multiply | 0.220525 (0.214375-0.243314) | 0.204681 (0.204143-0.217983) | 0.219037 (0.212691-0.241882) | 0.273960 (0.270173-0.283251) |
| 1.18.1 / nested | 0.249028 (0.239493-0.252058) | 0.218595 (0.209154-0.219580) | 0.252982 (0.237527-0.265619) | 0.325243 (0.317213-0.337302) |
| 2.2.1 / add | 0.156649 (0.152608-0.161492) | 0.177944 (0.177173-0.185652) | 0.166901 (0.157939-0.180192) | 0.216225 (0.209954-0.223065) |
| 2.2.1 / multiply | 0.229529 (0.223492-0.241392) | 0.207194 (0.202259-0.210548) | 0.228994 (0.211501-0.237611) | 0.281756 (0.274132-0.290865) |
| 2.2.1 / nested | 0.236939 (0.224113-0.247618) | 0.206135 (0.204783-0.215894) | 0.236026 (0.229298-0.253815) | 0.308385 (0.304602-0.325335) |

Native/stock elapsed-time ratios span 0.870-1.151;
native/previous ratios span 0.668-0.865. Ratios above one are slower.
Overlapping ranges, unfavorable samples and independent stock controls remain
visible; prepared qualification does not establish universal complete-job acceleration.

All 1088 focused JVM invocations and 12 fixture smokes pass on released Flink
1.18.1/2.2.1 and JDK 11/17, compiled for Java release 11. Complete 5,003-row
witnesses compare values, types, NULLs, IDs and cardinality over eleven precision/scale
pairs. Admitted arithmetic negation requires consumed native work, no fallback
and empty evaluator descriptors. Filters, grouping, CASE, empty batches, selected
failures, shared state, ABS controls and #577/#581/#582 defaults remain verified.
Safe numeric CAST and retained NULL-aware consumers preserve their native routes.
The unchanged release DSO retains the earlier fresh 1,019 Rust core passes and
two pre-existing ignores; that run is reused rather than presented as a new run.
Source/runtime hashes, allocator aliases and independently recompiled baseline
classes are recorded.

Reproduce prepared qualification using `integral_expression` and
`SF_INTEGRAL_EXPRESSION='-(v*v)>0 OR CAST(NULL AS BOOLEAN)'`. Complete jobs use
the same `sf.integral.expression`, `sf.integral.decimalPrecision=38` and
`sf.integral.decimalScale=18`. Further unary/sign/rounding coverage remains in
[#491](https://github.com/datafusion-contrib/StreamFusion/issues/491).

## DECIMAL SIGN over arithmetic

DECIMAL-only addition, subtraction and multiplication now retain native SIGN,
including verified SIGN nesting and same-scale precision widening. Each arithmetic
node preserves its resolved precision/scale and overflow NULL. SIGN reuses the
existing typed Decimal128 kernel: ordinary decimals return scaled -1, zero or one;
noncompact DECIMAL(p,p), p > 18, returns NULL for nonzero coefficients and scaled
zero for zero. Released Flink supplies these rules; Comet's Arrow unary batching
is a structural reference, not a source of Spark's result-type semantics.
Production Rust, the DSO, JNI/ownership and options are unchanged.

SIGN retains a separate recursive proof so compact DECIMAL(p,p), p <= 18,
intermediates stay generated even under arithmetic or widening. Division, remainder,
mixed numeric operands, ABS/negation, rescaling, narrowing and unverified producers
also retain generated evaluation. The #584 final-projection-default repair remains
active for affected NOT NULL basic-type consumers; it is not claimed as JVM-free
acceleration. Direct DECIMAL ERROR/DROP enforcement remains intact. Multiplication
of DECIMAL(38,38) inputs resolves to scale 37 in released Flink, so its SIGN can
represent one; input type alone does not determine whether SIGN returns NULL.
Character CAST/TRY_CAST keep their separate producer proofs.

The [result record](results/decimal-arithmetic-sign-2026-11-02.json) and
[raw trials](results/decimal-arithmetic-sign-2026-11-02.csv.gz) retain 128 configurations
and 12,800 trials. Release/default-feature/mimalloc, JDK 17.0.12, Apple M4 Pro,
one thread, 16 warmups/nine measurements and 131,072 rows/trial compare released
generated Java on matching prepared GenericRowData/BinaryRowData with production
Calc on Arrow. Native timing stays inside Rust; initial JNI/C Data import/export
and perimeter transposes are excluded, while intrinsic previous JVM callbacks
remain inside. The independently recompiled actual preceding encoder at 69e4198f
includes the SIGN projection-default repair and binds its real generated evaluator.
Both paths share the expanded four-type benchmark fixture and identical DSO.
The executable changes only to add the two zero-integral-width corpora; it is not
claimed byte-identical to the historical two-type executable. Separate old/new
fixture smokes verify unchanged original-type checksums. The candidate requires
empty descriptors and intended native work; every trial has a matching checksum.

DECIMAL(18,2)/(19,19)/(38,18)/(38,38), nullable/NOT NULL sources, signed extrema
and large coefficients cover `SIGN(v+v)`, `SIGN(v*v)` and `SIGN((v+v)*v)` through
`>0 OR CAST(NULL AS BOOLEAN)`. This nullable consumer retains overflow NULL even
for NOT NULL inputs. Timings measure composed arithmetic SIGN through a BOOLEAN
consumer, not final DECIMAL output. Bulk batches contain 1,024 rows; 16-row
multiplication controls remain separate. Untimed allocation probes retain requested
bytes and output buffers separately.

| Flink | Workload | Cells | Stock/native median range | Previous/native median range | Slower than stock |
| --- | --- | ---: | ---: | ---: | ---: |
| 1.18.1 | bulk | 48 | 1.495-7.225x | 3.745-9.152x | 0 |
| 1.18.1 | tiny control | 16 | 0.217-1.630x | 8.005-13.690x | 12 |
| 2.2.1 | bulk | 48 | 1.514-7.059x | 3.636-9.193x | 0 |
| 2.2.1 | tiny control | 16 | 0.221-1.644x | 8.235-13.466x | 11 |

All 96 bulk cells beat stock and actual previous behavior; 23/32 tiny
controls remain slower than stock. Independent stock-process drift spans
0.721-2.134x; every trial is retained.

Eight million-row row-source/blackhole configurations retain planning/startup,
source generation, production JNI and both transposes. Nullable DECIMAL(18,2)
addition, DECIMAL(38,18) multiplication/nested and DECIMAL(38,38) multiplication
use the same BOOLEAN consumers, parallelism one, two warmups/five measurements.
Both versions require consumed NativeCalc and no fallback; the candidate requires
empty evaluators while the actual preceding control retains its generated route.
Seconds are median (min-max):

| Flink / producer | Candidate stock | Native | Previous stock | Previous |
| --- | ---: | ---: | ---: | ---: |
| 1.18.1 / add | 0.158934 (0.153983-0.176404) | 0.179950 (0.178487-0.193787) | 0.159119 (0.156121-0.168153) | 0.212601 (0.202957-0.214075) |
| 1.18.1 / multiply | 0.226859 (0.223039-0.230969) | 0.205721 (0.198910-0.217739) | 0.232498 (0.219782-0.235035) | 0.279492 (0.276106-0.288636) |
| 1.18.1 / nested | 0.260404 (0.253915-0.266903) | 0.216729 (0.212598-0.222201) | 0.263372 (0.247378-0.275634) | 0.314720 (0.310512-0.334009) |
| 1.18.1 / zero-integer | 0.204853 (0.201356-0.214965) | 0.195758 (0.191983-0.201677) | 0.204023 (0.193139-0.204758) | 0.261931 (0.246114-0.269036) |
| 2.2.1 / add | 0.164609 (0.159899-0.179596) | 0.178191 (0.173004-0.184767) | 0.158762 (0.157087-0.171613) | 0.211523 (0.206122-0.228402) |
| 2.2.1 / multiply | 0.240034 (0.229098-0.243295) | 0.204904 (0.196789-0.211282) | 0.227246 (0.221138-0.231929) | 0.294640 (0.287326-0.313013) |
| 2.2.1 / nested | 0.259564 (0.251532-0.264922) | 0.213077 (0.209736-0.222779) | 0.262493 (0.255530-0.272066) | 0.331459 (0.315043-0.340202) |
| 2.2.1 / zero-integer | 0.205501 (0.198043-0.216030) | 0.196261 (0.187968-0.209492) | 0.205637 (0.195450-0.218607) | 0.262806 (0.252995-0.285873) |

Native/stock elapsed-time ratios span 0.821-1.132;
native/previous ratios span 0.643-0.846. Ratios above one are slower.
Overlapping ranges, unfavorable samples and independent stock controls remain
visible; prepared qualification does not establish universal complete-job acceleration.

All 480 focused JVM invocations pass on released Flink 1.18.1/2.2.1
and JDK 11/17, compiled for Java release 11. All 36 fixture smokes pass
on both Flink lines using JDK 17. Complete 5,003-row
witnesses compare values, types, NULLs, IDs and cardinality over eleven precision/scale
pairs, crossing the default 1,024-row Arrow batch limit. Admitted arithmetic SIGN
requires consumed native work, no fallback and empty evaluator descriptors.
Filters, grouping, CASE, empty batches, selected failures, shared state and SIGN/CAST
projection defaults remain verified. The unchanged released
`ScalarFunctionsTest.testSign` stock oracle also passes on each release/runtime pair;
separate SQL witnesses establish native work.
The unchanged release DSO retains the earlier fresh 1,019 Rust core passes and two
pre-existing ignores; that run is reused rather than presented as a new run.
Source/runtime hashes, allocator aliases and independently recompiled baseline
classes are recorded. The fixture example changes; no production Rust source changes.

Reproduce prepared qualification using `integral_expression` and
`SF_INTEGRAL_EXPRESSION='SIGN(v*v)>0 OR CAST(NULL AS BOOLEAN)'`. Complete jobs use
the same `sf.integral.expression`, `sf.integral.decimalPrecision=38` and
`sf.integral.decimalScale=38`. Further unary/sign/rounding coverage remains in
[#491](https://github.com/datafusion-contrib/StreamFusion/issues/491).

## Mixed exact GREATEST

GREATEST over verified direct/literal integer and DECIMAL operands now casts each argument
to Flink's resolved DECIMAL precision/scale before the existing Arrow extrema kernel.
Integer capacity must be sufficient and no DECIMAL may lose scale. A DECIMAL whose metadata
changes must be NOT NULL: released Flink can dereference it before checking its NULL flag.
Nullable metadata changes, scale reduction and computed/unverified producers retain generated
evaluation. These measurements qualify GREATEST; LEAST has its own qualification. No production Rust,
JNI/ownership or configuration changes.

The [result record](results/mixed-exact-greatest-2026-11-02.json) and
[raw trials](results/mixed-exact-greatest-2026-11-02.csv.gz) retain 112 configurations/11,200 trials against
canonical main `b0eb507c`. The actual preceding encoder is independently compiled from
that committed source; 1,390 unchanged Java source hashes match the complete previous build.
Both paths use the same production DSO and expanded two-column fixture. A fresh release
benchmark executable links the current production Rust. Separate old/new fixture smokes
confirm that adding the second input leaves existing one-column corpora unchanged.

Release/default-feature, JDK 17.0.12, Apple M4 Pro, one thread, 16 warmups/nine
measurements and 131,072 rows per trial compare stock generated Java over prepared
GenericRowData/BinaryRowData with production Calc over equal Arrow batches. Native timing
stays inside Rust; initial JNI/C Data entry/export and perimeter transposes are excluded.
Intrinsic previous JVM callbacks remain included. Every trial checksum matches. The
new path requires empty evaluators and intended native work.

DECIMAL(18,2)/(19,19)/(38,18) with INT/BIGINT, both operand orders, a three-argument control,
NULL/NOT NULL inputs, signed extrema, large coefficients and both row representations use
`GREATEST(v,n)>0 OR CAST(NULL AS BOOLEAN)`. This measures numeric extrema through a nullable
BOOLEAN consumer, not final DECIMAL output. Bulk batches contain 1,024 rows; 16-row controls
remain separate. Untimed allocation probes retain their requested-byte/output-buffer counts.

| Flink | Workload | Cells | Stock/native median range | Previous/native median range | Slower than stock |
| --- | --- | ---: | ---: | ---: | ---: |
| 1.18.1 | bulk | 38 | 1.959-14.317x | 5.929-16.405x | 0 |
| 1.18.1 | tiny control | 18 | 0.078-0.546x | 6.045-8.433x | 18 |
| 2.2.1 | bulk | 38 | 1.943-13.777x | 5.827-17.625x | 0 |
| 2.2.1 | tiny control | 18 | 0.081-0.580x | 6.844-8.663x | 18 |

All 76 bulk cells beat stock and the actual preceding implementation. All 36 tiny controls
remain slower than stock, although they improve on the preceding JVM route.

Eight million-row row-source/blackhole configurations retain planning/startup, source
generation, production JNI and both transposes, at parallelism one with two warmups/five
measurements. Compact controls use DECIMAL(18,2) with INT/BIGINT, wide uses DECIMAL(38,18)
with BIGINT, and fractional uses NOT NULL DECIMAL(19,19) with BIGINT. Both versions require
consumed NativeCalc and no fallback; only the candidate requires empty evaluators. Seconds
are median (min-max); independent stock controls and unfavorable samples are retained.

| Flink / inputs | Candidate stock | Native | Previous stock | Previous |
| --- | ---: | ---: | ---: | ---: |
| 1.18.1 / compact-int | 0.216645 (0.198815-0.224089) | 0.217573 (0.214520-0.243753) | 0.207151 (0.202257-0.211784) | 0.247769 (0.244072-0.264842) |
| 1.18.1 / compact-bigint | 0.198637 (0.188381-0.220365) | 0.209963 (0.201012-0.225706) | 0.204936 (0.193470-0.217870) | 0.250919 (0.240080-0.262518) |
| 1.18.1 / wide-bigint | 0.218562 (0.214016-0.223888) | 0.233461 (0.219373-0.241594) | 0.223502 (0.216588-0.238021) | 0.260326 (0.259093-0.304801) |
| 1.18.1 / fractional-bigint | 0.210791 (0.210339-0.241606) | 0.215612 (0.212575-0.223834) | 0.224164 (0.219900-0.236927) | 0.276282 (0.272253-0.284695) |
| 2.2.1 / compact-int | 0.203020 (0.195792-0.208881) | 0.210581 (0.209462-0.214752) | 0.195421 (0.192371-0.206530) | 0.257562 (0.243652-0.261182) |
| 2.2.1 / compact-bigint | 0.198064 (0.197332-0.206831) | 0.218062 (0.209673-0.236910) | 0.207929 (0.203583-0.210888) | 0.268514 (0.257670-0.275773) |
| 2.2.1 / wide-bigint | 0.219288 (0.214332-0.220702) | 0.229656 (0.224162-0.231757) | 0.219867 (0.212633-0.229906) | 0.276054 (0.268153-0.318449) |
| 2.2.1 / fractional-bigint | 0.231148 (0.228257-0.240856) | 0.224373 (0.220700-0.246401) | 0.230612 (0.228576-0.238758) | 0.294351 (0.283615-0.319577) |

Native/stock elapsed-time ratios span 0.971-1.101;
native/previous ratios span 0.762-0.897. Ratios above
one are slower. These measurements do not establish universal complete-job acceleration.

All 796 focused JVM invocations pass across released Flink 1.18.1/2.2.1 and JDK 11/17,
compiled for Java release 11. Complete 5,003-row witnesses verify values, types, NULLs, IDs,
cardinality and actual native consumption, plus consumers, grouping, lazy/empty batches,
nullable-metadata failures and the current DECIMAL projection-default repairs. The unchanged
released GreatestLeastFunctionsITCase passes 146 stock-oracle invocations; 44 fixture smokes
pass separately. Existing native production code and its DSO are unchanged; this is not a
claim of a new Rust core-suite run. Further mixed numeric, rounding and producer scope remains
in [#491](https://github.com/datafusion-contrib/StreamFusion/issues/491).

## Mixed exact LEAST

Verified mixed integer/DECIMAL LEAST reuses the lossless casts and existing Arrow minimum
kernel described for GREATEST. Nullable DECIMAL metadata changes, scale reduction, insufficient
integral capacity and unverified/computed producers retain generated evaluation. This preserves
released Flink's nullable-conversion failures. No production Rust, JNI, ownership or option changes.

The [result record](results/mixed-exact-least-2026-11-02.json) and
[raw trials](results/mixed-exact-least-2026-11-02.csv.gz) retain 112 configurations/11,200 trials.
The actual preceding encoder is independently recompiled from `a7eaea10`, the separate GREATEST
commit on canonical main `b0eb507c`. Both paths use the same release/default-feature DSO,
fresh benchmark executable and two-column fixture. JDK 17.0.12, Apple M4 Pro, one thread,
16 warmups/nine measurements and 131,072 rows per trial match the GREATEST methodology.
Prepared GenericRowData/BinaryRowData and Arrow contain equal values/types/NULLs. Rust timing
excludes initial JNI/import/export and perimeter transposes; intrinsic previous callbacks remain.
Checksums match, with consumed native function work and empty candidate evaluator descriptors.

DECIMAL(18,2)/(19,19)/(38,18) with INT/BIGINT, both orders, a three-argument control, NULL/NOT NULL,
signed extrema and large coefficients use `LEAST(v,n)>0 OR CAST(NULL AS BOOLEAN)`. These timings
consume a nullable BOOLEAN, not final DECIMAL output. Bulk batches contain 1,024 rows and tiny
controls 16. Requested-byte/output-buffer allocation probes remain in the result record.

| Flink | Workload | Cells | Stock/native median range | Previous/native median range | Slower than stock |
| --- | --- | ---: | ---: | ---: | ---: |
| 1.18.1 | bulk | 38 | 1.911-13.370x | 6.227-15.783x | 0 |
| 1.18.1 | tiny control | 18 | 0.070-0.552x | 6.984-8.190x | 18 |
| 2.2.1 | bulk | 38 | 1.200-13.535x | 5.905-15.711x | 0 |
| 2.2.1 | tiny control | 18 | 0.073-0.532x | 6.956-8.294x | 18 |

All 76 bulk medians beat stock and previous. All 36 tiny controls remain slower than stock.
Eight million-row row-source/blackhole controls retain startup/planning, source generation,
production JNI and both transposes at parallelism one, with two warmups/five measurements.
Seconds are median (min-max); independent stock controls and unfavorable samples are retained.

| Flink / inputs | Candidate stock | Native | Previous stock | Previous |
| --- | ---: | ---: | ---: | ---: |
| 1.18.1 / compact-int | 0.280502 (0.261990-0.367126) | 0.315172 (0.205658-0.333249) | 0.210022 (0.193419-0.293259) | 0.272373 (0.242126-0.282771) |
| 1.18.1 / compact-bigint | 0.216120 (0.200640-0.235634) | 0.229793 (0.212071-0.291695) | 0.222998 (0.195001-0.241038) | 0.343002 (0.275064-0.491480) |
| 1.18.1 / wide-bigint | 0.286441 (0.224139-0.691903) | 0.308327 (0.235885-0.503705) | 0.227179 (0.208461-0.231006) | 0.280126 (0.269329-0.381201) |
| 1.18.1 / fractional-bigint | 0.219772 (0.213547-0.227718) | 0.218514 (0.212634-0.229046) | 0.216152 (0.211148-0.221203) | 0.285881 (0.277306-0.305001) |
| 2.2.1 / compact-int | 0.204871 (0.198575-0.211131) | 0.209832 (0.204694-0.221375) | 0.204786 (0.196269-0.207228) | 0.250877 (0.243003-0.270677) |
| 2.2.1 / compact-bigint | 0.205481 (0.203301-0.217226) | 0.214091 (0.207014-0.227696) | 0.201066 (0.197392-0.217963) | 0.258565 (0.248192-0.292048) |
| 2.2.1 / wide-bigint | 0.215578 (0.211774-0.219300) | 0.217707 (0.214002-0.225476) | 0.214131 (0.210309-0.233518) | 0.272811 (0.254320-0.355432) |
| 2.2.1 / fractional-bigint | 0.298122 (0.272389-0.354822) | 0.255497 (0.245776-0.353708) | 0.308293 (0.245968-0.486820) | 0.474460 (0.336639-0.607842) |

Native/stock elapsed ratios span 0.857-1.124 and native/previous 0.539-1.157. Several
short jobs have wide sample ranges and differing stock controls; these are retained, not
discarded or interpreted as universal complete-job acceleration.

All 264 focused JVM invocations pass across released Flink 1.18.1/2.2.1 and JDK 11/17,
compiled for Java release 11, plus 44 fixture smokes. Complete 5,003-row runtime witnesses
compare values/types/NULLs/IDs/cardinality and require consumed NativeCalc work with no
fallback and empty evaluators. Consumers, grouping, selected failures, empty batches,
nullable metadata failures and current DECIMAL projection defaults remain covered. The 146
unchanged GreatestLeastFunctionsITCase stock-oracle invocations recorded for GREATEST cover
both extrema functions; they are not claimed as a second new run. Production Rust is unchanged.
Further mixed numeric and producer scope remains in
[#491](https://github.com/datafusion-contrib/StreamFusion/issues/491).


## Mixed exact IF

Direct/literal mixed integer/DECIMAL IF branches use a typed Arrow selector when both
conversions are lossless and the resolved DECIMAL precision exceeds 18. NULL conditions
select ELSE; selected NULL values stay NULL. The selector writes one output array without
filtered batches, separate cast arrays or a merge. Matching branch types retain the existing
native CASE path. Computed/unverified mixed branches, scale reduction, insufficient integral
capacity, stateful/volatile calls and compact-result mixed normalization retain their existing
routes. No JNI, ownership or configuration change is introduced.

The first generic CASE investigation had **16/48 bulk cells slower than stock**. Process
sampling identified `CaseBody::expr_or_expr`, `arrow_select::merge` and `MutableArrayData::extend`.
A representative 1,024-row compact-result control went from 109 allocations/68,528 requested
bytes to 56/20,418 after fusion. Its fused median was still 7.84 ns/row against stock's
6.78 ns/row, so compact mixed normalization stays generated. The
[result record](results/mixed-exact-if-2026-11-02.json) preserves this failed matrix and pilot;
[initial raw trials](results/mixed-exact-if-initial-2026-11-02.csv.gz) are separate from the
[final raw trials](results/mixed-exact-if-2026-11-02.csv.gz).

The final noncompact matrix retains 72 configurations/7,200 trials against independently
recompiled actual preceding commit `289b69f0`, on canonical main `b0eb507c`. The candidate
uses the new release/default-feature DSO; previous controls use their independently
preserved preceding DSO/executable and the same expanded logical corpus. Apple M4 Pro,
JDK 17.0.12, one thread, 16 warmups/nine measurements, 131,072 rows/trial and matching prepared
GenericRowData/BinaryRowData/Arrow inputs follow the preceding extrema method. Timing stays
inside Rust and includes intrinsic JVM callbacks, allocations, conversions and output consumption;
initial JNI/import/export and perimeter transposes are outside this small-function timer.
Every trial checksum agrees and the admitted candidate requires empty evaluators.

DECIMAL(4,2)/(18,2)/(19,19)/(38,18) with INT/BIGINT, both branch orders, NULL/NOT NULL,
signed extrema and large coefficients use `IF(n>=0,v,n)>0 OR CAST(NULL AS BOOLEAN)`.
Every selected bulk combination resolves to precision greater than 18. This measures a nullable
BOOLEAN consumer, not final DECIMAL output. Bulk batches have 1,024 rows and tiny controls 16.

| Flink | Workload | Cells | Stock/native median range | Previous/native median range | Slower than stock |
| --- | --- | ---: | ---: | ---: | ---: |
| 1.18.1 | bulk | 24 | 1.051-5.253x | 6.298-12.165x | 0 |
| 1.18.1 | tiny control | 12 | 0.065-0.326x | 6.936-8.202x | 12 |
| 2.2.1 | bulk | 24 | 1.523-5.820x | 5.314-14.075x | 0 |
| 2.2.1 | tiny control | 12 | 0.066-0.319x | 7.117-8.761x | 12 |

All 48 bulk medians beat stock and previous. All 24 tiny controls remain slower than stock.
Eight complete million-row source/blackhole jobs retain startup, JNI and both transposes, at
parallelism one with two warmups/five measurements. `fractional-int` uses DECIMAL(19,19)/INT;
`compact-bigint` uses DECIMAL(18,2)/BIGINT (the resolved result is noncompact); `wide-bigint`
uses DECIMAL(38,18)/BIGINT; `fractional-bigint` uses DECIMAL(19,19)/BIGINT. Seconds are median
(min-max); independent stock controls and unfavorable samples remain recorded.

| Flink / inputs | Candidate stock | Native | Previous stock | Previous |
| --- | ---: | ---: | ---: | ---: |
| 1.18.1 / fractional-int | 0.207477 (0.201241-0.208401) | 0.223703 (0.215123-0.231077) | 0.202862 (0.196273-0.206931) | 0.275605 (0.272918-0.285621) |
| 1.18.1 / compact-bigint | 0.215457 (0.205784-0.220610) | 0.224521 (0.213762-0.234266) | 0.202049 (0.192554-0.209908) | 0.258307 (0.256854-0.272890) |
| 1.18.1 / wide-bigint | 0.202431 (0.198542-0.209724) | 0.220264 (0.214817-0.227628) | 0.205333 (0.200273-0.229048) | 0.293870 (0.281837-0.301714) |
| 1.18.1 / fractional-bigint | 0.213749 (0.211021-0.221769) | 0.232539 (0.227125-0.249843) | 0.206994 (0.200390-0.297613) | 0.293367 (0.268386-0.505663) |
| 2.2.1 / fractional-int | 0.204839 (0.202637-0.214151) | 0.235065 (0.228285-0.237179) | 0.212130 (0.208221-0.220079) | 0.282727 (0.272990-0.290860) |
| 2.2.1 / compact-bigint | 0.199342 (0.197950-0.216623) | 0.217757 (0.211355-0.227652) | 0.191884 (0.185234-0.202888) | 0.253936 (0.243814-0.262679) |
| 2.2.1 / wide-bigint | 0.206973 (0.199895-0.215854) | 0.235268 (0.226704-0.252028) | 0.202695 (0.195838-0.210014) | 0.277684 (0.274433-0.317087) |
| 2.2.1 / fractional-bigint | 0.213100 (0.206608-0.223398) | 0.230963 (0.227135-0.242107) | 0.205177 (0.198572-0.223956) | 0.275922 (0.266477-0.288107) |

Native/stock elapsed ratios span 1.042-1.148: every measured complete job remains slower
than stock. Native/previous ratios span 0.750-0.869. This establishes improved function work
and previous-path costs, not whole-job acceleration.

The new Rust selector passes scalar, nullable array, sliced-offset, full-BIGINT and empty-input
regressions; 883 core tests pass with `--no-default-features`. The released upstream
`ScalarFunctionsTest.testIf` stock oracle passes on Flink 1.18.1/2.2.1 with JDK 11/17.
All 538 final focused JVM invocations pass (two released-1.18 negative-TIME controls abort
as expected), plus 48 fixture smokes. Runtime SQL witnesses compare complete 5,003-row
outputs, schemas, NULLs, IDs/cardinality,
consumers, grouping, lazy branches, selected failures, empty filtered batches and current
DECIMAL projection defaults, requiring consumed native work and empty evaluators for admitted
paths. Final source hashes and focused results accompany the measurement record. A final
admission-only restriction preserves matching-type CASE; measured mixed-signature lowering
is unchanged. Further producers, compact mixed normalization and rounding remain in
[#491](https://github.com/datafusion-contrib/StreamFusion/issues/491).

## Mixed selection latest-main regression

The six GREATEST, LEAST, IF and DATE HOUR/MINUTE/SECOND commits rebase onto canonical
main `be842d2d` with six equal patches in `git range-diff`. Historical measurements keep
their original source hashes, encoders, DSOs and pre-rebase baseline IDs; they are not
relabeled as new timings. The [regression record](results/mixed-selection-main-regression-2026-11-02.json)
maps those commits into the rebased history and records **768 fresh passing JVM invocations**
(192 per released Flink 1.18.1/2.2.1 and JDK 11/17 pair), compiled for Java release 11.
They cover complete mixed-selection/DATE result maps and native consumption, shared state,
DECIMAL defaults, calendar/timestamp controls and current nested bridge admission/fallback.

## UTC LTZ calendar FLOOR

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

## UTC LTZ calendar CEIL

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

## BIGINT epoch millisecond conversion

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

## Function allocator provenance and matched IF controls

The [artifact audit](results/function-allocator-provenance-2026-11-02.json) corrects blanket
mimalloc labels on this change's mixed-selection and UTC-rounding records. Prepared Rust
executables and JNI core DSOs are separate link products. CountingAllocator declares System,
but only inspecting the final linked artifact establishes whether allocator symbols were rebound.
The retained extrema executable imports System malloc while its shared `ef29d700` JNI DSO
uses mimalloc. The retained `4a10d804` UTC-rounding core DSO uses System. Original IF candidate
and calendar executable artifacts are not retained for independent symbol reinspection; their
hashes/raw measurements remain, without inventing an allocator attribution. These historical
numbers are not a mimalloc-specific prepared-function qualification.

A fresh [matched IF record](results/mixed-exact-if-allocator-matched-2026-11-02.json) and
[7,200 raw trials](results/mixed-exact-if-allocator-matched-2026-11-02.csv.gz) use the same
current release/System executable and core DSO on both sides. The independently compiled actual
pre-feature encoder `289b69f0` keeps its generated evaluator; candidate uses the typed selector.
This isolates the encoder/selection change on the same current primitive evaluator/transport;
it is not a replay of the complete historical DSO. Input values/checksums, DECIMAL signatures,
branch orders, nullabilities, GenericRowData/BinaryRowData, nullable BOOLEAN consumer, timing
boundary, warmup/repetition and complete-job perimeter match the original IF qualification.
Every checksum agrees and candidate native expressions require empty evaluators. Reused fixture
and released-oracle controls are identified as reused, not newly executed IF oracles.

| Flink | Workload | Cells | Stock/native median range | Prior encoder/native median range | Slower than stock |
| --- | --- | ---: | ---: | ---: | ---: |
| 1.18.1 | bulk | 24 | 1.243-6.803x | 7.068-10.927x | 0 |
| 1.18.1 | tiny control | 12 | 0.064-0.293x | 6.613-8.869x | 12 |
| 2.2.1 | bulk | 24 | 1.067-4.861x | 6.423-10.811x | 0 |
| 2.2.1 | tiny control | 12 | 0.066-0.289x | 6.625-8.397x | 12 |

All 48 bulk medians beat stock and the prior-encoder control. All 24 tiny controls still
lose to stock. Complete million-row source/blackhole jobs retain startup, JNI and both
transposes, two warmups/five measurements and independent stock controls. Seconds are
median (min-max).

| Flink / inputs | Candidate stock | Native | Prior-control stock | Prior encoder |
| --- | ---: | ---: | ---: | ---: |
| 1.18.1 / fractional-int | 0.211896 (0.205774-0.214337) | 0.226388 (0.214241-0.227146) | 0.201043 (0.194222-0.210099) | 0.270145 (0.267369-0.273343) |
| 1.18.1 / compact-bigint | 0.199994 (0.194616-0.210764) | 0.224322 (0.211926-0.235112) | 0.200780 (0.199217-0.201742) | 0.266821 (0.257370-0.278869) |
| 1.18.1 / wide-bigint | 0.208675 (0.202621-0.213879) | 0.225573 (0.219125-0.241540) | 0.214507 (0.204583-0.217570) | 0.291849 (0.284361-0.306561) |
| 1.18.1 / fractional-bigint | 0.215004 (0.209909-0.229445) | 0.228181 (0.221224-0.245202) | 0.208205 (0.202990-0.223996) | 0.282070 (0.271877-0.284726) |
| 2.2.1 / fractional-int | 0.206288 (0.203484-0.206633) | 0.225171 (0.223702-0.236765) | 0.202783 (0.201772-0.209552) | 0.287532 (0.285755-0.299882) |
| 2.2.1 / compact-bigint | 0.204214 (0.197145-0.210660) | 0.226605 (0.215100-0.238641) | 0.197689 (0.193144-0.209770) | 0.256399 (0.251695-0.270723) |
| 2.2.1 / wide-bigint | 0.209169 (0.201884-0.212275) | 0.226093 (0.221882-0.236119) | 0.200475 (0.194004-0.205439) | 0.278284 (0.276462-0.285379) |
| 2.2.1 / fractional-bigint | 0.201508 (0.196561-0.211933) | 0.222281 (0.220729-0.230879) | 0.207573 (0.204575-0.210746) | 0.279755 (0.270325-0.294065) |

Native/stock elapsed ratios span **1.061-1.122** and native/prior-encoder **0.773-0.884**.
Every measured complete job remains slower than stock. These matched controls replace an
assumption of matched historical allocator settings; original trials/hashes remain available.

Canonical main advances to `76b1e46f` through CI-status-only PR #586. All ten implementation/
record patches rebase equally in `git range-diff`; `git diff 14f4c6dc HEAD -- src native`
is empty at the rebased implementation head recorded in the audit. No runtime source changes
or new performance timings are inferred from that CI-only rebase. Historical base IDs remain
attached to the measurements they actually produced.

## BIGINT epoch second conversion

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

### Retained millisecond controls

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

## Narrow integer epoch conversion

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


## UTC LTZ fixed-unit FLOOR

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


## UTC LTZ fixed-unit CEIL

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


## Computed integer epoch conversion

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


## Integer ROUND with a bounded runtime position

The planner proves an INT CASE position always returns -38..38 or NULL, with only
failure-free BOOLEAN/logical/integer-comparison conditions. A typed selector chooses the
first TRUE branch in one Arrow pass, retaining NULL conditions and selected NULLs. Integer
ROUND consumes that position array, preserves resolved widths, signed overflow and HALF_UP
ties, and uses no JVM evaluator. Nested bounded CASE and verified integer value producers
are covered. Arbitrary position columns, extreme results and failing/stateful/unverified
producers remain generated. TRUNCATE was literal-position only in this qualification;
its later bounded-runtime extension is qualified separately below.

The [2026-11-03 record](results/bounded-runtime-round-2026-11-03.json) retains released
Flink 1.18.1/2.2.1, Oracle JDK 17.0.12, Apple M4 Pro, one thread and release/mimalloc-linked
artifacts. Equal prepared GenericRowData, BinaryRowData and Arrow contain nullable/NOT NULL
TINYINT/SMALLINT/INT/BIGINT, extrema and NULLs. Each trial consumes 131,072 rows, with
16 warmups and nine measurements. Rust times production operation and equivalent output
consumption after input import and before export. Prior intrinsic JVM callbacks remain
timed. Initial JNI/import/export and perimeter transposes are excluded only from this
small-function gate. Complete jobs retain those costs.

The short shape selects -2/-1; the wide-null shape selects -38/NULL/0; extra computed
INT/BIGINT controls add one in the resolved input width before rounding. The secondary
position-condition input is INT. Actual preceding encoder `7c8ffe00` is independently
compiled against the shared current runtime, fixture and allocator. This isolates encoder
and kernel changes, not a whole historical DSO replay. The independent prior-stock control
remains separate from the candidate's paired stock control.

| Flink | Bulk cells | Stock / native median range | Previous / native median range |
| --- | ---: | ---: | ---: |
| 2.2.1 | 36 | 1.417-3.557x | 3.872-10.091x |
| 1.18.1 | 36 | 1.020-4.796x | 3.963-8.528x |

All 72 bulk cells beat paired stock and the previous generated route.
Independent prior-stock/native spans **0.928-3.531x**;
2 bulk cells have a lower independent stock median than native. These
separate-process stock controls are retained rather than normalized to the paired controls.
The smallest paired gain is modest and measurement ranges can overlap.
Sixteen 16-row controls retain stock/native **0.126-0.291x**;
these small-function results do not establish complete-job acceleration.
The [8,800 raw trials](results/bounded-runtime-round-2026-11-03.csv.gz) retain warmups,
checksums and independent stock measurements.

Two failed/intermediate implementations remain public: the initial generic CASE pilot
loses on every nullable wide-null width, and the selector-only partial matrix still loses
on nullable INT/short GenericRowData (9.433 stock versus 10.452 native ns/row). The selector
removes generic filtering/scattering; precomputed signed-64-bit powers then remove per-row
i128 exponentiation/remainder, retaining an exact separate 10^19 wrapping case. Initial
and intermediate artifacts/classes are hashed. Raw
[initial pilot](results/bounded-runtime-round-2026-11-03-initial-pilot.csv.gz),
[selector pilot](results/bounded-runtime-round-2026-11-03-selector-pilot.csv.gz),
[partial selector qualification](results/bounded-runtime-round-2026-11-03-selector-partial.csv.gz),
[final pilot](results/bounded-runtime-round-2026-11-03-final-pilot.csv.gz) and
[retained literal controls](results/bounded-runtime-round-2026-11-03-literal-controls.csv.gz)
prevent replacing failed measurements with only favorable final numbers. The literal
controls replay the saved preceding executable/DSO independently; their variation is retained.

Eight complete million-row INT/BIGINT jobs use the bounded row source and blackhole sink,
two warmups, five measurements and alternating engine order. They retain startup, JNI and
both transposes. Candidate requires consumed NativeCalc and empty evaluator descriptors;
the actual preceding encoder requires the ROUND evaluator. Seconds, median:

| Flink / type / position shape | Paired stock | Native | Independent stock | Previous evaluator |
| --- | ---: | ---: | ---: | ---: |
| 2.2.1 / INT-short | 0.182302 | 0.195996 | 0.183552 | 0.228636 |
| 2.2.1 / INT-wide-null | 0.188430 | 0.200061 | 0.186238 | 0.227620 |
| 2.2.1 / BIGINT-short | 0.196292 | 0.213548 | 0.193110 | 0.231555 |
| 2.2.1 / BIGINT-wide-null | 0.197680 | 0.201215 | 0.186568 | 0.231936 |
| 1.18.1 / INT-short | 0.188607 | 0.198215 | 0.195201 | 0.228789 |
| 1.18.1 / INT-wide-null | 0.187114 | 0.203781 | 0.183182 | 0.229998 |
| 1.18.1 / BIGINT-short | 0.216962 | 0.223382 | 0.197413 | 0.237526 |
| 1.18.1 / BIGINT-wide-null | 0.196454 | 0.207811 | 0.195804 | 0.249493 |

Native/stock elapsed spans **1.018-1.089**; native/previous spans
**0.833-0.940**. Every trial and full range remains in the
record. No universal job speedup is claimed.

All **268 focused JVM invocations** pass without skips across both releases and JDK 11/17,
targeting Java release 11. Complete 5,003-row maps compare values, schema, NULLs, IDs and
cardinality for every integer width, arithmetic/widening/nesting, NULL/negative/extreme
positions, consumers and all-filtered batches. Admitted routes require actual NativeCalc
consumption and empty evaluators. Unverified positions, out-of-range branches and failing
conditions retain generated controls. The actual before encoder matches complete values
and schema, then fails eight new pure-route assertions; its retained control passes.
Four fresh unchanged released `ScalarFunctionsTest.testRound` stock-oracle invocations pass
separately. All **900 release Rust core tests** pass without ignores, including dynamic
positions, 10^19 ties, slices, ownership, scalar broadcasts and empty/rejected inputs.
Hosted CI is separate. Broader runtime/extreme numeric work remains in
[#491](https://github.com/datafusion-contrib/StreamFusion/issues/491).

Reproduce prepared measurements with the release `integral_expression` example,
`SF_INTEGRAL_SECONDARY_TYPE=INT` and expressions from the record; supply the independently
compiled preceding encoder for `SF_INTEGRAL_PREVIOUS=true`. Complete-job controls use
`IntegralExpressionPipelineBenchmark`, `sf.integral.secondaryType=INT`, the matching
`sf.integral.expression`, and `sf.integral.bigintInput=true` for BIGINT.


## Integer TRUNCATE with a bounded runtime position

INT/BIGINT TRUNCATE reuses the committed bounded-position selector and toward-zero integer
kernel when the planner proves every CASE result is an INT literal in -38..38 or NULL
and all conditions/value producers are failure-free. No new Rust implementation, opcode,
JNI, transport, dependency or configuration switch is required. Direct narrow signatures
retain released Flink's generated-compilation failure; explicit widening is supported.
Arbitrary columns, extreme/out-of-range results and unverified/failing/stateful producers
retain their preceding routes, including empty-batch and lazy-failure behavior.

The [2026-11-03 record](results/bounded-runtime-truncate-2026-11-03.json) measures released
Flink 1.18.1/2.2.1, Oracle JDK 17.0.12, Apple M4 Pro and one thread. Release/mimalloc-linked
executable and DSO hashes are identical to the preceding ROUND qualification; allocator
symbols remain verified. Equal prepared GenericRowData, BinaryRowData and Arrow contain
nullable/NOT NULL INT/BIGINT, signed extrema and NULLs. Each trial consumes 131,072 rows,
with 16 warmups and nine measurements. Rust times production Calc and equivalent output
consumption after import and before export. Intrinsic prior evaluator callbacks remain
timed. Initial JNI/import/export and perimeter transposes are excluded only from this
small-function gate; complete jobs retain those costs.

The short CASE chooses -2/-1; the wide-null CASE chooses -38/NULL/0. Extra computed controls
add one at the original input width before truncation. Secondary condition input is INT.
Actual preceding encoder `71b821c4` is independently compiled against the unchanged current
runtime/fixture/allocator; it still evaluates runtime TRUNCATE in the JVM. This is an
encoder comparison, not a whole historical DSO replay. Independent stock remains distinct.

| Flink | Bulk cells | Stock / native median range | Previous / native median range |
| --- | ---: | ---: | ---: |
| 2.2.1 | 20 | 3.801-15.378x | 7.653-19.165x |
| 1.18.1 | 20 | 3.915-16.393x | 6.796-20.044x |

All 40 bulk cells beat paired stock and the previous evaluator. Independent prior-stock/
native spans **3.970-16.296x**; all independent controls and
variability remain in the record. Eight 16-row controls retain stock/native
**0.565-1.466x**. The [4,800 raw trials](results/bounded-runtime-truncate-2026-11-03.csv.gz)
include warmups and checksums; [400 initial pilot trials](results/bounded-runtime-truncate-2026-11-03-pilot.csv.gz)
are separate. Reproduce with the release `integral_expression` example,
`SF_INTEGRAL_SECONDARY_TYPE=INT` and the expressions above; the prior control supplies its
independently compiled encoder and `SF_INTEGRAL_PREVIOUS=true`.

Eight complete million-row jobs retain the bounded row source, blackhole sink, startup,
JNI and both transposes. Two warmups and five measurements alternate engine order.
Candidate requires consumed NativeCalc work and empty evaluators; previous requires the
TRUNCATE evaluator. Seconds, median:

| Flink / type / position shape | Paired stock | Native | Independent stock | Previous evaluator |
| --- | ---: | ---: | ---: | ---: |
| 2.2.1 / INT-short | 0.251914 | 0.192503 | 0.254554 | 0.285888 |
| 2.2.1 / INT-wide-null | 0.219562 | 0.203798 | 0.211600 | 0.253925 |
| 2.2.1 / BIGINT-short | 0.266054 | 0.212577 | 0.266745 | 0.299863 |
| 2.2.1 / BIGINT-wide-null | 0.231545 | 0.210317 | 0.233702 | 0.272629 |
| 1.18.1 / INT-short | 0.247101 | 0.189082 | 0.239740 | 0.280074 |
| 1.18.1 / INT-wide-null | 0.226415 | 0.198152 | 0.212048 | 0.256564 |
| 1.18.1 / BIGINT-short | 0.269613 | 0.217554 | 0.262311 | 0.316432 |
| 1.18.1 / BIGINT-wide-null | 0.236126 | 0.214385 | 0.239261 | 0.274413 |

Native/stock elapsed spans **0.764-0.928**; native/previous spans
**0.673-0.803**. Every trial and range is retained. The prepared
function gate does not establish a universal job speedup. Reproduce complete jobs with
`IntegralExpressionPipelineBenchmark`, `sf.integral.secondaryType=INT`, matching
`sf.integral.expression`, and `sf.integral.bigintInput=true` for BIGINT.

All **344 focused JVM invocations** pass without skips on Flink 1.18.1/2.2.1 and JDK 11/17,
targeting Java release 11. Complete 5,003-row maps compare values, schema, NULLs, IDs and
cardinality for bounded runtime positions, toward-zero behavior, arithmetic/widening/nesting,
STRING/epoch consumers and all-filtered batches. Existing literal TRUNCATE and ROUND
encoding/rounding controls are included, preserving the narrow-input compilation contract.
Admitted paths require actual consumed NativeCalc and empty evaluator descriptors.
The actual preceding encoder matches full values/schema then fails the four new pure-route
assertions; its retained unverified-position control passes. Four fresh unchanged released
`ScalarFunctionsTest.testTruncate` stock-oracle invocations pass separately. The exact
unchanged Rust runtime already passed **900 release core tests** without ignores,
including dynamic-position TRUNCATE, slices, NULLs, ownership and empty arrays. Hosted CI
remains separate; broader runtime/extreme numeric scope stays in
[#491](https://github.com/datafusion-contrib/StreamFusion/issues/491).


## Integer PRINTF with a literal format

On released Flink 2.2.1, verified integer inputs/literals and failure-free arithmetic,
lossless widening or admitted rounding trees use an evaluator-free Arrow formatter with
an ASCII literal format of at most 512 characters. Conversions are `%s`/`%S`, `%d`,
`%x`/`%X`, `%o` and `%%`; width/precision are at most 256. The planner validates flags and
indexing during planning using the running JDK's `Formatter(Locale.US)`. Explicit index zero stays
on its preceding route because JDK 11 accepts it and JDK 17 rejects it. Every producer
must be failure-free, including unused arguments. Other formats/types and sensitive string
consumers retain their existing routes. Flink 1.18 has no PRINTF and retains SQL rejection.

The new path borrows the literal format, reuses an output scratch String, and appends into
one Arrow builder rather than creating an output String and materialized format per row.
The existing compatibility formatter remains shared; this does not substitute Rust format
syntax for Java Formatter. Parsing and per-conversion strings are still rowwise. Actual
preceding encoder `f910c352` is compiled independently against the shared current runtime,
fixture and allocator. Its direct compatible ASCII batches can already execute certified
Rust with a retained evaluator; computed producers retain generated evaluation. The previous
control is not assumed to perform callbacks on every batch, nor is it a whole historical
DSO replay.

The [2026-11-03 record](results/integer-literal-printf-2026-11-03.json) uses released Flink 2.2.1, Oracle JDK 17.0.12,
Apple M4 Pro, one thread and release/mimalloc-linked artifacts. Both executable and DSO
verify identical malloc/mi_malloc addresses. Prepared GenericRowData, BinaryRowData and
Arrow contain the same values, all signed integer widths, nullable/NOT NULL, extrema and
NULLs. Each trial consumes 131,072 rows, with 16 warmups and nine measurements. Rust times
production Calc and equivalent output consumption after input import and before output
export. Intrinsic callbacks remain timed; initial JNI/import/export and perimeter transposes
are excluded only from the prepared function gate. Complete jobs retain those costs.

Short formatting uses `PRINTF('%d/%s/%1$x',v,v)`; padded formatting uses
`PRINTF('%+020d:%2$-12.3S:%1$08X',v,v)`. Additional INT/BIGINT controls add one at the original
input width in the first formatter argument. Secondary input is not needed.

| Bulk cells | Paired stock / native median range | Previous / native median range | Independent stock / native |
| ---: | ---: | ---: | ---: |
| 40 | 2.532-5.180x | 1.072-5.250x | 2.462-5.094x |

All 40 bulk cells beat paired stock and the actual preceding encoder. Independent stock
has 0 lower medians than native; all independent measurements and variability remain.
The eight 16-row controls retain paired stock/native 2.174-4.008x,
including 0 controls slower than stock. The [4,800 raw trials](results/integer-literal-printf-2026-11-03.csv.gz)
include checksums and warmups.

The initial implementation merely removed the descriptor and reused certified formatting;
its previous/native pilot spans 0.968-1.033x and loses several controls. Its
[800 pilot trials](results/integer-literal-printf-2026-11-03-initial-pilot.csv.gz), source and artifact hashes remain
separate from the [800 optimized pilot trials](results/integer-literal-printf-2026-11-03-optimized-pilot.csv.gz).
For the 1,024-row nullable INT short probe, scratch/builder reuse reduces allocation requests
from 4,136/195,476 requested bytes to
3,102/91,842. Retained width-300 formats replay the saved pre-refactor
executable/DSO and current artifacts, both retaining evaluators; initial/current median
ratios span 0.987-1.005x. These [400 controls](results/integer-literal-printf-2026-11-03-retained-controls.csv.gz)
retain shared-formatter variation separately from the admitted scope.

Eight complete million-row INT/BIGINT jobs retain a bounded row source, blackhole sink,
startup, JNI and both transposes; two warmups and five measurements alternate engine order.
Candidate requires consumed NativeCalc work and empty evaluators. Previous requires its
retained evaluator, which does not prove callbacks for every compatible direct batch.
Seconds, median:

| Flink / type / format shape | Paired stock | Native | Independent stock | Previous route |
| --- | ---: | ---: | ---: | ---: |
| 2.2.1 / INT-short | 0.512502 | 0.282118 | 0.528230 | 0.308422 |
| 2.2.1 / INT-padded | 0.888427 | 0.326280 | 0.970378 | 0.331879 |
| 2.2.1 / INT-short-computed | 0.511739 | 0.288623 | 0.483461 | 0.507033 |
| 2.2.1 / INT-padded-computed | 0.983899 | 0.327239 | 0.933144 | 0.967021 |
| 2.2.1 / BIGINT-short | 0.566439 | 0.293006 | 0.580639 | 0.325968 |
| 2.2.1 / BIGINT-padded | 0.962960 | 0.319097 | 0.932796 | 0.343919 |
| 2.2.1 / BIGINT-short-computed | 0.556859 | 0.298003 | 0.558454 | 0.609614 |
| 2.2.1 / BIGINT-padded-computed | 0.925704 | 0.330283 | 0.969479 | 1.019794 |

Native/stock elapsed spans **0.331-0.564**; native/previous spans
**0.324-0.983**. All trials/ranges are retained; prepared function
gains do not imply a universal complete-job speedup.

All **392 focused JVM invocations** pass without skips across released Flink 1.18.1/2.2.1
and JDK 11/17, targeting Java release 11. Runtime tests compare complete 5,003-row maps,
resolved schemas, IDs, cardinality, all signed widths/extrema, NULLs, indexing, padding,
precision, computed wrapping producers and all-filtered batches. Generated controls retain
unverified formats/producers, JDK-dependent index zero and CHAR_LENGTH representation guards.
Existing certified/PRINTF tests qualify the shared formatter on Flink 2.2. Before controls
match full maps/schema then fail all eight new pure-route assertions; the generated control
passes. Two unchanged released `StringFunctionsITCase.printfTestCases` runs pass all **47
original assertions each**, separately as stock oracles. All **902 release Rust core tests**
pass without ignores, including slices, ownership, empty arrays and rejected contracts.
Hosted CI remains separate; broader formats/types/consumers stay in
[#497](https://github.com/datafusion-contrib/StreamFusion/issues/497).

Reproduce prepared controls with the release `integral_expression` example and
`SF_INTEGRAL_EXPRESSION` above; supply independently compiled previous encoder and
`SF_INTEGRAL_PREVIOUS=true` for that control. Complete jobs use
`IntegralExpressionPipelineBenchmark`, matching `sf.integral.expression`, and
`sf.integral.bigintInput=true` for BIGINT. Other resources/settings remain identical.


## Integer TRY_CAST

Verified TINYINT/SMALLINT/INT/BIGINT TRY_CAST reuses exact native primitive conversion.
Both modern and legacy Flink modes wrap integer narrowing overflow; they do not yield
NULL on out-of-range integer input. NULL input stays NULL and resolved schemas remain
unchanged. Inputs/literals, failure-free integer arithmetic, lossless widening and admitted
rounding producers qualify; division, narrowing producers and other unverified trees retain
fallback. Errors while computing an operand are not conversion errors. Same-type planner
rewrites are covered by parity but excluded from performance claims.

The first implementation only admitted the existing CAST kernel. All 112 bulk cells beat
paired stock, but four independent previous medians remain slightly lower than native.
The [initial 12,400 trials](results/integer-try-cast-2026-11-03-initial.csv.gz),
[600 pilot trials](results/integer-try-cast-2026-11-03-initial-pilot.csv.gz), source and runtime hashes remain.
The integer kernel used an intermediate i64 column and nullable row iteration. Following
Comet's legacy integer-cast structure, the final implementation maps the original typed
values in one Arrow unary pass and retains the immutable NULL buffer. Flink's own overflow
rules apply in both modes; Spark's ANSI/try semantics are not imported. DECIMAL/floating
paths remain separate. A nullable INT-to-TINYINT 1,024-row allocation probe drops from
28 requests/11,007 requested bytes to
21/2,279.

The [final record](results/integer-try-cast-2026-11-03.json) measures released Flink 1.18.1/2.2.1, Oracle JDK
17.0.12, Apple M4 Pro, one thread and release/mimalloc-linked artifacts, with verified
allocator symbols. Equal prepared GenericRowData, BinaryRowData and Arrow contain all
signed integer source/destination widths, nullable/NOT NULL, extrema and NULLs. Each trial
consumes 131,072 rows, with 16 warmups and nine measurements. Native timing starts/stops
inside Rust, after import and before export, retaining production Calc and equivalent
output consumption. Only initial JNI/import/export and perimeter transposes are excluded
from this small-function gate. Complete jobs retain them.

Actual preceding encoder `04e0cf86` rejects these signatures. Its Java prepared control
verifies the exact fallback reason, then measures the released generated expression on
RowData without artificial native overhead. Independent stock and previous remain distinct
samples of that stock fallback. Both controls use the same final fixture, which checks and
consumes the resolved output width. Short/wide destination casts and extra computed inputs
`v+CAST(1 AS source)` are qualified. No historical DSO callback is invented for the prior.

| Flink | Bulk cells | Paired stock / native | Previous fallback / native | Independent stock / native |
| --- | ---: | ---: | ---: | ---: |
| 2.2.1 | 56 | 2.468-15.672x | 2.435-13.883x | 2.468-13.879x |
| 1.18.1 | 56 | 2.463-12.864x | 2.440-12.919x | 2.445-12.987x |

All 112 bulk cells beat paired stock and the actual preceding fallback. Twelve 16-row
controls retain stock/native 0.276-0.368x, including 12 slower controls.
The [12,400 final trials](results/integer-try-cast-2026-11-03.csv.gz) retain warmups, checksums and variability;
[600 optimized pilot trials](results/integer-try-cast-2026-11-03-optimized-pilot.csv.gz) remain separate.
Ten [ordinary CAST controls](results/integer-try-cast-2026-11-03-retained-casts.csv.gz) replay saved initial
executable/DSO and optimized artifacts on the same final encoder/fixture. Eight integer
initial/current median ratios span 4.190-8.023x; two separate unchanged DECIMAL
controls span 0.987-1.040x. All variation is retained.

Eight complete million-row INT/BIGINT jobs retain the bounded row source, blackhole sink,
startup, JNI and both native transposes. Two warmups and five measurements alternate order.
Candidate requires consumed NativeCalc work and empty evaluators; actual previous must
fall back with the exact expected reason and zero native substitutions/operators.
Seconds, median:

| Flink / source / destination | Paired stock | Native | Independent stock | Previous fallback |
| --- | ---: | ---: | ---: | ---: |
| 2.2.1 / INT-TINYINT | 0.162741 | 0.172488 | 0.146897 | 0.148155 |
| 2.2.1 / INT-BIGINT | 0.160180 | 0.174861 | 0.143499 | 0.144603 |
| 2.2.1 / BIGINT-TINYINT | 0.154210 | 0.164889 | 0.147629 | 0.148952 |
| 2.2.1 / BIGINT-INT | 0.154995 | 0.168157 | 0.147347 | 0.152415 |
| 1.18.1 / INT-TINYINT | 0.165774 | 0.170204 | 0.150125 | 0.144074 |
| 1.18.1 / INT-BIGINT | 0.163677 | 0.172033 | 0.151377 | 0.149134 |
| 1.18.1 / BIGINT-TINYINT | 0.174743 | 0.177714 | 0.156687 | 0.154429 |
| 1.18.1 / BIGINT-INT | 0.163483 | 0.180986 | 0.168767 | 0.164599 |

Native/stock elapsed spans **1.017-1.107**; native/previous spans
**1.100-1.209**. Unfavorable complete jobs and independent controls remain;
prepared gains do not establish universal complete-job acceleration.

All **716 focused JVM invocations** pass without skips across released Flink 1.18.1/2.2.1
and JDK 11/17, targeting Java release 11. Complete 5,003-row maps/schema compare every
integer source/destination width, NULLs, extrema, modern/legacy modes, computed overflow,
filters, CASE and all-filtered batches. Failing/unverified producers retain explicit
fallback and lazy/empty/failure controls. Shared ARRAY CAST and adjacent rounding/epoch/
PRINTF paths are rechecked. Sixteen actual-before cases match full maps/schema, then fail
the old fallback assertion; two old filter controls stop at missing native substitution
before their final map assertion. Four unchanged released `CastRulesTest` integer-oracle
runs each pass **41 original assertions**, separately as stock semantics. All **903 release
Rust core tests** pass, including six integer narrowing pairs, signed wrapping, slices,
empty arrays, NULL-buffer sharing, scalar materialization and input ownership. Hosted CI
remains separate; broader scalar conversion work stays in
[#492](https://github.com/datafusion-contrib/StreamFusion/issues/492).

Reproduce native prepared controls with release `integral_expression`,
`SF_INTEGRAL_EXPRESSION='TRY_CAST(v AS target)'`, equal source type and nullable/row-layout
arguments. The preceding control uses `IntegralExpressionBenchmarkFunctions` on the actual
prior encoder with `sf.integral.previous=true`, `sf.integral.fallbackControl=true` and an
exact `sf.integral.fallbackReason` (for example `unsupported TRY_CAST BIGINT` followed by
U+2192 and `INTEGER`). Complete jobs use `IntegralExpressionPipelineBenchmark`, matching
`sf.integral.expression`, BIGINT input flag when needed, and those previous fallback controls.


## Integer boolean PRINTF formats

On released Flink 2.2.1, `%b`/`%B` treats any non-NULL integer object, **including zero**,
as `true`/`TRUE`; NULL becomes `false`/`FALSE`. Bounded literal ASCII formats use the
existing evaluator-free integer formatter, including precision, space padding, explicit
indices and argument reuse. This does not admit BOOLEAN-object or other argument types.
Invalid flags and unverified producers keep Flink's NULL/error and lazy/empty behavior.
Flink 1.18.1 has no PRINTF and retains its released SQL rejection.

The [record](results/integer-boolean-printf-2026-11-03.json) uses released Flink 2.2.1, Oracle JDK 17.0.12, Apple M4 Pro,
one thread and release/mimalloc-linked artifacts. Equal prepared GenericRowData,
BinaryRowData and Arrow include all four integer widths, nullable/NOT NULL, signed
extrema and zero. Each trial consumes 131,072 rows, with 16 warmups/nine measurements.
The 48 cells cover short and padded formats, plus computed INT/BIGINT arithmetic.
Timers start/stop inside Rust after import and before export, retaining production Calc,
equivalent output consumption and intrinsic callbacks. Stock starts on prepared RowData;
only initial JNI/import/export and perimeter transposes are outside the small-function gate.
Complete jobs retain them.

The actual preceding encoder and formatter-admission helper, plus saved **whole DSO and
executable** from `5d46dde1`, run against the same final fixture/JDK/data. Both artifacts
retain verified mimalloc aliases. This preserves the original integer boolean format's
JDK route instead of accidentally giving the preceding certified path the new kernel.
Existing compatible certified formats are not described as universal Java callbacks.

The [four numeric-format controls](results/integer-boolean-printf-2026-11-03-retained-numeric-formats.csv.gz) and
[reverse-order replay](results/integer-boolean-printf-2026-11-03-retained-numeric-formats-reversed.csv.gz) retain
whole-runtime before/after differences for existing numeric formats. Both BIGINT controls
have lower preceding-runtime medians in both orders, by approximately 3-6%. The new-format
gains below do not establish an improvement for those unchanged numeric signatures.
An attempted non-inlined default helper does not recover them
([four diagnostic cells](results/integer-boolean-printf-2026-11-03-helper-experiment.csv.gz), initial/current
0.938-1.011x); that extra indirection is discarded. Its [rejected patch](results/integer-boolean-printf-2026-11-03-rejected-helper.patch), source/artifact hashes and successful
904-test Rust result remain separately recorded. The exact originally measured production
source, DSO and executable are restored, checked by hash, then requalified with the final
SQL tests including independently nullable indexing arguments. The residual numeric-format
cost differences still need attribution/profile work; no cause is inferred from a source
branch alone.

| Scope | Cells | Paired stock / native | Actual previous / native | Independent stock / native |
| --- | ---: | ---: | ---: | ---: |
| Bulk (1,024 rows) | 40 | 4.947-7.411x | 5.723-8.108x | 5.346-7.519x |
| Tiny (16 rows) | 8 | 3.789-5.423x | 11.212-12.205x | 3.853-5.508x |

All bulk cells beat paired stock and the actual previous implementation. Tiny controls
retain 0 stock losses and 0 previous losses.
The [4,800 trials](results/integer-boolean-printf-2026-11-03.csv.gz) retain warmups, checksums and variability.
Four [numeric-format controls](results/integer-boolean-printf-2026-11-03-retained-numeric-formats.csv.gz) replay
initial/current artifacts on the same final encoder and fixture. Their initial/current
median ratios span 0.945-1.024x; all variation remains.

Eight complete million-row INT/BIGINT jobs include direct/computed short/padded formats.
They retain the bounded row source, blackhole sink, startup, JNI and both transposes.
Two warmups and five measurements alternate stock/native. Each native control requires
consumed NativeCalc work; new paths require empty evaluators and previous paths require
their original retained evaluators. Seconds, median:

| Flink / source / shape | Paired stock | Native | Independent stock | Actual previous |
| --- | ---: | ---: | ---: | ---: |
| 2.2.1 / INT-short | 0.621235 | 0.259344 | 0.606269 | 0.634951 |
| 2.2.1 / INT-padded | 0.750707 | 0.273240 | 0.784988 | 0.820727 |
| 2.2.1 / INT-short-computed | 0.597576 | 0.266733 | 0.607545 | 0.619766 |
| 2.2.1 / INT-padded-computed | 0.809255 | 0.270421 | 0.795140 | 0.804340 |
| 2.2.1 / BIGINT-short | 0.671868 | 0.266082 | 0.578773 | 0.597283 |
| 2.2.1 / BIGINT-padded | 0.828991 | 0.269570 | 0.783403 | 0.819152 |
| 2.2.1 / BIGINT-short-computed | 0.673516 | 0.257944 | 0.571059 | 0.568660 |
| 2.2.1 / BIGINT-padded-computed | 0.828161 | 0.274525 | 0.751099 | 0.753517 |

Native/stock elapsed spans **0.325-0.446**; native/previous spans
**0.329-0.454**. These are measured workloads; the prepared gate does
not establish universal complete-job acceleration.

All **716 focused JVM invocations** pass without skips across released Flink 1.18.1/2.2.1
and JDK 11/17, targeting Java release 11. Complete 5,003-row runtime maps/schema cover
integer widths, NULLs, signed extrema/zero, computed producers, boolean format precision/
padding/indexing with independently nullable arguments, all-filtered batches, remaining
generated controls and adjacent scalar/ARRAY CAST paths. Pure claims require consumed NativeCalc work and empty evaluators.
The actual preceding control compares full maps/schema then fails eight intended pure-route
assertions; its generated control passes. Two fresh unchanged released PRINTF oracle runs
pass **47 original assertions each**, separately as stock semantics. All **904 release Rust
core tests** pass, including sliced/empty/NULL inputs and input ownership. Hosted CI remains
separate. Remaining format/type/consumer scope stays in
[#497](https://github.com/datafusion-contrib/StreamFusion/issues/497).

Reproduce with release `integral_expression`,
`SF_INTEGRAL_EXPRESSION` set to `PRINTF('%b/%1$B/%1$.2b',v,v)`,
and matching signed type/nullability/row-layout arguments. The previous control supplies
its saved DSO/executable, separately compiled preceding encoder/helper and
`SF_INTEGRAL_PREVIOUS=true`. Complete jobs use `IntegralExpressionPipelineBenchmark` with
matching `sf.integral.expression`, `sf.integral.bigintInput=true` for BIGINT and previous
route settings. All resources and data otherwise match.


## BOOLEAN-to-integer TRY_CAST

Verified BOOLEAN input/literal, logical, NULL-check and integer-comparison trees now use
the existing primitive native cast for TRY_CAST to TINYINT/SMALLINT/INT/BIGINT. TRUE is 1,
FALSE is 0 and NULL stays NULL in modern/legacy modes. Conversion cannot fail; failing or
unverified producers keep their preceding route. Ordinary CAST already becomes Flink's
CASE expression during released planning and needs no admission change. This follows
Flink's BooleanToNumeric rule and Comet's DataFusion-compatible primitive conversion.
There is no new native opcode, JNI entry point, setting or Rust production change.

The [record](results/boolean-integer-try-cast-2026-11-03.json) retains 48 cells and
[4,800 raw trials](results/boolean-integer-try-cast-2026-11-03.csv.gz), including warmups/checksums/ranges.
Released Flink 1.18.1/2.2.1, Oracle JDK 17.0.12, Apple M4 Pro, one thread and release/mimalloc
artifacts are used. Equal prepared GenericRowData/BinaryRowData and Arrow contain true,
false and NULL with nullable/NOT NULL schemas and four target integer widths. Computed
controls use `NOT v OR v IS NULL`. Each trial consumes 131,072 rows, with 16 warmups and
nine measurements. The native timer runs inside Rust after import and before export,
covering production Calc and equivalent output consumption. Stock runs on prepared RowData;
setup and perimeter conversion are outside only the small-function gate. Intrinsic costs
remain timed. The actual previous encoder from `8ccec369` verifies each exact fallback
reason and executes released generated Java without invented native overhead. Current and
previous controls use identical logical data/resources. The existing release runtime and
its mimalloc aliases are checked by hash and reused unchanged.

| Scope | Cells | Paired stock / native | Actual previous / native | Independent stock / native |
| --- | ---: | ---: | ---: | ---: |
| Bulk (1,024 rows) | 40 | 1.092-2.819x | 1.075-2.135x | 1.071-2.147x |
| Tiny (16 rows) | 8 | 0.281-0.301x | 0.277-0.302x | 0.277-0.303x |

All bulk cells beat stock and actual previous fallback. Tiny controls retain
8 stock losses and 8 previous losses;
small batches are not described as accelerated when they lose.

Eight million-row complete jobs retain bounded row input, blackhole output, startup, JNI
and both transposes. Two warmups/five measurements alternate order. Candidate controls
require consumed NativeCalc work and empty evaluator descriptors. Actual previous controls
require exact fallback, zero substitutions and zero native operators. Seconds, median:

| Flink / source / target | Paired stock | Native | Independent stock | Actual previous |
| --- | ---: | ---: | ---: | ---: |
| 2.2.1 / BOOLEAN-TINYINT | 0.170140 | 0.167920 | 0.143392 | 0.145854 |
| 2.2.1 / BOOLEAN-SMALLINT | 0.158451 | 0.162561 | 0.142876 | 0.142270 |
| 2.2.1 / BOOLEAN-INT | 0.167679 | 0.173542 | 0.146190 | 0.146495 |
| 2.2.1 / BOOLEAN-BIGINT | 0.161557 | 0.169256 | 0.140627 | 0.157842 |
| 1.18.1 / BOOLEAN-TINYINT | 0.155952 | 0.170372 | 0.141720 | 0.140429 |
| 1.18.1 / BOOLEAN-SMALLINT | 0.165398 | 0.164696 | 0.136228 | 0.137099 |
| 1.18.1 / BOOLEAN-INT | 0.157851 | 0.164143 | 0.149238 | 0.144749 |
| 1.18.1 / BOOLEAN-BIGINT | 0.157443 | 0.170163 | 0.139337 | 0.143281 |

Native/stock elapsed spans **0.987-1.092**; native/previous spans
**1.072-1.213**. All slower results remain; prepared-kernel qualification does
not establish universal complete-job acceleration. Six of eight complete jobs lose to
paired stock; all eight lose to the independently measured actual previous fallback.

All **740 focused JVM invocations** pass without skips across both released Flink lines
and JDK 11/17, targeting Java release 11. Complete 5,003-row maps/schema, both cast modes,
NULLs/logical producers, filters, CASE, character consumers, all-filtered batches, selected
producer failures and adjacent function regressions pass. Admitted pure paths require
consumed NativeCalc work and empty evaluators. Sixteen actual previous positive cases
compare maps/schema then fail only the expected native-route assertion; eight control
methods stop earlier at their positive filter assertion, so their later negative checks
are not claimed as previous result comparisons. The unchanged released CastRules oracle
passes **eight original BOOLEAN-to-integer assertions** for each release/JDK pair. No Rust
code changes; the prior 904-test accepted-runtime result retains its historical provenance.
Other type/producer coverage remains in [#492](https://github.com/datafusion-contrib/StreamFusion/issues/492).

Reproduce with release `integral_expression`, `BOOLEAN` input, the matching nullable/binary/
batch arguments and `SF_INTEGRAL_EXPRESSION=TRY_CAST(v AS INT)`. Previous uses the frozen
encoder, `IntegralExpressionBenchmarkFunctions`, `sf.integral.previous=true`,
`sf.integral.fallbackControl=true` and the exact fallback reason. Complete jobs use
`IntegralExpressionPipelineBenchmark` with `sf.integral.booleanInput=true` and the same
expression/previous settings; all resources and data otherwise match.


## Integer-to-BOOLEAN TRY_CAST

TRY_CAST of verified TINYINT/SMALLINT/INT/BIGINT inputs and failure-free arithmetic,
widening/rounding producers uses the existing native comparison with a zero of the
input's resolved width. Nonzero values, including signed minima, become TRUE; zero is
FALSE and NULL stays NULL in both modes. This mirrors Flink's NumericToBoolean rule.
Comet also treats integer-to-BOOLEAN as a DataFusion-compatible conversion; using the
existing comparison avoids a new cast target/opcode, JNI entry point or Rust kernel.
Ordinary CAST already lowers to this comparison through released planning. Failing,
stateful or unverified producers keep their previous route. At this qualification revision,
character formatting of the new boolean TRY_CAST result
remained generated; the later character-consumer qualification extends that producer proof.

The [record](results/integer-boolean-try-cast-2026-11-03.json) retains 48 cells and
[4,800 raw trials](results/integer-boolean-try-cast-2026-11-03.csv.gz), including warmups/checksums/ranges.
Released Flink 1.18.1/2.2.1, Oracle JDK 17.0.12, Apple M4 Pro, one thread and release/mimalloc
artifacts are used. Equal prepared GenericRowData/BinaryRowData and Arrow include all
four signed integer source widths, nullable/NOT NULL, extrema and zero. Computed controls
use `v+CAST(1 AS source)`. Each trial consumes 131,072 rows, with 16 warmups and nine
measurements. The native timer runs inside Rust after import and before export, retaining
production Calc and equivalent output consumption. Stock uses prepared Java RowData.
Setup and perimeter conversions are outside only the small-function gate; intrinsic costs
remain. The actual previous encoder from `49f269d9` rejects each exact integer-to-BOOLEAN
TRY_CAST and its control executes released generated Java without synthetic native costs.
Current and previous inputs/resources match. The release runtime is unchanged and verified
by hash, including mimalloc malloc aliases.

| Scope | Cells | Paired stock / native | Actual previous / native | Independent stock / native |
| --- | ---: | ---: | ---: | ---: |
| Bulk (1,024 rows) | 40 | 2.658-20.095x | 2.656-6.831x | 2.645-6.790x |
| Tiny (16 rows) | 8 | 0.259-0.272x | 0.257-0.277x | 0.255-0.274x |

The largest paired ratio comes from the Flink 1.18.1 NOT NULL GenericRowData INT cell:
paired stock is 12.271 ns/row (3.524-17.732), versus independent stock 2.088 ns/row.
A [diagnostic replay](results/integer-boolean-try-cast-2026-11-03-stock-drift-replay.csv.gz) with 64 warmups/nine
measurements gives stock/native 3.524x
and previous/native 3.352x.
This retains all 292 replay samples separately and does not overwrite the primary data or
prove the cause of the stock drift. The original 20x ratio is not a stable speedup claim.

All bulk cells beat stock and actual previous fallback. Tiny controls retain
8 stock losses and 8 previous losses.
Small batches are not called accelerated when they lose.

Eight complete million-row jobs cover direct/computed INT/BIGINT inputs. They retain the
bounded row source, blackhole output, startup, JNI and both transposes. Two warmups/five
measurements alternate order. Candidate requires consumed NativeCalc work and empty JVM
evaluator descriptors. Actual previous controls require exact fallback, zero substitutions
and zero native operators. Seconds, median:

| Flink / source / shape | Paired stock | Native | Independent stock | Actual previous |
| --- | ---: | ---: | ---: | ---: |
| 2.2.1 / INT-BOOLEAN | 0.162491 | 0.166682 | 0.147023 | 0.148354 |
| 2.2.1 / INT-BOOLEAN-computed | 0.155750 | 0.167884 | 0.149599 | 0.156696 |
| 2.2.1 / BIGINT-BOOLEAN | 0.158948 | 0.167414 | 0.155004 | 0.155063 |
| 2.2.1 / BIGINT-BOOLEAN-computed | 0.161442 | 0.179230 | 0.147702 | 0.153635 |
| 1.18.1 / INT-BOOLEAN | 0.167345 | 0.173842 | 0.144691 | 0.145597 |
| 1.18.1 / INT-BOOLEAN-computed | 0.163659 | 0.167112 | 0.141206 | 0.142495 |
| 1.18.1 / BIGINT-BOOLEAN | 0.169389 | 0.180325 | 0.161704 | 0.161217 |
| 1.18.1 / BIGINT-BOOLEAN-computed | 0.169641 | 0.184478 | 0.152336 | 0.159917 |

Native/stock elapsed spans **1.021-1.110**; native/previous spans
**1.071-1.194**. 8 of eight complete jobs lose to paired
stock; 8 lose to independently measured actual previous fallback.
All slower results remain. Prepared-function gains do not establish universal complete-job
acceleration.

All **528 focused JVM invocations** pass without skips across both released Flink lines
and JDK 11/17, targeting Java release 11. The new complete 5,003-row maps/schema cover
four integer widths, modern/legacy modes, NULLs, signed extrema and zero, arithmetic,
widening, rounding, logic, CASE and empty filtered batches. Real filters and failing/lazy
producer controls pass; at this qualification revision, character consumers explicitly
retain their generated route. The following qualification removes those evaluators for
the separately verified character-consumer subset.
Related integer/BOOLEAN TRY_CAST, temporal parsing and BOOLEAN CASE regressions also pass.
Admitted pure claims require consumed NativeCalc work and empty evaluators. Sixty-four
actual previous positive cases compare full maps/schema before the expected fallback
assertion; their empty/character follow-ups are not reached. Eight control methods stop
at the positive filter route assertion before their later negative checks. Four unchanged
released CastRules oracle runs pass **12 original integer-to-BOOLEAN assertions each**.
No Rust production code changes; the previously qualified accepted runtime's 904-test
result retains its historical provenance. Other producers, types and unverified character
consumers remain in [#492](https://github.com/datafusion-contrib/StreamFusion/issues/492).

Reproduce with release `integral_expression`, matching integer type/nullability/row-layout/
batch arguments and `SF_INTEGRAL_EXPRESSION=TRY_CAST(v AS BOOLEAN)`. Previous uses its
frozen encoder, `IntegralExpressionBenchmarkFunctions`, `sf.integral.previous=true`,
`sf.integral.fallbackControl=true` and the exact fallback reason. Complete jobs use
`IntegralExpressionPipelineBenchmark`, `sf.integral.bigintInput=true` for BIGINT, the same
expression and previous settings, preserving resources/data and perimeter costs.


## Character consumers of integer truth

CAST/TRY_CAST of the verified integer-to-BOOLEAN TRY_CAST result to STRING/VARCHAR/CHAR
reuse the existing BOOLEAN formatter. The same total-integer producer proof governs the
inner nonzero comparison and its outer character/logical consumers. Integer-target casts
retain their separate preceding proof. This follows
Flink's NumericToBoolean and BooleanToString rules, including mode-dependent spelling,
NULL, CHAR padding and VARCHAR truncation. Comet's compatible numeric/boolean conversion
paths are consulted; no Rust kernel, opcode, JNI entry point, setting or dependency changes.
Division, narrowing and unverified/failing/stateful producers retain their preceding routes.
Ordinary integer-to-BOOLEAN CAST already has a released native comparison rewrite.

The [record](results/integer-truth-character-casts-2026-11-03.json) retains 96 prepared cells and
[9,600 raw samples](results/integer-truth-character-casts-2026-11-03.csv.gz), including warmups, checksums and variability.
Released Flink 1.18.1/2.2.1, Oracle JDK 17.0.12, Apple M4 Pro, one thread and release/mimalloc
artifacts are used. Equal GenericRowData/BinaryRowData and Arrow contain four signed
integer widths, nullable/NOT NULL, extrema and zero. Short STRING and padded CHAR(8)
forms use direct or addition producers. Each trial consumes 131,072 rows, with 16 warmups
and nine measurements. Rust times production Calc and equivalent output consumption after
import/before export; stock uses equal prepared RowData. Setup and perimeter conversions
are excluded only from this small-function timer; intrinsic costs remain. The actual
preceding encoder `45cdf27a` runs on the exact same unchanged DSO/executable and fixture.
It retains its registered evaluators and any production shortcuts. A retained descriptor
alone does not prove Java executes on every row. Artifact hashes and mimalloc aliases match.

| Scope | Cells | Paired stock / native | Actual previous / native | Independent stock / native |
| --- | ---: | ---: | ---: | ---: |
| Bulk (1,024 rows) | 80 | 1.301-10.373x | 9.137-18.033x | 1.399-7.896x |
| Tiny (16 rows) | 16 | 0.113-0.962x | 13.991-15.825x | 0.112-0.990x |

All bulk cells beat stock and the actual preceding behavior. Tiny controls retain
16 stock losses and 0 previous losses.
All independent samples remain; the function gate does not establish universal job gains.

Sixteen complete million-row INT/BIGINT jobs cover direct/computed short/padded forms.
They retain bounded row input, blackhole output, startup, JNI and both transposes. Two
warmups/five measurements alternate order. New controls require consumed NativeCalc work
and empty evaluators; actual preceding controls require retained evaluators and consumed
work. Seconds, median:

| Flink / source / shape | Paired stock | Native | Independent stock | Actual previous |
| --- | ---: | ---: | ---: | ---: |
| 2.2.1 / INT-short | 0.188565 | 0.190101 | 0.190131 | 0.222412 |
| 2.2.1 / INT-padded | 0.219376 | 0.191142 | 0.231627 | 0.248017 |
| 2.2.1 / INT-short-computed | 0.186346 | 0.195479 | 0.191713 | 0.223141 |
| 2.2.1 / INT-padded-computed | 0.220819 | 0.192531 | 0.224752 | 0.242662 |
| 2.2.1 / BIGINT-short | 0.191168 | 0.197340 | 0.190325 | 0.220678 |
| 2.2.1 / BIGINT-padded | 0.236820 | 0.198431 | 0.244500 | 0.255661 |
| 2.2.1 / BIGINT-short-computed | 0.203478 | 0.202725 | 0.190485 | 0.226375 |
| 2.2.1 / BIGINT-padded-computed | 0.237860 | 0.198294 | 0.233832 | 0.250998 |
| 1.18.1 / INT-short | 0.192091 | 0.203404 | 0.190491 | 0.216337 |
| 1.18.1 / INT-padded | 0.216036 | 0.193191 | 0.219586 | 0.238627 |
| 1.18.1 / INT-short-computed | 0.190646 | 0.194826 | 0.186903 | 0.218633 |
| 1.18.1 / INT-padded-computed | 0.228368 | 0.196992 | 0.220991 | 0.245635 |
| 1.18.1 / BIGINT-short | 0.201844 | 0.210436 | 0.202028 | 0.239229 |
| 1.18.1 / BIGINT-padded | 0.242544 | 0.208591 | 0.240534 | 0.258406 |
| 1.18.1 / BIGINT-short-computed | 0.206283 | 0.214572 | 0.203259 | 0.234902 |
| 1.18.1 / BIGINT-padded-computed | 0.229412 | 0.210455 | 0.233978 | 0.255656 |

Native/stock elapsed spans **0.834-1.059**, with 7 stock losses;
native/previous spans **0.771-0.940**, with 0 previous losses.
Every slower control remains; prepared-function gains are distinct from complete-job gains.

All **384 focused JVM invocations** pass without skips across both released Flink lines
and JDK 11/17, targeting Java release 11. Complete 5,003-row maps/schema cover all integer
widths, signed extrema/zero, nullable/NOT NULL, modern/legacy modes, both outer cast kinds,
STRING/VARCHAR(1/3)/CHAR(4/8), arithmetic/logical producers, explicitly retained
nested-integer fallback and all-filtered batches. Real filters, CONCAT/length,
selected failures and untaken branches
also pass, with explicit retained-route assertions. Related boolean CASE, integer casts
and the original BOOLEAN formatter regressions pass. Pure claims require consumed native
work and empty evaluator descriptors.

The actual previous 64 type/mode/nullability cases compare the first CAST query's full
maps/schema, then fail only the new evaluator-free expectation. Their empty query and
later outer TRY_CAST loop are not reached; candidate tests verify those separately.
All sixteen original compound/failing/empty controls across both outer operators pass.
Four freshly executed unchanged released CastRules oracle runs pass **eight original
BOOLEAN-to-character assertions each**, separately from native work evidence. No Rust
production changes; the accepted runtime's prior 904-test result remains historical.
Other type/producer/consumer gaps stay in [#492](https://github.com/datafusion-contrib/StreamFusion/issues/492).

Reproduce with release `integral_expression`, matching signed input/nullability/row-layout/
batch arguments and `SF_INTEGRAL_EXPRESSION=CAST(TRY_CAST(v AS BOOLEAN) AS STRING)` or
`TRY_CAST(TRY_CAST(v AS BOOLEAN) AS CHAR(8))`. Previous supplies the frozen encoder and
`SF_INTEGRAL_PREVIOUS=true` on the same release runtime. Complete jobs use
`IntegralExpressionPipelineBenchmark`, the corresponding expression, bigint input flag
and `sf.integral.previous=true` for the preceding control; other resources/data match.


## DECIMAL-to-BOOLEAN TRY_CAST

Direct DECIMAL inputs/literals use the existing native `<>` comparison against a
zero with identical precision/scale. Every nonzero coefficient is TRUE, zero is FALSE
and NULL remains NULL in both cast modes. This follows Flink's NumericToBoolean rule
and Comet's same-type Decimal128 zero comparison. Ordinary CAST already rewrites to a
comparison when precision exceeds scale; released ordinary DECIMAL(p,p) CAST fails
during planning and remains unchanged. Computed/metadata-sensitive/failing/stateful
producers retain their previous routes and character formatting retains its separate
proof. No new Rust production kernel, setting, dependency or JNI contract is added.

The [record](results/decimal-boolean-try-cast-2026-11-03.json) retains all 50 prepared configurations and
[5,000 raw samples](results/decimal-boolean-try-cast-2026-11-03.csv.gz), including warmups, checksums and variability.
Released Flink 1.18.1/2.2.1, Oracle JDK 17.0.12, Apple M4 Pro, one thread, and release/mimalloc
are used. Five compact/noncompact input types are DECIMAL(4,2), (18,2), (19,19), (38,18)
and (38,38), each nullable/NOT NULL and GenericRowData/BinaryRowData. Matching Arrow and
Java fixtures include signed extrema, both signs and periodic zeros. The explicit
`SF_INTEGRAL_CORPUS=decimal-truth` corpus adds the same zeros to both fixtures; the earlier
nonzero-only pilot is retained separately and is not the final qualification.

Each trial consumes 131,072 rows with 16 warmups/nine measurements. The Rust timer
encloses production Calc and equivalent consumption after import/before export; Flink
starts on equal prepared RowData. Setup/perimeter costs alone are outside this function
timer; intrinsic costs remain. Actual preceding encoder `907c078c` rejects TRY_CAST and
its real Java fallback is measured, not an invented callback. Both use the exact same
unchanged accepted DSO and final fixtures. Only the benchmark executable changes to
add the explicit zero corpus. Artifact/source hashes and matching mimalloc aliases remain.

| Scope | Cells | Paired stock / native | Actual previous / native | Independent stock / native |
| --- | ---: | ---: | ---: | ---: |
| Bulk (1,024 rows) | 40 | 4.245-52.674x | 4.250-45.661x | 4.222-45.962x |
| Tiny (16 rows) | 10 | 0.341-1.487x | 0.406-1.842x | 0.390-1.810x |

All bulk cells beat stock and actual previous behavior. Tiny cells retain
5 stock losses and 5 previous losses.
Independent stock ranges remain alongside paired controls.

Ten complete million-row configurations cover all five input types on both released
Flink lines. They retain bounded row input, blackhole output, startup, JNI and both
transposes, with two warmups/five measurements in alternating order. Candidate controls
require consumed NativeCalc work and empty evaluators; preceding controls require their
actual fallback and zero native substitutions. Seconds, median:

| Flink / source / target | Paired stock | Native | Independent stock | Actual previous |
| --- | ---: | ---: | ---: | ---: |
| 2.2.1 / DECIMAL(4,2)-BOOLEAN | 0.169193 | 0.184344 | 0.162222 | 0.157377 |
| 2.2.1 / DECIMAL(18,2)-BOOLEAN | 0.161567 | 0.177438 | 0.151877 | 0.156473 |
| 2.2.1 / DECIMAL(19,19)-BOOLEAN | 0.160679 | 0.194553 | 0.151313 | 0.161298 |
| 2.2.1 / DECIMAL(38,18)-BOOLEAN | 0.169855 | 0.193467 | 0.148120 | 0.160680 |
| 2.2.1 / DECIMAL(38,38)-BOOLEAN | 0.157564 | 0.192809 | 0.159520 | 0.148599 |
| 1.18.1 / DECIMAL(4,2)-BOOLEAN | 0.173802 | 0.178297 | 0.154189 | 0.154342 |
| 1.18.1 / DECIMAL(18,2)-BOOLEAN | 0.159733 | 0.180253 | 0.149595 | 0.154122 |
| 1.18.1 / DECIMAL(19,19)-BOOLEAN | 0.167480 | 0.186420 | 0.146684 | 0.153933 |
| 1.18.1 / DECIMAL(38,18)-BOOLEAN | 0.157908 | 0.190552 | 0.156317 | 0.147475 |
| 1.18.1 / DECIMAL(38,38)-BOOLEAN | 0.163782 | 0.187075 | 0.154887 | 0.149671 |

Native/stock elapsed spans **1.026-1.224**, retaining 10 stock losses;
native/previous spans **1.134-1.298**, retaining 10 previous losses.
Every unfavorable result remains; function qualification is distinct from full-job gains.

All **456 focused JVM invocations** pass without skips on released Flink 1.18.1/2.2.1
and JDK 11/17, targeting Java release 11. Full 5,003-row maps/schema cover eleven
precision/scale pairs from DECIMAL(1,0)/(1,1) to (38,38), nullable/NOT NULL, both cast
modes, zero, signed extrema, logical/NULL/CASE consumers and all-filtered batches.
At this qualification revision, the character control retains its generated evaluator;
the following qualification extends that proof. Filters, computed/failing producers and
lazy branches also pass. Admitted paths require actual consumed native work and empty evaluators.
The four ordinary DECIMAL(p,p) planning failures are separately verified in both modes.
Adjacent integer/BOOLEAN casts and DECIMAL projection/default regressions pass.

Actual-before runs compare 44 positive invocations' first NOT NULL/modern normal
maps/schema before the expected old-fallback assertion; later modes/nullability/empty
queries are not claimed as reached. Eight before character-consumer controls also compare
maps/schema before their fallback assertion, with later filters/negative checks unreached.
All sixteen ordinary CAST planning-failure invocations pass, each covering both modes.
Four fresh unchanged released CastRules oracles pass two original DECIMAL-to-BOOLEAN
assertions each. A fresh release core run passes **904 Rust tests**, no ignores; only the
benchmark fixture changes in Rust, and the production DSO hash stays identical.

Reproduce with release `integral_expression`, matching DECIMAL input/nullability/row-layout/
batch arguments, `SF_INTEGRAL_CORPUS=decimal-truth` and
`SF_INTEGRAL_EXPRESSION=TRY_CAST(v AS BOOLEAN)`. Actual previous uses its frozen encoder
with the same final Java fixture and explicit `sf.integral.fallbackControl=true` /
`sf.integral.fallbackReason=unsupported TRY_CAST DECIMAL→BOOLEAN` control. Complete jobs use
`IntegralExpressionPipelineBenchmark` and matching `sf.integral.decimalPrecision`/`decimalScale`;
the preceding control must keep its fallback. Remaining producers, consumers and source/
target families stay in [#492](https://github.com/datafusion-contrib/StreamFusion/issues/492).


## Character consumers of decimal truth

CAST/TRY_CAST of verified direct DECIMAL-to-BOOLEAN TRY_CAST to STRING/VARCHAR/CHAR
reuse the existing native BOOLEAN formatter. The producer proof permits verified logical
and NULL wrappers around that conversion. Same-precision/scale decimal zero comparison,
modern/legacy spelling, NULLs, VARCHAR truncation and CHAR padding remain unchanged.
The structure follows Flink's NumericToBoolean/BooleanToString rules and Comet's
same-type zero comparison. No native kernel, setting, dependency or JNI changes.
Computed/failing/stateful/metadata-sensitive producers retain their preceding routes.
Integer-target and bounded-position condition proofs remain separate. At this qualification
revision ordinary DECIMAL CAST uses a different comparison tree and retains its character
evaluator; the following qualification extends that direct-comparison proof. Ordinary
DECIMAL(p,p) CAST retains the released planning failure.

The [record](results/decimal-truth-character-casts-2026-11-03.json) retains 100 prepared configurations and
[10,000 raw samples](results/decimal-truth-character-casts-2026-11-03.csv.gz), including warmups, checksums and variability.
Released Flink 1.18.1/2.2.1, Oracle JDK 17.0.12, Apple M4 Pro, one thread and release/mimalloc
are used. Equal GenericRowData/BinaryRowData and Arrow contain DECIMAL(4,2), (18,2),
(19,19), (38,18) and (38,38), nullable/NOT NULL, signed extrema, zero and both signs.
`SF_INTEGRAL_CORPUS=decimal-truth` selects the same fixture. Shapes are outer CAST to
STRING and outer TRY_CAST to CHAR(8), each over the direct inner TRY_CAST.

Each trial consumes 131,072 rows, with 16 warmups/nine measurements. Rust times production
Calc and equivalent consumption after import/before export; Flink starts on equal prepared
RowData. Setup and perimeter costs alone are excluded from this function timer; intrinsic
costs remain. Actual preceding encoder `8b42f7c1` retains its character evaluator and any
production shortcuts on the exact same unchanged DSO/executable/fixture. A retained
descriptor alone is not proof of a JVM callback on every row. Hashes/mimalloc aliases match.

| Scope | Cells | Paired stock / native | Actual previous / native | Independent stock / native |
| --- | ---: | ---: | ---: | ---: |
| Bulk (1,024 rows) | 80 | 2.109-13.089x | 11.307-18.114x | 1.854-11.668x |
| Tiny (16 rows) | 20 | 0.211-1.308x | 13.644-15.236x | 0.204-1.244x |

All bulk cells beat stock and actual previous behavior. Tiny cells retain
14 stock losses and 0 previous losses.
All independent measurements and unfavorable controls remain.

Twenty complete million-row configurations cover five DECIMAL types and both shapes
on both released Flink lines. They retain bounded row input, blackhole output, startup,
JNI and both transposes. Two warmups/five measurements alternate order. Candidate controls
require consumed NativeCalc work and empty evaluators; previous controls require consumed
work and their retained evaluators. Seconds, median:

| Flink / source / shape | Paired stock | Native | Independent stock | Actual previous |
| --- | ---: | ---: | ---: | ---: |
| 2.2.1 / DECIMAL(4,2)-short | 0.207693 | 0.205985 | 0.211752 | 0.238406 |
| 2.2.1 / DECIMAL(4,2)-padded | 0.250289 | 0.213052 | 0.246135 | 0.267123 |
| 2.2.1 / DECIMAL(18,2)-short | 0.210433 | 0.206001 | 0.201563 | 0.232634 |
| 2.2.1 / DECIMAL(18,2)-padded | 0.240361 | 0.207615 | 0.244654 | 0.276751 |
| 2.2.1 / DECIMAL(19,19)-short | 0.206595 | 0.201382 | 0.208084 | 0.237834 |
| 2.2.1 / DECIMAL(19,19)-padded | 0.244125 | 0.222056 | 0.254456 | 0.282303 |
| 2.2.1 / DECIMAL(38,18)-short | 0.208986 | 0.211712 | 0.208985 | 0.241863 |
| 2.2.1 / DECIMAL(38,18)-padded | 0.242238 | 0.211111 | 0.249556 | 0.281209 |
| 2.2.1 / DECIMAL(38,38)-short | 0.215642 | 0.211295 | 0.207752 | 0.243736 |
| 2.2.1 / DECIMAL(38,38)-padded | 0.247046 | 0.217456 | 0.250570 | 0.282227 |
| 1.18.1 / DECIMAL(4,2)-short | 0.207685 | 0.209153 | 0.210604 | 0.236102 |
| 1.18.1 / DECIMAL(4,2)-padded | 0.250418 | 0.211956 | 0.243160 | 0.262985 |
| 1.18.1 / DECIMAL(18,2)-short | 0.210772 | 0.206240 | 0.220348 | 0.247438 |
| 1.18.1 / DECIMAL(18,2)-padded | 0.245597 | 0.208913 | 0.243005 | 0.266683 |
| 1.18.1 / DECIMAL(19,19)-short | 0.207183 | 0.228833 | 0.210799 | 0.240593 |
| 1.18.1 / DECIMAL(19,19)-padded | 0.249136 | 0.216391 | 0.240292 | 0.270399 |
| 1.18.1 / DECIMAL(38,18)-short | 0.205375 | 0.216000 | 0.215327 | 0.232637 |
| 1.18.1 / DECIMAL(38,18)-padded | 0.246364 | 0.232057 | 0.240270 | 0.273388 |
| 1.18.1 / DECIMAL(38,38)-short | 0.218105 | 0.214802 | 0.216118 | 0.249770 |
| 1.18.1 / DECIMAL(38,38)-padded | 0.246352 | 0.215345 | 0.241897 | 0.273894 |

Native/stock elapsed spans **0.846-1.104**, retaining 4 stock losses;
native/previous spans **0.750-0.951**, retaining 0 previous losses.
Prepared function gains are distinct from complete-job gains; no slower control is removed.

All **304 focused JVM invocations** pass without skips across released Flink 1.18.1/2.2.1
and JDK 11/17, targeting Java release 11. Complete 5,003-row maps/schema cover eleven
precision/scale pairs from DECIMAL(1,0)/(1,1) to (38,38), both outer cast kinds, nullable/
NOT NULL, modern/legacy modes, STRING/VARCHAR(1/3)/CHAR(4/8), signed extrema/zero, logical/
NULL wrappers and empty filtered batches. Filter/length/CONCAT consumers, selected errors,
untaken branches and explicitly retained integer targets pass. Related direct decimal,
integer truth/character and BOOLEAN/integer regressions pass. Admitted claims require
consumed NativeCalc work and empty evaluator descriptors.

Actual-before runs compare 44 first NOT NULL/modern outer-CAST queries' complete maps/
schema before the evaluator-free assertion; their subsequent modes/nullability/empty and
outer TRY_CAST queries are not claimed as reached. All sixteen preceding compound/failing/
empty/integer-target controls pass. Four fresh unchanged released CastRules oracles pass
eight original BOOLEAN-to-character assertions each. No Rust source/artifact changes;
the immediately preceding qualification's fresh 904-test Rust result remains historical.

Reproduce with release `integral_expression`, matching DECIMAL input/nullability/row-layout/
batch arguments, `SF_INTEGRAL_CORPUS=decimal-truth` and
`SF_INTEGRAL_EXPRESSION=CAST(TRY_CAST(v AS BOOLEAN) AS STRING)` or
`TRY_CAST(TRY_CAST(v AS BOOLEAN) AS CHAR(8))`. Actual previous supplies the frozen encoder
and `SF_INTEGRAL_PREVIOUS=true` on the same runtime/fixture. Complete jobs use
`IntegralExpressionPipelineBenchmark` with matching `sf.integral.decimalPrecision`/
`decimalScale` and `sf.integral.previous=true` for previous. Other resources/data match.
Remaining type/producer/consumer gaps stay in
[#492](https://github.com/datafusion-contrib/StreamFusion/issues/492).


## Character consumers of direct decimal predicates

CAST/TRY_CAST to STRING/VARCHAR/CHAR reuse the native BOOLEAN formatter for direct
same-precision/scale DECIMAL comparisons, direct DECIMAL NULL checks and verified logical/
NULL wrappers. The NULL proof also covers released nullable-comparison rewrites. The
ordinary DECIMAL-to-BOOLEAN CAST rewrite consequently qualifies when precision exceeds
scale; ordinary DECIMAL(p,p) retains released planning failure. Computed/failing/stateful/
mixed-type operands, integer-target TRY_CAST and bounded-position proofs stay separate.
No Rust kernel, setting, dependency or JNI/transport change is introduced by this extension.

The [record](results/decimal-comparison-character-casts-2026-11-04.json) retains 60 prepared configurations and
[6,000 raw samples](results/decimal-comparison-character-casts-2026-11-04.csv.gz), including warmups, checksums and variability.
Released Flink 1.18.1/2.2.1, Oracle JDK 17.0.12, Apple M4 Pro, one thread and release/mimalloc
are used. Equal GenericRowData/BinaryRowData and Arrow contain DECIMAL(4,2), (18,2),
and (38,18), nullable/NOT NULL, signed extrema, periodic zero and both signs.
Shapes are CAST(CAST(v AS BOOLEAN) AS STRING) and
TRY_CAST(CAST(v AS BOOLEAN) AS CHAR(8)); the inner ordinary CAST produces a verified
same-type zero comparison in released planning.

Each trial consumes 131,072 rows, with 16 warmups/nine measurements. Rust times production
Calc and equivalent consumption after import/before export; stock starts on equal prepared
RowData. Setup/perimeter conversion alone is excluded; intrinsic costs remain timed.
Actual preceding encoder `6305c6f9` retains its character evaluator and any production
shortcuts on the exact same newly rebuilt DSO/executable/fixture. A descriptor alone is
not proof of a Java callback on every row. Allocator aliases and artifact hashes match.

| Scope | Cells | Paired stock / native | Actual previous / native | Independent stock / native |
| --- | ---: | ---: | ---: | ---: |
| Bulk (1,024 rows) | 48 | 1.834-15.438x | 10.918-33.003x | 1.604-12.815x |
| Tiny (16 rows) | 12 | 0.169-1.127x | 13.296-14.487x | 0.167-1.185x |

All bulk cells beat paired stock and actual previous behavior. Tiny cells retain
10 paired-stock losses and 0 previous losses;
all independent measurements and unfavorable controls remain.

Eight complete million-row configurations cover compact DECIMAL(18,2) and noncompact
DECIMAL(38,18), both shapes and both released lines. Bounded row input, blackhole output,
startup, JNI and both transposes remain. Two warmups/five measurements alternate order.
Candidate needs consumed NativeCalc work and empty evaluators; preceding controls retain
consumed work and their character evaluator. Seconds, median:

| Flink / input / shape | Paired stock | Native | Independent stock | Actual previous |
| --- | ---: | ---: | ---: | ---: |
| 2.2.1 / DECIMAL(18,2)-short | 0.200580 | 0.192215 | 0.206523 | 0.233667 |
| 2.2.1 / DECIMAL(18,2)-padded | 0.246108 | 0.208744 | 0.243367 | 0.260601 |
| 2.2.1 / DECIMAL(38,18)-short | 0.201272 | 0.208937 | 0.205316 | 0.237216 |
| 2.2.1 / DECIMAL(38,18)-padded | 0.246396 | 0.215287 | 0.245370 | 0.277956 |
| 1.18.1 / DECIMAL(18,2)-short | 0.205336 | 0.203482 | 0.210707 | 0.244583 |
| 1.18.1 / DECIMAL(18,2)-padded | 0.242385 | 0.207672 | 0.233590 | 0.251610 |
| 1.18.1 / DECIMAL(38,18)-short | 0.203797 | 0.222972 | 0.194975 | 0.237246 |
| 1.18.1 / DECIMAL(38,18)-padded | 0.238009 | 0.210415 | 0.246277 | 0.265138 |

Native/stock elapsed spans **0.848-1.094**, retaining 2 stock losses;
native/previous spans **0.775-0.940**, retaining 0 previous losses.
Prepared function gains are distinct from complete-job gains; no slower control is removed.

All **260 focused JVM invocations** pass without skips on released Flink 1.18.1/2.2.1 and
JDK 11/17, targeting Java release 11. Complete 5,003-row maps/schema cover eleven precision/
scale pairs through (38,38), all six comparisons, independently varying DECIMAL columns
and NULL positions, NULL/logical wrappers, both outer cast kinds, STRING/VARCHAR(1/3)/
CHAR(4/8), modern/legacy modes and empty filtered batches. Computed operands retain their
generated controls; integer targets retain fallback. Filters/length, selected division
failures and untaken branches pass. Ordinary DECIMAL(p,p) planning failures remain covered.
Pure claims require consumed NativeCalc work and empty evaluator descriptors.

Actual-before runs compare all 44 first NOT NULL/modern outer-CAST queries' maps/schema
before failing the new evaluator-free assertion. Later modes/nullability/empty/outer-TRY_CAST
queries are not claimed as reached by those invocations. All eight retained-route controls
pass. Four fresh unchanged released DecimalTypeTest oracles each execute three original
comparison methods with their setup/teardown and original assertions. These are stock
oracles; the independent runtime-source SQL witnesses establish native parity and execution.

After rebasing onto merged main `2bb52ca2`, fresh full runtime Maven compilations for both lines,
**904 release Rust core tests** with no ignores and **304 preceding-character JVM controls**
pass on the rebuilt artifacts. A separate 260 invocations separately qualify this
comparison extension. Another **80 targeted metadata regressions** pass across the four
release/JDK pairs for NOT NULL arithmetic, narrowing CAST overflow and noncompact SIGN
character/default consumers. Eight fresh compact/noncompact direct-TRY_CAST character
prepared replays also beat stock and the actual latest-main encoder on the rebuilt runtime;
their raw samples are retained in the linked record. Earlier character measurements retain their original revision/DSO
provenance; none is relabeled as a fresh measurement of this rebuilt runtime. The
[800 fresh replay samples](results/decimal-truth-character-rebase-2026-11-04.csv.gz) are
separate prepared controls; they do not relabel the earlier twenty complete-job measurements.

Reproduce using release `integral_expression`, matching DECIMAL type/nullability/row layout/
batch, `SF_INTEGRAL_CORPUS=decimal-truth` and `SF_INTEGRAL_EXPRESSION` set to either shape.
Actual previous supplies the frozen encoder with `SF_INTEGRAL_PREVIOUS=true` on the same
runtime/fixture. Complete jobs use IntegralExpressionPipelineBenchmark with the matching
expression/precision/scale and `sf.integral.previous=true` for previous controls.
Remaining type/producer/consumer gaps stay in
[#492](https://github.com/datafusion-contrib/StreamFusion/issues/492).


## Runtime integer epoch precision

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


## Boolean-object and mixed integer PRINTF

On released Flink 2.2.1, verified total BOOLEAN producers and mixed signed-integer/BOOLEAN
arguments use the evaluator-free literal ASCII formatter. BOOLEAN `%b` observes true/false;
NULL is false. BOOLEAN `%s` uses true/false words and null for NULL. A non-NULL integer,
including zero, still formats as true with `%b`. Argument indices/reuse, uppercasing,
width, precision, NULLs and invalid-format routes retain Java Formatter semantics.
The running JDK validates each actual argument type at planning time. Explicit index zero
stays on the preceding route because JDK 11 and 17 disagree. Bounds remain 512 ASCII format
characters and width/precision at most 256. Other types/formats, unverified/failing/stateful
producers and representation-sensitive consumers keep their previous routes. Flink 1.18
has no PRINTF and retains released SQL rejection.

The [record](results/boolean-object-printf-2026-11-04.json) includes all final cells, initial slower versions,
source/artifact hashes and [16,000 raw trials](results/boolean-object-printf-2026-11-04.csv.gz).
Apple M4 Pro, macOS arm64, one thread, Oracle JDK 17.0.12, released Flink 2.2.1 and
release/mimalloc-linked DSO/executable with verified malloc aliases. Equal prepared
GenericRowData/BinaryRowData and Arrow cover true/false, nullable/NOT NULL and mixed
INT/BIGINT signed extrema/NULLs. Each trial consumes 131,072 rows, with 16 warmups and
nine measurements. Native timing starts/stops inside Rust after import and before export,
retaining production Calc, output consumption and intrinsic costs. Initial JNI/import/export
and perimeter transposes are excluded only from this small-function timer. Stock starts
on matching prepared RowData.

The actual preceding encoder/helper `1346ed57` runs with its saved **whole DSO/executable**
and the identical final fixture/JDK/data. It does not receive the new certified formatter.
New BOOLEAN controls retain their preceding evaluator; eight existing integer native controls
require empty evaluators on both versions. A retained descriptor alone does not prove a
Java callback on every batch. Independent stock measurements remain separate.

Shapes are `PRINTF('%b/%1$B',v)`, `PRINTF('%1$8.2S:%1$-8.3b',v)` and
`PRINTF('%1$+08d:%2$8.2B:%1$x:%2$s',n,v)` with INT or BIGINT n. Both row layouts and
NULL modes are retained, plus 16-row controls.

| Scope | Cells | Stock / native | Actual previous / native | Independent stock / native |
| --- | ---: | ---: | ---: | ---: |
| New bulk, 1,024 rows | 16 | 4.930-7.627x | 5.632-8.099x | 4.849-7.460x |
| New tiny, 16 rows | 4 | 3.382-5.248x | 10.339-12.905x | 3.367-5.101x |

All new bulk cells beat paired stock and the actual preceding implementation. Retain
0 tiny stock losses and
0 tiny previous losses.
Eight retained nullable INT/BIGINT `%d`/`%+08d:%1$x` native controls have previous/current
median ratios **1.285-1.440x**, with all ranges and checksums retained.
Eight additional unchanged certified DECIMAL(4,2)/DECIMAL(38,18) short/padded controls retain
their evaluators on both whole runtimes; previous/current spans
**1.103-1.214x**. Their matching output digests and allocation probes are
retained separately. The new pure scope does not include these DECIMAL arguments.

The first shared-loop implementation passes correctness but adds a stable BIGINT-simple
slowdown in the recorded order (previous/current 0.931x with nonoverlapping ranges).
CPU samples locate format parsing and integer conversion/allocation; inclusive counts overlap
and do not attribute wall time to a source branch. Separately compiling Boolean-capable and
integer-only loops recovers that simple-format probe, but padded BIGINT retains lower preceding
medians in both orders. The final implementation also reuses one conversion String across
rows/conversions and writes decimal integer digits through the existing `itoa` buffer.
Boolean/NULL words and hexadecimal/octal/character output append into this buffer. General
certified DECIMAL formatting retains its owned result without an additional copy. Parsing
is still rowwise. The intermediate buffered version also enables Boolean dispatch in general certified
formatting; DECIMAL controls expose previous/current as low as 0.927x. The final general
formatter uses its preceding non-Boolean specialization, with Boolean-object evaluation
retained outside the planner-verified literal path. An ownership-only experiment also
retains slower padded medians and is discarded. When every format in a certified batch
is equal and non-NULL, the final evaluator borrows it once and reuses the Arrow builder/
row/conversion buffers. This removes per-row output String materialization; varying/NULL
formats retain row handling. Existing complete-batch ASCII/cache checks still precede
formatting, and a failed formatter still rejects the whole certificate. All slower stages
and reverse-order numeric probes remain recorded.
For the nullable GenericRowData BIGINT padded control, one warmed 1,024-row operation
drops from **2,489 allocation requests / 80,164 requested bytes** on the actual preceding
whole runtime to **29 / 48,687**; output buffer bytes remain 35,159. Allocation traffic
is diagnostic, not peak memory or proof of a complete-job gain.

Four million-row jobs retain the bounded row source, blackhole sink, startup, JNI and
both transposes. Two warmups and five measurements alternate stock/native. New paths require
consumed NativeCalc work and empty evaluators; previous retains its original evaluator.
Median seconds:

| Shape | Paired stock | Native | Independent stock | Actual previous |
| --- | ---: | ---: | ---: | ---: |
| truth | 0.449773 | 0.287959 | 0.459998 | 0.460954 |
| padded | 0.709251 | 0.303006 | 0.704850 | 0.753436 |
| mixed-int | 1.046717 | 0.340149 | 1.021757 | 1.049034 |
| mixed-bigint | 1.065773 | 0.329840 | 1.059488 | 1.135357 |

Native/stock elapsed spans **0.309-0.640**;
native/previous spans **0.291-0.625**. All trials and
variability remain, including any losses; prepared gains do not establish universal
complete-job acceleration.

All **112 final focused JVM invocations** pass without skips across released Flink
1.18.1/2.2.1 and JDK 11/17, targeting Java 11: 24 new BOOLEAN/absence-control invocations,
20 existing integer-PRINTF/absence controls, 12 format-helper invocations, 36 preceding
epoch invocations and 20 existing PRINTF/shared-certificate invocations on Flink 2.2.
Complete 5,003-row maps/schema cover true/false/NULL, all signed widths, logical/comparison
producers, mixed independently nullable arguments, literals, padding/index/reuse/precision,
empty filtered batches and explicit invalid/dynamic/failing/lazy/protected-consumer controls.
Pure-route witnesses require consumed native work, no fallback and empty evaluator descriptors.
The actual preceding Flink 2.2 controls each compare ten complete first-query maps/schema
before failing the intended pure-route assertions; subsequent empty-query checks in those
failing invocations are not reached. Retained routes and both 1.18 absence controls pass.

All **912 release Rust core tests** pass without ignores, including Boolean/NULL/signed-width
formatting, independently sliced/empty arrays, rejected contracts and input ownership.
Two unchanged released `StringFunctionsITCase.printfTestCases` stock-oracle runs each pass
all **47 original assertions**. These stock oracles are separate from the native SQL and
execution witnesses. Earlier decimal/epoch commit timing records retain their own runtime
provenance; this rebuilt DSO is not substituted into historical measurements. Hosted CI
remains separate.

Reproduce with release `integral_expression`, the exact expression/type/secondary-type/
NULL/layout/batch from the record and the saved previous encoder/helper plus whole runtime.
New preceding routes set `SF_INTEGRAL_PREVIOUS=true`; retained old-native integer controls
leave it false. Certified DECIMAL controls set it true on both runtimes. Complete jobs use
`IntegralExpressionPipelineBenchmark`, `sf.integral.booleanInput=true`, matching expression
and secondary type. Remaining scope stays in
[#497](https://github.com/datafusion-contrib/StreamFusion/issues/497).


## Numeric Boolean producers for PRINTF

The literal primitive formatter also accepts the exact numeric BOOLEAN producer proof
already used by character CAST/TRY_CAST: integer truth TRY_CAST over total integer trees,
direct DECIMAL truth TRY_CAST, same-precision/scale direct DECIMAL comparisons, and their
verified logical/NULL wrappers. This keeps `PRINTF('%b',0)` as true while
`PRINTF('%b',TRY_CAST(0 AS BOOLEAN))` is false. Ordinary DECIMAL-to-BOOLEAN CAST benefits
from its released comparison rewrite where supported; DECIMAL(p,p) retains the released
planning failure. Computed/mixed decimal predicates, failing/stateful inputs, protected
consumers and integer-target/rounding-position proofs retain their preceding scopes.
Only released Flink 2.2 has PRINTF; 1.18 retains its SQL rejection.

This is a Java admission extension with **no native source/build change**. The exact preceding
encoder/helper `46a1f487` and its saved whole runtime execute the same fixture/data/JDK.
Candidate native artifacts are byte-identical. Final prepared and SQL-test encoder/context
bytecode matches within Flink 2.2, and before-control bytecode matches the saved sources.
The [record](results/exact-numeric-boolean-printf-2026-11-04.json) retains sources, hashes, all cells and
[4,050 raw prepared trials](results/exact-numeric-boolean-printf-2026-11-04.csv.gz).

Apple M4 Pro, macOS arm64, one thread, JDK 17.0.12, Flink 2.2.1 and the same verified
release/mimalloc DSO/executable as the preceding BOOLEAN formatter qualification.
Equal prepared GenericRowData/BinaryRowData and Arrow cover nullable/NOT NULL
DECIMAL(4,2)/DECIMAL(38,18), extrema/zero, independently nullable mixed INT and signed
INT/BIGINT truth controls. Each trial consumes 131,072 rows with 16 warmups and nine
measurements. The production Calc/output-consumption timer starts/stops inside Rust after
import and before export, retaining intrinsic allocation, conversions and callbacks.
Only this prepared gate excludes initial JNI/import/export and perimeter transposes.
Stock starts on matching prepared RowData. New paths require empty evaluators; actual
preceding paths retain their original evaluator.

| Scope | Cells | Stock / native | Actual previous / native | Independent stock / native |
| --- | ---: | ---: | ---: | ---: |
| Bulk, 1,024 rows | 32 | 4.646-8.479x | 5.200-8.644x | 4.495-8.516x |
| Tiny, 16 rows | 8 | 2.603-4.874x | 8.343-10.742x | 2.168-4.687x |

All bulk cells beat paired stock and actual preceding behavior. Retain
0 tiny stock losses and 0 tiny previous losses.
The initial candidate run completed 50 paired stock/native samples with matching digests,
but the local parser failed on the comma in the existing unquoted DECIMAL type field before
launching its preceding control. Those samples remain a separate `parser-attempt` stage;
they are not paired previous evidence. Normalizing that field before parsing and quoting
public CSV preserves the engine outputs without an engine/runtime modification.

Four million-row jobs retain the bounded row source, blackhole sink, job submission/startup,
JNI and both perimeter transposes. Environment setup and surrounding JVM process startup
are outside the job timer. Two warmups and five measurements alternate stock/native.
NativeCalc must consume work and the new routes require empty evaluators; preceding routes
keep their original evaluators. Median seconds:

| Shape | Paired stock | Native | Independent stock | Actual previous |
| --- | ---: | ---: | ---: | ---: |
| truth-DECIMAL(4,2) | 0.707504 | 0.258533 | 0.698759 | 0.696531 |
| mixed-DECIMAL(4,2) | 0.836147 | 0.324166 | 0.819374 | 0.819529 |
| truth-DECIMAL(38,18) | 0.691274 | 0.258629 | 0.661155 | 0.712982 |
| mixed-DECIMAL(38,18) | 0.835070 | 0.323816 | 0.816792 | 0.830866 |

Native/stock elapsed spans **0.365-0.388**
(0 stock losses); native/previous spans
**0.363-0.396** (0 previous losses).
All variability and independent stock measurements remain. These prepared and complete-job
results do not establish universal job acceleration.

All **76 fresh focused JVM invocations** pass without skips across released Flink
1.18.1/2.2.1 and JDK 11/17, targeting Java 11. Complete 5,003-row maps/schema cover eight
decimal precision/scale pairs, direct and comparison truth, independently nullable inputs,
modern/legacy modes, string precision/padding, mixed integer arguments, all-filtered
batches and retained invalid/failing/lazy/consumer controls. Signed-width cases also
compare a boxed integer's `%b` with its explicit BOOLEAN truth conversion. Admitted paths
require consumed NativeCalc work, no fallback and empty evaluator descriptors.

Actual preceding controls on each JDK compare sixteen complete first-query maps/schema
before the intended new pure-route assertion fails. Subsequent queries/modes/empty checks
in those failing invocations are not counted. Four other dynamic tests per JDK pass,
covering existing pure BOOLEAN routes and retained generated routes. The unchanged native
artifacts retain the preceding **912 release Rust core tests** and two unchanged released
PRINTF stock-oracle runs with **47 assertions** each; these are not new Rust/oracle runs.
Focused current JVM verification is separate from full hosted CI on the exact pushed head.

Reproduce with `integral_expression` using the record's expression/type, secondary type,
NULL/layout/batch and `SF_INTEGRAL_CORPUS=decimal-truth`; previous controls set
`SF_INTEGRAL_PREVIOUS=true` and use the saved actual encoder/helper and whole runtime.
Complete jobs use `IntegralExpressionPipelineBenchmark`, matching decimal precision/scale,
secondary type and expression. General STRING/DECIMAL printf arguments, remaining formats,
producers and consumers remain in [#497](https://github.com/datafusion-contrib/StreamFusion/issues/497).

## Length of primitive ASCII PRINTF

On 2026-11-04, direct released built-in `CHAR_LENGTH`/`CHARACTER_LENGTH` over the
already-qualified primitive ASCII formatter reuse the native length kernel. ASCII has
identical byte, character and UTF-16 lengths, so this consumer does not need Java String
identity. The proof includes total integer/BOOLEAN and admitted exact numeric truth
producers. Operator identity prevents a same-name catalog UDF from being substituted.
Generic Unicode, invalid/dynamic formats, unverified producers, other consumers and
relational representation guards remain unchanged. No native runtime or transport change.

The [record](results/primitive-printf-length-2026-11-04.json) retains source/artifact hashes,
every configuration, allocation diagnostics and [3,000 raw trials](results/primitive-printf-length-2026-11-04.csv.gz).
The actual preceding encoder/helper is `bbada54b`, replayed on its saved whole runtime;
candidate DSO and prepared executable are byte-identical. Apple M4 Pro, macOS arm64,
one thread, JDK 17.0.12, released Flink 2.2.1, Java `--release 11`, release/mimalloc build.

Stock starts on prepared GenericRowData/BinaryRowData and native starts inside Rust on
equal prepared Arrow batches. The timer retains production Calc, consumption, allocation,
intrinsic conversions and callbacks after input import and before output export. Only
initial JNI/import/export and perimeter transposes are outside this small-function timer.
Each cell uses 131,072 logical rows, 16 warmups and nine alternating measured trials.
Both NULL modes and row layouts cover INT/BIGINT extrema, BOOLEAN/mixed INT, compact/wide
DECIMAL truth/comparisons, indexed formats and padding. Digests match for all engines.

| Scope | Cells | Stock / native | Actual previous / native | Independent stock / native |
| --- | ---: | ---: | ---: | ---: |
| Bulk, 1,024 rows | 24 | 2.224-6.884x | 2.718-7.269x | 2.159-6.921x |
| Tiny, 16 rows | 6 | 1.352-4.703x | 8.432-10.516x | 1.406-5.079x |

Four complete million-row jobs retain job submission/startup, JNI and both RowData/Arrow
transposes, with bounded row sources and blackhole sinks. Each engine has two warmups and
five alternating measured jobs; the surrounding JVM startup and setup remain outside the
job timer. Median seconds:

| Shape | Paired stock | Native | Independent stock | Actual previous |
| --- | ---: | ---: | ---: | ---: |
| int-length | 0.319771 | 0.210095 | 0.318141 | 0.343298 |
| boolean-mixed | 0.762079 | 0.277136 | 0.779459 | 0.808125 |
| decimal-truth | 0.608789 | 0.226033 | 0.658311 | 0.658383 |
| decimal-comparison | 0.780619 | 0.298393 | 0.755788 | 0.810500 |

Native/stock elapsed spans **0.364-0.657** and native/previous **0.343-0.612**.
All measured bulk, tiny and complete-job cells improve; these bounded measurements do
not establish universal job acceleration. Raw trials retain variability and independent
stock controls.

All **114 final focused JVM invocations** pass, zero skips, across released Flink
1.18.1/2.2.1 and JDK 11/17. Full 5,003-row maps and resolved types cover all integer
widths, BOOLEAN, numeric truth, decimal extremes/NULLs, aliases, modern/legacy casts,
empty filtered batches, invalid/dynamic/non-ASCII formats and failing/lazy controls.
An executed same-name catalog UDF keeps its implementation. Pure routes require consumed
NativeCalc work, no fallback and empty evaluator descriptors. Each JDK's actual preceding
control compares fourteen complete first-query maps/schema before the intended pure-route
assertion fails; later queries in those failing invocations are not counted. Its retained
route and catalog UDF controls pass. An initial local UDF control needed quoted SQL
identifiers; final controls use the corrected test with no production-code change.

Four unchanged released `ScalarFunctionsTest.testCharLength` stock-oracle runs pass
separately, one per release/JDK pair. The byte-identical native artifacts retain their
earlier 912-core-test and PRINTF-oracle qualifications; these are not new Rust/PRINTF
oracle runs. Full hosted suites must qualify the exact pushed head separately.

Reproduce with release `integral_expression`, the record's expression/type/secondary type,
NULL/layout/batch and `SF_INTEGRAL_CORPUS=decimal-truth`. Previous controls set
`SF_INTEGRAL_PREVIOUS=true` and use the saved actual preceding encoder/helper and runtime.
Complete jobs use `IntegralExpressionPipelineBenchmark` with matching input and expression
properties. Remaining consumers/representations stay in
[#497](https://github.com/datafusion-contrib/StreamFusion/issues/497).

## Comparisons of primitive ASCII PRINTF

On 2026-11-04, direct built-in `=`, `<>`, `<`, `<=`, `>`, `>=` comparisons use native
kernels when both sides are qualified primitive ASCII formatter results or ASCII/NULL
character literals, with at least one formatter. Cached Java UTF-16 and serialized UTF-8
have identical order on this proven subset. Native filters can consume the predicates.
This Java-only admission proof preserves built-in identity and the complete existing
formatter producer proof. Arbitrary STRING inputs, non-ASCII literals, unknown/invalid
formats, other consumers and relational representation guards retain their prior paths.

The [record](results/primitive-printf-comparison-2026-11-04.json) retains every cell, source/
artifact hashes, variability, route evidence and [3,000 raw trials](results/primitive-printf-comparison-2026-11-04.csv.gz).
Apple M4 Pro, macOS arm64, one thread, JDK 17.0.12, released Flink 2.2.1, Java release 11,
release/mimalloc. The DSO and prepared executable are byte-identical to the qualified
preceding runtime; the actual previous encoder/helper is `40f26521`.

Prepared stock starts on equal GenericRowData/BinaryRowData and native starts inside Rust
on prebuilt Arrow. Each cell has 131,072 logical rows, 16 warmups and nine alternating
measurements. The Rust timer retains production Calc, consumption, allocation, intrinsic
conversions and callbacks after import and before export. Only initial JNI/import/export
and perimeter transposes are outside this small-function timer. Signed INT/BIGINT extrema,
BOOLEAN/mixed INT, compact/wide DECIMAL truth/comparisons, both NULL modes and row layouts
are covered; all engine digests match.

The previous route is verified per shape. Equality/inequality retain a native Calc with
the generated evaluator on the saved whole runtime. Ordered prepared controls verify
the actual previous whole-Calc stock fallback and exact representation reason, then time
the released generated evaluator on prepared RowData using the existing fallback fixture.
They do not substitute a hypothetical native callback for a rejected signature.

| Scope | Cells | Stock / native | Actual previous / native | Independent stock / native |
| --- | ---: | ---: | ---: | ---: |
| Bulk, 1,024 rows | 24 | 1.420-6.704x | 2.192-7.800x | 1.464-7.000x |
| Tiny, 16 rows | 6 | 0.774-4.163x | 3.509-9.424x | 0.627-4.129x |

All bulk cells pass the prepared bulk-function gate. The 16-row INT equality control
loses to stock; its timing and independent stock control remain in the record. The other
five tiny controls improve. No universal small-batch improvement is claimed.

Four complete million-row jobs retain job submission/startup, JNI and both transposes,
using bounded external Row sources and blackhole sinks. The external source proves
Java-backed inputs to the preceding planner, so its ordered complete-job controls retain
the actual native Calc with generated evaluation. This differs from the conservative
prepared/internal-RowData fallback. Both runtimes must consume native work, while the new
route additionally requires empty evaluators. Each engine has two warmups and five
alternating measured jobs; surrounding JVM startup and setup are outside timing. Median seconds:

| Shape | Paired stock | Native | Independent stock | Actual previous |
| --- | ---: | ---: | ---: | ---: |
| int-order | 0.218030 | 0.197722 | 0.202463 | 0.232047 |
| boolean-mixed-order | 0.795598 | 0.299646 | 0.748803 | 0.744022 |
| decimal-truth | 0.567530 | 0.232338 | 0.595066 | 0.585824 |
| decimal-comparison-order | 0.707602 | 0.300004 | 0.710702 | 0.776674 |

Native/stock elapsed spans **0.377-0.907**, native/previous **0.386-0.852**.
All four measured complete-job medians improve; independent controls and variability
remain, and the bounded qualification does not imply every job will improve.

All **148 final focused JVM invocations** pass, zero skips, on released Flink
1.18.1/2.2.1 and JDK 11/17. Complete 5,003-row maps/schema cover all six comparisons,
operand reversal, two formatter operands, all integer widths, BOOLEAN, DECIMAL truth,
NULLs, padding, empty filtered batches and the retained Unicode/format/failing/lazy paths.
An executed filter compares every surviving ID/value with stock. Admitted paths require
consumed NativeCalc work, no fallback and empty evaluators. The previous encoder compares
fourteen complete first-query projection maps/schema plus the filter result per JDK before
fifteen expected route failures. Later queries/modes in those failing invocations are not
counted. The unverified-path control passes with its explicit fallback/generated expectations.
An initial residual test expected generated Calc where the existing guard correctly used
whole-Calc fallback; only that test expectation changed.

Four unchanged released stock-oracle invocations each pass `ScalarOperatorsTest.testCompareOperator`
and `testMoreEqualAndNonEqual`, separately from native qualification. The unchanged native
artifacts keep their earlier 912-core-test/PRINTF-oracle qualification; those are not new
Rust/PRINTF runs. Hosted suites must qualify the exact pushed head separately.

Reproduce prepared cells with release `integral_expression` and the record's type/expression,
NULL/layout/batch/secondary type and `SF_INTEGRAL_CORPUS=decimal-truth`. Previous generated
controls set `SF_INTEGRAL_PREVIOUS=true` on the saved encoder/helper/runtime. Ordered
previous controls run `IntegralExpressionBenchmarkFunctions` with `sf.integral.previous=true`,
`sf.integral.fallbackControl=true` and the recorded exact fallback reason. Complete jobs
use `IntegralExpressionPipelineBenchmark` with matching inputs and expressions. Remaining
scope stays in [#497](https://github.com/datafusion-contrib/StreamFusion/issues/497) and
[#508](https://github.com/datafusion-contrib/StreamFusion/issues/508).

## Integer text producers for PRINTF

On 2026-11-04, unbounded STRING CAST/TRY_CAST of a proven total integer producer can
feed the primitive formatter without materializing intermediate text. The planner first
validates conversions against the logical STRING argument. Only then does it emit the
underlying integer: `%s`/`%S` and `%b`/`%B` have the same result for its decimal text,
including NULLs, precision, padding and indexing. Numeric conversions remain invalid for
STRING; their NULL-specific released behavior stays on the preceding route. Bounded
character targets, BOOLEAN/DECIMAL text and unverified/failing producers are excluded.
In particular, BOOLEAN false formatted with `%b` differs from its non-NULL text `false`.

This Java-only elision reuses the byte-identical native runtime and its buffered formatter.
Existing direct length/comparison consumers reuse the same ASCII proof; it does not widen
general STRING/representation admission. The [record](results/integer-text-printf-2026-11-04.json)
retains all configurations, hashes, allocation diagnostics and [3,500 raw trials](results/integer-text-printf-2026-11-04.csv.gz).
Apple M4 Pro, macOS arm64, one thread, JDK 17.0.12, released Flink 2.2.1, Java release 11,
release/mimalloc. Actual preceding encoder/helper `5e18ac8d` runs on its saved whole
DSO/executable, including its generated evaluator; measured and validated bytecode match.

Prepared stock starts on GenericRowData/BinaryRowData and native starts inside Rust on
equal prebuilt Arrow. Each cell has 131,072 logical rows, 16 warmups and nine alternating
measurements. Production Calc, output consumption, allocation, intrinsic conversions and
callbacks remain inside the Rust timer after import and before export. Initial JNI/import/
export and perimeter transposes are outside this small-function timer only. Four signed
integer widths, both NULL modes/layouts, computed wrapping, mixed BOOLEAN/integer arguments,
general string/Boolean formats, indexing, precision and padding are covered; all digests match.

| Scope | Cells | Stock / native | Actual previous / native | Independent stock / native |
| --- | ---: | ---: | ---: | ---: |
| Bulk, 1,024 rows | 28 | 2.059-7.947x | 3.023-8.367x | 2.102-8.084x |
| Tiny, 16 rows | 7 | 1.184-5.362x | 8.791-12.787x | 1.138-5.676x |

Four complete million-row jobs use bounded external Row sources and blackhole sinks,
retaining job submission/startup, JNI and both transposes. Each engine has two warmups and
five alternating measured jobs; surrounding JVM startup and setup are outside timing.
Candidate and preceding native operators must consume work; only the new route requires
empty evaluators. Median seconds:

| Shape | Paired stock | Native | Independent stock | Actual previous |
| --- | ---: | ---: | ---: | ---: |
| int-text | 0.267157 | 0.225393 | 0.268707 | 0.292513 |
| bigint-text | 0.691436 | 0.250366 | 0.622890 | 0.655572 |
| boolean-and-text | 0.816311 | 0.279250 | 0.753133 | 0.738243 |
| computed-text | 0.668061 | 0.249539 | 0.682569 | 0.675910 |

Native/stock elapsed spans **0.342-0.844** and native/previous **0.369-0.771**.
All measured cells improve, with raw variability and independent stock controls retained.
These gains qualify this bounded feature, not universal job acceleration or the earlier
16-row comparison regression, which remains in its own record.

All **172 final focused JVM invocations** pass without skips across released Flink
1.18.1/2.2.1 and JDK 11/17, targeting Java 11. Full 5,003-row maps/schema cover CAST and
TRY_CAST, signed extrema/NULLs, modern/legacy modes, arithmetic wrapping, mixed BOOLEAN,
length/comparison consumers, all-filtered batches, invalid STRING numeric conversions,
bounded/BOOLEAN/DECIMAL exclusions and failing/lazy/unused arguments. Admitted paths
require consumed NativeCalc work, no fallback and empty evaluators. On each JDK the actual
previous encoder compares eight first-query maps/schema, then fails the intended route
assertions; later legacy/empty queries in those invocations are not counted. Those broad
queries include ordered consumers and encounter the preceding internal-RowData whole-Calc
fallback. Its residual dynamic control passes; prepared direct-PRINTF controls independently
verify and time its generated native Calc route.

The unchanged DSO/executable retain their earlier 912-core-test and released PRINTF stock-
oracle qualification; these are not new Rust/oracle runs. Flink 1.18's lack of PRINTF remains
an absence control, not a native gap. Hosted suites must qualify the exact pushed head.

Reproduce prepared measurements with release `integral_expression` and the record's
type/expression, secondary type, NULL/layout/batch and `SF_INTEGRAL_CORPUS=decimal-truth`.
Previous controls set `SF_INTEGRAL_PREVIOUS=true` with the actual saved encoder/helper/runtime.
Complete jobs use `IntegralExpressionPipelineBenchmark`, matching boolean/BIGINT input,
secondary type and expression properties. Remaining direct STRING/DECIMAL arguments,
formats and consumers stay in [#497](https://github.com/datafusion-contrib/StreamFusion/issues/497).

## Primitive text UPPER

On 2026-11-04, built-in UPPER executes in Rust for verified total integer/BOOLEAN casts
to unbounded STRING on both released lines. Qualified primitive ASCII PRINTF also
participates on Flink 2.2 when its literal contains no lowercase `i`. Primitive conversions
cannot write `i`, so this domain has identical Java-backed/binary-only and worker-locale
behavior. Integer text has no letters and reuses the native CAST result; BOOLEAN text
writes uppercase directly. Only formatted text needs the existing Arrow case kernel.
General STRING/Unicode, bounded casts, unverified/failing producers, dependent consumers
and existing relational guards retain their previous routes.

The [record](results/primitive-upper-2026-11-04.json) and
[10,500 raw samples](results/primitive-upper-2026-11-04.csv.gz) retain 7,000 final trials
and 3,500 trials from the initial redundant-case implementation. Apple M4 Pro, macOS arm64,
one thread, JDK 17.0.12, released Flink 1.18.1/2.2.1, Java release 11, release/mimalloc.
Actual preceding encoder/helper `534099a2` uses its saved whole runtime, including the
generated evaluator. Native artifacts are byte-identical; measured/validated encoder
bytecode and source/artifact hashes match.

Prepared stock starts on equal GenericRowData/BinaryRowData and native starts inside
Rust on equal Arrow. Each cell processes 131,072 logical rows with sixteen warmups and
nine alternating measurements. Production Calc, output consumption, allocation and
intrinsic conversions/callbacks remain inside timing; initial JNI/import/export and
perimeter transposes are outside this small-function gate only. Four integer widths,
NULL modes/layouts, modern/legacy BOOLEAN text and padded/indexed PRINTF are covered.

| Flink | Batch rows | Cells | Stock / native | Actual previous / native | Independent stock / native |
| --- | ---: | ---: | ---: | ---: | ---: |
| 2.2 | 1,024 | 32 | 1.569-18.254x | 2.964-27.255x | 1.592-15.203x |
| 1.18 | 1,024 | 24 | 1.511-21.388x | 2.916-27.811x | 1.569-18.239x |
| 2.2 | 16 | 8 | 0.489-4.960x | 10.057-26.178x | 0.471-5.085x |
| 1.18 | 16 | 6 | 0.519-1.293x | 11.934-24.137x | 0.502-1.142x |

All 56 bulk cells beat stock and previous. Seven of fourteen tiny controls remain slower
than stock. A three-second native-only INT/16-row sample identifies scalar dispatch,
casts and case conversion. Removing the redundant integer case scan/output reduces a
representative batch from 38 allocations/2,760 requested bytes to 33/2,208. BOOLEAN text
also writes its final case directly. The original 2.2 tiny BOOLEAN control improves from
0.824x to 1.205x stock; simple small integers retain fixed Calc/CAST overhead and losses.
All initial measurements and allocation diagnostics remain; bulk qualification does not
establish universal small-batch acceleration.

Eight complete million-row configurations use bounded external Row sources and blackhole
sinks, retaining job submission/startup, JNI and both transposes. Each engine has two
warmups and five alternating measured jobs. Candidate and previous must consume native
work; only the candidate requires empty evaluators. Median seconds:

| Flink/shape | Paired stock | Native | Independent stock | Actual previous |
| --- | ---: | ---: | ---: | ---: |
| 22-int-text | 0.231095 | 0.215401 | 0.234017 | 0.257369 |
| 22-boolean-text | 0.209379 | 0.250789 | 0.210564 | 0.259598 |
| 22-int-format | 0.713247 | 0.257157 | 0.677347 | 0.699764 |
| 22-boolean-format | 0.680982 | 0.297569 | 0.654967 | 0.707587 |
| 22-boolean-legacy | 0.210135 | 0.197164 | 0.231739 | 0.299726 |
| 118-int-text | 0.231382 | 0.204040 | 0.227603 | 0.253104 |
| 118-boolean-text | 0.220297 | 0.247613 | 0.209224 | 0.241832 |
| 118-boolean-legacy | 0.237388 | 0.257726 | 0.228105 | 0.289418 |

Native/stock elapsed spans **0.361-1.198** and native/previous **0.367-1.024**.
Three complete-job medians lose to stock. The 1.18 modern BOOLEAN median also loses
2.4% to previous, with overlapping ranges. All losses, stock-process drift and raw
variability remain; the prepared bulk gate is distinct from complete-job acceleration.

All **238 final focused JVM invocations** pass without skips on released Flink
1.18.1/2.2.1 and JDK 11/17, compiling with Java release 11: 106 case/existing-function
invocations plus 132 BOOLEAN CAST regression invocations. Full 5,003-row maps/schema
cover signed extrema, NULLs, total wrapping/BOOLEAN producers, modern/legacy modes,
empty batches, invalid formats, bounded/DECIMAL exclusions, failing/lazy paths and
same-name catalog UDFs. Four runtime locales and exhaustive single-byte ASCII validate
both released string representations. Admitted paths require consumed NativeCalc work,
no fallback and empty evaluators. Four unchanged released `testUpperCase` stock-oracle
invocations pass separately.

Per JDK, the previous encoder compares thirteen full first-query maps/schema on 2.2
and ten on 1.18, plus the first locale query, before the intended evaluator-route
failures. Later modes/queries in failing invocations are not counted. Its two residual/
catalog controls per version/JDK pass. The unchanged native artifacts retain their
archived 912-core-test qualification; these are not new Rust runs. Hosted suites must
qualify the exact pushed head separately.

Reproduce prepared cells with release `integral_expression`, the record's type/expression,
NULL/layout/batch and `SF_INTEGRAL_CORPUS=decimal-truth`. Legacy controls also set
`SF_INTEGRAL_LEGACY=true`. Previous controls set `SF_INTEGRAL_PREVIOUS=true` with the saved
encoder/helper/runtime. Complete jobs use `IntegralExpressionPipelineBenchmark` with
matching input, expression and `sf.integral.legacy` properties. General Unicode casing
and other residual contracts remain in [#494](https://github.com/datafusion-contrib/StreamFusion/issues/494).
