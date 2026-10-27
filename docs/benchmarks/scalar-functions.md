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
