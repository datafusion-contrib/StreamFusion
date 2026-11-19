# Recorded scalar workloads: diagnostics

[Topic index and original links](scalar-functions.md). Sections retain their recorded scope, dates and evidence.

## Extended function coverage {#extended-function-coverage}

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


## Floating conversion and POWER diagnostic (2026-09-16) {#floating-conversion-and-power-diagnostic-2026-09-16}

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


## Inputs {#inputs}

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


## Reproduce {#reproduce}

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


## Raw data {#raw-data}

The current trials, source-matched identity controls, result medians, workload catalog and
checksums are attached to [PR #44](https://github.com/datafusion-contrib/StreamFusion/pull/44)
as `pr44-flink-native-benchmarks.zip`. The repository keeps benchmark code and final result
tables; generated CSVs are not versioned. Controls are retained for checking the run, and are
not subtracted from function times because their result types and lengths can differ.


## IFNULL coverage diagnostic (2026-09-16) {#ifnull-coverage-diagnostic-2026-09-16}

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


## INSTR overloads diagnostic (2026-09-16) {#instr-overloads-diagnostic-2026-09-16}

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

