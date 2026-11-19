# Recorded scalar workloads: json

[Topic index and original links](scalar-functions.md). Sections retain their recorded scope, dates and evidence.

## SQL/JSON native-first routing (2026-09-18) {#sqljson-native-first-routing-2026-09-18}

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


## SQL/JSON JVM bridge prototype (2026-09-18) {#sqljson-jvm-bridge-prototype-2026-09-18}

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


## Nested SQL/JSON batch boundaries (2026-09-19) {#nested-sqljson-batch-boundaries-2026-09-19}

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


## Spaced JSON paths diagnostic (2026-09-16) {#spaced-json-paths-diagnostic-2026-09-16}

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


## Empty JSON member diagnostic (2026-09-16) {#empty-json-member-diagnostic-2026-09-16}

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


## JSON_QUOTE {#json_quote}

`JSON_QUOTE`: `JSON_QUOTE(s)`

| Case | Input | Flink (s) | Native (s) | Flink / native |
|---|---|---:|---:|---:|
| `JSON_QUOTE` | ASCII, 264-byte budget | 1.550 | 1.570 | 0.99x |
| `JSON_QUOTE` | Unicode, 264-byte budget, NULL/8 | 26.048 | 2.972 | 8.77x |


## JSON_UNQUOTE {#json_unquote}

`JSON_UNQUOTE`: `JSON_UNQUOTE(s)`

| Case | Input | Flink (s) | Native (s) | Flink / native |
|---|---|---:|---:|---:|
| `JSON_UNQUOTE` | ASCII, 264-byte budget | 1.615 | 1.317 | 1.23x |
| `JSON_UNQUOTE` | Unicode, 264-byte budget, NULL/8 | 1.719 | 1.312 | 1.31x |


## SQL/JSON measurements {#sqljson-measurements}

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


## JSON_VALUE {#json_value}

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


## JSON_EXISTS {#json_exists}

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


## JSON_STRING scalars {#json_string-scalars}

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


## JSON_OBJECT scalar values {#json_object-scalar-values}

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


## IS JSON predicates {#is-json-predicates}

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


## JSON_VALUE RETURNING {#json_value-returning}

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


## Byte characters and JSON extensions (2026-09-15) {#byte-characters-and-json-extensions-2026-09-15}

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

### ASCII {#ascii}

`ASCII`: `ASCII(s)` over 264-byte strings. ASCII inputs begin with `a`/`c`;
Unicode inputs begin with a Chinese character or accented letter, exercising signed bytes.

| Input | Flink (s) | Native (s) |
|---|---:|---:|
| ASCII, no NULLs | 0.753 | 1.017 |
| Unicode, NULL every eighth row | 0.532 | 0.774 |

### CHR {#chr}

`CHR`: `CHR(n)` over BIGINT values, including negative values and repeated low bytes.

| Input | Flink (s) | Native (s) |
|---|---:|---:|
| BIGINT, no NULLs | 0.294 | 0.458 |
| BIGINT, NULL every eighth row | 0.292 | 0.431 |

### JSON_STRING DECIMAL {#json_string-decimal}

`JSON_STRING_DECIMAL`: `JSON_STRING(n)` over DECIMAL(38,9), alternating
`12345678901234567890.123456700` and `-0.000000100` (scientific notation in JSON).
The 264-byte string budget does not apply to decimal input.

| Input | Flink (s) | Native (s) |
|---|---:|---:|
| DECIMAL(38,9), no NULLs | 0.434 | 0.640 |
| DECIMAL(38,9), NULL every eighth row | 0.415 | 0.599 |

### JSON_OBJECT DECIMAL {#json_object-decimal}

`JSON_OBJECT_DECIMAL`: `JSON_OBJECT('n' VALUE n)` with the same two DECIMAL(38,9)
values and default NULL ON NULL policy. The 264-byte string budget does not apply.

| Input | Flink (s) | Native (s) |
|---|---:|---:|
| DECIMAL(38,9), no NULLs | 0.599 | 0.655 |
| DECIMAL(38,9), NULL every eighth row | 0.591 | 0.611 |

### Unicode JSON member paths {#unicode-json-member-paths}

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

### DECIMAL and VARBINARY scalar UDFs {#decimal-and-varbinary-scalar-udfs}

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


## JSON string consumers {#json-string-consumers}

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


## Negative JSON array indexes {#negative-json-array-indexes}

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


## JSON_VALUE exception parity (2026-09-19) {#json_value-exception-parity-2026-09-19}

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



## MAP SQL/JSON batch boundaries (2026-09-19) {#map-sqljson-batch-boundaries-2026-09-19}

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



## Final JSON_VALUE and UNION ALL (2026-10-22) {#final-json_value-and-union-all-2026-10-22}

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


## Flat scalar JSON_ARRAY (2026-11-06) {#flat-scalar-json_array-2026-11-06}

Flat scalar JSON_ARRAY now writes directly into Arrow string buffers. CHAR/VARCHAR,
BOOLEAN and all integer widths qualify on released Flink 1.18.1 and 2.2.1; DECIMAL
precision/scale through 38 additionally qualifies on 2.2. It reuses the existing
Jackson-compatible string and decimal writer. Typed column views, logical null masks and
constant arguments are prepared once per batch, avoiding per-row JSON trees and JVM
callbacks. Default/explicit ABSENT ON NULL omits SQL NULL elements; NULL ON NULL emits
JSON null. Empty and entirely omitted arrays return `[]`. Argument order is preserved.
Released 1.18 decimal rendering, direct nested raw JSON constructors, other scalar types
and containers retain generated evaluation. There is no new configuration switch.

All **88 focused test invocations** pass across both released Flink lines and JDK 11/17,
targeting Java 11: **64 new invocations and 24 related JSON_STRING invocations**, four
fresh JVM processes, no skipped/aborted/failed tests. Complete 5,003-row maps/schema cover
nullable/NOT NULL Generic/Binary sources, runtime CHAR(8)/VARCHAR, integer extrema,
booleans, ten DECIMAL precision/scale pairs, NULL policies, constants, empty arrays,
controls, supplementary and unpaired-surrogate inputs, long text, ordinary consumers,
CASE-selected strings and lazy/all-filtered divide-by-zero expressions. Pure paths
require consumed NativeCalc work, no fallback and empty JVM evaluator descriptors.
Retained nested/complex routes are explicit controls. **930 fresh release Rust core
tests pass**.

The unchanged released JsonFunctionsITCase also passes with the suite agent on JDK 17:
**489 invocations on 2.2.1 and 142 on 1.18.1**, no skips/aborts/failures. This is broad
regression evidence; the focused SQL tests separately establish pure JSON_ARRAY execution.
Actual preceding `b00ff8538` first compares full map/schema, then fails fifteen expected
pure-route assertions on 2.2/JDK17 and five on 1.18/JDK17. Retained controls pass. Later
queries not reached after an assertion are not preceding-parity evidence. Final
parity/performance encoder instructions match, including nested classes, and the
candidate/preceding benchmark fixture classes are byte-identical.

Prepared release timing uses production whole-Calc encoding on both versions. The prior
production encoder and native runtime execute the actual generated whole-Calc JVM bridge;
its intrinsic conversions and callback remain timed. The raw single-expression encoder
cannot represent that prior production path. `sf.integral.wholeCalc=true` selects the
production Calc entry in the benchmark fixture only. Stock starts on prebuilt
Generic/Binary RowData, native on prebuilt Arrow RecordBatch. The native timer starts
inside Rust after setup/import and stops before export, retaining production Calc,
allocations and equivalent string length/first/last checksums. Matched inputs include
independent NULL positions, INT/BIGINT mixtures, VARCHAR(4096)/INT with NULL and ABSENT
policies, and 2.2 DECIMAL(38,18)/INT. Text includes quotes/backslashes/controls,
literal Unicode escape spelling, Chinese/supplementary characters, JSON text and
640-byte strings. Full-value correctness is established separately by the SQL tests.

Each released line has nullable/nonnullable and Generic/Binary controls at 1,024 rows,
plus nullable/Binary 16-row controls. Each trial processes 131,072 logical rows with
sixteen warmups, nine measurements, matched 1 GiB heaps and UTC. All **36 bulk cells**
beat stock **1.902-5.864x** and actual previous execution **2.676-7.064x**. All **nine tiny
controls** beat stock **1.711-2.194x** and previous **8.132-13.036x**. Independent stock
controls, min/max variability and all **4,500 raw samples** remain in the report.

Seven complete million-row jobs cover INT, BIGINT and VARCHAR on both lines and DECIMAL
on 2.2. The row source, blackhole sink, submission/startup, JNI and both perimeter
transposes remain measured; every engine uses parallelism one, matched 2 GiB heaps, two
warmups and five measured repetitions. Each native job verifies consumed NativeCalc work
and empty JVM evaluator descriptors; each prior job verifies its generated route.
**All seven beat stock and previous**: native/stock elapsed **0.603-0.723**,
native/previous **0.605-0.697**. Blackhole timing is not a full-result comparison.
These gains apply to the measured scalar-array workloads, not every JSON signature.

See the [qualification, complete-job trials and exact source/runtime provenance](results/json-array-scalars-2026-11-06.json)
and [4,500 raw prepared samples](results/json-array-scalars-2026-11-06.csv.gz).
Dynamic/complex JSON_QUERY paths, nested serialization and additional types remain in
[#506](https://github.com/datafusion-contrib/StreamFusion/issues/506).


## Definite JSON_QUERY (2026-11-06) {#definite-json_query-2026-11-06}

Verified final VARCHAR projections over direct character documents and definite literal paths
now run in Rust on both released Flink lines. Strict/lax, WITHOUT/CONDITIONAL/UNCONDITIONAL
ARRAY wrappers and independent NULL/EMPTY ARRAY/EMPTY OBJECT policies qualify. Other paths,
ERROR policies, computed documents, non-final string consumers and unverified container
boundaries retain generated evaluation. In this measurement snapshot, Flink 2.2 combinations
with another parser or JVM evaluator retain generated row-major execution for shared recycler
state. Current combined-reader admission is described by the [coverage contract](../operators/calc-filter.md#json_query)
and open [#608](https://github.com/datafusion-contrib/StreamFusion/issues/608).

The writer preserves insertion order on released 1.18 and Java UTF-16 member order on 2.2,
last duplicate values, version-specific numeric spelling and original surrogate identity
until final UTF-8 serialization. Compatible wide documents serialize directly from the
existing recycled SIMD tape. Floating-point nodes, Unicode escape spelling and Jackson
edge documents retain the exact parser plus a flat selected-tree arena. Neither route
uses a JVM JSON evaluator; per-batch JNI synchronization of Jackson recycler metadata remains.

All **188 focused invocations** pass on Flink 1.18.1/2.2.1 and JDK 11/17, targeting Java 11.
These include 56 new QUERY/history invocations, 44 existing history and 88 ARRAY/STRING
invocations. Complete 5,003-row maps/schema, Generic/Binary and NULL layouts, wrappers,
policies, signed paths, duplicate/surrogate keys, numeric/depth/token limits and empty
filtered batches are compared. Pure SQL paths require consumed NativeCalc work, no fallback
and empty JVM descriptors. Related regression classes pass **148 invocations**, with two
pre-existing 1.18 JSON_QUOTE/JSON_UNQUOTE capability aborts. Updated 1.18 literal-path route
contracts execute their value comparisons. Unchanged released JsonFunctionsITCase passes
**489 + 142 invocations**, without skips/aborts/failures. **935 release Rust core tests pass**.

Actual preceding `e5337b1f9` supplies both its production encoder and native runtime; it
executes the generated whole-Calc JVM bridge. Candidate/previous benchmark fixtures are
byte-identical, and final parity/performance encoder instructions match. A generated-route
control also fixes premature replacement of raw isolated-surrogate member names before
branching-path selection; the general multi-reader scheduling issue remains open.

Prepared trials use release/System allocation on Apple M4 Pro/macOS arm64, JDK 17, UTC,
matched 1 GiB heaps and production whole-Calc encoding. Stock starts on prebuilt Generic/
Binary RowData; native starts on prebuilt Arrow RecordBatch. The Rust timer begins after
setup/import and ends before export, retaining Calc execution, output consumption, allocation
and intrinsic conversions/JNI. Independent stock controls accompany actual previous runs.
Each engine has eight warmups and seven measured samples, alternating stock/native order.
Mixed trials process 32,768 rows; wide/deep trials process 16,384. Full-value correctness is
established separately; timed output consumption compares equivalent length/first/last checksums.

Mixed documents cover objects/arrays, missing/null/scalar selections, duplicate escaped keys,
Unicode/surrogate escapes, decimals/exponents and 640-byte strings. Wide objects contain 32
reverse-inserted members with three-element arrays; deep objects contain 24 single-member
levels. Each bulk shape has four nullable/nonnullable and Generic/Binary controls at 1,024 rows.

| Flink | Bulk shape | Stock / native range | Previous / native range |
| --- | --- | ---: | ---: |
| 2.2.1 | mixed-without | 2.130-3.355x | 2.058-2.694x |
| 2.2.1 | mixed-conditional | 1.978-3.510x | 1.984-2.756x |
| 2.2.1 | mixed-unconditional | 2.424-3.788x | 2.171-3.782x |
| 2.2.1 | wide-object | 1.639-1.866x | 1.558-1.972x |
| 2.2.1 | deep-object | 3.015-3.584x | 3.153-3.469x |
| 1.18.1 | mixed-without | 2.383-3.361x | 2.279-3.443x |
| 1.18.1 | mixed-conditional | 1.771-3.362x | 1.945-3.593x |
| 1.18.1 | mixed-unconditional | 2.327-3.720x | 2.093-2.178x |
| 1.18.1 | wide-object | 1.351-1.748x | 1.613-1.802x |
| 1.18.1 | deep-object | 3.127-3.416x | 3.245-3.409x |

All **40 bulk cells** improve over stock **1.351-3.788x**
and actual previous **1.558-3.782x**.
The ten nullable/Binary 16-row controls range **1.267-2.550x** versus
stock (0 losses) and **1.802-4.495x** versus previous
(0 losses). Min/max sample variability and all paired stock controls remain in the report.

Six complete million-row jobs retain row source, blackhole sink, planning/submission, JNI
and both transposes, with parallelism one, matched 2 GiB heaps, two warmups and five measured
repetitions. Candidate jobs require consumed NativeCalc and empty JVM descriptors; previous
jobs require their generated route. Blackhole timing is not a result comparison.

| Flink | Complete job | Stock (s) | Previous (s) | Native (s) | Native / stock |
| --- | --- | ---: | ---: | ---: | ---: |
| 2.2.1 | mixed-without | 0.936 | 1.074 | 0.735 | 0.786 |
| 2.2.1 | mixed-conditional | 0.822 | 0.910 | 0.723 | 0.880 |
| 2.2.1 | wide-object | 6.150 | 6.069 | 4.655 | 0.757 |
| 1.18.1 | mixed-without | 0.946 | 1.065 | 0.752 | 0.795 |
| 1.18.1 | mixed-conditional | 0.812 | 0.869 | 0.779 | 0.960 |
| 1.18.1 | wide-object | 6.090 | 6.327 | 4.806 | 0.789 |

Complete native/stock elapsed ranges **0.757-0.960**
(0 losses); native/previous ranges **0.685-0.896**
(0 losses). These are the measured workloads, without a universal end-to-end claim.

The first implementation lost on wide/deep objects; it was not published as accelerated.
Profiling identified per-container allocations, legacy deduplication hashing and recursive
destruction. A flat arena and borrowed ordinary keys remove those costs and accelerate deep
objects, but an intermediate 1.18 wide case still loses. A second profile identifies
validation/reparsing, member sorting and arena reallocation; reusing the compatible SIMD
tape removes the second parse/tree construction. The report retains both slower cohorts
(720 raw samples each), their source/runtime hashes and untimed allocation diagnostics.
Requested allocation bytes are cumulative traffic, not retained memory; JVM allocations
are outside the Rust allocation counter. Sampled runs are excluded from throughput results.

Reproduce with the release `integral_expression` fixture, the record's SQL, VARCHAR(4096),
NULL/layout/batch controls, `JAVA_TOOL_OPTIONS=-Dsf.integral.wholeCalc=true`,
`SF_INTEGRAL_CORPUS=json-query|json-query-wide|json-query-deep` and
`SF_INTEGRAL_REQUIRED_FUNCTION=JSON_QUERY`. Previous controls use the actual saved encoder/
runtime and `SF_INTEGRAL_PREVIOUS=true`. Complete jobs use
`IntegralExpressionPipelineBenchmark` with matching expression/corpus and string input.

See [qualification, variability, full job trials and exact provenance](results/json-query-definite-2026-11-06.json)
and [4,440 raw prepared samples, including both slower cohorts](results/json-query-definite-2026-11-06.csv.gz).


## Shared JSON reader history (2026-11-06) {#shared-json-reader-history-2026-11-06}

The fix for [#608](https://github.com/datafusion-contrib/StreamFusion/issues/608) preserves
successful results that depend on Jackson's recycled input-buffer capacity. Evaluating complete
columns could otherwise grow the buffer with a later row before a different projection reads
an earlier row. Compatible final readers now share one batch metadata acquisition/release and
use capacities computed in Flink row/projection order. No JVM JSON evaluator processes their
documents or results. Ordinary short documents avoid per-row capacity-table allocation.

Qualification uses Flink 1.18.1/2.2.1, JDK 11/17 and Java 11 bytecode. All **160 focused
invocations** pass on the final measured DSO, including direct reader-pair comparisons at
capacities 4,000/8,000/16,000/32,768 and batch sizes 1/3/all. SQL tests compare complete
5,003-row maps and schema, require consumed NativeCalc with no evaluator descriptors, and
cover nullable/Binary inputs, filters, empty results and retained generated routes. All
**938 Rust core tests** pass. A broader related suite passes **1,418 invocations**, with
14 classified released-1.18 capability aborts: 13 JSON_QUOTE/JSON_UNQUOTE cases and one JSON()
case. The unchanged released JsonFunctionsITCase passes **489 + 142** invocations. These
broader runs used the same production source before Cargo-target relocation; the rebuilt DSO
has a different hash and was requalified in all four focused combinations before measurement.

Performance uses a release build on Apple M4 Pro, macOS 26.5, JDK 17 and a 1 GiB JVM heap.
Each prepared trial contains 32,768 rows; eight warmups precede seven measured trials. Four
shapes combine two VALUE readers on the same field, two VALUE readers on independent fields,
VALUE with IS JSON OBJECT, and two QUERY readers on independent fields. The mixed document
corpus includes Unicode, escaped/duplicate keys, numeric spelling and 640-byte text. Independent
fields use different NULL patterns and a corpus offset of three. Bulk measurements cover all
Generic/Binary and nullable/NOT NULL combinations at batch size 1,024; tiny measurements use
nullable Binary rows at batch size 16. Growth-sensitive documents are qualified separately
for correctness; these throughput numbers describe ordinary documents.

Stock starts on prepared RowData and native starts on equivalent Arrow batches. The Rust
timer excludes input import and output export but includes production Calc, output consumption,
intrinsic conversions/allocations and recycler metadata JNI. Every output column is checked and
consumed. The previous control is the actual preceding `b01eba4d5` encoder/library with identical
benchmark fixture bytecode: previous native execution for VALUE/predicates and 1.18 QUERY,
previous generated whole-Calc execution with its intrinsic callbacks/conversions for 2.2 QUERY.

| Flink | Batch | Shape | Stock / native | Previous / native |
| --- | ---: | --- | ---: | ---: |
| 2.2.1 | 1,024 | Same-field VALUE pair | 7.29-8.03x | 0.971-1.013x |
| 2.2.1 | 1,024 | Independent VALUE pair | 8.04-9.02x | 0.971-1.015x |
| 2.2.1 | 1,024 | VALUE + predicate | 5.57-7.94x | 0.988-1.018x |
| 2.2.1 | 1,024 | Independent QUERY pair | 1.61-2.03x | 2.258-2.330x |
| 1.18.1 | 1,024 | Same-field VALUE pair | 7.21-7.57x | 0.936-1.004x |
| 1.18.1 | 1,024 | Independent VALUE pair | 7.46-8.10x | 0.985-1.028x |
| 1.18.1 | 1,024 | VALUE + predicate | 5.04-6.97x | 0.981-1.003x |
| 1.18.1 | 1,024 | Independent QUERY pair | 1.56-1.90x | 0.944-0.977x |
| 2.2.1 | 16 | Four nullable Binary shapes | 1.38-3.25x | 1.280-4.979x |
| 1.18.1 | 16 | Four nullable Binary shapes | 1.21-3.15x | 1.139-1.367x |

All 40 prepared stock gates win. Fifteen of the 32 bulk cells lose to the preceding
implementation, with the largest measured duration increase about 6.9%. This is a successful-value
correctness repair; retaining the preceding column-order history is not a valid control for
the affected long documents. The report retains those costs rather than calling every path faster.

Complete jobs use one million runtime rows, parallelism one, a 2 GiB heap, two warmups and five
measured trials. They retain planning/submission, the rowwise source/sink, JNI and both transposes.
Candidate and previous controls each have their own paired stock runs. Medians below are seconds;
the linked report retains individual trials and min/max variability.

| Flink | Shape | Paired stock | Previous | Native | Native / stock | Native / previous |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| 2.2.1 | Independent VALUE pair | 2.316 | 0.580 | 0.582 | 0.251 | 1.004 |
| 2.2.1 | VALUE + predicate | 1.769 | 0.621 | 0.631 | 0.357 | 1.017 |
| 2.2.1 | Independent QUERY pair | 1.672 | 2.370 | 1.409 | 0.842 | 0.595 |
| 1.18.1 | Independent VALUE pair | 2.328 | 0.588 | 0.589 | 0.253 | 1.002 |
| 1.18.1 | VALUE + predicate | 1.679 | 0.618 | 0.625 | 0.372 | 1.011 |
| 1.18.1 | Independent QUERY pair | 1.632 | 1.459 | 1.493 | 0.915 | 1.023 |

All six jobs beat their paired stock controls; five are slower than the preceding implementation,
by 0.2-2.3%. The audit launched no concurrent tests/builds/benchmarks during timing, but interactive
desktop and unrelated work remained active. These observations do not establish an idle-machine
or universal speedup claim. Mixed represented-string SQL boundaries retain their compatibility
gate even though raw JNI tests verify additional reader-kind combinations.

See [qualification, per-cell variability, complete trials and exact provenance](results/json-reader-schedule-2026-11-06.json)
and [2,400 raw prepared observations](results/json-reader-schedule-2026-11-06.csv.gz).
Remaining dynamic/branching paths, nested serialization and additional types stay in
[#506](https://github.com/datafusion-contrib/StreamFusion/issues/506). Hosted checks must
qualify the exact pushed head separately.



## ARRAY/ROW JSON_STRING (2026-11-06) {#arrayrow-json_string-2026-11-06}

Final JSON_STRING projections over direct ARRAY/ROW fields now prepare a borrowed Arrow
writer once per batch and serialize directly into the output string buffer. Nested nullable
children retain their NULL/empty distinction; ROW keys sort in Java UTF-16 order and the
internal row-kind sidecar is excluded. Character, BOOLEAN and signed integer leaves are
verified on both released lines; DECIMAL leaves additionally use 2.2's verified provider.
Computed/intermediate containers, nonnullable children, additional types and unverified
field names retain generated evaluation. No new opcode, dependency or switch is introduced.

Qualification uses released Flink 1.18.1/2.2.1 and JDK 11/17 with Java 11 bytecode.
All **108 focused invocations** pass on the measured release DSO. Tests compare complete
5,003-row maps and resolved schemas, including Generic/Binary input, nullable/NOT NULL top
containers, nested arrays/rows, every signed integer width, BOOLEAN/CHAR, decimal extremes,
controls, isolated surrogates, Unicode field-name order, nested RowKinds and filtered/empty
results. Admitted cases require consumed NativeCalc and empty JVM evaluator descriptors.
All **941 Rust core tests** pass. Related tests pass **205 invocations**, with **13**
released-1.18 capability aborts involving JSON, JSON_QUOTE, SPLIT and selected array functions.
Those aborts are not passing evidence. The unchanged released JsonFunctionsITCase passes
**489 + 142** invocations. Final SQL/performance encoder bytecode matches, and candidate/
previous benchmark fixtures are byte-identical on each released line.

A constructed mixed-expression regression found that JSON_STRING decodes cold string leaves,
changing the later ARRAY_MAX result for supplementary Unicode versus U+E000. Cache-observing
sibling readers and other representation-sensitive outputs therefore retain generated
whole-Calc execution; tests verify both projection orders and collection passthrough.
An earlier ROW-order prototype was also corrected against released 1.18 before any reported
timings. The published performance scope contains standalone final serializers, not these
retained mixed programs.

Release measurements use Apple M4 Pro, macOS 26.5, JDK 17, a 1 GiB heap and parallelism one.
Each prepared trial has 32,768 rows, eight warmups and seven measurements. ARRAY widths are
4/32; text contains slash, backslash, quote, NUL and supplementary Unicode. Inputs include
empty arrays, NULL leaves and independently NULL ROW fields. Bulk cells cover Generic/Binary
and nullable/NOT NULL combinations at batch size 1,024; tiny cells use nullable Binary rows
at size 16. Prepared Arrow strings are ordinary Utf8 values. Complete SQL jobs retain the
production represented-collection conversion.

Stock starts on prebuilt RowData and native on logically equivalent prebuilt Arrow batches.
The Rust timer starts after input import and ends before output export, retaining production
Calc execution, output consumption and all intrinsic work. The previous control is the exact
preceding `57004bcba` encoder/library with its generated whole-Calc callbacks and intrinsic
conversions. Scalar controls retain preceding native JSON_STRING execution. Each control
also has an independent stock run; full variability is retained in the linked report.

| Flink | Shape | Stock / native | Previous / native |
| --- | --- | ---: | ---: |
| 2.2.1 | int-4 | 8.745-10.312x | 9.739-13.024x |
| 2.2.1 | int-32 | 1.961-2.191x | 2.692-4.324x |
| 2.2.1 | string-4 | 2.853-4.047x | 5.098-5.975x |
| 2.2.1 | string-32 | 2.113-2.624x | 3.596-3.817x |
| 2.2.1 | row | 7.566-8.939x | 9.735-11.767x |
| 1.18.1 | int-4 | 8.477-12.285x | 10.063-12.138x |
| 1.18.1 | int-32 | 1.663-2.217x | 2.647-3.073x |
| 1.18.1 | string-4 | 2.689-4.327x | 5.288-5.388x |
| 1.18.1 | string-32 | 2.044-2.866x | 3.459-3.662x |
| 1.18.1 | row | 6.975-8.087x | 11.296-13.266x |

All **40 bulk** cells beat stock by **1.663-12.285x** and previous
by **2.647-13.266x**. Ten tiny controls span
**1.719-4.438x** stock and **5.460-26.487x** previous,
with **0** stock losses. Four retained scalar controls span
**0.944-1.005x** previous, with **2** lower candidate medians;
these are recorded costs/variation, not a claim that existing scalars improve.

Complete jobs process one million runtime rows, using two warmups, five measured trials,
parallelism one and a 2 GiB heap. They retain planning/submission, rowwise source/sink, JNI
and both transposes. Medians are seconds:

| Flink | Shape | Paired stock | Previous | Native | Native / stock | Native / previous |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| 2.2.1 | int-32 | 0.960 | 1.431 | 0.758 | 0.790 | 0.530 |
| 2.2.1 | string-32 | 3.254 | 7.052 | 4.414 | 1.357 | 0.626 |
| 2.2.1 | row | 0.538 | 0.862 | 0.397 | 0.738 | 0.461 |
| 1.18.1 | int-32 | 1.071 | 1.381 | 0.786 | 0.734 | 0.569 |
| 1.18.1 | string-32 | 3.274 | 7.019 | 4.415 | 1.349 | 0.629 |
| 1.18.1 | row | 0.538 | 0.846 | 0.426 | 0.793 | 0.504 |

Complete native/stock elapsed spans **0.734-1.357** with
**2 losses**; native/previous spans
**0.461-0.629** with **0 losses**.
In particular, the wide string-array jobs still lose to stock despite improving on the
preceding generated evaluator. Passing the prepared operator-format gate does not establish
a speedup for every row-fed query. The first measured cohort, including its slower scalar
and complete-job controls, remains separately labeled in the report; it predates the
Java-only mixed-expression guard and is not relabeled as final-source qualification.
The audit runs no concurrent tests/builds/benchmarks during timing; desktop/unrelated work
may remain active, so this is not an idle-machine claim.

Reproduce with the release `integral_expression` example, `JAVA_TOOL_OPTIONS=-Dsf.integral.wholeCalc=true`,
`SF_INTEGRAL_EXPRESSION=JSON_STRING(v)`, `SF_INTEGRAL_REQUIRED_FUNCTION=JSON_STRING`,
`SF_INTEGRAL_CONTAINER_WIDTH=4|32`, the report's ARRAY/ROW logical types and NULL/layout
controls. Previous runs use the saved encoder/runtime and `SF_INTEGRAL_PREVIOUS=true`;
retained scalar controls additionally set `SF_INTEGRAL_PREVIOUS_NATIVE=true`. Complete jobs
use `IntegralExpressionPipelineBenchmark` with `sf.integral.containerType`, the same expression
and width, retaining the runtime source and blackhole sink.

See [per-cell variability, both cohorts, complete trials and provenance](results/json-container-string-2026-11-06.json)
and [6,480 raw prepared observations](results/json-container-string-2026-11-06.csv.gz).
The [separate JFR diagnostic](results/json-container-string-profile-2026-11-06.json)
records allocation/copy stacks inside the input transpose and Flink's source/chaining
serializers. Its ten Java execution samples cannot establish CPU-time shares or prove a
dominant bottleneck; inclusive allocation samples overlap, and native interior work is
unresolved. Profiled durations are excluded from all timing tables. Paired profiling and
full-job copy/conversion optimization remain in
[#610](https://github.com/datafusion-contrib/StreamFusion/issues/610).
Additional SQL/JSON signatures remain in
[#506](https://github.com/datafusion-contrib/StreamFusion/issues/506).

