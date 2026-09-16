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


## Integer casts (2026-09-16)

Measured on an Apple M3 Pro (18 GiB), JDK 17.0.14, UTC, Flink 2.2.1, DataFusion 54.0.0,
and the release/mimalloc native build. These measurements concern the default-mode INT/string
kernels only; they do not extend the [cast admission contract](../operators/calc-filter.md#integerstring-kernels).

### Batch boundary comparison

`IntegerCastBatchBenchmark` compares the new Rust expression with the existing production
`HostCastFunction` in the **same build and JVM**, alternating execution order on every trial.
The host branch explicitly registers the old JVM UDF node; the Rust branch has no JVM UDF nodes.
The two branches use identical prebuilt Arrow inputs, two warmups and five measured trials.
Each trial runs 256 batches. Input construction and expression registration are outside the timer;
Calc evaluation, outer JNI/Arrow export/import, output materialization/close, and the host branch's
nested JVM/Arrow callback are inside it. No row/Arrow transpose is measured by this diagnostic.
The Java importer consumes the output C structs, so each batch owns fresh structs on both branches.

String samples cover zero, positive/negative values, INT bounds, signs/leading zeros/ASCII spaces,
and decimal text (`12.9`). Integer samples are the corresponding parsed values. A separate JVM
repeats the experiment with every eighth row NULL. Batch sizes are 128, 1,024 and 4,096;
all trials are in [the raw batch data](integer-cast-batches-2026-09-16.csv).
The table shows median milliseconds for 256 batches of 4,096 rows (1,048,576 rows per trial):

| Expression | NULL every | Host cast (ms) | Rust cast (ms) | Host/Rust |
|---|---:|---:|---:|---:|
| STRING -> INT | 0 | 85.213 | 13.790 | 6.18x |
| INT -> STRING | 0 | 46.039 | 22.000 | 2.09x |
| INT -> VARCHAR(2) | 0 | 107.268 | 21.893 | 4.90x |
| STRING -> INT -> STRING | 0 | 136.768 | 36.839 | 3.71x |
| STRING -> INT | 8 | 81.426 | 14.959 | 5.44x |
| INT -> STRING | 8 | 48.746 | 23.764 | 2.05x |
| INT -> VARCHAR(2) | 8 | 86.267 | 23.180 | 3.72x |
| STRING -> INT -> STRING | 8 | 126.276 | 35.664 | 3.54x |

Reproduce each scenario in a fresh JVM, with no other build, test or benchmark running:

```sh
TZ=UTC SF_BENCHMARK=true mvn -pl :streamfusion-runtime test -Pbench \
  -Dnative.cargo.packages='-p streamfusion' -Dtest=IntegerCastBatchBenchmark \
  -Dcast.batchSizes=128,1024,4096 -Dcast.batches=256 -Dcast.warmup=2 -Dcast.runs=5 \
  -Dcast.nullEvery=0 -Dcast.output=target/integer-cast-batches.csv
```

Repeat with `cast.nullEvery=8` and a different output file. The benchmark verifies input/output
row counts and closes every result; semantic validation belongs to the released-Flink differential
tests, not to timing assertions. These ratios are not end-to-end Flink speedups.

### Complete-job before/after comparison

The same `ScalarFunctionBenchmark` harness runs four projections over 2,000,000 rows,
parallelism 1, two warmups and five measured trials per JVM, alternating Flink/native order. Two
rounds use fresh JVMs: before then after in round 1, after then before in round 2. The before
build is `ebe550c69122909cd08fca8ee89380478a92cc5c` with the identical benchmark and shared-fixture
files applied;
the after build includes the integer/string kernels. Both use release/mimalloc. Native plans
must contain `NativeCalc`, `RowDataToArrow` and `ArrowToRowData`; the source remains rowwise
and the sink remains Flink's blackhole sink. Times include planning and job startup/teardown.

The two INT formatting cases use alternating nonnegative/negative sequence values. STRING-to-INT
cycles the batch diagnostic's valid string samples. The nested projection uses the issue's exact
`LPAD(CAST(CAST(SUBSTR(s, 11, 2) AS INT) / 15 * 15 AS VARCHAR), 2, '0')` expression over
`timestamp:00`, `timestamp:17`, `timestamp:29`, and `timestamp:59`. The substring must be
`00`, `17`, `29`, and `59`, respectively, and the final output must be `00`, `15`, `15`, and `45`.
Both engines verify these expected results using the shared fixtures before any timing; the regular
SQL parity suite runs the same check without enabling benchmarks. This exercises zero and nonzero
buckets and both LPAD padding and no-padding paths. These dedicated inputs do not use `scalar.bytes`;
the retained generic CSV field is not their actual string length. This complete-job run has no NULLs.

All four projections were remeasured on both builds after correcting the nested projection's fixtures.
The earlier date-formatted fixtures put a space at position 11 and collapsed every bucket to zero;
those measurements are superseded, not mixed into this data. No other Java/Rust build or test process
was observed during the retained measurement rounds. All recorded trials, including job-lifecycle
outliers, are retained in [the raw job data](integer-cast-jobs-2026-09-16.csv).
The CSV identifies each round separately. The table uses pooled medians of all ten trials per
build/engine in seconds, not best-of timings:

| Projection | Flink before | Native before | Flink after | Native after | Native before/after |
|---|---:|---:|---:|---:|---:|
| CAST_STRING_INT | 0.481 | 0.854 | 0.542 | 0.794 | 1.08x |
| CAST_INT_STRING | 0.435 | 0.807 | 0.444 | 0.765 | 1.05x |
| CAST_INT_VARCHAR2 | 0.551 | 0.968 | 0.611 | 0.806 | 1.20x |
| CAST_INTEGER_PIPELINE | 0.729 | 1.128 | 0.743 | 0.938 | 1.20x |

These before/after differences include complete-job variability; source-matched identity controls
and both sets of Flink timings remain in the CSV. They must not be confused with the isolated
callback comparison above, nor treated as a guarantee that every standalone native CAST beats
stock Flink. In particular, STRING-to-INT's per-round native before/after ratios were 0.74x and
1.24x, so the pooled 1.08x is not evidence of a stable standalone-job speedup. The corrected nested
pipeline's per-round ratios were 1.33x and 1.09x; the pooled result replaces the earlier 1.52x
measurement on all-zero buckets. No slower round or lifecycle outlier was removed.
The batch comparison isolates the removed callback more directly, while these jobs
retain the perimeter cost that can dominate a small projection.
All four after-build native jobs remain slower than their paired stock Flink runs in this measurement.

```sh
TZ=UTC SF_BENCHMARK=true mvn -pl :streamfusion-runtime test -Pbench \
  -Dnative.cargo.packages='-p streamfusion' \
  '-Dtest=ScalarFunctionBenchmark#individualFunctions' \
  -Dscalar.functions=CAST_STRING_INT,CAST_INT_STRING,CAST_INT_VARCHAR2,CAST_INTEGER_PIPELINE \
  -Dscalar.rows=2000000 -Dscalar.warmup=2 -Dscalar.runs=5 \
  -Dscalar.output=target/integer-cast-jobs.csv
```

The same-build batch benchmark remains the reproducible old/new comparison after the planner
switch ships; reproducing the job-level before numbers requires the stated baseline plus the
identical benchmark-only fixtures, not disabling native execution altogether. Run the command in a
fresh JVM for each build in each round, reversing build order in the second round and preserving
separate output files before pooling the trials.
