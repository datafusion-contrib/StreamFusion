# Recorded scalar workloads: strings

[Topic index and original links](scalar-functions.md). Sections retain their recorded scope, dates and evidence.

## STRING to BOOLEAN coverage diagnostic (2026-09-16) {#string-to-boolean-coverage-diagnostic-2026-09-16}

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


## STARTSWITH {#startswith}

`STARTSWITH_LITERAL`: `STARTSWITH(s, 'row:')`; `STARTSWITH_COLUMN`: `STARTSWITH(s, needle)`

| Case | Input | Flink (s) | Native (s) | Flink / native |
|---|---|---:|---:|---:|
| `STARTSWITH_LITERAL` | ASCII, 264-byte budget | 0.447 | 0.766 | 0.58x |
| `STARTSWITH_COLUMN` | ASCII, 264-byte budget | 0.554 | 0.938 | 0.59x |
| `STARTSWITH_LITERAL` | Unicode, 264-byte budget, NULL/8 | 0.599 | 0.796 | 0.75x |
| `STARTSWITH_COLUMN` | Unicode, 264-byte budget, NULL/8 | 0.700 | 0.981 | 0.71x |
| `STARTSWITH_LITERAL` | ASCII, 8-byte padding | 0.375 | 0.556 | 0.67x |
| `STARTSWITH_COLUMN` | ASCII, 8-byte padding | 0.504 | 0.773 | 0.65x |


## ENDSWITH {#endswith}

`ENDSWITH_LITERAL`: `ENDSWITH(s, ':match')`; `ENDSWITH_COLUMN`: `ENDSWITH(s, needle)`

| Case | Input | Flink (s) | Native (s) | Flink / native |
|---|---|---:|---:|---:|
| `ENDSWITH_LITERAL` | ASCII, 264-byte budget | 0.445 | 0.759 | 0.59x |
| `ENDSWITH_COLUMN` | ASCII, 264-byte budget | 0.567 | 0.942 | 0.60x |
| `ENDSWITH_LITERAL` | Unicode, 264-byte budget, NULL/8 | 0.579 | 0.832 | 0.70x |
| `ENDSWITH_COLUMN` | Unicode, 264-byte budget, NULL/8 | 0.700 | 1.003 | 0.70x |
| `ENDSWITH_LITERAL` | ASCII, 8-byte padding | 0.380 | 0.552 | 0.69x |
| `ENDSWITH_COLUMN` | ASCII, 8-byte padding | 0.497 | 0.766 | 0.65x |


## INSTR {#instr}

`INSTR_LITERAL`: `INSTR(s, ':match')`; `INSTR_COLUMN`: `INSTR(s, needle)`

| Case | Input | Flink (s) | Native (s) | Flink / native |
|---|---|---:|---:|---:|
| `INSTR_LITERAL` | ASCII, 264-byte budget | 1.461 | 0.768 | 1.90x |
| `INSTR_COLUMN` | ASCII, 264-byte budget | 1.622 | 0.945 | 1.72x |
| `INSTR_LITERAL` | Unicode, 264-byte budget, NULL/8 | 0.935 | 0.817 | 1.14x |
| `INSTR_COLUMN` | Unicode, 264-byte budget, NULL/8 | 1.107 | 1.012 | 1.09x |
| `INSTR_LITERAL` | ASCII, 8-byte padding | 0.425 | 0.579 | 0.73x |
| `INSTR_COLUMN` | ASCII, 8-byte padding | 0.556 | 0.793 | 0.70x |


## LOCATE {#locate}

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


## BIN {#bin}

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


## HEX {#hex}

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


## TO_BASE64 {#to_base64}

`TO_BASE64`: `TO_BASE64(s)`

| Case | Input | Flink (s) | Native (s) | Flink / native |
|---|---|---:|---:|---:|
| `TO_BASE64` | ASCII, 264-byte budget | 0.657 | 0.963 | 0.68x |
| `TO_BASE64` | Unicode, 264-byte budget, NULL/8 | 0.840 | 1.037 | 0.81x |


## UNHEX {#unhex}

`UNHEX`: `UNHEX(s)`

| Case | Input | Flink (s) | Native (s) | Flink / native |
|---|---|---:|---:|---:|
| `UNHEX` | 528 hex bytes, non-null | 1.203 | 1.295 | 0.93x |
| `UNHEX` | 528 hex bytes, NULL/8 | 1.130 | 1.063 | 1.06x |


## GREATEST {#greatest}

`GREATEST`: `GREATEST(n, m, 17)`

| Case | Input | Flink (s) | Native (s) | Flink / native |
|---|---|---:|---:|---:|
| `GREATEST` | non-null | 0.308 | 0.471 | 0.65x |
| `GREATEST` | NULL/8 | 0.287 | 0.453 | 0.63x |


## LEAST {#least}

`LEAST`: `LEAST(n, m, 17)`

| Case | Input | Flink (s) | Native (s) | Flink / native |
|---|---|---:|---:|---:|
| `LEAST` | non-null | 0.294 | 0.449 | 0.65x |
| `LEAST` | NULL/8 | 0.276 | 0.453 | 0.61x |


## INITCAP {#initcap}

`INITCAP`: `INITCAP(s)`

| Case | Input | Flink (s) | Native (s) | Flink / native |
|---|---|---:|---:|---:|
| `INITCAP` | ASCII, 264-byte budget | 1.347 | 1.266 | 1.06x |
| `INITCAP` | Unicode, 264-byte budget, NULL/8 | 1.812 | 1.463 | 1.24x |


## TRANSLATE {#translate}

`TRANSLATE`: `TRANSLATE(s, 'abcdef', 'ABCDEF')`

| Case | Input | Flink (s) | Native (s) | Flink / native |
|---|---|---:|---:|---:|
| `TRANSLATE` | ASCII, 264-byte budget | 2.377 | 1.350 | 1.76x |
| `TRANSLATE` | Unicode, 264-byte budget, NULL/8 | 3.138 | 1.240 | 2.53x |


## BTRIM {#btrim}

`BTRIM`: `BTRIM(s)`

| Case | Input | Flink (s) | Native (s) | Flink / native |
|---|---|---:|---:|---:|
| `BTRIM` | ASCII, 264-byte budget | 0.566 | 0.868 | 0.65x |
| `BTRIM` | Unicode, 264-byte budget, NULL/8 | 0.728 | 0.937 | 0.78x |


## ELT {#elt}

`ELT`: `ELT(i, s, t)`

| Case | Input | Flink (s) | Native (s) | Flink / native |
|---|---|---:|---:|---:|
| `ELT` | ASCII, 264-byte budget | 0.753 | 1.342 | 0.56x |
| `ELT` | Unicode, 264-byte budget, NULL/8 | 1.112 | 1.575 | 0.71x |


## URL_ENCODE {#url_encode}

`URL_ENCODE`: `URL_ENCODE(s)`

| Case | Input | Flink (s) | Native (s) | Flink / native |
|---|---|---:|---:|---:|
| `URL_ENCODE` | ASCII, 264-byte budget | 1.450 | 1.285 | 1.13x |
| `URL_ENCODE` | Unicode, 264-byte budget, NULL/8 | 4.400 | 1.448 | 3.04x |


## OVERLAY {#overlay}

`OVERLAY`: `OVERLAY(s PLACING t FROM i FOR n)`

| Case | Input | Flink (s) | Native (s) | Flink / native |
|---|---|---:|---:|---:|
| `OVERLAY` | ASCII, 264-byte budget | 0.834 | 1.197 | 0.70x |
| `OVERLAY` | Unicode, 264-byte budget, NULL/8 | 1.201 | 1.415 | 0.85x |


## URL_DECODE {#url_decode}

`URL_DECODE`: `URL_DECODE(s)`

| Case | Input | Flink (s) | Native (s) | Flink / native |
|---|---|---:|---:|---:|
| `URL_DECODE` | ASCII, 264-byte budget | 1.713 | 1.507 | 1.14x |
| `URL_DECODE` | Unicode, 264-byte budget, NULL/8 | 1.521 | 1.257 | 1.21x |


## ENCODE {#encode}

`ENCODE_UTF8`: `ENCODE(s, 'UTF-8')`

| Case | Input | Flink (s) | Native (s) | Flink / native |
|---|---|---:|---:|---:|
| `ENCODE_UTF8` | ASCII, 264-byte budget | 0.454 | 0.755 | 0.60x |
| `ENCODE_UTF8` | Unicode, 264-byte budget, NULL/8 | 0.900 | 0.821 | 1.10x |


## DECODE {#decode}

`DECODE_UTF8`: `DECODE(b, 'UTF-8')`

| Case | Input | Flink (s) | Native (s) | Flink / native |
|---|---|---:|---:|---:|
| `DECODE_UTF8` | ASCII, 264-byte budget | 0.442 | 0.657 | 0.67x |
| `DECODE_UTF8` | Unicode, 264-byte budget, NULL/8 | 0.846 | 0.735 | 1.15x |


## SPLIT {#split}

`SPLIT`: `SPLIT(s, '|')`

| Case | Input | Flink (s) | Native (s) | Flink / native |
|---|---|---:|---:|---:|
| `SPLIT` | ASCII, 264-byte budget | 3.241 | 6.581 | 0.49x |
| `SPLIT` | Unicode, 264-byte budget, NULL/8 | 2.535 | 4.821 | 0.53x |


## SUBSTRING {#substring}

`SUBSTRING_DYNAMIC`: `SUBSTRING(s, n, len)`

| Case | Input | Flink (s) | Native (s) | Flink / native |
|---|---|---:|---:|---:|
| `SUBSTRING_DYNAMIC` | ASCII, 264-byte budget | 0.760 | 0.929 | 0.82x |
| `SUBSTRING_DYNAMIC` | Unicode, 264-byte budget, NULL/8 | 0.937 | 1.005 | 0.93x |


## LEFT {#left}

`LEFT_DYNAMIC`: `LEFT(s, n)`

| Case | Input | Flink (s) | Native (s) | Flink / native |
|---|---|---:|---:|---:|
| `LEFT_DYNAMIC` | ASCII, 264-byte budget | 0.615 | 0.880 | 0.70x |
| `LEFT_DYNAMIC` | Unicode, 264-byte budget, NULL/8 | 0.800 | 0.967 | 0.83x |


## RIGHT {#right}

`RIGHT_DYNAMIC`: `RIGHT(s, n)`

| Case | Input | Flink (s) | Native (s) | Flink / native |
|---|---|---:|---:|---:|
| `RIGHT_DYNAMIC` | ASCII, 264-byte budget | 0.822 | 0.864 | 0.95x |
| `RIGHT_DYNAMIC` | Unicode, 264-byte budget, NULL/8 | 0.983 | 0.912 | 1.08x |


## LPAD {#lpad}

`LPAD_DYNAMIC`: `LPAD(s, n, p)`

| Case | Input | Flink (s) | Native (s) | Flink / native |
|---|---|---:|---:|---:|
| `LPAD_DYNAMIC` | ASCII, 264-byte budget | 0.878 | 1.123 | 0.78x |
| `LPAD_DYNAMIC` | Unicode, 264-byte budget, NULL/8 | 1.199 | 1.400 | 0.86x |


## RPAD {#rpad}

`RPAD_DYNAMIC`: `RPAD(s, n, p)`

| Case | Input | Flink (s) | Native (s) | Flink / native |
|---|---|---:|---:|---:|
| `RPAD_DYNAMIC` | ASCII, 264-byte budget | 0.815 | 1.173 | 0.70x |
| `RPAD_DYNAMIC` | Unicode, 264-byte budget, NULL/8 | 1.189 | 1.410 | 0.84x |


## SPLIT_INDEX {#split_index}

`SPLIT_INDEX_DYNAMIC`: `SPLIT_INDEX(s, p, n)`

| Case | Input | Flink (s) | Native (s) | Flink / native |
|---|---|---:|---:|---:|
| `SPLIT_INDEX_DYNAMIC` | ASCII, 264-byte budget | 2.099 | 1.065 | 1.97x |
| `SPLIT_INDEX_DYNAMIC` | Unicode, 264-byte budget, NULL/8 | 2.029 | 1.121 | 1.81x |

### Integer-Delimiter Split Index {#integer-delimiter-split-index}

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


## Runtime SHA2 Widths {#runtime-sha2-widths}

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


## LTRIM {#ltrim}

`LTRIM_LITERAL_SET`: `LTRIM(s, ' |ab')`

| Case | Input | Flink (s) | Native (s) | Flink / native |
|---|---|---:|---:|---:|
| `LTRIM_LITERAL_SET` | ASCII, 264-byte budget | 0.756 | 1.107 | 0.68x |
| `LTRIM_LITERAL_SET` | Unicode, 264-byte budget, NULL/8 | 0.851 | 1.072 | 0.79x |


## RTRIM {#rtrim}

`RTRIM_LITERAL_SET`: `RTRIM(s, ' |ab')`

| Case | Input | Flink (s) | Native (s) | Flink / native |
|---|---|---:|---:|---:|
| `RTRIM_LITERAL_SET` | ASCII, 264-byte budget | 1.923 | 0.990 | 1.94x |
| `RTRIM_LITERAL_SET` | Unicode, 264-byte budget, NULL/8 | 1.581 | 1.063 | 1.49x |


## TO_BASE64 binary input {#to_base64-binary-input}

Measured on 2026-09-10 with the release profile, 2,000,000 rows, two warmups and
five interleaved trials, including both transposes. Run with
`scalar.functions=TO_BASE64_BINARY`. The `tt_bytes` fixture supplies binary columns
containing the ASCII/Unicode payload bytes; encoding is the only measured function.

| Scenario | Flink (s) | Native (s) |
|---|---:|---:|
| ASCII, 32-byte budget | 0.357 | 0.495 |
| ASCII, 264-byte budget | 0.499 | 0.767 |
| Unicode, 264-byte budget, NULL/8 | 0.485 | 0.753 |


## ENCODE UTF-16 charsets {#encode-utf-16-charsets}

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


## DECODE UTF-16 charsets {#decode-utf-16-charsets}

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


## TRIM directions and literal sets {#trim-directions-and-literal-sets}

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


## SHA1 {#sha1}

`SHA1(s)` measured on 2026-09-10 with the same release, interleaved method as TRIM,
2,000,000 rows, two warmups and five trials, including both transposes. Run with
`scalar.functions=SHA1`. The input is the `tt_text` fixture.

| Scenario | Flink (s) | Native (s) | Flink / native |
|---|---:|---:|---:|
| ASCII, 32-byte budget | 0.837 | 0.697 | 1.20x |
| ASCII, 264-byte budget | 2.229 | 1.299 | 1.72x |
| Unicode, 264-byte budget, NULL/8 | 1.882 | 1.151 | 1.64x |


## Fixed binary Base64 encoding {#fixed-binary-base64-encoding}

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


## FROM_BASE64 {#from_base64}

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


## Runtime SQL TRIM {#runtime-sql-trim}

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


## Binary STARTSWITH {#binary-startswith}

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


## Binary ENDSWITH {#binary-endswith}

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


## Binary to STRING {#binary-to-string}

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

### Complete row-source jobs {#complete-row-source-jobs}

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

### Profiling and validation {#profiling-and-validation}

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

### Reproduce {#reproduce_1}

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

### Modern bounded CHAR/VARCHAR targets {#modern-bounded-charvarchar-targets}

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

### Latest-main bounded-cast requalification (2026-11-03) {#latest-main-bounded-cast-requalification-2026-11-03}

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


## STRING IS_ALPHA {#string-is_alpha}

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


## STRING IS_DIGIT {#string-is_digit}

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


## STRING IS_DECIMAL {#string-is_decimal}

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


## Declared NOT NULL classification controls {#declared-not-null-classification-controls}

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


## Numeric IS_ALPHA {#numeric-is_alpha}

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


## Numeric IS_DIGIT {#numeric-is_digit}

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


## Primitive text UPPER {#primitive-text-upper}

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


## Primitive text LOWER {#primitive-text-lower}

On 2026-11-04, LOWER reuses the primitive producer proof and native text writers qualified
for UPPER. Total integer/BOOLEAN casts to unbounded STRING are admitted on both released
lines. Qualified primitive ASCII PRINTF participates on Flink 2.2 when its literal contains
no uppercase `I`: cached Java strings otherwise lowercase it differently in Turkish/Azeri
locales. Primitive conversions cannot introduce `I`. Integer text reuses its existing
native CAST result, BOOLEAN writes lowercase directly, and formatted text uses the
existing Arrow case kernel. General STRING/Unicode and consumers keep their exact paths.

The [record](results/primitive-lower-2026-11-04.json) and
[7,000 raw trials](results/primitive-lower-2026-11-04.csv.gz) retain all cells, allocation
diagnostics, variability and actual source/artifact hashes. Apple M4 Pro, macOS arm64,
one thread, JDK 17.0.12, released Flink 1.18.1/2.2.1, Java release 11, release/mimalloc.
Actual preceding encoder/helper `60d1e655` includes native UPPER but retains generated
LOWER evaluation. Both whole runtimes are byte-identical; measured and validated scalar
bytecode match. No new configuration or transport contract is introduced.

Prepared stock starts on GenericRowData/BinaryRowData and native starts inside Rust on
equal Arrow. Each cell processes 131,072 logical rows, sixteen warmups and nine alternating
measurements. Production Calc, result consumption, allocation and intrinsic conversions/
callbacks remain inside timing; initial JNI/import/export and perimeter transposes are
outside this small-function gate only. Four integer widths, both NULL modes/layouts,
modern/legacy BOOLEAN text and padded/indexed formats are covered; all digests match.

| Flink | Batch rows | Cells | Stock / native | Actual previous / native | Independent stock / native |
| --- | ---: | ---: | ---: | ---: | ---: |
| 2.2 | 1,024 | 32 | 1.514-16.338x | 2.959-28.997x | 1.687-14.814x |
| 1.18 | 1,024 | 24 | 1.508-21.269x | 3.020-30.765x | 1.638-15.777x |
| 2.2 | 16 | 8 | 0.514-4.932x | 10.132-26.523x | 0.525-4.992x |
| 1.18 | 16 | 6 | 0.498-1.158x | 12.343-24.451x | 0.543-1.212x |

All 56 bulk medians beat stock and previous; nine of fourteen tiny controls still lose
to stock. This extension already uses the integer identity and final-case BOOLEAN writer
profiled in the UPPER qualification. Remaining small-batch Calc/CAST costs, independent
stock drift and the slightly slower legacy BOOLEAN tiny control are retained.

Eight complete million-row configurations use bounded external Row sources and blackhole
sinks, retaining job submission/startup, JNI and both transposes. Each engine has two
warmups and five alternating measured jobs. Both candidate/previous require consumed
native work; only the candidate requires empty evaluators. Median seconds:

| Flink/shape | Paired stock | Native | Independent stock | Actual previous |
| --- | ---: | ---: | ---: | ---: |
| 22-int-text | 0.240047 | 0.201903 | 0.231463 | 0.261967 |
| 22-boolean-text | 0.210758 | 0.208022 | 0.212839 | 0.264560 |
| 22-int-format | 0.645009 | 0.263799 | 0.631092 | 0.650668 |
| 22-boolean-format | 0.645045 | 0.286106 | 0.661133 | 0.700037 |
| 22-boolean-legacy | 0.230676 | 0.244194 | 0.211327 | 0.241696 |
| 118-int-text | 0.233692 | 0.214742 | 0.234665 | 0.260876 |
| 118-boolean-text | 0.233153 | 0.247825 | 0.218829 | 0.265762 |
| 118-boolean-legacy | 0.229295 | 0.238191 | 0.212077 | 0.288261 |

Native/stock elapsed spans **0.409-1.063**, native/previous **0.405-1.010**.
Three complete-job medians lose to stock; 2.2 legacy BOOLEAN also loses 1.0% to previous,
with overlapping ranges. These losses remain and are distinct from the bulk gate.

All **196 fresh focused JVM invocations** pass without skips on released Flink
1.18.1/2.2.1 and JDK 11/17, compiling with Java release 11: 164 shared case/existing-function
invocations plus 32 DECIMAL-truth/predicate composition invocations. The shared tests
revalidate UPPER and LOWER over full 5,003-row maps/schema, signed extrema/NULLs, total
wrapping, modern/legacy casts, empty batches, locale-sensitive/Unicode exclusions,
retained consumers, failing/lazy producers and same-name catalog UDFs. Additional compact
DECIMAL(4,2) and noncompact DECIMAL(38,18) controls feed both case functions through
verified truth/predicate text. Admitted routes require consumed NativeCalc work, no fallback
and empty evaluator descriptors. Four unchanged released `testLowerCase` stock oracles
pass separately. The earlier 132-invocation BOOLEAN CAST qualification is retained in
the UPPER record, not relabeled as a new test run.

Per JDK, preceding LOWER controls compare thirteen first-query maps/schema on 2.2 and
ten on 1.18, plus the first locale query, before expected evaluator-route failures.
Later modes/queries in those failing invocations are not counted. Its residual/catalog
controls pass. In the additional DECIMAL matrix, all four preceding UPPER controls pass;
four preceding LOWER controls compare results and then fail the intended route assertion.
The unchanged native artifacts retain the archived 912-core-test qualification; these
are not new Rust runs. Hosted suites must qualify the exact pushed head.

Reproduce prepared cells with release `integral_expression`, the record's type/expression,
NULL/layout/batch and `SF_INTEGRAL_CORPUS=decimal-truth`; legacy cells also set
`SF_INTEGRAL_LEGACY=true`. Previous controls set `SF_INTEGRAL_PREVIOUS=true` on the saved
encoder/helper/runtime. Complete jobs use `IntegralExpressionPipelineBenchmark` with
matching inputs, expression and `sf.integral.legacy`. Remaining general Unicode casing
and other signatures stay in [#494](https://github.com/datafusion-contrib/StreamFusion/issues/494).



## Length of proven primitive case results {#length-of-proven-primitive-case-results}

Direct built-in `CHAR_LENGTH` and `CHARACTER_LENGTH` now consume qualified native
UPPER/LOWER results without a JVM evaluator. Their existing producer proof establishes
locale-independent ASCII from total integer/BOOLEAN text, direct DECIMAL truth/predicate
text and qualified Flink 2.2 primitive PRINTF. UTF-8 and Java UTF-16 lengths therefore
agree. Integer casing still shares its CAST result; BOOLEAN writes its final case directly.
The existing Arrow length kernel consumes those results. This introduces no Rust source,
transport, opcode or switch change. General Unicode, bounded casts, locale-sensitive
formats, fallible/unverified producers and further dependent expressions keep prior routes.

The [record](results/primitive-case-length-2026-11-04.json) and
[9,400 raw trials](results/primitive-case-length-2026-11-04.csv.gz) preserve full ranges,
independent stock controls, pilot provenance, checksums, allocations and source/
bytecode/artifact hashes. Actual preceding encoder `519f221f` already supports
primitive UPPER/LOWER but delegates these length consumers. Both versions run on identical
release/mimalloc runtime artifacts; archived 912-core-test qualification is retained rather
than called a new Rust run. Final validated and measured scalar bytecode matches.

### Functionality gate {#functionality-gate}

One thread on Apple M4 Pro/macOS arm64, JDK 17.0.12, Java release 11, released Flink
1.18.1/2.2.1. Stock starts on equal prepared GenericRowData/BinaryRowData; native starts
inside Rust on prepared Arrow. Input/setup, initial JNI/import/export and perimeter
transposes are outside this small-function timer only. Production Calc, output consumption,
allocation and intrinsic conversions/callbacks remain inside. Every cell processes 131,072
logical rows with sixteen warmups and nine measured alternating trials; all digests match.

The matrix covers four integer widths, BOOLEAN, compact DECIMAL(4,2) truth and noncompact
DECIMAL(38,18) predicates, both NULL modes and row layouts, modern/legacy text and indexed/
padded formatter output. PRINTF is absent from Flink 1.18 and its cells run only on 2.2.
Every successful encoder control now uses the production context-aware entry, passing the
real TableConfig even when the output is INT. The old output-type heuristic missed inner
character casts in numeric length outputs and failed registration before any timing. The
corrected common fixture is used by stock/actual-prior/native, with equal fixture bytecode
for both encoders. Setup/configuration remains outside timing; rejected-signature controls
keep their diagnostic path. No timings from the failed registration are used.

| Flink | Batch rows | Cells | Stock/native throughput | Previous/native throughput | Independent stock/native |
| --- | ---: | ---: | ---: | ---: | ---: |
| 2.2.1 | 1,024 | 40 | 1.580-16.236x | 2.898-21.935x | 1.551-16.865x |
| 2.2.1 | 16 | 10 | 0.424-4.561x | 8.989-19.484x | 0.430-4.827x |
| 1.18.1 | 1,024 | 32 | 1.437-16.119x | 2.626-21.305x | 1.629-15.155x |
| 1.18.1 | 16 | 8 | 0.413-0.903x | 10.027-18.291x | 0.427-0.934x |

All 72 bulk cells beat stock by 1.437-16.236x and previous by
2.626-21.935x. Of 18 sixteen-row controls,
15 lose to stock. Those losses, independent stock drift and the
four-cell pilot are retained. The final common fixture removes only unreachable rejected-
signature diagnostic branches after that pilot; pilot source/bytecode hashes remain in the
record. There is no broad small-batch acceleration claim.

### Complete jobs and correctness {#complete-jobs-and-correctness_2}

Twelve complete 1M-row configurations retain the runtime rowwise source, blackhole sink,
parallelism one, submission/startup, JNI and both RowData/Arrow transposes. Each engine has
two warmups and five alternating measured trials. Shapes include integer and BOOLEAN text,
compact/noncompact DECIMAL producers, modern/legacy text and applicable formatters. Seconds,
median (minimum-maximum):

| Release/shape | Stock | Native | Independent stock | Previous |
| --- | ---: | ---: | ---: | ---: |
| 22-int-text | 0.196109 (0.186710-0.216379) | 0.180605 (0.175743-0.197065) | 0.184473 (0.181079-0.204818) | 0.221067 (0.216402-0.246059) |
| 22-boolean-text | 0.194744 (0.173471-0.206837) | 0.167066 (0.161800-0.183659) | 0.174838 (0.163094-0.179601) | 0.205228 (0.201143-0.219627) |
| 22-int-format | 0.672973 (0.655778-0.678616) | 0.230926 (0.226071-0.246572) | 0.631091 (0.618354-0.633107) | 0.620273 (0.603027-0.631882) |
| 22-boolean-format | 0.664518 (0.664071-0.673431) | 0.218722 (0.212579-0.230046) | 0.668366 (0.661009-0.669023) | 0.639350 (0.630945-0.648802) |
| 22-decimal-truth | 0.202152 (0.194988-0.217200) | 0.189457 (0.182427-0.207775) | 0.191021 (0.186779-0.209982) | 0.234690 (0.232336-0.238282) |
| 22-decimal-predicate | 0.190472 (0.185858-0.201505) | 0.189945 (0.185299-0.199012) | 0.187950 (0.186691-0.195766) | 0.245734 (0.236047-0.250349) |
| 22-boolean-legacy | 0.187480 (0.180378-0.200136) | 0.171613 (0.164190-0.181044) | 0.180563 (0.175631-0.187807) | 0.211343 (0.200118-0.223612) |
| 118-int-text | 0.204004 (0.191636-0.216645) | 0.188930 (0.181423-0.206237) | 0.190948 (0.187601-0.198028) | 0.220120 (0.217568-0.234642) |
| 118-boolean-text | 0.168852 (0.162029-0.199493) | 0.168579 (0.161640-0.182174) | 0.174042 (0.168368-0.176570) | 0.207406 (0.197169-0.213397) |
| 118-decimal-truth | 0.186952 (0.181923-0.200563) | 0.178256 (0.173796-0.191289) | 0.195962 (0.187249-0.206562) | 0.224311 (0.213567-0.235870) |
| 118-decimal-predicate | 0.190382 (0.181827-0.206279) | 0.190054 (0.184949-0.242657) | 0.186770 (0.182483-0.198041) | 0.251478 (0.233013-0.253193) |
| 118-boolean-legacy | 0.175627 (0.167726-0.193887) | 0.174471 (0.164651-0.196198) | 0.174645 (0.161150-0.194922) | 0.215897 (0.210575-0.231338) |

Native/stock elapsed ranges from 0.329 to
0.998; 0 configurations
lose to paired stock. Native/previous elapsed is
0.342-0.858,
with 0 previous-path losses. Variability and independent
controls remain in the record. Two independent prior-JVM stock medians beat native
in DECIMAL predicate jobs, and several paired ranges overlap. No stable complete-job
gain is claimed for those near-parity cases. Function-level gains are not universal job gains.

190 fresh focused JVM invocations pass with no skips across both released
lines and JDK 11/17, targeting Java 11. They revalidate UPPER/LOWER and compare complete
5,003-row maps/schema for integer/BOOLEAN text, compact/noncompact DECIMAL truth/predicates,
NULLs, wrapping, both cast modes, four JVM locales, lazy failures and empty batches.
Positive routes require consumed NativeCalc work, no fallback and empty JVM evaluators.
The actual prior encoder compares every positive first-query map/schema before the intended
route assertion fails; later queries in those failing invocations are not counted.
Generated/consumer controls and same-name catalog UDFs pass. Four unchanged released
`testCharLength` stock-oracle invocations pass separately. Hosted suites still need the
exact pushed PR head; earlier per-function records keep their original provenance.

Use the existing release `integral_expression` example with, for example,
`SF_INTEGRAL_EXPRESSION='CHAR_LENGTH(UPPER(CAST(v AS STRING)))'`, then select the input
width/NULL/layout/batch and candidate or actual preceding encoder on its classpath.
`IntegralExpressionPipelineBenchmark` uses the same expression through `sf.integral.expression`;
previous controls set `sf.integral.previous=true`. Records retain exact hashes/settings.


## Primitive-text GREATEST {#primitive-text-greatest}

The built-in selects extrema of proven ASCII/NULL character literals, total verified
integer/BOOLEAN casts to unbounded STRING, qualified primitive ASCII PRINTF and
locale-independent UPPER/LOWER. Both released Flink lines retain their own types,
NULLs and cast modes; PRINTF applies only to 2.2. Ordinary source strings, bounded
casts, Unicode/unverified and fallible producers retain their prior routes. This
does not remove the source-representation gate tracked in
[#508](https://github.com/datafusion-contrib/StreamFusion/issues/508).

```sql
SELECT GREATEST(CAST(v AS STRING), TRY_CAST(n AS STRING)) FROM inputs;
```

The same output has identical UTF-8 byte and Java UTF-16 ordering under this producer
proof. A single typed selection writes the chosen slices once, with strict union
validity. Uniform winners reuse their immutable input offsets/values. Equivalent
BOOLEAN casts select the strict BOOLEAN extremum before formatting once; mixed
casing never uses that rewrite. Integer text is written through a reusable itoa
buffer, sharing input validity and preserving bounded truncation/CHAR padding.

Release qualification uses 348 fresh JVM invocations on Flink 1.18.1/2.2.1
and JDK 11/17 with zero skips, Java release 11, actual preceding-encoder controls,
and unchanged released GreatestLeastFunctionsITCase stock oracles. The rebuilt
native runtime passes all 919 Rust core tests, including independent sorting,
sliced NULL bitmaps, scalar/array/empty controls, buffer ownership and formatting
oracles. Admitted SQL consumes NativeCalc with empty evaluator descriptors. The
previous prepared input falls back for unproven string representation; its complete
job uses a generated evaluator because the external Row source proves Java-backed
inputs. These actual routes are asserted separately.

Prepared timing uses equivalent prebuilt Java Generic/Binary RowData and Arrow
inputs, both NULL modes, modern/legacy casts, BOOLEAN/all signed widths and three DECIMAL truth precisions, 1,024/16-row
batches, 131,072 rows per trial, 16 warmups and nine measurements. The native timer
runs inside Rust after import and before export, retaining production Calc,
formatting, allocations and output consumption. Only this prepared gate excludes
perimeter JNI/import/export and transposes.

All 108 bulk cells beat stock by 1.071-11.929x and actual previous by
1.096-12.888x. 26/27 tiny cells remain slower than stock
(stock/native 0.106-3.489x). No universal small-batch speedup is claimed.

Nine complete million-row jobs retain submission/startup, JNI and both row/Arrow
transposes, one thread, two warmups and five alternating measurements per engine.
Native/stock elapsed is 0.474-1.035;
native/previous is 0.443-0.888.
2/9 complete medians lose to the candidate-process stock control,
2/9 lose to the independent stock control, and
0/9 lose to previous. Ranges and unfavorable results are retained;
prepared admission is not a complete-pipeline acceleration claim.

| Flink/shape | Stock seconds | Native seconds | Independent stock seconds | Previous seconds |
|---|---:|---:|---:|---:|
| 22-int-pair | 0.252068 | 0.235734 | 0.259810 | 0.294924 |
| 22-bigint-literal | 0.224076 | 0.206773 | 0.225453 | 0.260644 |
| 22-boolean-pair | 0.227179 | 0.235179 | 0.223270 | 0.266883 |
| 22-boolean-integer | 0.250538 | 0.247003 | 0.251650 | 0.278111 |
| 22-printf | 0.695091 | 0.329240 | 0.699164 | 0.743174 |
| 118-int-pair | 0.257102 | 0.241080 | 0.255740 | 0.290259 |
| 118-bigint-literal | 0.250537 | 0.208740 | 0.231161 | 0.271153 |
| 118-boolean-pair | 0.225301 | 0.229217 | 0.223491 | 0.260773 |
| 118-boolean-integer | 0.228776 | 0.222973 | 0.236081 | 0.255155 |

The [machine-readable record](results/primitive-greatest-2026-11-05.json) and
[all 28,900 raw prepared samples](results/primitive-greatest-2026-11-05.csv.gz)
retain initial losing attempts, medians/ranges, actual routes, allocator bindings,
source/bytecode/runtime hashes and full-job trials. Initial process profiling found
intermediate comparison/zip arrays and generic integer formatting. A first builder
pass still lost bulk controls; direct output buffers, shared validity, BOOLEAN
selection before text and uniform-winner reuse remove that work. Exact-head hosted
CI is separate from this local qualification.



## Primitive-text LEAST {#primitive-text-least}

The built-in selects extrema of proven ASCII/NULL character literals, total verified
integer/BOOLEAN casts to unbounded STRING, qualified primitive ASCII PRINTF and
locale-independent UPPER/LOWER. Both released Flink lines retain their own types,
NULLs and cast modes; PRINTF applies only to 2.2. Ordinary source strings, bounded
casts, Unicode/unverified and fallible producers retain their prior routes. This
does not remove the source-representation gate tracked in
[#508](https://github.com/datafusion-contrib/StreamFusion/issues/508).

```sql
SELECT LEAST(CAST(v AS STRING), TRY_CAST(n AS STRING)) FROM inputs;
```

The same output has identical UTF-8 byte and Java UTF-16 ordering under this producer
proof. A single typed selection writes the chosen slices once, with strict union
validity. Uniform winners reuse their immutable input offsets/values. Equivalent
BOOLEAN casts select the strict BOOLEAN extremum before formatting once; mixed
casing never uses that rewrite. Integer text is written through a reusable itoa
buffer, sharing input validity and preserving bounded truncation/CHAR padding.

Release qualification uses 402 fresh JVM invocations on Flink 1.18.1/2.2.1
and JDK 11/17 with zero skips, Java release 11, actual preceding-encoder controls,
and unchanged released GreatestLeastFunctionsITCase stock oracles. The rebuilt
native runtime passes all 921 Rust core tests, including independent sorting,
sliced NULL bitmaps, scalar/array/empty controls, buffer ownership and formatting
oracles. Admitted SQL consumes NativeCalc with empty evaluator descriptors. The
previous prepared input falls back for unproven string representation; its complete
job uses a generated evaluator because the external Row source proves Java-backed
inputs. These actual routes are asserted separately.

Prepared timing uses equivalent prebuilt Java Generic/Binary RowData and Arrow
inputs, both NULL modes, modern/legacy casts, BOOLEAN/all signed widths and three DECIMAL truth precisions, 1,024/16-row
batches, 131,072 rows per trial, 16 warmups and nine measurements. The native timer
runs inside Rust after import and before export, retaining production Calc,
formatting, allocations and output consumption. Only this prepared gate excludes
perimeter JNI/import/export and transposes.

All 108 bulk cells beat stock by 1.263-14.862x and actual previous by
1.227-13.121x. 26/27 tiny cells remain slower than stock
(stock/native 0.179-3.378x). No universal small-batch speedup is claimed.

Nine complete million-row jobs retain submission/startup, JNI and both row/Arrow
transposes, one thread, two warmups and five alternating measurements per engine.
Native/stock elapsed is 0.416-1.096;
native/previous is 0.397-0.922.
2/9 complete medians lose to the candidate-process stock control,
2/9 lose to the independent stock control, and
0/9 lose to previous. Ranges and unfavorable results are retained;
prepared admission is not a complete-pipeline acceleration claim.

| Flink/shape | Stock seconds | Native seconds | Independent stock seconds | Previous seconds |
|---|---:|---:|---:|---:|
| 22-int-pair | 0.243379 | 0.232681 | 0.256746 | 0.300773 |
| 22-bigint-literal | 0.225372 | 0.207914 | 0.231243 | 0.264847 |
| 22-boolean-pair | 0.224270 | 0.230709 | 0.221438 | 0.268155 |
| 22-boolean-integer | 0.242662 | 0.238090 | 0.244949 | 0.292659 |
| 22-printf | 0.788043 | 0.327847 | 0.763934 | 0.824953 |
| 118-int-pair | 0.260317 | 0.241753 | 0.251691 | 0.291510 |
| 118-bigint-literal | 0.216809 | 0.210058 | 0.224630 | 0.265566 |
| 118-boolean-pair | 0.228650 | 0.250603 | 0.226020 | 0.271904 |
| 118-boolean-integer | 0.248352 | 0.243139 | 0.244889 | 0.295120 |

The [machine-readable record](results/primitive-least-2026-11-05.json) and
[all 17,100 raw prepared samples](results/primitive-least-2026-11-05.csv.gz)
retain development controls, medians/ranges, actual routes, allocator bindings,
source/bytecode/runtime hashes and full-job trials. The first eight-cell pilot lost its BOOLEAN pair to stock
(stock/native 0.921x) and actual previous (0.914x). A five-second native process sample
includes 1,243 zip frames; counts identify work, not elapsed-time percentages. The new
typed Boolean extremum uses OR/AND over packed value bitmaps with strict union validity,
reusing identities and avoiding generic comparison/zip arrays. It reuses the preceding
string selection and integer formatting kernels. Both extrema rerun after this shared change. Exact-head hosted
CI is separate from this local qualification.


### Shared Boolean kernel controls {#shared-boolean-kernel-controls}

A separate GREATEST replay isolates the shared bitmap change on an already JVM-free
path. Both the actual preceding encoder/runtime and candidate require native GREATEST
with empty evaluators; the old route is not relabeled as fallback. Encoder/fixture
bytecodes match the independently qualified source records. Sixteen additional
prepared bulk controls across both released lines, both NULL modes, Generic/Binary
rows and modern/legacy casts beat stock 2.520-13.333x and actual previous 1.326-2.455x.
All four 16-row controls still lose to stock (0.190-0.504x); their previous/native
ratios are 1.711-1.838x. They remain prepared controls, not new complete-job timings.
The same machine-readable record and raw CSV retain all 2,000 additional samples.



## Byte-preserving CONCAT {#byte-preserving-concat}

Measured on Apple M4 Pro, macOS, JDK 17.0.12 and released Flink 2.2.1 on 2026-11-10, based on
main `87d6edbf`. Cargo uses release, the system allocator and `--no-default-features`; no state
backend is involved. Separate correctness tests use JDK 11.0.27 with released Flink 1.18.1 and
2.2.1, comparing raw bytes and actual native consumption.

The `byte_string_concat` example runs the production compiled Calc for `CONCAT(s, '!')` on
prebuilt Arrow buffers. Stock Flink uses its generated expression evaluator on prebuilt byte-backed
RowData. Allocation, concatenation and output consumption remain timed; native timing is inside
Rust. Input preparation, initial JNI entry and perimeter transposes are excluded. Both engines
consume lengths and first/last bytes and must produce matching checksums. The Utf8 control uses
the existing DataFusion-backed CONCAT path in the same binary. Invalid input cannot be represented
as valid Arrow Utf8, so that control is omitted for malformed data.

Each trial processes 4096 rows 256 times (1,048,576 rows), with 12 warmups and nine retained trials.
Engine order rotates. Entries are median milliseconds, with min-max in parentheses.

| Input | Stock Flink | Existing Utf8 | Binary |
|---|---:|---:|---:|
| 1-byte ASCII | 36.075 (35.485-36.545) | 7.591 (7.162-7.701) | 5.529 (5.300-5.654) |
| 16-byte ASCII | 33.640 (31.892-34.203) | 8.972 (8.489-9.942) | 6.549 (6.075-7.270) |
| 64-byte ASCII | 40.182 (38.457-42.355) | 9.725 (9.286-10.451) | 6.160 (5.781-6.864) |
| 16 three-byte characters | 36.667 (35.634-39.760) | 32.145 (30.914-36.918) | 7.454 (7.318-7.671) |
| 16-byte ASCII, every seventh row NULL | 31.496 (30.931-33.900) | 6.268 (5.939-6.388) | 6.301 (6.178-6.434) |
| 16 bytes, first byte FF every 1000 rows | 32.633 (32.019-32.994) | not applicable | 6.297 (6.130-6.418) |
| 16 bytes, first byte FF in every row | 32.433 (31.687-34.300) | not applicable | 6.339 (6.275-6.727) |

The Binary path needs no UTF-8 decode, validation or JVM fallback. These prepared cases exceed
stock throughput; nullable input is effectively tied with the existing native path. These results
do not establish that other string functions or query shapes improve.

`ByteStringConcatPipelineBenchmark` separately retains a row source, both transposes and a blackhole
row sink, and asserts its Binary plan and positive native consumption. It runs two million rows at
parallelism one, each containing 16 ASCII characters plus a runtime suffix from 0 to 255. Timing
includes query execution through completion. There are three warmups and five retained trials,
alternating engines, with an 8 GiB Java heap, Arrow unsafe access enabled and Arrow null checks
disabled.

| Complete-job cohort | Flink seconds, median (min-max) | Native seconds, median (min-max) |
|---|---:|---:|
| New Binary boundary | 0.451 (0.449-0.456) | 0.279 (0.277-0.286) |
| Prior Java boundary / existing Utf8 kernel | 0.440 (0.431-0.445) | 0.283 (0.280-0.304) |

The boundary control compiles the changed Java production classes from `87d6edbf`, places them
before the new classes and runs the same fixture with an asserted Utf8 plan. It uses the existing
Utf8 kernel from the same native library, not a separately rebuilt historical native library.
The new job is about 1.62x stock Flink in its cohort. Native times against the prior boundary
control are effectively tied; their small difference is within observed variability. The first
cold job took 1.313 s for Flink and 0.469 s for Binary native. These short complete jobs remain
sensitive to startup/JIT and do not establish long-running production throughput.

Use JDK 17 for the standalone example on macOS: the embedded-JVM driver did not successfully
start the Flink baseline with JDK 11 on the measured host; those failed runs are excluded.
Compile Java test classes and obtain the test dependency classpath with Maven, then set
`SF_NATIVE_BENCH_CLASSPATH` to the test classes, production classes and dependency classpath.
From `native/`, run:

```sh
cargo run -p streamfusion --release --no-default-features --example byte_string_concat -- 16 false false 0
```

Arguments are character repetitions, multibyte input, nullable input and the malformed-row interval
(zero disables malformed input). For the complete job, first build the release JNI library and run:

```sh
SF_BENCHMARK=true mvn -Pbench -pl streamfusion-runtime -am test \
  -Dnative.build.skip=true -Dtest=ByteStringConcatPipelineBenchmark \
  -Dsurefire.failIfNoSpecifiedTests=false
```

The [coverage contract](../operators/calc-filter.md#byte-preserving-string-input) lists the limited
plan shape admitted by these measurements; broader representation work remains in
[#622](https://github.com/datafusion-contrib/StreamFusion/issues/622).

### Byte-backed strings through a streaming watermark {#byte-backed-strings-through-a-streaming-watermark}

`ByteStringConcatPipelineBenchmark` also measures a streaming watermark followed by either a
numeric filter that forwards the original STRING or a filter and `CONCAT(s, '!')`. Both queries
include a RowData source and a blackhole row sink. The input has two million 16-byte strings,
with an invalid first byte every 1,000 rows, and the numeric predicate retains half the rows.
StreamFusion's route includes both transposes, native watermark assignment, and the native
filter or Calc. The benchmark asserts the Binary route and positive native consumption.
The same queries used stock execution under the final-Calc-only ingress policy in #673.

These release runs used Flink 2.2.1, JDK 17.0.17, the system allocator and parallelism one on an
Apple M4. Each cohort alternated engines for three warmup pairs and five measured pairs.
The times are complete-job seconds, reported as median (min–max).

| Query after watermark assignment | Flink | StreamFusion |
|---|---:|---:|
| Numeric filter and passthrough | 0.484 (0.480–0.487) | 0.385 (0.378–0.394) |
| Numeric filter and CONCAT | 0.528 (0.522–0.530) | 0.346 (0.340–0.351) |

### Byte-preserving CONCAT_WS {#byte-preserving-concat_ws}

The same prepared-input fixture also compares `CONCAT_WS(',', s, '!')` and a runtime separator
`CONCAT_WS(s, 'x', '!')`. Measurements on the same host/toolchain use 4096 rows, 256 iterations,
12 warmups and nine retained trials, with rotating engine order and matching output checksums.
The existing Utf8 implementation is unchanged and runs in the same release binary. Times are
median milliseconds (min-max); these exclude perimeter transposes and do not establish whole-job
throughput. Both released Flink lines also pass raw-byte SQL and native-execution regression cases.

| CONCAT_WS input | Stock Flink | Existing Utf8 | Binary |
|---|---:|---:|---:|
| 16-byte ASCII | 37.161 (36.942-37.867) | 16.486 (15.355-16.839) | 12.068 (11.869-13.077) |
| 16 three-byte characters | 42.157 (40.786-47.199) | 40.005 (39.559-41.089) | 13.520 (13.028-14.259) |
| 16-byte ASCII, every seventh value NULL | 39.416 (38.966-40.679) | 15.658 (15.194-16.146) | 12.960 (12.747-14.049) |
| 16 bytes, first byte FF in every row | 37.272 (36.668-37.702) | not applicable | 12.118 (12.085-12.965) |
| 16-byte runtime separator, every seventh separator NULL | 35.641 (35.034-37.462) | 15.376 (15.195-16.228) | 13.728 (13.313-13.972) |
| 64-byte ASCII | 42.808 (42.203-48.289) | 16.163 (15.575-16.937) | 12.156 (11.759-13.153) |

Append `concat_ws` or `concat_ws_separator` as the example's optional fifth argument; the default
remains `concat`. For example, with the released Flink test classpath configured as above:

```sh
cargo run -p streamfusion --release --no-default-features --example byte_string_concat -- 16 false true 0 concat_ws
```

### Byte-preserving digest functions {#byte-preserving-digest-functions}

Digest measurements use the same prepared-input methodology, release build, host and JDK 17 /
released Flink 2.2.1 baseline. The Utf8 control uses the shared digest implementation in the same
library; it is a physical-input control, not a separately rebuilt historical native library.
Only Binary carries malformed inputs. Both native paths hash slices directly and write ASCII hex;
the Binary change does not require a faster digest algorithm or add a text decode. Times are
median milliseconds (min-max), with 12 warmups and nine retained trials over 1,048,576 rows.
These measurements exclude perimeter conversions and are not complete-job throughput claims.

| Function / input | Stock Flink | Utf8 control | Binary |
|---|---:|---:|---:|
| MD5 / 16-byte ASCII | 142.125 (140.288-144.328) | 100.219 (99.414-102.289) | 99.744 (98.823-102.406) |
| MD5 / 64-byte ASCII | 214.757 (213.239-218.880) | 158.851 (157.379-160.303) | 158.983 (157.764-163.708) |
| MD5 / 16 three-byte characters, every seventh row NULL | 124.280 (122.569-139.306) | 80.385 (79.720-83.036) | 80.928 (79.789-87.255) |
| MD5 / 16 bytes, first byte FF every 1000 rows | 140.061 (138.178-140.446) | not applicable | 99.184 (98.692-100.480) |
| SHA1 / 16-byte ASCII | 227.107 (217.566-232.026) | 51.824 (51.021-57.554) | 51.780 (50.584-58.086) |
| SHA1 / 64-byte ASCII | 402.564 (399.945-403.784) | 66.687 (65.993-67.618) | 66.897 (66.328-67.864) |
| SHA1 / 16 three-byte characters, every seventh row NULL | 226.049 (224.862-226.681) | 43.298 (42.891-43.784) | 43.310 (43.020-43.898) |
| SHA1 / 16 bytes, first byte FF every 1000 rows | 216.588 (215.762-222.181) | not applicable | 51.453 (50.733-55.020) |
| SHA224 / 16-byte ASCII | 277.532 (273.514-281.558) | 65.747 (64.647-67.995) | 65.027 (59.379-68.244) |
| SHA224 / 64-byte ASCII | 454.228 (450.826-483.856) | 80.992 (79.750-84.927) | 81.129 (79.260-84.351) |
| SHA224 / 16 three-byte characters, every seventh row NULL | 250.720 (247.997-252.590) | 52.705 (52.191-52.884) | 52.535 (51.946-52.896) |
| SHA224 / 16 bytes, first byte FF every 1000 rows | 272.086 (271.529-272.948) | not applicable | 61.953 (60.624-65.954) |
| SHA256 / 16-byte ASCII | 269.703 (266.459-279.051) | 70.883 (68.966-72.328) | 70.896 (68.735-80.806) |
| SHA256 / 64-byte ASCII | 450.879 (446.939-476.584) | 88.444 (87.225-89.654) | 87.916 (86.227-89.757) |
| SHA256 / 16 three-byte characters, every seventh row NULL | 238.066 (237.045-240.322) | 55.380 (54.881-55.740) | 55.315 (54.906-55.663) |
| SHA256 / 16 bytes, first byte FF every 1000 rows | 281.690 (273.910-298.308) | not applicable | 72.929 (70.544-78.957) |
| SHA384 / 16-byte ASCII | 345.439 (343.361-356.431) | 128.910 (128.231-144.637) | 128.981 (127.749-136.228) |
| SHA384 / 64-byte ASCII | 349.841 (345.483-355.210) | 137.260 (135.284-141.566) | 137.031 (135.481-139.822) |
| SHA384 / 16 three-byte characters, every seventh row NULL | 301.758 (298.975-339.238) | 112.866 (111.615-113.385) | 113.615 (111.772-120.467) |
| SHA384 / 16 bytes, first byte FF every 1000 rows | 344.420 (335.709-352.626) | not applicable | 130.765 (126.323-133.883) |
| SHA512 / 16-byte ASCII | 359.354 (356.755-366.973) | 144.464 (143.812-147.107) | 144.679 (143.481-147.590) |
| SHA512 / 64-byte ASCII | 363.800 (360.321-368.657) | 147.665 (147.099-153.321) | 148.605 (146.798-154.250) |
| SHA512 / 16 three-byte characters, every seventh row NULL | 330.875 (327.549-340.789) | 126.535 (124.588-138.229) | 126.188 (124.534-139.890) |
| SHA512 / 16 bytes, first byte FF every 1000 rows | 363.619 (360.899-383.855) | not applicable | 146.437 (143.182-147.750) |

Select the function using the example's optional fifth argument, for example:

```sh
cargo run -p streamfusion --release --no-default-features --example byte_string_concat -- 16 false false 0 md5
```

### Byte-preserving TO_BASE64 {#byte-preserving-to_base64}

The same release prepared-input fixture uses Flink 2.2.1 and JDK 17, prebuilt RowData versus
Arrow, 4096 rows per batch, 256 iterations, 12 warmups and nine measured trials. Engine order
rotates and every trial checks matching output consumption. Times are median milliseconds
(min-max); no perimeter transposes are timed. These are not complete-job throughput claims.

| Input | Stock Flink | Existing Utf8 | Binary |
|---|---:|---:|---:|
| 16-byte ASCII | 57.639 (57.301-63.955) | 5.933 (5.885-6.279) | 5.902 (5.847-6.453) |
| 64-byte ASCII | 80.099 (79.674-80.862) | 10.592 (10.411-10.686) | 10.623 (10.318-10.712) |
| 16 three-byte characters, every seventh row NULL | 59.131 (58.780-60.191) | 8.668 (8.622-8.784) | 8.677 (8.560-8.745) |
| 16 bytes, first byte FF every 1000 rows | 57.025 (56.669-57.436) | not applicable | 5.921 (5.765-5.956) |
| Empty strings, every seventh row NULL | 30.259 (30.032-30.680) | 4.498 (4.469-4.697) | 4.471 (4.386-4.519) |

Both physical input paths reuse the existing SIMD Base64 encoder. Their measured times are
effectively tied; admitting arbitrary STRING bytes requires no new encoding algorithm.
Select `to_base64` as the example's fifth argument.

### Byte-preserving ASCII {#byte-preserving-ascii}

The same release prepared-input fixture uses Flink 2.2.1 and JDK 17, prebuilt RowData versus
Arrow, 4096 rows per batch, 256 iterations, 12 warmups and nine measured trials. Engine order
rotates and every trial checks matching output consumption. Times are median milliseconds
(min-max); no perimeter transposes are timed. These are not complete-job throughput claims.

| Input | Stock Flink | Existing Utf8 | Binary |
|---|---:|---:|---:|
| 16-byte ASCII | 15.901 (14.275-16.055) | 0.729 (0.726-0.732) | 0.739 (0.720-0.769) |
| 64-byte ASCII | 18.799 (18.568-19.159) | 0.878 (0.865-0.890) | 0.899 (0.879-0.917) |
| 16 three-byte characters, every seventh row NULL | 17.877 (15.132-18.371) | 1.150 (1.137-1.167) | 1.163 (1.131-1.173) |
| 16 bytes, first byte FF every 1000 rows | 15.791 (13.240-16.667) | not applicable | 0.739 (0.725-0.800) |
| Empty strings, every seventh row NULL | 13.917 (12.392-14.200) | 1.106 (1.089-1.127) | 1.109 (1.065-1.113) |

ASCII reads the first byte as a signed Java byte and shares the input validity mask with its
integer result. Its small difference from the existing Utf8 path is within or near observed
variability; both outperform the measured Flink control. Select `ascii` as the fifth argument.

### Literal-width SHA2 over original bytes {#literal-width-sha2-over-original-bytes}

Legal literal SHA2 widths lower to the corresponding fixed digest kernel. The prior generic
native SHA2 control uses its existing opcode with the same literal width and Utf8 input; this
isolates expression execution and excludes its planner-side JVM binding and string-cache setup.
It is not a rebuilt historical binary or a whole-job comparison. Binary and Utf8 fixed-kernel
controls use the same release library. Methodology, host, data sizes, rotating order and warmups
match the prepared digest measurements above. Times are median milliseconds (min-max).

| Function / input | Stock Flink | Previous generic native | Fixed Utf8 | Fixed Binary |
|---|---:|---:|---:|---:|
| SHA2(s, 224) / 16-byte ASCII | 271.744 (268.600-293.026) | 65.465 (63.105-68.147) | 63.642 (60.904-67.334) | 62.735 (59.663-68.498) |
| SHA2(s, 256) / 16-byte ASCII | 273.117 (269.786-274.151) | 72.315 (69.879-72.941) | 70.035 (69.422-71.931) | 70.456 (69.902-71.741) |
| SHA2(s, 384) / 16-byte ASCII | 346.470 (344.195-355.980) | 129.831 (127.923-146.387) | 130.295 (127.954-135.686) | 128.494 (123.834-133.306) |
| SHA2(s, 512) / 16-byte ASCII | 363.501 (362.569-380.429) | 148.684 (148.249-153.441) | 146.882 (145.765-148.326) | 148.269 (145.622-148.966) |
| SHA2(s, 256) / 64-byte ASCII | 448.885 (443.249-474.302) | 89.023 (87.767-91.299) | 87.690 (86.314-89.025) | 88.449 (86.888-89.513) |
| SHA2(s, 256) / 16 three-byte characters, every seventh row NULL | 240.275 (238.613-252.558) | 58.100 (57.652-61.340) | 55.630 (54.911-57.897) | 55.381 (54.787-56.131) |
| SHA2(s, 256) / 16 bytes, first byte FF every 1000 rows | 273.397 (272.490-273.798) | not applicable | not applicable | 73.244 (70.468-74.635) |

All measured Binary cases exceed the Flink control. Differences from the previous native path
are small, with overlapping ranges in most cases; the change establishes byte preservation
without a material measured execution regression. Select `sha2_224`, `sha2_256`, `sha2_384` or
`sha2_512` as the example's fifth argument. Runtime, NULL and illegal widths keep their prior
routing; SQL regressions cover both row-evaluation and initialization failures.

## On-demand UTF-8 validation {#on-demand-utf-8-validation}

The prepared-input comparison uses 1,024-row batches of 29, 197, and 773 bytes per
non-null value. Inputs contain ASCII, three-byte Unicode characters, or Unicode with
the first byte changed to `FF` on two rows per batch. Every 31st value is NULL.
`CHAR_LENGTH` counts code points and `POSITION('match' IN s)` searches for the five-byte
suffix present on half the rows. The Java driver times released Flink over prepared
GenericRowData and BinaryRowData. The previous StreamFusion planner used the same Flink
expressions for these unverified inputs. The Rust driver times production Calc over
equivalent Arrow Binary batches, including validation and result consumption. These measurements
exclude initial JNI entry, Arrow export and transposes.

The release run used Apple M4, Flink 2.2.1, JDK 17.0.17, Rust 1.95 and mimalloc.
Each trial consumes 262,144 results, with five warmups and seven measured trials per
configuration. The two stock controls and production Calc returned matching checksums
in all 18 configurations. Median nanoseconds per row for mixed batches, with the
observed minimum and maximum in parentheses:

| Function | Bytes | Flink GenericRowData | Flink BinaryRowData | StreamFusion Binary |
| --- | ---: | ---: | ---: | ---: |
| CHAR_LENGTH | 29 | 28.02 (19.01–29.64) | 29.04 (28.27–37.56) | 23.00 (22.88–23.17) |
| CHAR_LENGTH | 197 | 81.79 (81.40–86.47) | 69.67 (68.52–84.49) | 33.77 (33.61–34.11) |
| CHAR_LENGTH | 773 | 221.09 (220.77–221.66) | 220.33 (211.73–222.48) | 133.10 (132.70–133.56) |
| POSITION | 29 | 36.65 (35.87–37.49) | 36.81 (34.97–42.13) | 35.53 (35.41–35.63) |
| POSITION | 197 | 138.85 (137.50–142.09) | 139.74 (135.55–144.08) | 50.06 (49.79–50.27) |
| POSITION | 773 | 470.95 (468.23–472.25) | 465.31 (464.13–467.89) | 145.72 (145.59–146.20) |

The 29-byte mixed cases have overlapping ranges with at least one stock control.
For all-valid ASCII and Unicode, median stock/native ratios were 2.70–10.65 for
CHAR_LENGTH and 1.32–22.02 for POSITION across widths and stock row representations.
These prepared timings measure expression execution on matching inputs.

Run both drivers on the same idle host. Their checksums should agree for each input.

```sh
cargo build --manifest-path native/Cargo.toml --locked --release --features mimalloc \
  -p streamfusion --example byte_string_text
native/target/release/examples/byte_string_text 256 5 7
SF_BENCHMARK=true mvn -pl streamfusion-runtime -am test -Pbench,benchmarks \
  -Dtest=CertifiedScalarBenchmark#byteBackedText -Dsurefire.failIfNoSpecifiedTests=false
```

### Conversion-inclusive streaming text

A separate release job ran `REPLACE(s, 'a', 'b')` after a streaming watermark and
numeric filter. It uses Flink 2.2.1, JDK 17.0.17 and the Apple M4 host above. The row
source emits 500,000 192-byte strings with `FF` in the first byte every 1,000 rows.
The filter keeps half of the rows, and the job retains both row/Arrow conversions
and the blackhole row sink. Native metrics confirm rows ran through both the
existing Calc and the generated Flink evaluator. The previous planner uses stock Flink
for this job.

Two warmups and five interleaved trials gave 0.244 s (0.240–0.248 s) for Flink and
0.182 s (0.177–0.185 s) for StreamFusion. This job includes startup and establishes
the result for this workload and malformed-row distribution.

```sh
SF_BENCHMARK=true mvn -pl streamfusion-runtime -am test -Pbench,benchmarks \
  -Dtest=ByteStringConcatPipelineBenchmark#mixedTextThroughWatermarkToRowSink \
  -Dsurefire.failIfNoSpecifiedTests=false -Dbyte.concat.rows=500000 \
  -Dbyte.concat.width=192 -Dbyte.concat.warmups=2 -Dbyte.concat.runs=5
```
