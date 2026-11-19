# Recorded scalar workloads: decimal

[Topic index and original links](scalar-functions.md). Sections retain their recorded scope, dates and evidence.

## DECIMAL UDF consumer diagnostic (2026-09-16) {#decimal-udf-consumer-diagnostic-2026-09-16}

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


## Exact DECIMAL coverage diagnostic (2026-09-16) {#exact-decimal-coverage-diagnostic-2026-09-16}

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


## DECIMAL ABS {#decimal-abs}

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


## DECIMAL unary minus {#decimal-unary-minus}

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



## DECIMAL SIGN {#decimal-sign}

The typed Decimal128 kernel replaces the scalar generated callback for verified direct/literal,
same-scale lossless-widening and nested SIGN inputs. Ordinary `p > s` returns scaled -1/0/1;
noncompact `p = s` returns NULL for nonzero values and scaled zero otherwise. Compact `p = s`
remains generated because its out-of-precision internal value has consumer-specific behavior.
DECIMAL CASE/IF containing that compact intermediate retains the released branch conversion.
This bounded implementation does not complete [#491](https://github.com/datafusion-contrib/StreamFusion/issues/491).

### Prepared operation {#prepared-operation}

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

### Complete row-source job {#complete-row-source-job}

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

### Validation and reproduction {#validation-and-reproduction}

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


## DECIMAL to STRING {#decimal-to-string}

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


## DECIMAL to STRING TRY_CAST {#decimal-to-string-try_cast}

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


## Numeric IS_DECIMAL {#numeric-is_decimal}

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


## Bounded DECIMAL CAST to character {#bounded-decimal-cast-to-character}

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


## Bounded DECIMAL TRY_CAST to character {#bounded-decimal-try_cast-to-character}

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


## DECIMAL unary CAST to character {#decimal-unary-cast-to-character}

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


## DECIMAL unary TRY_CAST to character {#decimal-unary-try_cast-to-character}

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


## Nullable DECIMAL arithmetic CAST to character {#nullable-decimal-arithmetic-cast-to-character}

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


## DECIMAL arithmetic TRY_CAST to character {#decimal-arithmetic-try_cast-to-character}

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


## DECIMAL ABS over arithmetic {#decimal-abs-over-arithmetic}

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


## DECIMAL negation over arithmetic {#decimal-negation-over-arithmetic}

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


## DECIMAL SIGN over arithmetic {#decimal-sign-over-arithmetic}

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


## Mixed exact GREATEST {#mixed-exact-greatest}

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


## Mixed exact LEAST {#mixed-exact-least}

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



## Mixed exact IF {#mixed-exact-if}

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


## DECIMAL from proven integer text {#decimal-from-proven-integer-text}

A genuine ordinary-Calc `TRY_CAST(CAST(n AS STRING) AS DECIMAL(p,s))` can use the
existing exact numeric kernel when the inner CAST/TRY_CAST formats a verified total
signed integer producer and `p-s` covers the complete resolved width: 3/5/10/19 digits
for TINYINT/SMALLINT/INT/BIGINT. Integer text is ASCII with no extra leading zeros,
point or exponent, so both released parser branches give exactly integer times
10^scale, without overflow. This removes intermediate formatting, parsing and a JVM
evaluator. It preserves NULLs, resolved result schema/scale, original integer width,
modern/legacy behavior, lazy consumers and empty batches. Ordinary CAST roundtrips
that Flink already simplifies to numeric casts remain existing controls.

Source STRING, bounded CHAR/VARCHAR, insufficient capacity, fallible/stateful or
unverified producers and UNNEST retain their preceding routes. This is not a generic
STRING parser: the released compact parser has precision/byte-length and malformed-text
behavior that differs from ordinary decimal parsing.

After rebase onto canonical main `9954a19c`, the complete core is rebuilt
against released Flink 1.18.1/2.2.1, targeting Java 11. All 132 focused
JVM invocations pass on JDK 11/17, with full 5,003-row maps/schema at all four signed
widths, scales 0/3/38-widthDigits, both NULL modes, modern/legacy modes, lazy branches
and empty batches. Admitted routes require consumed NativeCalc, no fallback and empty
evaluators. Each actual-before release/JDK run compares the complete first-query
map/schema before eight expected pure-route failures; two retained controls pass.
Later unreached queries are not preceding-parity evidence.

An independent released utility oracle matches 243,968 cases across every fitting
target precision/scale and all signed widths on both release/JDK pairs. Four unchanged
released `CastRulesTest#castTests` runs pass; these are stock-only tests, separate from
native SQL qualification. An initial local 1.18 oracle run lacked its released
`TableAssertions` test dependency; that classpath failure is excluded. Native runtime
sources/artifacts do not change. The preceding 925 Rust core tests are reused with
their original runtime hashes, rather than described as a fresh run.

Release prepared measurements use JDK 17, equivalent Generic/Binary RowData and Arrow,
all four signed widths at three target scales, both NULL modes and signed extremes.
Both engines evaluate the same TRY_CAST roundtrip compared with decimal zero. Each
trial processes 131,072 rows, with 16 warmups and nine measurements. The native timer
runs inside Rust after setup/import and before export, retaining production Calc,
allocation, exact conversion, comparison and equivalent output consumption. Only this
small-function gate excludes perimeter JNI/C Data entry and transposes. Actual previous
behavior retains its generated evaluator inside NativeCalc, not whole-Calc fallback.
Both processes retain independent stock controls; process order reverses on 1.18.

All 96 prepared bulk cells beat stock **6.574-23.579x**
and actual previous behavior **12.440-35.978x**.
6/24 tiny controls lose to stock
(stock/native 0.811-1.795x), and
0/24 lose to previous. All losses remain.

Six complete million-row configurations retain startup/submission, JNI and both
row/Arrow transposes, with two warmups and five alternating trials per engine.
Native/stock elapsed is 0.679-0.891,
native/previous 0.576-0.736.
0/6 medians lose to paired stock,
0/6 to independent stock and
0/6 to previous. Prepared gains do not establish
universal complete-job acceleration.

| Flink/target | Stock seconds | Native seconds | Independent stock seconds | Previous seconds |
|---|---:|---:|---:|---:|
| 22-int-p13s3 | 0.194477 | 0.173282 | 0.195940 | 0.235402 |
| 22-int-p38s28 | 0.235606 | 0.173231 | 0.226662 | 0.265245 |
| 22-bigint-p38s19 | 0.258878 | 0.175869 | 0.240012 | 0.305235 |
| 118-int-p13s3 | 0.203184 | 0.176220 | 0.198230 | 0.241878 |
| 118-int-p38s28 | 0.223859 | 0.170982 | 0.225316 | 0.271301 |
| 118-bigint-p38s19 | 0.254095 | 0.182445 | 0.254451 | 0.294057 |

The [machine-readable qualification](results/integer-text-decimal-2026-11-05.json) and
[12,850 raw prepared samples](results/integer-text-decimal-2026-11-05.csv.gz) retain
full trials, independent controls, source/encoder/fixture/core bytecode and release
artifact hashes. The 800 pilot samples retain their pre-rebase provenance. Fifty
unpaired ordinary-CAST probe samples are an already-native control, not a new-feature
speedup. Hosted CI must qualify the exact final PR head separately.



## DECIMAL ABS over negative producers (2026-11-05) {#decimal-abs-over-negative-producers-2026-11-05}

Verified DECIMAL ABS now consumes unary-minus trees and same-scale widening, including
`ABS(-d)`, `ABS(CAST(-d AS DECIMAL(38,s)))` and negative subtrees of the existing
DECIMAL-only arithmetic proof. Leading verified negations are omitted rather than
materializing an intermediate array. Released Flink's decimal ABS and negation retain
precision and scale, and every valid SQL DECIMAL coefficient has an exact negation.
The existing typed ABS kernel retains arithmetic overflow NULLs and constraint enforcement.
No runtime opcode, JNI boundary, configuration switch or production Rust change is added.
Negation over ABS, narrowing, scale changes, fallible/unverified inputs and arithmetic
epoch producers retain their generated paths.

All **124 fresh focused JVM invocations** pass on released Flink 1.18.1/2.2.1 and
JDK 11/17, targeting Java 11. These include **52 new SQL invocations** over ten
precision/scale pairs, nullable/NOT NULL inputs, strict/legacy modes, complete 5,003-row
maps/schema, consumers, overflow, lazy/all-filtered paths and catalog identity. Pure
admitted routes consume NativeCalc with no fallback and empty JVM evaluators. Another
**138 existing ABS/negation/CAST regression invocations** pass on both released lines/JDK17,
including encoding checks across every DECIMAL precision/scale. Four unchanged released
`ScalarFunctionsTest.testAbs` stock-oracle invocations pass independently. Nothing in this
selection is skipped or aborted. Actual preceding encoder `a23b3e3b4` matches the first
query's values/schema, then fails ten expected new route assertions per released-line
JDK17 control; three retained controls pass. Later unreached queries are not prior-parity evidence.
The unchanged runtime retains the preceding calendar repair's 927-core-test qualification;
this is not reported as a fresh Rust run.

Prepared release measurements use `(ABS(...)>0 OR CAST(NULL AS BOOLEAN))` so both
engines consume the same nullable BOOLEAN output through the existing checksum fixture.
Stock starts on equivalent prebuilt Generic/Binary RowData; native starts on prebuilt
Arrow. The timer is inside Rust after input setup/import and before output export;
intrinsic allocations, conversions and prior JVM callbacks remain timed. Five producer/type
shapes cover DECIMAL(18,3), (38,18), (38,38), widening from (19,0), and negative arithmetic.
Each runs nullable/nonnullable and Generic/Binary controls with 1,024-row batches and an
additional 16-row nullable/Binary control on each released line. Each trial processes
131,072 rows, with sixteen warmups and nine measured repetitions. No other tests or
benchmarks run concurrently. The actual previous encoder uses the same unchanged release
runtime and fixture; independently measured stock controls remain in the raw data.

All **40 bulk cells** beat stock by **1.460-20.950x** and previous by **5.485-28.144x**.
All **10 tiny controls lose to stock**, with stock/native **0.094-0.481x**. These are
bounded prepared-function results, not a universal job-speedup claim.

Six complete million-row jobs output DECIMAL directly, retaining submission/startup,
production JNI and both RowData/Arrow transposes. Each engine has two warmups and five
measured repetitions at parallelism one. Five of six native medians lose to paired stock;
the remaining 0.18% difference is near parity and does not establish a stable gain.
Native/stock elapsed is **0.998-1.222**, native/previous **0.771-0.890**. All six beat the
preceding callback route, but row-source boundary costs remain material. Complete-job
ranges, independent stock controls and all slower results are retained.

See the [qualification and exact source/runtime provenance](results/decimal-abs-negate-2026-11-05.json)
and [5,000 raw prepared samples](results/decimal-abs-negate-2026-11-05.csv.gz).
The broader exact numeric scope remains tracked in
[#491](https://github.com/datafusion-contrib/StreamFusion/issues/491).


## DECIMAL negation over absolute values (2026-11-05) {#decimal-negation-over-absolute-values-2026-11-05}

Verified DECIMAL unary minus now consumes ABS and alternating trees of both operators,
including `-ABS(d)`, `ABS(-ABS(d))` and same-scale widening. The existing decimal-only
arithmetic proof admits the same verified unary subtrees. Released ABS and negation keep
precision and scale; every valid coefficient has an exact signed counterpart. The
existing native kernels preserve NULLs, resolved intermediate types and arithmetic
overflow. No production Rust/JNI change or performance switch is added. SIGN's separate
representation proof, narrowing, rescaling and fallible/unverified producers remain unchanged.
Total unary/widened character and epoch consumers also stay native; arithmetic epochs
and unverified arithmetic character consumers keep their previous routes.

All **212 focused/related JVM invocations** pass across released Flink 1.18.1/2.2.1 and
JDK 11/17, targeting Java 11. Another **12 consumer-control invocations** pass on the
same four combinations. These contain 64 new SQL invocations and 160 related invocations,
with no skipped or aborted cases. Complete 5,003-row maps/schema cover ten precision/scale
pairs, strict/legacy modes, independent NULLs, NOT NULL inputs, alternating trees,
character/Boolean/CASE/epoch consumers and all-filtered batches. Positive pure paths consume
NativeCalc with no fallback and empty JVM evaluators. Direct overflow keeps ERROR/DROP
constraint enforcement; final NOT NULL defaults and predicate NULL flags are checked separately.
`TRY_CAST(-ABS(d*d) AS CHAR(48))` explicitly retains its existing fallback and matches stock;
this qualification does not silently extend character admission over arithmetic roots.
Twelve unchanged released ABS and numeric/decimal unary stock-oracle invocations pass.
Actual preceding encoder `6454ff64a` matches each first query's complete values/schema,
then fails ten expected new route assertions per released-line JDK17 control; three
retained controls pass. Unreached later queries are not prior-parity evidence.
The unchanged release runtime retains its preceding 927-core-test qualification, not a
new Rust run. Focused and final performance encoder instructions match on both released lines.

Prepared release measurements use `(-ABS(...)<0 OR CAST(NULL AS BOOLEAN))` through the
existing checksum fixture. Six shapes cover DECIMAL(18,3), (38,18), (38,38), widening
from (19,0), arithmetic and alternating (19,19) trees. Each uses nullable/nonnullable and
Generic/Binary RowData controls, 1,024-row batches and a separate 16-row nullable/Binary
control, on both released lines. Stock starts on equivalent prebuilt RowData; native
starts on prebuilt Arrow. The Rust timer excludes initial input preparation/import and
final export, while retaining allocations, conversions and prior JVM callbacks intrinsic
to the expression. Each trial processes 131,072 rows, with sixteen warmups and nine
measurements. Actual preceding and candidate encoders share the same release runtime
and fixture. Independent stock controls, warmups and all variability remain in the records;
no other tests or benchmarks run concurrently.

All **48 bulk cells** beat stock by **1.562-13.880x** and previous by **4.686-21.670x**.
All **12 tiny controls lose to stock**, with stock/native **0.114-0.484x**. Six complete
million-row jobs output DECIMAL directly, retaining submission/startup, production JNI
and both RowData/Arrow transposes. At parallelism one, each engine has two warmups and
five measured repetitions. All six native medians lose to paired stock, with
native/stock elapsed **1.001-1.228**; the narrowest 0.12% difference is near parity.
All six beat the preceding evaluator, with native/previous **0.772-0.921**. These are
bounded prepared-function gains; row-source boundary costs remain material and no
complete-job acceleration is claimed.

See the [qualification and exact provenance](results/decimal-negate-abs-2026-11-05.json)
and [6,000 raw prepared samples](results/decimal-negate-abs-2026-11-05.csv.gz).
The remaining exact numeric and character-conversion scopes stay open in
[#491](https://github.com/datafusion-contrib/StreamFusion/issues/491) and
[#492](https://github.com/datafusion-contrib/StreamFusion/issues/492).


## DECIMAL unary arithmetic TRY_CAST to character (2026-11-05) {#decimal-unary-arithmetic-try_cast-to-character-2026-11-05}

Nullable character TRY_CAST now admits verified ABS/unary-minus wrappers over
DECIMAL-only addition, subtraction and multiplication. For example,
`TRY_CAST(-ABS(d*d) AS CHAR(48))` formats through existing native decimal kernels.
Resolved intermediate precision/scale and overflow NULLs remain unchanged. This
verified TRY_CAST boundary resolves arithmetic NULLs before consumers such as IS NULL
observe them, including NOT NULL source fields. Ordinary CAST retains its separate
producer and projection-default guards; narrowing, rescaling and fallible/unverified
producers retain their existing routes. No production Rust/JNI change or switch is added.

All **428 fresh JVM invocations** pass on released Flink 1.18.1/2.2.1 and JDK 11/17,
targeting Java 11: 48 new SQL invocations and 380 related invocations, with no skips or
aborts. Complete 5,003-row maps/schema cover ten precision/scale pairs, strict/legacy
modes, nullable/NOT NULL sources, STRING/CHAR/VARCHAR, arithmetic overflow,
IS NULL/CHAR_LENGTH/CASE consumers, runtime filters and all-filtered batches. Admitted
pure routes consume NativeCalc, have no fallback and have empty JVM evaluator descriptors.
Ordinary CAST defaults and unverified/fallible producer controls remain explicit.
Four unchanged released `CastRulesTest#castTests` stock-oracle runs pass 535 cases each;
these are separate stock evidence, not 2,140 new native tests.

Actual preceding encoder `83d032683` fails eleven expected route assertions and passes
one retained control per released-line JDK17 run. Ten core methods compare first-query
full maps/schema before their expected fallback assertion fails. The mixed ordinary-CAST
control fails its NativeSubstitution assertion before multiset comparison and is not
preceding-value evidence; later unreached queries are not counted either. Final parity
and performance encoder instructions, including nested classes, match on both released
lines. The unchanged release runtime retains its historical 927-core-test qualification.

Prepared release measurements output STRING/CHAR/VARCHAR directly with equivalent
UTF-8 checksum consumption. Four shapes cover ABS of a product at DECIMAL(18,3),
negative ABS of a product at (38,18), a negative sum at (38,38), and nested unary
arithmetic at (19,0). Each uses strict/legacy, nullable/nonnullable and Generic/Binary
RowData controls with 1,024-row batches, plus 16-row nullable/Binary controls.
Stock starts on prebuilt RowData and native on equivalent prebuilt Arrow. The timer
starts inside Rust after setup/import and stops before export; production Calc,
allocations and conversions intrinsic to the operation remain timed. Every trial
processes 131,072 rows, with sixteen warmups and nine measured repetitions.
Candidate and preceding prepared JVMs both use `-Xmx1g`; no other tests or benchmarks
run concurrently. Independent preceding stock controls remain in the raw records.

These direct character-root expressions used **whole-Calc stock fallback** before
the change. The preceding control verifies that actual rejection and times Flink,
rather than fabricating a callback inside NativeCalc. This performance scope does
not cover the distinct preceding whole-projection evaluator route that a NOT NULL
arithmetic TRY_CAST observed by IS NULL can use; that consumer has pure-native
correctness evidence but no separate speedup claim here.

All **64 bulk cells** beat stock by **1.283-8.044x** and previous by **1.280-8.426x**.
Nine of sixteen tiny controls lose to stock, and eight lose to previous; tiny
stock/native ranges **0.808-2.431x**. Six complete million-row jobs output character
values directly and retain submission/startup, production JNI and both RowData/Arrow
transposes. At parallelism one and `-Xmx2g` for every engine, each has two warmups
and five measured repetitions. All six beat paired stock and actual preceding
fallback: native/stock elapsed is **0.502-0.705**, native/previous **0.519-0.744**.
These measured shapes do not establish universal job or tiny-batch acceleration.

See the [qualification, raw full-job trials and exact provenance](results/decimal-unary-arithmetic-try-cast-2026-11-05.json)
and [8,000 prepared samples](results/decimal-unary-arithmetic-try-cast-2026-11-05.csv.gz).
Remaining character conversions stay tracked in
[#492](https://github.com/datafusion-contrib/StreamFusion/issues/492).


## DECIMAL SIGN over verified unary producers (2026-11-05) {#decimal-sign-over-verified-unary-producers-2026-11-05}

SIGN now composes verified DECIMAL ABS/unary-minus producers, their alternating unary
trees and admitted arithmetic through existing typed kernels. For example,
`SIGN(ABS(d))`, `SIGN(-ABS(d))` and `SIGN(ABS(d*d))` no longer need a generated
evaluator when their result/producer types satisfy the existing SIGN proof. Released
ABS/negation preserve precision and scale over valid coefficients. This does not admit
SIGN as an ABS/negation producer or remove the SIGN-specific representation guards.

Released SIGN directly constructs compact scaled +/-1, which can exceed a declared
compact DECIMAL(p,p) precision. Its noncompact branch reconstructs through BigDecimal
and returns NULL for nonzero DECIMAL(p,p) values. Those contracts remain distinct:
compact SIGN and compact SIGN intermediates under widening/arithmetic stay generated;
noncompact zero/NULL results, final defaults versus NULL flags and NOT NULL ERROR/DROP
enforcement remain unchanged. Narrowing, rescaling, division/remainder and unverified
producers retain their existing routes. Character CAST/TRY_CAST keep separate proofs;
`TRY_CAST(SIGN(ABS(d)) AS STRING)` retains its explicit fallback and matches stock.
No production Rust/JNI change, new opcode or performance switch is added.

All **448 fresh JVM invocations** pass across released Flink 1.18.1/2.2.1 and
JDK 11/17, targeting Java 11: 60 new SQL invocations and 388 related invocations,
with no skips or aborts. Full 5,003-row maps/schema cover nine admitted precision/scale
pairs, nullable/NOT NULL fields, same-scale widening, alternating/arithmetic trees,
overflow, lazy/all-filtered paths and retained compact/consumer routes. Admitted pure
queries consume NativeCalc, have no fallback and have empty JVM evaluators. Encoding
controls cover every DECIMAL precision/scale, retaining the compact result restriction.
Four fresh unchanged released ScalarFunctionsTest.testSign stock-oracle runs pass
separately. Actual preceding encoder `902876488` matches each first-query map/schema
before nine expected new pure-route failures per released-line JDK17 run; six retained
controls pass. Later unreached queries are not preceding-parity evidence. Final parity
and performance encoder instructions match on both lines, and candidate/previous
fixture classes are byte-identical. The unchanged runtime retains its historical
927-core-test qualification, not a fresh Rust run.

Prepared release measurements consume `(SIGN(...)>0 OR CAST(NULL AS BOOLEAN))` with
equivalent nullable BOOLEAN checksums. Five shapes cover ABS at DECIMAL(18,3), negation
at (19,0), negative ABS at (38,18), alternating unary trees at (38,38), and ABS of a
product at (18,3). Each uses nullable/nonnullable and Generic/Binary RowData controls
with 1,024-row batches, plus a 16-row nullable/Binary control on each released line.
Stock starts on equivalent prebuilt RowData; native starts on prebuilt Arrow. The Rust
timer runs after setup/import and before export, retaining production Calc, allocation
and intrinsic conversions/callbacks. Actual previous behavior uses generated evaluation
inside NativeCalc. Each trial processes 131,072 rows, with sixteen warmups and nine
measurements. Prepared JVM heaps match at 1 GiB. No other tests or benchmarks run
concurrently; independent stock controls and all variability remain in the records.

All **40 bulk cells** beat stock **1.503-12.203x** and previous **5.932-19.853x**.
All **10 tiny controls lose to stock**, with stock/native **0.088-0.462x**; all beat
previous. Six complete million-row jobs output DECIMAL directly and retain submission,
production JNI and both RowData/Arrow transposes. At parallelism one and 2 GiB heaps
for every engine, each has two warmups and five measured repetitions. Four of six
lose to paired stock: native/stock elapsed **0.936-1.107**, native/previous
**0.767-0.880**. All six beat previous. The conversion-inclusive stock losses remain
material; these prepared-function gains do not establish universal job acceleration.

See the [qualification, complete-job trials and exact provenance](results/decimal-sign-unary-2026-11-05.json)
and [5,000 raw prepared samples](results/decimal-sign-unary-2026-11-05.csv.gz).
Remaining exact numeric work stays open in
[#491](https://github.com/datafusion-contrib/StreamFusion/issues/491).

