# Recorded scalar workloads: integers

[Topic index and original links](scalar-functions.md). Sections retain their recorded scope, dates and evidence.

## DOUBLE TRUNCATE consumer and nullable workloads {#double-truncate-consumer-and-nullable-workloads}

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

### Rejected manual BINARY exit projection {#rejected-manual-binary-exit-projection}

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

### Rejected generated BINARY segment projection {#rejected-generated-binary-segment-projection}

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


## Integral SIGN {#integral-sign}

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


## Integral ABS {#integral-abs}

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


## Integral unary minus {#integral-unary-minus}

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


## Integer FLOOR {#integer-floor}

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


## Integer CEIL / CEILING {#integer-ceil-ceiling}

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


## Single-argument integer ROUND {#single-argument-integer-round}

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


## Single-argument integer TRUNCATE {#single-argument-integer-truncate}

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


## Integer ROUND with a literal position {#integer-round-with-a-literal-position}

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


## Integer TRUNCATE with a literal position {#integer-truncate-with-a-literal-position}

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


## Integer ABS over arithmetic {#integer-abs-over-arithmetic}

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


## Integer unary minus over arithmetic {#integer-unary-minus-over-arithmetic}

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


## Integer SIGN over arithmetic {#integer-sign-over-arithmetic}

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


## Integer FLOOR over arithmetic {#integer-floor-over-arithmetic}

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


## Integer CEIL over arithmetic {#integer-ceil-over-arithmetic}

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


## Integer ROUND over arithmetic {#integer-round-over-arithmetic}

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


## Integer TRUNCATE over arithmetic {#integer-truncate-over-arithmetic}

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


## Mixed selection latest-main regression {#mixed-selection-latest-main-regression}

The six GREATEST, LEAST, IF and DATE HOUR/MINUTE/SECOND commits rebase onto canonical
main `be842d2d` with six equal patches in `git range-diff`. Historical measurements keep
their original source hashes, encoders, DSOs and pre-rebase baseline IDs; they are not
relabeled as new timings. The [regression record](results/mixed-selection-main-regression-2026-11-02.json)
maps those commits into the rebased history and records **768 fresh passing JVM invocations**
(192 per released Flink 1.18.1/2.2.1 and JDK 11/17 pair), compiled for Java release 11.
They cover complete mixed-selection/DATE result maps and native consumption, shared state,
DECIMAL defaults, calendar/timestamp controls and current nested bridge admission/fallback.


## Function allocator provenance and matched IF controls {#function-allocator-provenance-and-matched-if-controls}

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


## Integer ROUND with a bounded runtime position {#integer-round-with-a-bounded-runtime-position}

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



## Integer TRUNCATE with a bounded runtime position {#integer-truncate-with-a-bounded-runtime-position}

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



## Integer TRY_CAST {#integer-try_cast}

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



## Integer ROUND with clamped runtime positions {#integer-round-with-clamped-runtime-positions}

This independent extension admits total INT position trees bounded by built-in GREATEST,
LEAST and CASE. For example, `ROUND(v,GREATEST(-38,LEAST(n,38)))` stays inside the existing
verified -38..38 range. Range propagation is conservative across wrapping arithmetic;
every condition/argument must be total, including unselected and NULL-dominated arguments.
Only all signed integer widths are admitted. Unproven
ranges/producers and custom functions keep their preceding routes. Epoch precision remains
on its separate 0/3/NULL proof. No runtime opcode, JNI, representation or switch is added.

The [public record](results/clamped-round-2026-11-04.json) and
[8,800 raw trials](results/clamped-round-2026-11-04.csv.gz) retain full ranges,
independent stock controls, allocation probes, checksums and source/bytecode/artifact hashes.
The actual preceding encoder is `2539fabb`; it binds the ROUND JVM evaluator
for these clamps. Both encoders run on byte-identical frozen release DSOs/executables with
verified mimalloc aliases. This isolates Java admission on the same production kernels;
it is not a replay of a different historical whole runtime. Archived 912-core-test runtime
qualification is retained, rather than relabeled as new Rust tests.

### Prepared operation {#prepared-operation_1}

Stock starts on equal prepared GenericRowData or BinaryRowData; native starts on prepared
Arrow buffers with its timer inside Rust, after import and before export. Input/setup,
initial JNI/C Data entry and perimeter transposes are outside this small-functionality
timer only. Production Calc evaluation, output consumption, allocations and intrinsic
conversions/callbacks remain inside. One thread, JDK 17, Java release 11, both released
Flink lines, 131,072 logical rows per trial, 16 warmups and nine measured repetitions.
Both nullable modes, independent position NULLs, signed extrema and BIGINT values beyond
2^53 are included. Each comparison checks matching consumed-output digests.

Each type has `GREATEST(-38,LEAST(n,38))` and `GREATEST(-3,LEAST(n,-1))` shapes. The wide
prepared corpus mostly saturates at -38/38; its results alone would overrepresent zero or
identity shortcuts. The separate negative clamp executes nontrivial -3/-1 rounding.
Functional SQL sources additionally walk all interior positions through id-2500, with
nested/NULL CASE, arithmetic, STRING/epoch consumers and empty batches.

All 64 bulk cells (1,024 rows/batch) beat paired stock by 2.052-20.494x
and previous by 6.389-29.439x. Ratios of paired medians:

| Released Flink | Input | Stock/native throughput | Previous/native throughput |
| --- | --- | ---: | ---: |
| 2.2.1 | TINYINT | 2.424-19.783x | 7.466-29.439x |
| 2.2.1 | SMALLINT | 2.412-17.210x | 7.726-27.877x |
| 2.2.1 | INT | 3.298-17.027x | 6.685-24.739x |
| 2.2.1 | BIGINT | 2.593-19.992x | 7.215-26.252x |
| 1.18.1 | TINYINT | 2.052-20.494x | 6.389-27.700x |
| 1.18.1 | SMALLINT | 2.322-18.250x | 7.877-26.777x |
| 1.18.1 | INT | 2.710-14.844x | 7.842-23.883x |
| 1.18.1 | BIGINT | 3.020-18.607x | 7.274-26.679x |

Of 16 sixteen-row controls, 12 lose to stock. Their stock/native ratios
range from 0.211 to 1.189x. These are retained diagnostics, not removed
from the data or used to claim universal tiny-batch acceleration. Independent prior-JVM
stock timings remain alongside candidate-JVM stock, with no subtraction or normalization.

### Complete jobs and correctness {#complete-jobs-and-correctness}

Eight complete configurations process 1M rows each: INT/BIGINT, both clamp shapes and
both Flink releases. Runtime rowwise source to blackhole, parallelism one, two warmups,
five alternating stock/native measurements per engine, submission/startup, JNI and both
RowData/Arrow transposes remain in the job timer. Seconds, median (minimum-maximum):

| Release/shape | Stock | Native | Independent stock | Previous |
| --- | ---: | ---: | ---: | ---: |
| 22-int-wide | 0.211375 (0.208757-0.216321) | 0.188357 (0.186368-0.215522) | 0.214924 (0.211917-0.232876) | 0.246277 (0.243375-0.281274) |
| 22-int-negative | 0.192676 (0.189297-0.204379) | 0.194442 (0.191757-0.353703) | 0.187509 (0.184368-0.193174) | 0.220544 (0.217093-0.246181) |
| 22-bigint-wide | 0.236759 (0.234225-0.242784) | 0.214993 (0.201897-0.233321) | 0.232573 (0.225093-0.236360) | 0.272229 (0.264901-0.288339) |
| 22-bigint-negative | 0.199538 (0.192411-0.207178) | 0.209872 (0.200907-0.212597) | 0.201076 (0.200125-0.215894) | 0.238390 (0.229101-0.250444) |
| 118-int-wide | 0.215960 (0.204185-0.230945) | 0.189556 (0.189021-0.207103) | 0.218499 (0.210813-0.220883) | 0.255599 (0.250429-0.270002) |
| 118-int-negative | 0.209546 (0.188340-0.211879) | 0.199818 (0.194083-0.222225) | 0.190139 (0.185749-0.218386) | 0.239198 (0.221494-0.266854) |
| 118-bigint-wide | 0.235236 (0.223393-0.250472) | 0.216688 (0.209132-0.226110) | 0.240505 (0.236140-0.253585) | 0.283365 (0.273492-0.312494) |
| 118-bigint-negative | 0.206729 (0.200961-0.242730) | 0.218570 (0.215633-0.229089) | 0.205106 (0.196390-0.219563) | 0.253952 (0.247074-0.277491) |

Native/stock elapsed is 0.878-1.057;
3 configurations lose to paired stock. Native/previous is
0.742-0.882,
with 0 previous-path losses. Whole-job variability remains in the raw record;
prepared-operation speedups do not establish universal full-job acceleration.

240 fresh focused JVM invocations pass with no skips across released
Flink 1.18.1/2.2.1 and JDK 11/17, targeting Java 11. Complete maps/schema compare 5,003-row
sources, widths, signed overflow, HALF_UP/toward-zero behavior as applicable, independent
NULLs, lazy/failing paths and empty batches. Admitted queries require consumed NativeCalc
work, no fallback and empty evaluators. Original literal/bounded CASE and temporal-precision
controls pass. Four unchanged released `ROUND` stock-oracle invocations pass separately.
The preceding encoder compares every positive first-query map/schema and then fails the
intended pure-route assertion; later queries in those failing invocations are not counted.
Both retained/generated and same-name catalog-UDF controls pass. These local qualifications
do not stand in for the hosted suites on the exact pushed PR head.

Reproduce prepared controls with the existing release `integral_expression` example,
`SF_INTEGRAL_SECONDARY_TYPE=INT` and
`SF_INTEGRAL_EXPRESSION='ROUND(v,GREATEST(-3,LEAST(n,-1)))'`, selecting each input width,
nullable/row-representation/batch configuration and the preceding encoder first on its
classpath. Complete jobs use `IntegralExpressionPipelineBenchmark`, the same expression
and `-Dsf.integral.secondaryType=INT`; the prior control sets `sf.integral.previous=true`.
The records retain the exact source/artifact hashes and trial configuration.



## Integer TRUNCATE with clamped runtime positions {#integer-truncate-with-clamped-runtime-positions}

This independent extension admits total INT position trees bounded by built-in GREATEST,
LEAST and CASE. For example, `TRUNCATE(v,GREATEST(-38,LEAST(n,38)))` stays inside the existing
verified -38..38 range. Range propagation is conservative across wrapping arithmetic;
every condition/argument must be total, including unselected and NULL-dominated arguments.
Only INT/BIGINT are admitted. Unproven
ranges/producers and custom functions keep their preceding routes. Epoch precision remains
on its separate 0/3/NULL proof. No runtime opcode, JNI, representation or switch is added.

The [public record](results/clamped-truncate-2026-11-04.json) and
[4,000 raw trials](results/clamped-truncate-2026-11-04.csv.gz) retain full ranges,
independent stock controls, allocation probes, checksums and source/bytecode/artifact hashes.
The actual preceding encoder is `0acdd9b7`; it binds the TRUNCATE JVM evaluator
for these clamps. Both encoders run on byte-identical frozen release DSOs/executables with
verified mimalloc aliases. This isolates Java admission on the same production kernels;
it is not a replay of a different historical whole runtime. Archived 912-core-test runtime
qualification is retained, rather than relabeled as new Rust tests.

### Prepared operation {#prepared-operation_2}

Stock starts on equal prepared GenericRowData or BinaryRowData; native starts on prepared
Arrow buffers with its timer inside Rust, after import and before export. Input/setup,
initial JNI/C Data entry and perimeter transposes are outside this small-functionality
timer only. Production Calc evaluation, output consumption, allocations and intrinsic
conversions/callbacks remain inside. One thread, JDK 17, Java release 11, both released
Flink lines, 131,072 logical rows per trial, 16 warmups and nine measured repetitions.
Both nullable modes, independent position NULLs, signed extrema and BIGINT values beyond
2^53 are included. Each comparison checks matching consumed-output digests.

Each type has `GREATEST(-38,LEAST(n,38))` and `GREATEST(-3,LEAST(n,-1))` shapes. The wide
prepared corpus mostly saturates at -38/38; its results alone would overrepresent zero or
identity shortcuts. The separate negative clamp executes nontrivial -3/-1 rounding.
Functional SQL sources additionally walk all interior positions through id-2500, with
nested/NULL CASE, arithmetic, STRING/epoch consumers and empty batches.

All 32 bulk cells (1,024 rows/batch) beat paired stock by 6.593-28.118x
and previous by 11.913-36.111x. Ratios of paired medians:

| Released Flink | Input | Stock/native throughput | Previous/native throughput |
| --- | --- | ---: | ---: |
| 2.2.1 | INT | 6.746-26.683x | 11.959-33.213x |
| 2.2.1 | BIGINT | 8.351-28.118x | 12.890-34.283x |
| 1.18.1 | INT | 6.593-26.033x | 11.913-36.111x |
| 1.18.1 | BIGINT | 9.130-26.918x | 12.208-35.569x |

Of 8 sixteen-row controls, 3 lose to stock. Their stock/native ratios
range from 0.826 to 1.613x. These are retained diagnostics, not removed
from the data or used to claim universal tiny-batch acceleration. Independent prior-JVM
stock timings remain alongside candidate-JVM stock, with no subtraction or normalization.

### Complete jobs and correctness {#complete-jobs-and-correctness_1}

Eight complete configurations process 1M rows each: INT/BIGINT, both clamp shapes and
both Flink releases. Runtime rowwise source to blackhole, parallelism one, two warmups,
five alternating stock/native measurements per engine, submission/startup, JNI and both
RowData/Arrow transposes remain in the job timer. Seconds, median (minimum-maximum):

| Release/shape | Stock | Native | Independent stock | Previous |
| --- | ---: | ---: | ---: | ---: |
| 22-int-wide | 0.216388 (0.213630-0.223902) | 0.190361 (0.185447-0.204954) | 0.220159 (0.213937-0.223426) | 0.258516 (0.248854-0.274577) |
| 22-int-negative | 0.248733 (0.244749-0.253861) | 0.190804 (0.190657-0.208462) | 0.237455 (0.233748-0.249631) | 0.276953 (0.272416-0.291678) |
| 22-bigint-wide | 0.235817 (0.230376-0.243834) | 0.208476 (0.202216-0.217662) | 0.243563 (0.239602-0.250967) | 0.276771 (0.260991-0.290315) |
| 22-bigint-negative | 0.263149 (0.255959-0.270185) | 0.201221 (0.196207-0.217436) | 0.254803 (0.250489-0.266496) | 0.293021 (0.287375-0.307211) |
| 118-int-wide | 0.222041 (0.216154-0.231507) | 0.197143 (0.194934-0.198736) | 0.216478 (0.215683-0.242135) | 0.253723 (0.248861-0.282366) |
| 118-int-negative | 0.240669 (0.230602-0.243193) | 0.197705 (0.187929-0.199502) | 0.234460 (0.230710-0.239697) | 0.269294 (0.262753-0.283474) |
| 118-bigint-wide | 0.242870 (0.237979-0.257082) | 0.210705 (0.209571-0.218985) | 0.249933 (0.234538-0.263448) | 0.275957 (0.267568-0.288913) |
| 118-bigint-negative | 0.252105 (0.246873-0.266810) | 0.214418 (0.206610-0.221217) | 0.265535 (0.260429-0.270674) | 0.301647 (0.293013-0.315090) |

Native/stock elapsed is 0.765-0.888;
0 configurations lose to paired stock. Native/previous is
0.687-0.777,
with 0 previous-path losses. Whole-job variability remains in the raw record;
prepared-operation speedups do not establish universal full-job acceleration.

264 fresh focused JVM invocations pass with no skips across released
Flink 1.18.1/2.2.1 and JDK 11/17, targeting Java 11. Complete maps/schema compare 5,003-row
sources, widths, signed overflow, HALF_UP/toward-zero behavior as applicable, independent
NULLs, lazy/failing paths and empty batches. Admitted queries require consumed NativeCalc
work, no fallback and empty evaluators. Original literal/bounded CASE and temporal-precision
controls pass. Four unchanged released `TRUNCATE` stock-oracle invocations pass separately.
The preceding encoder compares every positive first-query map/schema and then fails the
intended pure-route assertion; later queries in those failing invocations are not counted.
Both retained/generated and same-name catalog-UDF controls pass. These local qualifications
do not stand in for the hosted suites on the exact pushed PR head.

Reproduce prepared controls with the existing release `integral_expression` example,
`SF_INTEGRAL_SECONDARY_TYPE=INT` and
`SF_INTEGRAL_EXPRESSION='TRUNCATE(v,GREATEST(-3,LEAST(n,-1)))'`, selecting each input width,
nullable/row-representation/batch configuration and the preceding encoder first on its
classpath. Complete jobs use `IntegralExpressionPipelineBenchmark`, the same expression
and `-Dsf.integral.secondaryType=INT`; the prior control sets `sf.integral.previous=true`.
The records retain the exact source/artifact hashes and trial configuration.


