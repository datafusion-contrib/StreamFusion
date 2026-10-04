# Avoid redundant copies at the exit transpose

**Applies to:** the Arrow→RowData exit transpose

The exit transpose reads a reusable `ColumnarRowData` view while the Arrow batch remains open.
For the primitive and binary schemas described below, a generated projection materializes a reusable
`BinaryRowData`; other schemas retain the view. With Flink object reuse disabled, the runtime's
`CopyingChainingOutput` deep-copies the chosen representation synchronously before delivering it
to the next operator. Network outputs serialize it synchronously. The projection reuses its
storage rather than allocating another owned row per record.

With object reuse enabled, chained outputs do not copy. The transpose supplies an owned
`RowDataSerializer` copy for each emitted row so a host operator can retain it after the Arrow
batch closes. Enabling object reuse never permits an Arrow-backed view to escape its batch's
lifetime. Branches receive their normal Flink ownership semantics in either mode.

This follows Comet's columnar-row conversion and generated row projection while respecting
Flink's distinct chained-output contract. Tests retain rows containing nested strings and binary values after
batch closure with object reuse both enabled and disabled. The benchmark keeps the application's
object-reuse setting unchanged for both engines.

The earlier unconditional borrowed-view implementation reported roughly doubled native q0
throughput (`713a0a3`), but that historical result predates the owned-row requirement for
object-reuse-enabled consumers and is not a claim about the current implementation.

Current end-to-end measurements and the isolated entry/exit steps are recorded in the
[row-major transpose ledger](row-major-transpose.md#end-to-end-ownership-copy-measurements).


## Generated projection for primitive and binary outputs

A boolean-cast CPU profile attributed about 15% of native samples to the exit transpose and
7% of leaf samples to a host row field getter. Rows composed entirely of BOOLEAN, integer,
floating-point, DATE, TIME, interval, compact DECIMAL (precision at most 18), BINARY or
VARBINARY fields use
Flink's generated binary-row projection. The downstream serializer can copy a contiguous row
instead of invoking generic field getters and boxing primitive values. Other output schemas
retain their existing view path. The projection is generated while constructing the operator;
the worker instantiates the serialized generated class with its user-code classloader.

This mirrors Comet's `UnsafeProjection` at its columnar-to-row boundary, using Flink's own row
layout and code generator. Row kinds are transferred explicitly. Tests retain values after
batch closure with object reuse on and off, covering NULLs, negative zero, NaN payloads,
compact decimals, changing row kinds and zero-column record counts.

A release+mimalloc comparison uses JDK17/Flink2.2.1, Core i7-12650H/Linux, 2 GiB heap,
parallelism one, default 1,024-row batches, two warmups and five alternating measured trials.
The 20M-row boolean source cycles through `true`, `FALSE`, `t`, `0`, `yes`, and `n`, with no
injected NULLs. Both transposes and the row sink remain in the measured job; the benchmark
asserts the native Calc and both boundaries. Each run completed without detected competing
build/test processes.

| TRY_CAST STRING to BOOLEAN, 20M rows | Native median (range), s | Flink median (range), s |
| --- | ---: | ---: |
| Fresh published baseline | 3.545 (3.532–3.548) | 3.388 (3.335–3.416) |
| Generated exit, first run | 3.234 (3.226–3.246) | 3.342 (3.267–3.371) |
| Generated exit, repeat | 3.357 (3.313–3.375) | 3.452 (3.423–3.480) |

The baseline trails its matched Flink run by 4.6%; the two candidate runs take 3.2% and
2.8% less time than their matched Flink controls, each with non-overlapping ranges. The
repeat native median is 5.3% below the fresh native baseline, but that difference should
not all be attributed to the projection: the unchanged STRING identity control varies from
4.053 native / 3.321 Flink in the baseline to 3.845 / 3.161 and 3.910 / 3.339 in the candidate
runs. Those unfavorable identity results remain included in the raw measurements.

Two-million-row DATE and TIME checks on the same candidate measure:

| Expression | Native median (range), s | Flink median (range), s |
| --- | ---: | ---: |
| TRY_CAST STRING to DATE | 0.513 (0.502–0.520) | 0.602 (0.598–0.614) |
| TRY_CAST STRING to TIME(3) | 0.507 (0.505–0.516) | 0.620 (0.612–0.633) |

These establish the measured scalar workloads, not every schema or native island. See
[all trials and identity controls](../benchmarks/scalar-fixed-width-exit-2026-09-28.csv).
Reproduce with `ScalarFunctionBenchmark#individualFunctions` under `-Pbench`,
`SF_BENCHMARK=true`, `-Dsf.extraJvmArgs=-Xmx2g`, `-Dscalar.nullEvery=0`,
`-Dscalar.warmup=2 -Dscalar.runs=5`, and either
`-Dscalar.functions=TRY_STRING_TO_BOOLEAN -Dscalar.rows=20000000` or
`-Dscalar.functions=TRY_STRING_TO_DATE,TRY_STRING_TO_TIME -Dscalar.rows=2000000`.

Validation passes 119 focused SQL, transpose and recovery checks on each of Flink 1.18.1 and
2.2.1, including the ownership and value-preservation cases above, Boolean errors and temporal
precision/time-zone behavior.

### Binary output measurements

The generated projection also avoids repeated generic field dispatch when the exit contains
binary fields. It preserves Flink's binary row layout and ownership; it still copies Arrow
bytes into that row. Tests cover fixed binary, empty and 16 KiB variable binary values, NULLs,
all row kinds, buffer growth, and retained rows after two batches close, with object reuse
both enabled and disabled. The focused suite passes 40 checks on Flink 2.2.1 and 35 checks
with five version-specific skips on Flink 1.18.1.

Two million rows, with the same release build, heap, batch size, alternating trials and
production boundaries described above, give:

| Expression / implementation | Native median (range), s | Flink median (range), s |
| --- | ---: | ---: |
| STRING to BINARY(16), fresh previous exit | 0.960 (0.954–0.970) | 0.869 (0.847–0.874) |
| STRING to BINARY(16), generated exit | 0.939 (0.924–1.019) | 0.864 (0.858–0.894) |
| STRING to BINARY(16), generated exit repeat | 0.925 (0.918–0.932) | 0.858 (0.829–0.876) |
| ELT BINARY(16), fresh previous exit | 0.437 (0.432–0.447) | 0.313 (0.309–0.330) |
| ELT BINARY(16), generated exit | 0.414 (0.413–0.419) | 0.311 (0.301–0.326) |
| ELT BINARY(16), generated exit repeat | 0.420 (0.412–0.424) | 0.308 (0.307–0.320) |

ELT takes 5.4% and 4.0% less native time than the fresh previous implementation, with
non-overlapping native ranges. Its Flink control shifts by 0.5% and 1.4%, respectively.
The cast medians improve by 2.2% and 3.7%, but the first candidate range overlaps the baseline
and the Flink controls shift too. These are partial improvements: both workloads still lose
to stock Flink, and PR #264 remains performance-blocked. The unchanged STRING identity
control also remains slower than Flink. No general VARBINARY speedup is established here.

[Raw trials, including all identity controls](../benchmarks/scalar-binary-exit-2026-09-28.csv)
retain those limits. Reproduce with the scalar benchmark settings above and
`-Dscalar.functions=TRY_STRING_TO_FIXED_BINARY,ELT_FIXED_BINARY -Dscalar.rows=2000000`.
