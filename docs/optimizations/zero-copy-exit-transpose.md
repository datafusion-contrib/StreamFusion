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

## Experimental borrowed STRING exit

`streamfusion.experimental.borrowedStringExit=true` removes an intermediate UTF-8 byte-array
copy for top-level CHAR/VARCHAR fields at the exit transpose. It defaults to false. A mutable
Flink binary-string view points at the current Arrow value, following Comet's borrowed UTF-8
view pattern. The existing downstream copy or synchronous serialization still establishes
ownership before the batch closes. Object-reuse-enabled outputs still receive an owned row;
this change does not remove that mandatory copy. Generic Arrow input readers retain their
existing string ownership, and explicit UTF-16 cache columns preserve Java string values.
Unsupported vectors or buffers exceeding the Java NIO size limit retain the copying reader.
The same reader supplies the JVM aggregate's synchronous key projection, whose released
selector returns an owned key; aggregate callback inputs retain the ordinary reader.

[Matched Java/Rust profiles and complete-job trials](../benchmarks/results/jvm-aggregate-borrowed-string-exit-2026-10-16.json)
retain both enabled and disabled recordings on identical source, queries and resources.
Over 5M native UDAF rows, inclusive aggregate-ingest sampled allocations decrease from
4.150 GB to 3.236 GB (22%). CPU samples in that method are nearly unchanged (1,283 versus
1,273); allocation savings do not establish a throughput gain. The profiles also retain
symbolized Rust builtin-SUM control stacks. Allocation samples are estimates, not copied
bytes, peak memory or native heap totals, and all profiled timings are excluded.

Separate unprofiled Flink 2.2.1 release jobs use 2M STRING-key rows, 2 GiB heap, parallelism one,
production IPC, both 1,024-row transposes, symmetric preflight, two warmups and five pairs:

| Distinct keys | Stock median (range), s | Experimental native median (range), s |
| --- | ---: | ---: |
| 16 | .801 (.732–.812) | .647 (.565–.693) |
| 65,536 | 1.310 (1.154–1.489) | 1.187 (1.118–1.329) |
| 2M | 2.008 (1.869–2.068) | 2.160 (2.041–2.203) |

The medium and distinct-key ranges overlap; distinct keys still regress by 7.6%. Earlier
disabled-flag jobs use separate JVMs and exhibit control drift, so these comparisons do not
isolate the exit change's contribution. Candidate performance on Flink 1.18 remains pending.
This remains a default-off experiment, not admitted acceleration.

The subsequent [isolated two-line matrix](../benchmarks/results/jvm-aggregates-string-exit-isolated-2026-10-16.json)
completed with this exit flag enabled, before borrowed key projection. Low/medium STRING
medians improve versus stock on both lines, but distinct keys still regress: 3.607 s native
versus 3.450 s stock on 2.2, and 2.774 versus 2.717 s on 1.18. Wide table-function output has
overlapping stock/native ranges on both lines. The full artifact retains unfavorable typed-key
results and mixed Java/Rust profiles; success does not establish admission. Control drift
between runs limits attributing changes to this technique alone.

Validation includes 73 aggregate, checkpoint/savepoint rescale, transpose and ownership cases
on each Flink line at the normal cache capacity. At capacity three, adding JSON/JVM and nested
JSON boundaries passes 108 cases on 2.2 and 106 with two compatibility skips on 1.18. Four
exit ownership cases cover object reuse and UTF-16 caches on/off, NULL/empty/Unicode/NUL strings,
unpaired surrogates with caches, all RowKinds, buffer growth and retained rows after batch
closure. Both released-line reactor test compilations pass.


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
