# UDFs via a columnar JVM upcall

**Applies to:** user `ScalarFunction`s and generated Flink expressions executed inside native islands

A user `ScalarFunction` the native engine can't implement itself runs *inside* the island instead of
falling the whole query back to Flink: the argument columns are packed into one batch, exported over
the C Data Interface, evaluated by the real function on the JVM, and the result column imported back
— one JNI crossing per batch, never per row. The design is modelled on Comet's `JvmScalarUdfExpr`.

The JVM runs the actual user function. Argument/result conversion and call ordering still need
Flink parity checks: DECIMAL results use Flink's precision/scale conversion, and VARBINARY values
cross as raw bytes. See the [UDF admission rules](../operators/calc-filter.md#user-scalar-functions)
for nested decimal-result and repeated binary-call gates.

Functions are serialized into the operator and registered per-task at `open()`, so this survives
distributed execution, where the UDF instance must be reconstructed on each task's JVM rather than
shared from the planner.
Call-site registrations carry argument/result signatures, while the operator binding owns one
lifecycle per distinct function instance. Repeated and nested calls share initialization and
cleanup. A failed binding removes its registrations and closes successfully opened functions;
cleanup continues through function-close exceptions and retains those failures for reporting.

## Exception-safe Arrow release

When a scalar callback fails, the native bridge retains its original Java throwable in a global
reference and clears the pending JNI exception before unwinding the argument and result buffers.
Arrow release callbacks can then call Java safely. The outer JNI guard rethrows the same throwable
after cleanup, preserving its type and message. This follows Comet's capture-and-clear handling
and the existing native output-callback contract. Leaving the exception pending lets an Arrow
release callback consume it, replacing the original failure with a generic native error.

The bridge regression repeatedly transfers ownership of an input batch, closes its producer,
throws from the scalar callback, and verifies throwable identity and complete buffer release.
The recovery stress test also exercises decimal overflow through restarted streaming tasks.
The new regression fails against the previous native build. With the fix, 182 focused checks
pass on each of Flink 1.18.1 and 2.2.1 with JNI checking enabled, including 100 recovery
iterations per release. This validates exception handover and cleanup; the intermittent CI
process abort still requires verification in the full suite.

## Direct generated-expression dispatch

Known, final internal Flink expression evaluators without top-level string arguments receive a
reusable Arrow-backed `RowData` view of the imported argument batch and invoke the same released
Flink generated code. Generated expressions with string arguments retain typed column
materialization and Java-backed `StringData`: Flink's UTF-16 comparison, string identity and
empty-string trim behavior depend on this representation. They still call the generated
evaluator directly, without reflective dispatch. The borrowed path avoids
materializing `Object[][]` argument columns, repacking a generic row, reflective varargs packing,
and `Method.invoke` on every row. The borrowed view remains inside the imported batch's lifetime;
each result is written before advancing to the next row. Generic user functions retain their
resolved reflective call path and typed column materialization. Arrow conversion and the
once-per-batch JNI boundary remain in the measured path.
Failures retain the original cause through the existing native exception handover, including checked
exceptions and `Error`s. Open/close and classloader ownership are unchanged.

Focused validation runs 227 cases on released Flink 2.2.1 (all pass) and 1.18.1
(216 pass, 11 version-capability skips). Coverage includes surrogate identity, shared-function
ordering, decimal consumers, lifecycle, checked/runtime/Error causes and Arrow cleanup.

An ARM64/JDK 17 release+mimalloc diagnostic on Flink 2.2.1 compares an INT identity through direct
dispatch with a reflective wrapper invoking the same generated evaluator. It includes C Data
import/export, argument/result conversion and result release, but excludes SQL/source/sink execution.
Each batch size has three warmups and five alternating measured trials of 500 batches:

| Rows/batch | Reflective ns/batch | Direct ns/batch | Reflective allocated bytes/batch | Direct allocated bytes/batch |
| --- | ---: | ---: | ---: | ---: |
| 1 | 18,898 | 17,923 | 34,918 | 34,407 |
| 128 | 26,492 | 16,375 | 37,320 | 37,207 |
| 1,024 | 59,228 | 19,624 | 63,984 | 63,912 |

Allocation savings are small; the larger-batch win is dispatch and argument packing. This reference
comparison predates the Arrow-backed input view and includes a wrapper call; it is not an
end-to-end speedup claim. The unchanged SQL
benchmark separately compares main's actual reflective implementation with direct dispatch:
2M runtime rows, 264-byte Unicode text, NULL every seventh row, dynamic UTF-8/UTF-16LE/windows-1252,
two warmups and five host/native alternating trials per expression, with both row/Arrow transposes.

| Query | Main native seconds | Direct native seconds | Main Flink seconds | Direct Flink seconds |
| --- | ---: | ---: | ---: | ---: |
| Source-matched identity | 1.056 | 1.124 | 0.734 | 0.744 |
| Dynamic ENCODE | 2.722 | 2.629 | 1.434 | 1.406 |
| Dynamic DECODE | 2.017 | 2.009 | 1.219 | 1.264 |

ENCODE improves 3.4% in this sample; DECODE is essentially unchanged. Control drift limits the
precision of whole-job attribution, and both expressions remain slower than stock Flink. The small
direct dispatch path is retained for its isolated larger-batch benefit. These historical results
precede the borrowed input and cached timestamp writer measurements below.

```sh
SF_BENCHMARK=true mvn test -Pbench -pl streamfusion-runtime -am \
  -Dtest=GeneratedExpressionDispatchBenchmark -Dsurefire.failIfNoSpecifiedTests=false
SF_BENCHMARK=true mvn test -Pbench -pl streamfusion-runtime -am \
  -Dtest=ScalarFunctionBenchmark -Dsurefire.failIfNoSpecifiedTests=false \
  -Dscalar.functions=ENCODE_DYNAMIC,DECODE_DYNAMIC -Dscalar.unicode=true -Dscalar.nullEvery=7
```

## Borrowed inputs and cached timestamp writers

Timestamp results use one validated accessor per result batch, and row-to-Arrow timestamp writers
reuse one accessor for their lifetime. The accessor retains vector objects, not buffer addresses,
so vector growth and reset remain safe. This removes repeated timestamp schema reconstruction,
metadata-map comparisons, and child-vector lookup from the row loop without changing timestamp
precision, negative epochs, timezone interpretation, or NULL handling.

A JDK 17/x86-64 release+mimalloc profile of Boolean and timestamp TRY_CAST found repeated timestamp
schema and child-vector lookup alongside Flink's general date parser. The initial 2M-row timestamp
experiment reduced median native time from 4.329 s to 3.691 s (14.7%); stock Flink measured 3.413 s
and 3.418 s respectively. Borrowed inputs alone measured 4.277 s, so most of this gain comes from
cached timestamp writers. The baseline used five alternating trials; the two diagnostic changes
used three, all after two warmups, at parallelism one and a 2 GiB heap with both transposes and the
row source/sink included. The diagnostic native range was 3.672–3.703 s. This remains slower than
stock Flink and does not complete the feature's performance requirement.

A final five-trial repeat with the same configuration measured:

| TRY_CAST target | Before native median (range), s | Updated native median (range), s | Stock Flink median (range), s |
| --- | ---: | ---: | ---: |
| TIMESTAMP(9) | 4.329 (4.312–4.346) | 3.776 (3.764–3.813) | 3.459 (3.443–3.475) |
| TIMESTAMP_LTZ(9) | 4.381 (4.350–4.401) | 3.791 (3.779–3.865) | 3.490 (3.471–3.513) |

Native medians improve 12.8% and 13.5%. The original stock medians were 3.413 s and 3.460 s;
the small control drift does not close the remaining native deficit. The source-matched identity
control measured 0.660 s native versus 0.410 s stock in the final run. The unchanged benchmark's
input alternates canonical timestamp strings; other timestamp spellings and NULL/error behavior
are covered by parity tests, not by this performance measurement.


### String representation regression fix

Full Java CI exposed representation-sensitive regressions when generated string
callbacks borrowed Arrow rows: supplementary Unicode extrema compared UTF-8 bytes
instead of Java UTF-16, constant-folded JSON string identity changed, and dynamic
trim with an empty string could divide by zero in Flink's byte access. String
arguments now use the original typed materialization and Java-backed `StringData`
while retaining direct generated dispatch. Non-string arguments can still borrow
Arrow rows. Existing extrema, JSON identity and trim SQL tests cover these cases.

With this correction, canonical timestamp TRY_CAST remains faster than Flink.
Release+mimalloc, JDK 17/Flink 2.2.1, Core i7-12650H, 2M rows, parallelism one,
2 GiB heap, two warmups and five alternating trials, row source/sink and both
transposes give:

| Query | Flink median (range), s | Native median (range), s | Flink/native |
| --- | ---: | ---: | ---: |
| STRING to TIMESTAMP(9) | 3.406 (3.392–3.434) | 0.632 (0.625–0.643) | 5.386x |
| STRING to TIMESTAMP_LTZ(9) | 3.500 (3.457–3.538) | 0.656 (0.652–0.667) | 5.336x |
| String identity control | 0.407 (0.393–0.430) | 0.482 (0.465–0.510) | 0.846x |

These results supersede the borrowed-string input measurements for this fixture;
they do not establish a win for every generated string callback.

The focused regression suite passes all 109 checks on Flink 2.2.1 and 93 on
Flink 1.18.1, with 16 documented host-capability skips and no failures.

## Decimal runtime-scale consumers

A release CPU profile of `CAST(ROUND(d, scale_column) AS STRING)` attributed about 40% of
inclusive samples to the scalar callback and about 10% to decimal Arrow access. Generated
expressions without character arguments now read a borrowed Arrow row during the synchronous
callback, avoiding temporary argument columns. Character arguments retain materialization to
preserve Java string semantics. Wide decimals retain their existing `BigDecimal` directly
instead of converting through an unscaled byte array and a second `BigInteger`.

For a standalone STRING cast of ROUND/TRUNCATE with direct decimal and INT column arguments,
a native batch kernel rounds the Arrow decimal integers and formats each row using its resulting
precision and scale. This avoids the decimal Arrow-to-Java conversion, `BigDecimal` rounding,
and JNI callback for ordinary positions. A non-NULL position below -38 sends the whole batch
through the existing generated evaluator, preserving released scale errors and row order.
That evaluator uses a single equivalent `setScale` for ordinary positions. Multiple potentially
failing projections and other expression shapes retain generated evaluation and its ordering. Floating-point TRUNCATE
retains the released implementation: a separate helper experiment did not improve whole-job time.

Linux x86-64 Core i7-12650H, JDK 17, Flink 2.2.1, release+mimalloc, 2 GiB heap,
2M runtime rows, parallelism one, default 1,024-row batches, no injected NULLs, two warmups
and five alternating trials per engine; both row/Arrow transposes and the row sink remain.
Reproduce with `ScalarFunctionBenchmark#individualFunctions`,
`-Dscalar.functions=DECIMAL_ROUND_RUNTIME_STRING,DECIMAL_TRUNCATE_RUNTIME_STRING`,
`-Dscalar.rows=2000000 -Dscalar.warmup=2 -Dscalar.runs=5` under `-Pbench` with
`SF_BENCHMARK=true` and `-Dsf.extraJvmArgs=-Xmx2g`.

The native text kernel, under the same release setup, gives these two-million-row results.
The decimal input is DECIMAL(38,9); positions cycle through -3/0/2/9/12/NULL, with no
additional input-value NULL injection. Values are read from the runtime row source, and the
benchmark asserts native Calc and both boundary transposes. All five measured trials are retained.

| Expression | Published callback native | Native text kernel, median (range) | Flink, median (range) |
| --- | ---: | ---: | ---: |
| Runtime ROUND to STRING | 0.938 | 0.634 (0.624–0.648) | 0.690 (0.682–0.697) |
| Runtime TRUNCATE to STRING | 0.936 | 0.635 (0.616–0.701) | 0.694 (0.691–0.702) |

This reduces native time by about 32% against the published callback implementation and by
8.0%/8.5% against stock Flink in this run. The identity control was 0.479 native / 0.312 Flink;
the previous control was 0.466 / 0.305. The TRUNCATE native range includes its slower 0.701 s
trial. These figures do not establish performance for nested consumers or batches containing
exceptional positions, which still use the callback. Reproduce using the command above with
`-Dscalar.nullEvery=0`; [raw trials](../benchmarks/decimal-runtime-text-2026-09-28.csv) include
both identity controls and every alternating measured trial.

A five-million-row follow-up with the same configuration confirms the direct STRING
consumer improvement over stock Flink beyond the initial two-million-row measurement:

| Expression | Native median (range), s | Flink median (range), s |
| --- | ---: | ---: |
| Runtime ROUND to STRING | 1.458 (1.451–1.492) | 1.605 (1.565–1.610) |
| Runtime TRUNCATE to STRING | 1.463 (1.448–1.464) | 1.576 (1.569–1.593) |

Native elapsed time is 9.1% and 7.2% lower, respectively. The identity control remains
slower natively: 1.054 s (1.053–1.064) versus Flink's 0.647 s (0.640–0.658); it is retained
in the raw data, and no conversion cost is subtracted from expression timings. Use
`-Dscalar.rows=5000000` to reproduce. Both sizes completed without detected competing
build/test processes. The attempted 20M runs overlapped other builds and were discarded;
these results make no claim for that size or for other expression shapes.

## Complete temporal Calc batches

Released Flink 2.2.1 and 1.18.1/JDK 17, release+mimalloc, 2M runtime rows, precision 9/UTC,
parallelism one, 2 GiB heap and 1,024-row batches. Each fixture rotates stock Flink, the actual
preceding native callback, current direct batches and explicit fallback in one JVM, with two
warmups and five measured trials per route. Both row/Arrow transposes, production IPC, intrinsic
JNI/state work and the rowwise blackhole sink remain measured. No conversion cost is subtracted.

The 16 standalone identity/mixed/text/epoch fixtures take **11.2–31.0% less time than the preceding
callback**. Mixed ordinary-ID/timestamp projections improve **20.5–23.3%**. Identity and epoch
remain slower than stock Flink; the text fixtures beat stock. Correct coverage remains enabled
under the accepted UDF exception, without a claim of universal acceleration.

| Flink | Cardinality | Projection | Stock, s | Previous callback, s | Direct, s | Fallback/control, s | Less time vs previous |
| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: |
| 2.2.1 | 8 | identity | 0.626486 | 0.876332 | 0.655840 | 0.621821 | 25.2% |
| 2.2.1 | 8 | mixed | 0.784143 | 1.061744 | 0.835392 | 0.792814 | 21.3% |
| 2.2.1 | 8 | text | 0.984974 | 0.999364 | 0.870664 | 0.987015 | 12.9% |
| 2.2.1 | 8 | epoch | 0.482180 | 0.610077 | 0.509908 | 0.479378 | 16.4% |
| 2.2.1 | 8 | pipeline | 0.821139 | 0.931297 | 0.931894 | 0.835589 | -0.1% |
| 2.2.1 | 8 | perimeter | 0.548423 | 0.609138 | 0.603545 | 0.608395 | 0.9% |
| 2.2.1 | 16,384 | identity | 0.601491 | 0.889786 | 0.728874 | 0.638969 | 18.1% |
| 2.2.1 | 16,384 | mixed | 0.747300 | 1.116912 | 0.885804 | 0.726614 | 20.7% |
| 2.2.1 | 16,384 | text | 0.912428 | 0.944967 | 0.829954 | 0.907935 | 12.2% |
| 2.2.1 | 16,384 | epoch | 0.489568 | 0.633479 | 0.519975 | 0.488483 | 17.9% |
| 2.2.1 | 16,384 | pipeline | 0.890517 | 1.148790 | 1.125059 | 0.893769 | 2.1% |
| 2.2.1 | 16,384 | perimeter | 0.563010 | 0.624952 | 0.620714 | 0.629874 | 0.7% |
| 1.18.1 | 8 | identity | 0.615707 | 0.916241 | 0.678095 | 0.608433 | 26.0% |
| 1.18.1 | 8 | mixed | 0.706688 | 1.001359 | 0.768459 | 0.712445 | 23.3% |
| 1.18.1 | 8 | text | 1.012707 | 1.075385 | 0.923865 | 1.004198 | 14.1% |
| 1.18.1 | 8 | epoch | 0.517560 | 0.668971 | 0.555390 | 0.516121 | 17.0% |
| 1.18.1 | 8 | pipeline | 0.849500 | 0.977936 | 0.976542 | 0.830228 | 0.1% |
| 1.18.1 | 8 | perimeter | 0.546435 | 0.647929 | 0.645330 | 0.656894 | 0.4% |
| 1.18.1 | 16,384 | identity | 0.610417 | 0.980038 | 0.676501 | 0.613570 | 31.0% |
| 1.18.1 | 16,384 | mixed | 0.734832 | 1.094801 | 0.870739 | 0.743405 | 20.5% |
| 1.18.1 | 16,384 | text | 0.965445 | 0.981860 | 0.871530 | 0.962128 | 11.2% |
| 1.18.1 | 16,384 | epoch | 0.513625 | 0.645747 | 0.543074 | 0.514911 | 15.9% |
| 1.18.1 | 16,384 | pipeline | 0.798026 | 1.045632 | 1.103815 | 0.877072 | -5.6% |
| 1.18.1 | 16,384 | perimeter | 0.513575 | 0.544080 | 0.553816 | 0.550550 | -1.8% |

`identity` projects TIMESTAMP/LTZ UDF results; `mixed` retains an ordinary ID beside them;
`text` formats TIMESTAMP and `epoch` returns LTZ milliseconds. `pipeline` feeds the timestamp
UDF into native GROUP BY/COUNT and asserts work in both operators. `perimeter` projects the
same source timestamp columns without UDFs. Its false-option route remains native because it
contains no temporal UDF; the other false-option routes assert whole-query stock fallback.

Grouped jobs show **no consistent throughput improvement**. The Flink 1.18/high-cardinality
median is 5.6% higher than the preceding callback, and its trial ranges overlap
(previous 1.037–1.116 s, current 1.029–1.157 s). All four grouped comparisons have overlapping
ranges. Retain this unfavorable observation; the standalone conversion win does not establish
a pipeline speedup. The source-matched perimeter controls also do not establish an improvement.

These are complete-job observations on a shared 16-logical-CPU host, not an isolated-machine
claim. One-second samples record other Java/Doris activity, memory and load. Retained final
fixtures have no observed external compiler processes or outside-process churn and less than
two outside logical CPUs per sample. This does not remove startup/JIT variation, and small
differences remain inconclusive. Every warmup and measured trial, earlier prototype, rejected
overlap attempt, min/max range, route counter and exact command is retained in the
[result manifest](../benchmarks/results/temporal-direct-calc-2026-10-22.json),
[raw trials](../benchmarks/results/temporal-direct-calc-2026-10-22-trials.csv) and
[compressed host observations](../benchmarks/results/temporal-direct-calc-2026-10-22-host.json.gz).
Each retained current route consumes all 2M input rows directly; both identity columns share
timestamp buffers after every UDF invocation. The preceding route uses the actual JNI callback,
not a synthetic reflective wrapper.

Temporal UDF execution invokes a complete generated Calc directly against borrowed
Arrow input columns and returns flat Arrow output columns. This avoids exporting Java Arrow
buffers to Rust merely to import them back into the same JVM for the whole Calc, then performing
the reverse handoff for the result. The verified input mapping travels with the serialized,
task-local UDF binding. Changelog-tagged batches and other argument shapes retain the established
native callback. Timestamp inputs can be mixed with the already-verified basic numeric/BOOLEAN
columns, retaining their task-local input mapping. This is columnar Java execution inside an
island, not Rust UDF computation.
Result schemas requiring representation metadata retain the callback. The
[coverage page](../operators/calc-filter.md#temporal-scalar-conversion) describes the verified
boundary and the existing system-property controls, which now default to true.

For unconditional timestamp-only output with matching width, exact per-row value comparison
permits sharing an unchanged input column. Functions always execute in the original order;
this does not infer purity or skip calls. The first different value allocates a destination,
copies the equal prefix and writes the rest. Arrow transfers target preinitialized fields so
component metadata survives, and retain buffers before the input closes.

Release+mimalloc, Flink 2.2.1/JDK17, i7-12650H, 2 GiB heap, p1, precision9, 1,024-row batches,
2M runtime rows, two warmups and five alternating measured pairs, production IPC, row source
and blackhole sink, and both transposes:

| Projection | Stock median (range), s | Direct batch median (range), s |
| --- | ---: | ---: |
| Identity | .645 (.640–.718) | .651 (.640–.736) |
| Text | .969 (.954–.973) | .830 (.826–.849) |
| Epoch | .570 (.561–.574) | .536 (.535–.544) |

The counters verify 14M rows consumed directly per case and 28M timestamp values sharing buffers
in identity, including warmups. Text and epoch improve against their paired controls; identity
remains about 1% slower. Cross-JVM control drift and the missing final repeat against the previous
correct fallback limited that historical experiment. Direct execution and output sharing now
default on under the accepted UDF performance exception. The semantic support shipped in [PR #368](https://github.com/datafusion-contrib/StreamFusion/pull/368);
[issue #389](https://github.com/datafusion-contrib/StreamFusion/issues/389) delivers the default direct path and the comparisons above; conversion and perimeter costs remain measured limits.

### Java and Rust profiling evidence

Profile both sides before choosing another optimization. Released async-profiler 4.0 captures
Java CPU and allocations together with symbolized native stacks. Hardware cycle counters were
unavailable on this WSL host; the supported perf `cpu-clock` software event supplies CPU samples.
DWARF unwinding uses the production DSO's unwind tables and symbol table. Java runs with
`-XX:+UnlockDiagnosticVMOptions -XX:+DebugNonSafepoints`.

Each profile runs the same 5M-row identity query, two warmups and five paired repetitions. The old
callback records 7,621 StreamFusion task CPU samples; direct batches record 5,297. Stock controls
record 5,073 and 5,162 respectively. These include startup and warmup and are diagnostic sample
counts, not speedup measurements. Deepest observed stages show 1,072 old Calc-dispatch samples
plus 1,187 callback samples outside generated evaluation. The direct path has 32 dispatch samples,
112 direct setup/writer samples and 188 alias-loop samples. Generated evaluation remains similar
(581 versus 593 samples). Rust symbols resolve Arrow import/export, schema/layout construction,
reference release, allocator and JNI work. Inclusive native frames include Java callbacks and must
not be interpreted as Rust self time.

Weighted allocation samples estimate 38.9 GB versus 35.4 GB of native-job heap pressure over all
35M rows per profile; these are sampled estimates, not exact allocated or retained bytes. The
improved path still reconstructs TimestampData at callback input reads (about 1.49 GB sampled)
and Arrow exit serialization (about 1.53 GB). Those concrete sites are the next profiling targets;
sharing output buffers alone does not remove timestamp object reconstruction. Most remaining
CPU samples belong to Java row-source conversion, field getter dispatch and the row perimeter.

The [raw results](../benchmarks/results/temporal-udf-2026-10-15.json) include hashes, full compressed
CPU and allocation stacks for both variants, stage counts, leading allocation sites and limitations.
Profiler startup writes `Profiling started` to the native stream, which produces a Surefire
channel warning; each profiled test still completes successfully. Profiled times are excluded
from the table above.

To reproduce, use the official released async-profiler archive and start the benchmark fork with:

```text
-agentpath:<profiler>/lib/libasyncProfiler.so=start,event=cpu-clock,interval=2000000,cstack=dwarf,threads,alloc=1m,file=<output>.jfr
```

Compile the fixture first, then run `mvn -o surefire:test@default-test -Pbench -pl streamfusion-runtime`
with `SF_BENCHMARK=true`, `-Dtest=TemporalUdfBenchmark`, `-Dtemporal.cases=identity`,
`-Dtemporal.rows=5000000`, `-Dtemporal.warmup=2`, `-Dtemporal.runs=5` and the JVM options through
`sf.extraJvmArgs`. The direct variant adds both experimental system properties. Convert the JFR
with `jfrconv --cpu --threads -o collapsed` and `jfrconv --alloc --threads --total -o collapsed`.
Using the explicit Surefire execution prevents an unrelated empty fork from overwriting a fixed
recording filename. Run unprofiled measurements separately before making admission decisions.


### Temporal benchmark isolation correction (2026-10-16)

The temporal fixture now closes its metrics cluster after each job, checks native work
for each native job, and rejects native execution in the stock control. Earlier trials
shared one cluster across all jobs in a case. This corrects resource lifetime; it does
not establish that retained history caused any previous regression. Cluster construction
and plan checks remain outside the clock; complete `executeSql().await()` jobs retain
the row source, both transposes, conversions and row sink inside it.

Fresh Flink 2.2.1/JDK 17 release runs use 2M rows, precision 9, production IPC, 2 GiB,
two warmups and five alternating measured pairs. No competing Java/Rust build or job
activity was observed. Median elapsed seconds are:

| Case | Cardinality | Stock Flink | StreamFusion |
| --- | ---: | ---: | ---: |
| Identity | 8 | 0.617 | 0.659 |
| Text | 8 | 0.961 | 0.869 |
| Epoch | 8 | 0.495 | 0.520 |
| Identity | 16384 | 0.634 | 0.746 |
| Text | 16384 | 0.939 | 0.887 |
| Epoch | 16384 | 0.491 | 0.592 |

[All trials, counters and configuration](../benchmarks/results/temporal-udf-per-job-2026-10-16.json)
are retained. Text conversion wins at both cardinalities, but identity and epoch still
regress. These measurements supersede earlier figures for the current fixture; they
are not a repeat against the previous fallback or the second supported Flink line.
The temporal semantic implementation has merged in PR #368; all temporal experiments remain off by default.


The same corrected fixture has separate CPU/allocation profiles for identity, epoch
and a projection-only Rust perimeter control. The linked data includes full compressed
stacks. Identity and epoch recordings had no observed external work; the perimeter
recording overlapped another Java process and remains diagnostic only. All profile
timings are excluded. The direct temporal cases execute generated Java against Arrow
columns, so Rust projection samples occur in the perimeter control instead.

Allocation stacks still identify timestamp reconstruction in Arrow input reads;
identity also reconstructs at output conversion and Flink's copying chain. Whole-process
CPU profiles include cluster setup and substantial JIT work, and cannot alone establish
a steady-state CPU bottleneck. These observations identify investigation targets, without
claiming an optimization or enabling a slower path.


## Complete generated Calcs with primitive inputs

When the complete Calc already uses Flink's generated evaluator, no native expression remains
outside that program. Insert-only batches with primitive numeric/boolean arguments and ordinary
nonempty numeric/boolean/character outputs now borrow the input vectors directly and write a flat Arrow
result in the existing columnar operator. This avoids the Java-to-Rust-to-Java batch handover,
the outer nullable struct writer, and exporting/importing the result merely to expose its fields.
The predicate and projections still execute together, one row at a time, using the same task-bound
function instances and lifecycle. Rejected rows are omitted immediately. Result ownership remains
independent of the consumed input, and failures close partial results.

This follows the existing borrowed-row evaluation contract. Unlike Comet's general scalar
expression callback, the complete generated program has no remaining native expressions to
compose; see the [boundary decision](https://github.com/datafusion-contrib/StreamFusion/blob/main/divergences/47-complete-jvm-calc-without-native-roundtrip.md).
String arguments, represented values, binary/decimal/temporal outputs and batches carrying
RowKind metadata retain the previous bridge. Expression and signature admission are unchanged.
Shared stateful UDFs now default on under the accepted UDF performance exception; the historical
comparison below predates that default change. The temporal experiments keep their separate eligibility.

The release/mimalloc comparison uses main `4222bc0c`, released Flink 2.2.1, JDK 11.0.27 for
both compilation and runtime,
Apple M4 Pro ARM64, a 2 GiB heap, parallelism one, five million rows, two warmups and five
alternating engine pairs per cohort. Complete `executeSql(...).await()` jobs retain the runtime
row source, both transposes and rowwise blackhole sink. Explain-plan checks and environment
construction are outside the clock. Cohorts run serially in separate JVMs; no other audit build
or test runs during these measurements. Timings include startup and are not isolated kernel costs.

| Two shared calls | Main opt-in (s) | Direct opt-in (s) | Main paired Flink (s) | Direct paired Flink (s) | Main default fallback (s) |
| --- | ---: | ---: | ---: | ---: | ---: |
| INT | 0.736596 | 0.527483 | 0.433012 | 0.403507 | 0.407535 |
| BIGINT | 0.765150 | 0.523923 | 0.457788 | 0.446857 | 0.447063 |
| STRING | 1.577065 | 1.416182 | 1.003320 | 0.957119 | 0.851145 |

These are medians. The opt-in reduction is about 28%, 32% and 10%, respectively; paired stock
controls drift, particularly between the separate default-fallback and STRING cohorts. Every
measured array is retained in the [raw results](../benchmarks/results/shared-stateful-direct-calc-2026-10-18.json).
The headline uses the final-source JDK 11 cohort; earlier candidate and final-source JDK 17-compiled
diagnostics remain labeled separately. The final direct trials range from 0.516-0.556 s for INT,
0.522-0.540 s for BIGINT and 1.389-1.433 s for STRING. Their paired stock ranges are 0.394-0.433 s,
0.432-0.464 s and 0.953-0.969 s, respectively.
The direct path remains 31%, 17% and 48% slower than its paired stock controls. These historical
results improve the preceding opt-in implementation. Verified UDF execution now defaults on
under the accepted UDF performance exception; it does not require a standalone speedup.

```sh
SF_BENCHMARK=true mvn -Pbench -pl :streamfusion-runtime -am test \
  -Dtest=SharedStatefulScalarBenchmark -Dsurefire.failIfNoSpecifiedTests=false \
  -Dsharedscalar.rows=5000000 -Dsharedscalar.warmup=2 -Dsharedscalar.runs=5 \
  -Dsharedscalar.expectDirect=true -Dsf.extraJvmArgs=-Xmx2g
```

Use the same benchmark source in each worktree; its additional switches and assertions are
test-only. Historical checkouts whose shared-stateful default was false also need
`-Dsharedscalar.enabled=true`. On the checkout preceding direct evaluation, omit
`sharedscalar.expectDirect` to measure its opt-in bridge. To reproduce the stock-fallback control,
pass `-Dsharedscalar.enabled=false -Dsharedscalar.expectNative=false`. The benchmark asserts the native plan, both transposes,
consumed row counts and the direct-evaluation counter for the candidate.


Shared stateful and temporal UDF coverage now defaults on under the maintainer-approved UDF
performance exception. The earlier opt-in/fallback controls above retain their original scope;
they establish optimization effects, not a faster-than-Flink claim. The alternative direct-temporal
and timestamp-alias execution experiments retain their own defaults and type guards.


### Generated batch loops for shared stateful calls

Complete JVM Calcs with the existing direct primitive-input eligibility and numeric/boolean
results enter their generated evaluator once per Arrow batch. The generated class owns the row
loop and calls its existing row evaluator in order, writing each accepted result before evaluating
the next row. This removes repeated interface dispatch through the shared evaluator wrapper
without splitting predicates from projections or changing function instances, lifecycle, NULLs or
exception handling. Older serialized evaluators use the existing row loop. The generated batch
method reports completion even when every row is filtered, preventing a second evaluation of
stateful calls. STRING-result programs keep their existing direct row loop because the measured
batch-loop prototypes did not establish a repeatable STRING benefit. Changelog batches and other
layouts retain their existing JNI route. Signature admission and configuration defaults are unchanged.

The bounded #398 investigation compares main `c5fd8dd1` with this change on Linux x86-64,
JDK 17, released Flink 2.2.1 and 1.18.1, release+mimalloc, parallelism one, a 2 GiB heap and
1,024-row batches. Each final JVM cohort runs five million runtime-source rows per job, two warmups
and five alternating stock/native pairs for INT, BIGINT and STRING. The preceding and current
implementations run serially, with identical benchmark sources and native libraries; compiled
production classes select the revision. Every complete job retains both transposes and the rowwise
blackhole sink. The benchmark verifies actual Calc consumption and the exact direct-evaluation
row-count delta for every native job, leaving the shared-stateful option at its default.
No external Java/Rust/C build or javadoc processes were observed during these final cohorts.

| Flink | Two shared calls | Previous native (s) | Current native (s) | Previous paired stock (s) | Current paired stock (s) |
| --- | --- | ---: | ---: | ---: | ---: |
| 2.2.1 | INT | 0.616466 | 0.609197 | 0.443351 | 0.446468 |
| 2.2.1 | BIGINT | 0.601337 | 0.567549 | 0.475448 | 0.494892 |
| 2.2.1 | STRING | 1.334913 | 1.290989 | 1.124278 | 1.090521 |
| 1.18.1 | INT | 0.630854 | 0.627694 | 0.454156 | 0.452564 |
| 1.18.1 | BIGINT | 0.596902 | 0.576945 | 0.462691 | 0.482146 |
| 1.18.1 | STRING | 1.285901 | 1.293444 | 1.098261 | 1.112885 |

These are five-trial medians. BIGINT improves about 5.6% on Flink 2.2 and 3.3% on 1.18; INT changes
by about 1.2% and 0.5%, with overlapping ranges. Paired stock controls drift, including about 4%
for BIGINT, so these are modest complete-job observations rather than isolated kernel estimates.
STRING retains the preceding evaluation loop; its timing differences should not be attributed to
an optimized STRING kernel. Every native median remains slower than stock Flink. Default UDF
admission follows the accepted performance exception, not a standalone speedup claim.

Separate one-million-row comparisons use 64-row batches, NULL every seventh input, two warmups
and three alternating engine pairs on both Flink lines. They verify the same default route and
retain previous/current/stock trials and variability in the results artifact. Earlier all-result
batch-loop comparisons and a small-batch cohort that overlapped external Java builds, including
a 14.146 s stock outlier, remain labeled diagnostic; no unfavorable measurements are removed.

Separate async-profiler 4.0 CPU/allocation recordings use ten million rows, two warmups and three
pairs, a 1 ms software CPU timer, DWARF stacks and 512 KiB Java allocation sampling. Among stacks
inside direct Calc evaluation, sampled interface-dispatch leaves are 392 in the baseline and 207
in the retained variant. Java allocation estimates remain roughly unchanged at 16.94 versus
16.82 GB. These are sampled observations and allocation estimates, not exact allocation requests,
copied bytes or an additive cost breakdown. Profiles include both engines, warmup, setup and JIT;
profiled durations are excluded from performance claims. Earlier diagnostic recordings overlapped
external Java work. Inlined UDF work can be charged to generated evaluator frames, so sampled
stacks do not cleanly separate useful user-function execution from argument/result boxing.

Two broader prototypes were removed: an all-result batch loop did not establish a repeatable
STRING benefit, and embedding the generated row body directly into the loop added no convincing
complete-job improvement or allocation reduction. The retained numeric/boolean specialization
follows Comet's generated batch-kernel structure while keeping Flink's existing row evaluator and
StreamFusion's Arrow writer. Broad buffer pooling or ownership changes remain outside this bounded
investigation. The separate [bounded JNI/Arrow investigation](calc-schema-once-c-data.md#cache-calc-output-schemas)
retains an output-schema cache and records why broader ownership changes remain deferred.

Both released lines pass 223 focused checks plus six isolated-worker cases without planner or
Calcite classes. Coverage includes default and explicit-false routes, shared and independent
instances, predicate/projection ordering, nested and conditional calls, nullable INT/BIGINT/STRING
results, empty/all-filtered batches followed by later matches, all changelog tags, lifecycle,
checked/runtime/error failures, sliced input and independently owned outputs. Worker class-loading
provenance confirms the measured runtime loads the selected compiled production classes.

All measured timing trials, diagnostic profiles' summaries, source/library hashes, commands,
rejected prototype patches and validation results are retained in
[the #398 results](../benchmarks/results/shared-stateful-scalar-2026-10-21.json).
The benchmark accepts `sharedscalar.types`, `sharedscalar.batchRows` and `sharedscalar.nullEvery`
for focused and nullable-input comparisons. Set `sharedscalar.enabled=false` together with
`sharedscalar.expectNative=false` for the stock-fallback control; otherwise leave the option unset
and use `sharedscalar.expectDirect=true` to require the default direct route.
