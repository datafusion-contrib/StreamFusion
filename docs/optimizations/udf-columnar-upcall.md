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

## Direct generated-expression dispatch

Known, final internal Flink expression evaluators receive each row directly from the materialized
argument columns. They populate the same reusable internal row and invoke the same released Flink
generated code, avoiding reflective varargs packing and `Method.invoke` on every row. Generic user
functions retain their resolved reflective call path. This does not replace column materialization,
boxing, Arrow conversion, Flink's string/decimal conversion, or the once-per-batch JNI boundary.
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
comparison includes a wrapper call and is not an end-to-end speedup claim. The unchanged SQL
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
direct dispatch path is retained for its isolated larger-batch benefit; column materialization and
conversion remain future targets requiring their own measurements.

```sh
SF_BENCHMARK=true mvn test -Pbench -pl streamfusion-runtime -am \
  -Dtest=GeneratedExpressionDispatchBenchmark -Dsurefire.failIfNoSpecifiedTests=false
SF_BENCHMARK=true mvn test -Pbench -pl streamfusion-runtime -am \
  -Dtest=ScalarFunctionBenchmark -Dsurefire.failIfNoSpecifiedTests=false \
  -Dscalar.functions=ENCODE_DYNAMIC,DECODE_DYNAMIC -Dscalar.unicode=true -Dscalar.nullEvery=7
```

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
