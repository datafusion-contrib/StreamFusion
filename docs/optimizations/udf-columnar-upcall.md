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
