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
