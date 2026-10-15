# Temporal callback row-output shortcuts

Seven local experiments for [#364](https://github.com/datafusion-contrib/StreamFusion/issues/364)
were rejected after release end-to-end measurements on 2026-10-15. This rejects only these
shortcuts; temporal scalar conversion and its performance work remain open.

The complete-Calc generated evaluator already borrows non-string Arrow inputs and batches JNI
callbacks, following Comet's batch bridge. Flink's released LocalDateTime/Instant converters
preserve TIMESTAMP calendar fields and LTZ instants, including full-range fractions. A valid
26-second JFR of the 5M-row identity fixture found 82 execution samples containing native Java
classes: 26 had ArrowWriter.write and 23 RowWriter.doWrite as their top frame. Many sampled
StreamRecord allocations also appeared. Inclusive stacks overlap; this is a diagnostic, not an
additive timing or exact allocation/copy breakdown.

1. Writing result StructVector children directly removed an outer GenericRowData wrapper and
   RowWriter. The 89-case focused suite passed on both released Flink versions, but 2M-row
   native identity/text/epoch medians were 0.886/1.003/0.687 s, versus the original prototype's
   0.878/1.003/0.675 s. The experiment was reverted.
2. Reusing the exit StreamRecord through Flink's TimestampedCollector passed temporal,
   transpose and synchronous-input checks. Native medians worsened to 1.005/1.049/0.737 s.
   The experiment was reverted.
3. Extending the generated binary exit projection to TIMESTAMP and TIMESTAMP_LTZ passed
   50 focused checks on Flink 2.2.1, including precision 0/3/6/9, full-range timestamps,
   NULLs, all row kinds and ownership across two closed batches with reuse on/off.
   The 2M-row identity median remained 0.868410 s native versus 0.521488 s stock.
   Control drift prevents claiming an improvement over the original prototype. Reverted.
4. Generating the batch result loop with one unrolled child-writer call per output column
   retained Flink Calc evaluation and immediate row-major writes, and passed all 90
   focused checks on Flink 2.2.1. Native identity/text/epoch medians were
   0.930478/0.966846/0.638500 s versus paired stock 0.605787/0.956217/0.549004 s.
   None beats stock; the modest text/epoch change does not isolate an optimization from
   control drift. Reverted.

5. Appending timestamp component data directly and filling both non-null component validity
   buffers once at finish passed 69 checks on Flink 2.2.1, including growth to 8,193 rows,
   reset, sliced input ownership, NULLs and primitive timestamp handling. Native
   identity/text/epoch medians were 0.841686/0.949466/0.627721 s versus stock
   0.581088/0.932850/0.524120 s. All still lose to stock. Reverted.

6. Embedding Flink's Calc process code, per-record code and locals directly in the generated
   batch loop removes the remaining per-row evaluator call from experiment 4. It passed
   all 90 focused checks on Flink 2.2.1. Native identity/text/epoch medians were
   0.884336/0.995430/0.652057 s versus stock 0.583994/0.933870/0.559680 s.
   No UDF case beats stock; the same-run projection-only control is retained too. Reverted.

7. A typed batch kernel follows Comet's direct Arrow result pattern, using the released
   Flink expression generator and the same expressions as ordinary row evaluation. It
   evaluates unconditional projections and writes TIMESTAMP/LTZ, BIGINT and character
   results directly, without publishing values into the reusable result row or invoking
   field writers. The first variant passed all 90 focused checks, but native
   identity/text/epoch medians were 0.860441/1.045273/0.660315 s versus stock
   0.617549/0.967403/0.563940 s. Replacing the timestamp accessor with direct component
   stores passed 31 temporal/ownership checks and an explicit typed-path assertion;
   identity still took 0.835652 s versus stock 0.598743 s. Text/epoch kernel code was
   unchanged by the second variant. Both variants were reverted.

All runs use release+mimalloc, Flink 2.2.1/JDK 17, precision 9, parallelism one, 2 GiB heap,
2M runtime rows, two warmups and five alternating trials, both transposes and the row sink.
Stock controls drift across JVMs, so these numbers do not isolate costs. Retain every sample
in [the raw artifact](../../docs/benchmarks/results/temporal-udf-2026-10-15.json).

Removing wrappers, reusing the exit record, adding a timestamp binary projection and
unrolling the batch result writes, embedding Calc evaluation in that loop and batching
component validity, or generating direct typed output have not established
whole-job acceleration. Further
experiments must address measured conversion or state-layout costs while preserving Flink's
predicate, shared-call and mutable-result ordering, then compare the complete query with
stock and the previous fallback. The opt-in semantic path
keeps stock execution as the default pending performance admission or explicit maintainer
agreement; #364 remains its tracking issue.
