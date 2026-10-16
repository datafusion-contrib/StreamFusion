# Temporal callback row-output shortcuts

Nine local experiments for [#364](https://github.com/datafusion-contrib/StreamFusion/issues/364)
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

The first seven experiments use release+mimalloc, Flink 2.2.1/JDK 17, precision 9, parallelism one, 2 GiB heap,
2M runtime rows, two warmups and five alternating trials, both transposes and the row sink.
Stock controls drift across JVMs, so these numbers do not isolate costs. Retain every sample
in [the raw artifact](../../docs/benchmarks/results/temporal-udf-2026-10-15.json).

8. A bounded 64-entry adaptive cache reuses immutable `TimestampData` values in the timestamp
   reader. Mixed Java/Rust CPU and Java allocation profiles motivated this experiment: the
   direct-batch path still materializes timestamps at callback input and row exit. The cache
   passed three cache tests and 28 focused temporal checks. With 2M rows, one warmup and three
   alternating pairs, identity took 0.763 s native versus 0.606 s stock at cardinality eight,
   and 0.775 versus 0.697 s at cardinality 16,384. A separate 5M-row profile reduced timestamp
   accessor allocation to about 15.7 MB over 25M rows per engine, but cache lookup remained
   visible in CPU samples. Lower allocation did not establish a throughput win. Reverted.

9. Local Arrow batches retained already-read immutable timestamp values, avoiding cache lookup
   and repeated materialization at callback input and row exit. Every UDF still executed;
   runtime counters verified hinted input, output and exit consumption. The vector-reader
   variant eliminated sampled timestamp accessor allocation, but 2M-row identity still took
   0.760/0.731 s native versus 0.686/0.674 s stock at cardinalities eight/16,384. A reusable
   row-reader variant measured 0.673/0.693 s versus stock 0.555/0.580 s. Focused temporal,
   exact-type and failure-release checks passed. Neither reader established admission. Reverted.

Experiments eight and nine retain production IPC, both transposes, row source/sink and the
direct-batch/alias switches from the draft. Controls use the actual preceding prototype at
`42bccae8`, including the same high-cardinality fixture. Stock controls drift between JVMs;
three-pair diagnostics do not isolate exact gains between variants. The artifact retains every
measured array, available warmup lines and four compressed CPU/allocation profiles. One early
vector-reader raw log was overwritten; its measured arrays survive, but its warmups do not.
Profiled durations are excluded from acceleration claims. CPU stacks use DWARF native unwinding;
Java allocation samples do not measure Rust allocations or peak memory.

Removing wrappers, reusing the exit record, adding a timestamp binary projection and
unrolling the batch result writes, embedding Calc evaluation in that loop and batching
component validity, or generating direct typed output have not established
whole-job acceleration. Further
experiments must address measured conversion or state-layout costs while preserving Flink's
predicate, shared-call and mutable-result ordering, then compare the complete query with
stock and the previous fallback. The opt-in semantic path
keeps stock execution as the default pending performance admission or explicit maintainer
agreement; #364 remains its tracking issue.
