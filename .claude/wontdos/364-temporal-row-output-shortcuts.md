# Temporal callback row-output shortcuts

Two local experiments for [#364](https://github.com/datafusion-contrib/StreamFusion/issues/364)
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

All runs use release+mimalloc, Flink 2.2.1/JDK 17, precision 9, parallelism one, 2 GiB heap,
2M runtime rows, two warmups and five alternating trials, both transposes and the row sink.
Stock controls drift across JVMs, so these numbers do not isolate costs. Retain every sample
in [the raw artifact](../../docs/benchmarks/results/temporal-udf-2026-10-15.json).

Removing a single wrapper is insufficient. A future experiment should address the typed batch
output loop while preserving Flink's predicate, shared-call and mutable-result ordering, then
measure the complete query against stock and the previous fallback. The opt-in semantic path
keeps stock execution as the default pending performance admission or explicit maintainer
agreement; #364 remains its tracking issue.
