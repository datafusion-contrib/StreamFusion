# Calc projection before filtering

**Applies to:** native Calc operators with a condition and projection

Flink's generated Calc evaluates its condition before constructing the projected output row. The
native columnar equivalent first narrows an input batch to the columns referenced by its projection,
then applies the condition's selection vector to that narrow batch. Condition-only columns and other
unused fields are not copied through Arrow's filter kernel merely to be discarded by the projection.

This matters most after a shared wide source, where several selective branches read different nested
fields from the same decoded batch. Projection expressions are remapped once when the Calc is
compiled, so the per-batch path remains evaluation plus Arrow kernels rather than planner work.

For native programs without JVM bindings, the Java C Data entry also exports only
referenced top-level columns and the hidden changelog column. Input references are
remapped once per input schema. The export wrapper borrows the original vectors;
it does not copy payloads or own their release. JVM-bound programs retain their
original positional schema. This avoids exporting unused wide structs when a
columnar watermark assigner hides the source from planner projection pushdown.
The existing Rust projection-before-filter path remains unchanged.

An empty Calc result is closed at the operator boundary. It does not enter downstream
window/exchange operators, where an empty fragment would still allocate transport and
accumulator wrappers. Watermarks continue through their independent callback. This applies
to both native and direct JVM evaluation; inputs and retained nonempty outputs keep their
existing ownership. Both transport changes were measured together below.

The combined transport changes pass an isolated Linux release Q8 comparison at
200k events, four source/sink tasks, heap state and mini-batching off. Two paired
passes in both orders give six timed trials per engine/variant: native medians
7,824 -> 6,910 ms (1.13×), Hudi-disabled SF 8,385 -> 7,182 ms. Stock in the
candidate runs is 1,890 ms, so native remains 3.66× slower. This complete-job gain
does not isolate the two transport changes or qualify the two-million-event
matrix. [All 72 Q8/Q12 observations](../benchmarks/hudi-calc-transport-20261114.json)
retain ranges, library/source hashes and unfavorable results.

The release-mode Criterion A/B over a 4,096-row Q3-shaped event batch measured the filtering kernel
at 4.01 microseconds for the former full-schema path and 1.18 microseconds after pruning projection
inputs, a 3.39x operator-level speedup. Two clean 2-million-event exactly-once Kafka Q3 reruns put
StreamFusion at 1.338 and 1.429 seconds versus Flink at 1.363 and 1.623 seconds respectively. The
whole-job variance is larger than the expected Calc gain, so this is not presented as a new
end-to-end headline result.

## Deferred-copy investigation

The investigation for [#249](https://github.com/datafusion-contrib/StreamFusion/issues/249)
measured local substring gathering after projection pruning. It cut isolated allocations and
copying substantially, but the release end-to-end grid did not establish a reliable speedup;
a controlled repeat improved substring time 2.7% while the unchanged control improved 1.4%.
The extra execution path was rejected. Calc retains the pruned filtering path described above.

The [decision and measurements](https://github.com/datafusion-contrib/StreamFusion/blob/main/.claude/wontdos/249-deferred-calc-filtering.md)
record the microbenchmark, full selectivity/payload grid, controlled repeat and reopening criteria.
The `calc_selection` Criterion diagnostic and `CalcSelectionBenchmark` whole-job harness remain
for reproduction. The investigation also found a COALESCE failure-parity gap; known fallible
operands now use the existing generated Flink evaluator, as documented on the
[Calc coverage page](../operators/calc-filter.md).
