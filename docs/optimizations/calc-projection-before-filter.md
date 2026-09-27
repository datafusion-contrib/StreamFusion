# Calc projection before filtering

**Applies to:** native Calc operators with a condition and projection

Flink's generated Calc evaluates its condition before constructing the projected output row. The
native columnar equivalent first narrows an input batch to the columns referenced by its projection,
then applies the condition's selection vector to that narrow batch. Condition-only columns and other
unused fields are not copied through Arrow's filter kernel merely to be discarded by the projection.

This matters most after a shared wide source, where several selective branches read different nested
fields from the same decoded batch. Projection expressions are remapped once when the Calc is
compiled, so the per-batch path remains evaluation plus Arrow kernels rather than planner work.

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
