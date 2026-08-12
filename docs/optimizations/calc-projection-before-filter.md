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
