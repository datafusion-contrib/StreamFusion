# Columnar flow with transposes only at boundaries

**Applies to:** the plan-transition pass between rowwise and columnar operators

Rather than fusing operator subtrees, each operator is tagged rowwise or columnar; columnar
operators flow Arrow batches into one another and a row↔Arrow transpose is inserted only where a
columnar operator meets a rowwise one. The conversion is paid once at the region's edge, never
inside a chain.

This was the change the first end-to-end benchmarks demanded: a lone native operator paid two
conversions per batch and ran below Flink (filter 0.58x, window 0.81x); a fully-columnar Parquet
copy runs 3–5x.
