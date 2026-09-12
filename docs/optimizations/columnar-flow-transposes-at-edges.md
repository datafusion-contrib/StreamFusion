# Columnar flow with transposes only at boundaries

**Applies to:** the plan-transition pass between rowwise and columnar operators

Rather than fusing operator subtrees, each operator is tagged rowwise or columnar; columnar
operators flow Arrow batches into one another and a row↔Arrow transpose is inserted only where a
columnar operator meets a rowwise one. The conversion is paid once at the region's edge, never
inside a chain.

This was the change the first end-to-end benchmarks demanded: a lone native operator paid two
conversions per batch and ran below Flink (filter 0.58x, window 0.81x); a fully-columnar Parquet
copy runs 3–5x.

Paimon's primary-key sink retains that columnar boundary through first-row, partial-update and
aggregation merges. Pick-style field reducers keep indices into the input Arrow columns; arithmetic,
boolean operations and string concatenation create new scalar values. A gather per column constructs the merged
batch, including nested values, for the native Parquet writer. Released Java Paimon parses options
and owns compaction and commits without receiving input rows on this path.

The [release writer diagnostic](../connectors/paimon.md#merge-engine-writer-diagnostic) measured
1.20–1.27× throughput against stock ingestion for the four new modes, including routing and the
ingress RowData-to-Arrow conversion. Unsupported merge combinations retain the stock writer.
