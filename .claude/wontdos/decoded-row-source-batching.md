# Decoded-row source batching

Mailbox-level row batching and wrapping the stock decoded BulkFormat both retained
rowwise writes into Arrow. Neither established complete-job acceleration over
stock Flink on Q6/Q14/Q15, so both implementations were removed. Their
[retained trials](../../docs/benchmarks/results/tpch-rejected-row-source-batching-20261110.json)
are separate cohorts with varying shared-host load. The native Parquet follow-up
reuses the existing decoder to avoid row materialization altogether. Do not
restore these row-batching adapters merely because they executed native compute.
