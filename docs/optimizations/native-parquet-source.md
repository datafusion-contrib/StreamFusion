# Native Parquet input batches

Bounded filesystem Parquet scans can reuse the optional decoder already used by
Paimon and feed Arrow batches directly into the native island. This removes Java
row materialization and the source row-to-Arrow write. Flink still owns file I/O,
file discovery and assignment, reader queues and checkpoint serialization. The
collecting sink and Arrow-to-row exit remain in complete-job timings.

The first admission is fixed-parallelism, unpartitioned, physical-only tables with
supported top-level projections and default reader options. Whole-file splits
limit parallelism to file count. Unsupported contracts retain the stock source.
Shared Arrow consumers reuse one source through the existing retained-view share
operator. The shared task-off-heap authority reserves projected row-group workspace
before decode; imported output buffers retain their own allocator accounting.
Cancellation closes queued batches promptly.

A focused SF1 cohort with identical SNAPPY files, SQL, resources, three warmups and
seven alternating trials measured Q6 at 689.94 ms stock / 334.08 ms native (2.065×),
Q14 at 601.05 / 339.47 ms (1.771×), and Q15 at 584.67 / 345.59 ms (1.692×).
The previous stock-reader cohorts lost on all three. The source change therefore
addresses an actual complete-job cost, rather than only increasing native coverage.
Shared-host variation remains in the [trials and full-workload follow-up](../benchmarks/tpch.md#native-parquet-source-follow-up).

A fresh complete SF1 cohort after projected workspace accounting measured a
**3.0278×** geometric mean of median query speedups, with all 22 query medians
faster. Summed query medians were 69,827.69 ms stock and 14,744.03 ms native
(4.7360×); this is not a separately timed serial suite. The full cohort, including
outliers and shared-host limitations, is retained in the linked benchmark page.

After syncing upstream and rebuilding both libraries with the exact benchmark
profile's mimalloc feature, the review-head cohort measured **2.8137×** geometric
mean, again **22/22** median wins. Summed query medians were 64,430.12 ms stock
and 13,820.95 ms native (4.6618×). Direct entry-transpose counters are zero for
every native trial, with a positive stock-reader sensitivity control. The two
complete cohorts remain separate; their difference is not an allocator-only A/B.
