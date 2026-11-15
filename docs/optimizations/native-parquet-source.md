# Native Parquet input batches

Bounded filesystem Parquet scans can reuse the optional decoder already used by
Paimon and feed Arrow batches directly into the native island. This removes Java
row materialization and the source row-to-Arrow write. Flink still owns file I/O,
file discovery and assignment, reader queues and checkpoint serialization. The
collecting sink and Arrow-to-row exit remain in complete-job timings.

The first admission is fixed-parallelism, unpartitioned, physical-only tables with
supported top-level projections and default reader options. Native footer enumeration
creates one split per row group, so source parallelism is no longer bounded by file
count, including on filesystems without block locations. Each decoder selects only
the groups within its half-open byte range and checkpoints split-relative logical
rows. Single-group files remain indivisible. Unsupported contracts retain the stock source.
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

Row-group enumeration now removes the file-count cap without changing the
filesystem, decoder, Arrow ownership or memory authority. The decoder applies
parquet-mr's half-open physical-midpoint ownership independently of projection;
its workspace estimate covers only the selected groups. Both INT96 timestamp
readers use the same group selection. Restoring an older whole-file native split
still works, while newly enumerated splits replay only their own logical rows.

A separate release source-read diagnostic on one eight-million-row SNAPPY file
with five row groups measured 167.08 ms stock, 182.97 ms with the native whole-file
policy, and 108.06 ms with row-group splits at parallelism two: **1.546× versus
stock and 1.693× versus the whole-file control**. A two-million-row, two-group file
improved by 1.097× and 1.139× respectively. These include enumeration, I/O, decode,
JNI/Arrow import, checksum consumption and close; they exclude query compute,
shuffles and sinks. The whole-file control uses the same candidate native decoder.
See the [method, trials and validation](../benchmarks/tpch.md#row-group-source-splits-november-12).
The earlier complete-query cohorts above retain their measured whole-file behavior.
