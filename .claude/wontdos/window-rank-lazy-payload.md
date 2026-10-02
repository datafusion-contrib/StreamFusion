# Window rank: defer payload materialization until admission

**Status:** rejected prototype, 2026-10-02. The original production ranking loop is retained.
This decision does not close any issue or complete the Rust benchmark inventory.

The prototype inspected an occupied group's bounded buffer before extracting the full
row. It materialized only sort columns, used the original partition-point comparison
and tie policy, and skipped losing payloads. A single map entry avoided a second lookup;
new groups were inserted after full-row extraction. Existing snapshot formats, state
writes, memory accounting and JNI contracts were retained.

The admission check applied only when the buffer length exactly equaled a positive
limit. Overfull buffers restored with a smaller limit must still trim on the next
input, even when it loses. Comparing only against the worst retained row was also
rejected: the current floating comparator treats unordered NaN comparisons as equal,
so that shortcut can miss a candidate admitted by the original partition-point search.

## Evidence and decision

Twenty matched release Criterion cases use 16,384 rows, eight or 16,384 keys, UTF8
payload suffixes of 8 or 264 bytes, nullable and non-null inputs, rank limits one/four,
and first/last tie modes. Last-tie mode is restricted to limit one. Append creates an
empty operator; update starts from an independently populated operator. Setup and
teardown are outside timing. Identical fixtures validate complete retained output.
Three seconds of warmup, 100 samples and at least five seconds targeted measurement
run sequentially without another local compilation or whole-job benchmark.

Rust 1.94.0, Arrow 58.3.0, Linux WSL2/Core i7-12650H, counting System allocator;
these are memory-operator microbenchmarks, not production mimalloc whole-job results.
The candidate was measured before a fresh original control. Confidence intervals
and raw samples are retained, including unfavorable cases.

Repeated-key cases improve 35.79–51.75%, but unique-key initial ingestion regresses
3.01–13.44%. Unique-key keep-last updates regress 19.45%, and limit-four updates
regress 24.06%. The fresh original confirms that the initial-ingestion regression
is not resolved by the single-entry revision. Reject this implementation rather
than treating the repeated-key improvement as a general production win.

The earlier 216-profile memory allocation comparison found 64 lower, 136 unchanged
and 16 higher requested-byte totals. Each higher case adds a 320-byte scratch buffer.
Requested allocation traffic is neither copied bytes nor peak memory and excludes
C++, JVM and worker-thread allocations. Allocation reductions do not override the
retained-row timing regressions.

The candidate's 18 ranking and nine persistent recovery tests passed; its 1,176
allocation fixtures also passed. The runtime prototype was removed. Retained recovery
tests cover smaller restored limits, NaN ordering, string/secondary sorts, NULLs,
ties and exact payload/rank preservation. All 18 ranking and nine persistent recovery tests also pass
independently against the unchanged production loop.

[All 20 comparisons](../../docs/benchmarks/window-rank-lazy-payload-rejected-comparison-2026-10-02.csv),
[40 estimates with confidence intervals](../../docs/benchmarks/window-rank-lazy-payload-rejected-timing-2026-10-02.csv)
and [4,000 raw samples](../../docs/benchmarks/window-rank-lazy-payload-rejected-samples-2026-10-02.csv).
A future approach must reduce rejected-payload work without the measured new-group
and retained-row costs, then pass release whole-job comparisons against stock Flink
and previous production with both transposes retained.
