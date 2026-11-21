# Bounded join and subplan planning

The bounded planner follows DataFusion's cross-join elimination for a narrow
physical pattern: `(A × B) JOIN C` with equality links from both dimensions to C.
When both dimension estimates are no larger than C, it can build each dimension
and probe C through two linked joins instead of materializing the Cartesian
intermediate. A projection and, when required, blocking Arrow exchange restore
the original schema and distribution. Hints, residuals, NULL-safe equality and
ordered joins retain the original plan. The SQL remains unchanged.

This allowed TPC-H Q9 at scale factor one to finish within the original 512 MiB
native authority. Its repeated release comparison measured 27,230.35 ms stock
versus 1,919.77 ms native (14.184×). A separate regression-check cohort measured
Q2 at 3,427.85 ms stock versus 535.95 ms native (6.396×), replacing an earlier
native Cartesian plan that lost to stock. These shared-host cohorts include
full scans, transposes, JNI, blocking shuffles, sinks and completed job results;
their variability and earlier failures remain in the [TPC-H evidence](../benchmarks/tpch.md).

The planner also preserves Flink's reuse of eligible shared columnar subplans,
including aggregates, joins, unions and connector scans. It runs the host subplan
reuser before native substitution and only preserves a native subtree's digest
when it can attach the retained-buffer share operator. A reused hash exchange
shares the producer before the shuffle and remains visible to joins and aggregates
that verify its distribution. If Flink's input-priority resolver must split a
shared hash boundary, the planner uses another blocking hash exchange over that
producer with the same routing keys. Both host reuse switches and native source
sharing must be enabled. The share operator uses the deployed output count when
retaining Arrow buffers.

Originally, sharing reduced Q15's native median from 1,393.73 ms to 794.67 ms in separate
cohorts, but the matching stock median was 596.21 ms. This remains a regression
to optimize and is not claimed as acceleration. The complete 22-query goal
remains open while its performance and validation work is unfinished.

For a composite NULL-safe hash semi-join with no residual predicate, DataFusion
groups the non-preserved build side by its typed keys before constructing the hash
table. A probe row needs only one match. Grouping removes repeated build tuples,
including tuples containing NULL, while retaining every matching probe row. The
aggregate and join use the task's DataFusion memory pool. This follows the same
membership reduction principle as Comet's right-key reduction before a semi-join
sort, using the existing DataFusion aggregate executor for composite keys. The
[Iceberg TPC-DS follow-up](../connectors/iceberg.md#tpc-ds-regression-follow-up)
records the worker profiles and matched release timings for Q14a and Q14b.

## Bounded hash join spill

Small builds retain the existing DataFusion hash join. The build buffer reserves
space against Flink's task off-heap authority for retained batches, consolidation,
and the hash table. When the reservation is denied, DataFusion's batch partitioner
hashes both inputs into 32 local partitions using their existing key expressions.
Each pair is replayed through the same DataFusion hash join in order. When skew
keeps a single partition above the available budget, the partition can replay
through the existing external sort and sort-merge join before any of its rows
are emitted. The spill files and temporary memory retire when the task finishes
or is cancelled.

The [nested Iceberg comparison](../connectors/iceberg.md#nested-batch-hash-join-with-native-iceberg-io)
verified complete native scan-to-commit execution with one million rows on each
side and 3.11 GB of partition spills under a 256 MiB task off-heap budget.
