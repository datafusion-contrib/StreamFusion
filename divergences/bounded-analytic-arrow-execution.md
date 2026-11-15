# Bounded analytic plans over Flink Arrow edges

Arroyo's window-function planner partitions and sorts input before executing a DataFusion
window plan. Its worker keeps a plan per time bin and releases it when the watermark closes
the bin (`arroyo-planner/src/plan/window_fn.rs`, `extension/window_fn.rs`, and
`arroyo-worker/src/arrow/window_fn.rs`). Batch execution uses the same partition/sort/window
structure but closes one bounded plan at EOF, through StreamFusion's existing mailbox-driven
bounded task. It does not add time bins, streaming state or watermark logic to finite input.

Comet's window builder selects DataFusion window execution and casts ranking output to the
host's declared integer type (`native/core/src/execution/planner/window.rs`). This adapter
uses WindowAggExec for verified whole-partition and unbounded-preceding frames, with the
existing Flink-compatible decimal accumulator. The host selects the distribution; an Arrow
hash exchange establishes compatible native routing. Sorting remains spillable through the
existing task memory pool. A window can still fail its configured memory budget; admission
is not a promise that every partition fits any budget.

Flink's OVER collation takes the direction and NULL placement from ORDER BY when a
partition key also appears there (`OverAggregateUtil.createCollation`). The native
sort uses that combined collation and removes duplicate keys. Physical Rank instead
prefixes partition keys in ascending, NULLS FIRST order; their ordering is constant
within each partition. Keeping these two contracts distinct preserves the ordering
traits that Flink's later operators may rely on.

Comet Expand evaluates each projection over one retained input batch and emits separate
owned batches (`native/operators/src/expand.rs`). StreamFusion follows that structure using
its existing native Calc evaluator for each physical reference/literal projection, avoiding
another expression implementation. Flink retains grouping-set planning and grouping IDs.

Arroyo rejects unordered LIMIT in its streaming planner. A finite Flink input instead permits
an Arrow slice operator that consumes the full input and forwards the selected range. It
retains upstream failures and uses no row conversion. Native rank ranges compose the same
DataFusion ranking plan with a filter and optional rank-column projection.

Flink can serve several logical consumers through one serialized network output and can
deduplicate physical outputs after the logical share count is computed. The Java
share point therefore counts the deployed chained and network outputs before handing out
retained Arrow views. This is the actual ownership boundary; neither a stale logical count
nor garbage collection should decide when successfully consumed batches release memory.
