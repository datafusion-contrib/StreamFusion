# Memory accounting designed off the hot path

**Applies to:** native operators, Arrow boundary buffers, and native Kafka queues

The accounting itself must not cost throughput. State footprint is tracked incrementally — only the
groups a batch touches are re-measured, O(batch) rather than O(open state) — and there is no
per-row JNI upcall. Operator state updates its reservation once per processed bundle through a
DataFusion `MemoryPool`; Arrow and Kafka queue allocations reserve at their natural allocation or
queue-lifetime boundaries. All of those requests reach one process-wide JVM authority capped by
Flink's `taskmanager.memory.task.off-heap.size`, allowing one operator to use headroom another is
not using without exceeding the TaskManager cap. Measured cost of the incremental state sizing was
**~2%** on the accounted keyed-tumbling bench, statistically unchanged on the unaccounted hot paths
(`66fcfe3`, `2c1c487`).

The [GROUP BY](../operators/group-by.md) aggregate's touched-group measurement runs twice per row
(before and after the fold), and sizing its cached last-emitted tuple — the same cache described on
[Aggregate specialization fast paths](aggregate-specialization-fast-paths.md) — re-walked the
tuple's `ScalarValue`s each time. A 2026-07-12 q17 profile flagged the cost, so the size is now
cached next to the tuple and maintained wherever the cache changes, making the per-row measurement
pure arithmetic.
