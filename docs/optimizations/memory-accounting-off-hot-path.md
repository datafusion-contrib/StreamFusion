# Memory accounting designed off the hot path

**Applies to:** every managed-memory-accounted native operator (keyed aggregates, dedup, changelog
normalize, joins, Top-N)

The accounting itself must not cost throughput. State footprint is tracked incrementally — only the
groups a batch touches are re-measured, O(batch) rather than O(open state) — and there is no
per-allocation JNI upcall into Flink's memory arbiter, the model Comet uses for Spark; the budget
crosses JNI once at handle creation and is enforced by a local check. Measured
cost: **~2%** on the accounted keyed-tumbling bench, statistically unchanged on the unaccounted hot
paths (`66fcfe3`, `2c1c487`).

The [GROUP BY](../operators/group-by.md) aggregate's touched-group measurement runs twice per row
(before and after the fold), and sizing its cached last-emitted tuple — the same cache described on
[Aggregate specialization fast paths](aggregate-specialization-fast-paths.md) — re-walked the
tuple's `ScalarValue`s each time. A 2026-07-12 q17 profile flagged the cost, so the size is now
cached next to the tuple and maintained wherever the cache changes, making the per-row measurement
pure arithmetic.
