# Aggregate specialization fast paths

**Applies to:** the two-phase local aggregate's numeric MIN/MAX, and mini-batch group-aggregate
`DISTINCT` (q15/q16/q17-shaped queries)

Two of the local aggregate's hot leaves were paying for generality their actual input doesn't need:
an insert-only MIN/MAX carrying full retraction support, and a `DISTINCT` accumulator boxing every
probe value into a `ScalarValue`. Specializing each to what its input actually requires turned into
two of the larger single-operator wins in the ledger.

## Append-only local numeric MIN/MAX keeps one running extreme

The two-phase local aggregate had been giving every numeric MIN/MAX group a retractable
`BTreeMap<value, count>`, even though the local half of an insert-only plan can only ever add
values — it never needs to know what to fall back to when the current extreme is retracted.
It now uses the existing scalar running MIN/MAX state when no row-kind column is present;
retracting input, strings, decimals, and the global merge still retain the counted tree and its
delete semantics, since those genuinely need multiset bookkeeping.

Criterion's 4096-row, 64-key MIN/MAX logical bundle rose from **9.50 to 33.89 M rows/s** (**3.57x,
+258%**). A contemporaneous release+mimalloc q17 mini-batch A/B rose from 1.535 to
**1.661 M events/s (+8.2%)**; the immediate path, which does not use the local pre-aggregate,
remained approximately flat at 1.750 versus 1.745 M events/s. The matched 25-second CPU profile
completed 180 iterations versus 163 before and removed the local aggregate's 87-sample tree search,
68-sample tree destruction, and 37-sample aggregate-state destruction leaves; `GroupAggState::accumulate`
fell from 55 to 31 samples. The few remaining tree samples come from the downstream global
aggregate, whose input is retracting partial updates and so still needs the tree.

## Group-aggregate DISTINCT folds primitives; the changelog emit reads its cache

The multi-`DISTINCT` day/channel [GROUP BY](../operators/group-by.md) aggregates (q15/q16/q17)
owned the largest native islands, and their hot leaves were `ScalarValue` construct/hash/clone/drop:
every row built a scalar per distinct agg call just to probe the distinct sets, and each emit
materialized the group's full output tuple twice — the pre-update value for the changelog `-U`, the
post-update value for the `+U`.

Distinct sets are now typed — a BIGINT distinct column keys a plain `i64` map read straight off the
array, no scalar involved — and each group caches its last-emitted tuple, so the pre-update value is
a take-from-cache (recomputed only after restore) and the `-U` moves it out instead of cloning it.
The emit protocol stays byte-identical, including the unchanged-result suppression Flink itself
applies. Measured on the generator profile loop: **q16 +17%, q17 +4%, q15 +3%** — q16, long the
floor of the Parquet/Kafka tables, gains the most. The cached tuple's own size accounting is covered
on [Memory accounting designed off the hot path](memory-accounting-off-hot-path.md).
