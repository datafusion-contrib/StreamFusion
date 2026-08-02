# Checkpoint partitioning in one pass

**Applies to:** Top-N, updating/interval joins, dedup, changelog normalizer (raw keyed-state
snapshots)

Several native operators' raw keyed-state bridges originally discovered which key groups held data
by running the operator's full snapshot/partition routine once, then ran that same routine *again*
per non-empty key group to actually write each group's payload — an O(key groups × state) checkpoint
cost. Each fix below removed one layer of that redundant work, until the final step replaced the
whole decode/re-encode pipeline with a format that writes what's already on hand.

## One partitioning pass discovers and writes key groups together

**Top-N** was fixed first: the JNI boundary now returns all key-group-framed payloads from a single
partitioning pass, and Java streams them directly into Flink's raw keyed-state output, instead of
listing groups in one call and re-partitioning in another. Snapshot/restore bytes and rescale
behavior are unchanged. In the release+mimalloc 50K-event q19 exactly-once Kafka profile loop, 60
seconds completed 25 jobs instead of 15 — **67% more end-to-end work** — with the repeated snapshot
call removed from the hot path.

**Updating and interval joins** had the identical multiplicative shape: serialize and partition both
sides once to discover non-empty key groups, then repeat the complete operation per group. Their JNI
calls now return the complete framed partition set in one pass; interval joins keep their existing
per-key-group processing-time timer frame. In the release+mimalloc 500K-event exactly-once Kafka
matrix, the former outliers moved from q4 **0.13x to 1.77x** Flink and q7 **0.07x to 2.44x** Flink
with mini-batching off; q20 moved from **0.07x to 1.55x** and q23 from **0.05x to 0.96x**. With
mini-batching on, those four queries measured **2.19x, 2.44x, 1.77x, and 1.00x** Flink respectively.
Snapshot payloads, restore behavior, and Flink rescaling ownership were unchanged by this step.

## Updating joins dropped a redundant IPC round trip

The one-pass join checkpoint above still encoded each complete join side to IPC, decoded it
immediately, split the decoded batch by Flink key group, and encoded each partition again. The
partitioner was changed to keep that first materialized Arrow batch in memory and write only the
final per-key-group IPC payloads — checkpoint bytes and the restore/rescale contract unchanged.
Criterion on 4,096 rows per side (`checkpoint_4096_rows_per_side`, release+mimalloc) improved from
2.116 ms to 1.940 ms per checkpoint, an **8.3% latency reduction**; the native-symbol q9 profile that
motivated it was dominated by exactly this removed IPC round trip. The 50K-event exactly-once Kafka
profile loop subsequently completed 50 jobs in 60 seconds instead of 45 — **11.1% more end-to-end
work**.

## Raw keyed-state snapshots write stored bytes, not decoded columns

*(2026-07-27, supersedes the IPC-round-trip fix above — the materialized batch is gone entirely.)*

The q9 differential profile put ~25% of native task-thread CPU inside the 1 s barrier, nearly all of
it the updating join's memory-state snapshot: it decoded every stored arrow-row back to typed Arrow
columns and re-derived every row's key group by re-encoding and hashing its key, per barrier, over
state that only grows. Stock Flink pays its equivalent serialization on the async checkpoint pool,
off the task thread — the barrier gap was ours alone.

The raw snapshot format now carries the state maps verbatim: binary columns holding the stored
Flink-BinaryRow bucket key and arrow-row payload, with the key group derived from **one hash of the
bucket key's bytes** (that encoding's hash *is* Flink's key-group input) — computed per bucket, not
per row. Restore reads the same bytes straight back into the maps: no decode, no re-encode. Snapshots
written by the older decoded format still restore through the kept legacy path.

Measured on exactly-once Kafka q9 (500K events, 60 s profile windows): join-snapshot samples dropped
866 → 261, task-thread barrier share 25% → 12%, and the profile loop completed **55 → 63 jobs in
150 s** (per-job 2.73 s → 2.38 s), closing the off-mode gap to stock Flink from 24% to 8%. The
format's worst case — narrow all-fixed-width rows under unique keys, where per-bucket hashing
amortizes nothing and the removed decode was cheapest — was checked on the checkpoint microbench
(`checkpoint_4096_rows_per_side`, same-day A/B): 2.50 ms old vs. 2.30 ms new, still ~8% ahead.

### Propagation to other map-shaped stores

The format then propagated to every other map-shaped byte store: both Top-N rankers, the keep-last
deduplicator, and the changelog normalizer. Their raw batches carry the typed payload schema in
metadata, so converters rebuild it on restore before any input arrives; Top-N additionally adds the
memcomparable sort key as a column, and dedup adds the exact rowtime. Same-day A/B on the shape with
the most per-key state — q18's `(bidder, auction)` keep-last dedup — measured **50 → 69 jobs in
150 s (+38%)**.

### Operators surveyed and left unchanged

A survey of the remaining operators found no equivalent pathology to port:

- The time-shaped operators (`OVER`, window, session, temporal/window/interval joins) buffer typed
  batches, so their snapshots split columns without any row decode.
- The group aggregate already hashes stored key bytes per bucket and must serialize accumulator
  values by construction.

### The one deliberate remaining exception

Keep-first dedup's emitted-key set is left on the decoded format. Its keys are arrow-row (not
Flink-BinaryRow) encoded, so its key groups cannot be derived from stored bytes without a re-encode —
a different design from the bucket-keyed stores above, and not worth forcing to match.
