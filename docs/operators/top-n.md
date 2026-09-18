# Top-N

FLOAT/DOUBLE sort keys treat positive and negative zero as a tie, so following ORDER BY
columns decide their rank. Only sort-key copies normalize zero; emitted payloads and partition
keys keep the original bits. Snapshot restore normalizes retained sort keys as well, including
older raw snapshots, before ranking new input.

MAP and MULTISET fields in retained rows fall back at planning time, including fields nested
inside ARRAY or ROW. Arrow's row codec cannot store them even when they are only payloads.
This restriction also applies to LIMIT/SortLimit, which use the same state representation.

**Status:** Native — all three rank strategies, idle-state TTL included; a small set of matcher
gaps below.

Flink lowers a rank filter (`ROW_NUMBER()`/`RANK()`/`DENSE_RANK() OVER (PARTITION BY ... ORDER BY
...)` restricted by `WHERE rn <= n` or `rn BETWEEN offset+1 AND offset+n`) to one of three ranker
implementations, chosen from the input's changelog kind and key structure. StreamFusion runs all
three natively:

- **Append-only ranker** — the input is insert-only; a per-partition sorted list of the current
  top rows. TTL expires per sort-key list — every list write refreshes all of that list's tie rows.
- **Update-fast ranker** — the input carries a unique key and the sort key is inferred monotonic
  against updates on that key (e.g. ranking by a descending `COUNT(*)`), mirroring Flink's
  `UpdatableTopNFunction`. For the constant `rn <= 1` special case this is `FastTop1Function`: rather than
  keeping bounded state for every row, a new row for a key is dropped immediately — no state
  update, no emission — the moment it fails to outrank the currently-held top row, since a
  monotonic sort key means a non-improving challenger can never later become the top row. TTL
  expires per row-key entry.
- **Retracting ranker** — the general case for an arbitrary retracting input, mirroring Flink's
  `RetractableTopNFunction`. TTL expires the *whole* per-partition buffer at once, on a clock
  refreshed by every record processed for that partition — modeling Flink's own per-record
  `SortedMap` rewrite.

Idle-state TTL is native across all three — see [TTL semantics](index.md#idle-state-ttl) and
[Configuration](../configuration.md) for the flag surface.

Also native: a projected rank number and both insert-only and retracting changelog input.
`OFFSET` runs natively over insert-only, update-fast and general retracting inputs, with either
a projected or hidden rank. `RANK`/`DENSE_RANK` never reach the matcher at all —
Flink itself rejects them in streaming, so that's parity, not a gap.

An update-fast offset retains ranks 1 through rankEnd, including the hidden prefix, so unique-key
updates can move rows across the visible boundary. Its output uses Flink's positional update
cascades even when rank is not projected. An updated row moving into the hidden prefix retracts
its former visible position before the remaining visible transitions. Checkpoints and canonical
memory/RocksDB transitions retain the prefix and reapply the selected range on restore.

A hidden-rank retracting offset follows Flink's heap-state emission semantics: a cascade mutates
the retained row kinds, and later retractions compare those kinds along with the full payload.
Sort-key counts advance even when a payload removal fails. Native memory and RocksDB state both
persist the kinds and independent counts, including zero-count keys with retained payloads.
This path preserves every per-record cascade even when mini-batching is enabled. Projected-rank
and update-fast paths retain their existing behavior.

Without OFFSET, each retraction of a selected row emits its removal and any promoted row,
even when an identical duplicate replaces it. Projected ranks retain the corresponding
UPDATE_BEFORE/UPDATE_AFTER pairs at every shifted position. These per-record transitions
apply to constant and partition-derived bounds; mini-batch net diffs may still cancel pairs
whose final materialized value is unchanged.

## Data-dependent bounds

All three value-ordered `ROW_NUMBER` strategies can use a non-null SMALLINT, INT or BIGINT upper bound
that is fixed within each partition. The bound must be a partition key itself, or a pure numeric
expression of directly projected partition keys in the preceding Calc. The current proof admits
arithmetic, MOD, casts and COALESCE. For example, a non-null `k` supports
`PARTITION BY k ... WHERE rn <= MOD(k, 3) + 1` without fixing one N for the entire operator.
The rank column can be projected or omitted, and mini-batch materializations remain supported.

Released Flink stores the first bound per partition and ignores later changes while incrementing
`topn.invalidTopSize`. Proving the bound cannot change lets the native ranker read it from each
Arrow row and reuse its existing rank-buffer state, TTL and memory/RocksDB checkpoint formats.
The proof retains Calc expressions through native substitution and input pruning. Independently
changing bounds retain an explicit fallback until their first-bound state and separate TTL
contract are implemented. Nullable bounds remain on Flink because its primitive row access does not express ordinary SQL null propagation here.

Zero and negative bounds select no materialized rows. For insert-only and update-fast input,
large bounds preserve Flink 2.2.1's variable-range admission rule: after 100 retained rows, a new sort key must strictly improve on the current worst
key, even when the selected bound is greater than 100. This is an admission threshold, not a
100-row output cap; improving arrivals can fill the selected range.
The general retracting strategy retains the full sorted buffer, so a retraction can promote
rows beyond the selected range. Its partition-derived bound applies to both per-record changes
and mini-batch output. Every retained row carries that same bound; a bundle flush can recover
it from the retained payload without a separate keyed state entry. Empty buffers emit the
retractions for their former selected rows.

Update-fast variable bounds use `UpdatableTopNFunction` semantics even when N is one:
a same-sort-key update refreshes the payload and emits an update. With a projected rank,
the buffer also retains the first sort-key group extending beyond N, so a later improvement
still recognizes those unique keys. Without a projected rank, an admitted arrival that is
immediately evicted emits DELETE followed by INSERT, including for nonpositive bounds.
These pairs cancel in the materialized result but remain in the raw changelog.

SQL tests compare exact changelog order, NULL payloads, ties, integral widths and signed 64-bit
boundaries. Updating cases cover replacements, deletions, empty groups and mini-batch
materializations. Checkpoint tests continue the selected windows across memory/RocksDB
transitions, including a partially filled bundle flushed before the checkpoint barrier.
Update-fast coverage also verifies rescaling, restored TTL timestamps, equal-sort updates at
N=1, and retained overflow ties. A retained-metrics SQL test requires nonempty native Top-N
input and output. Mini-batch aggregate comparisons use an explicit tie-breaker because each
engine may emit a bundle's groups in a different map order; tied arrivals are checked with
ordered per-record changelogs.

The unchanged Flink 2.2.1 streaming `RankITCase`, `DeduplicateITCase`, `LimitITCase` and
`SortLimitITCase` also pass with StreamFusion injected: 131 passed, seven skipped. The matching
batch rank/limit classes add 27 passing cases. The state-suite run verifies
both native memory and RocksDB initialization. These are broader rank regressions; the local
SQL tests explicitly assert native routing for the newly admitted variable-bound queries.
The upstream retracting GROUP BY/Top-N case additionally requires successful nonempty native
updates from both operators. The upstream nullable, independently changing bound case must retain its
explicit nullable-bound fallback; loading the agent or opening an operator alone cannot satisfy either contract.

A row-fed release measurement on an Apple M1 Max used 1,000,000 rows, 4,096 keys,
`MOD(k, 3) + 1` bounds, descending value order, projected rank and parallelism 1.
After two warmups, five alternating trials per engine measured median elapsed times of
**2.053428 s Flink / 0.966069 s native (2.126x)**. The timed path includes the row source,
both row/Arrow transposes and row blackhole sink; the benchmark asserts the native Top-N
node and both transposes. This is a standalone coverage benchmark, not a Nexmark result.

Reproduce with `SF_BENCHMARK=true mvn -Pbench -pl :streamfusion-runtime -am test
-Dtest=VariableTopNBenchmark -Dsurefire.failIfNoSpecifiedTests=false`.

The general retracting variant uses the same setup and one million changelog rows, alternating
inserts and deletes in groups of 16,384 rows (four values per key). Release/mimalloc medians
were **0.926187 s Flink / 0.447620 s native (2.069x)** with both transposes included.
Add `-Dvariabletopn.retracting=true` to reproduce it.

The update-fast variant ranks a grouped `COUNT(*)` with 16 row IDs per partition and an ID
sort tie-breaker, using the same one million rows, 4,096 partitions and variable bounds.
Release/mimalloc medians after two warmups and five alternating trials were
**2.598992 s Flink / 0.458960 s native (5.663x)**. This measures the complete GROUP BY → Top-N
pipeline, including both transposes and the row blackhole sink; it does not isolate the ranker
from the aggregate. Add `-Dvariabletopn.updateFast=true` to reproduce it.

## Processing-time first-N

An insert-only `ROW_NUMBER() OVER (PARTITION BY key ORDER BY pt ASC)` filtered to
`rn <= N`, where `pt` is a `PROCTIME()` attribute and N is a positive constant no greater
than 2,147,483,647, runs as an arrival counter. The rank may be projected or omitted. Each key,
including a NULL key, emits its first N rows in arrival order as inserts. No clock value is
compared and no payload rows are retained: state is one integer counter per key, independent of N.
The same column-type gates as value-ordered Top-N apply. The existing rank-1 deduplication
path remains in use whenever Flink lowers the query to deduplication, including when it
replaces a projected rank with the constant 1.

This follows Flink's `AppendOnlyFirstNFunction`, including under mini-batch configuration.
Accepted rows increment the counter and refresh its TTL; rejected rows do neither. After expiry,
the next arrival starts again at rank 1 without retracting earlier output. Counters and TTL
timestamps survive memory and RocksDB checkpoints, rescaling by Flink key group, and canonical
savepoints across the two backends. The `streamfusion.operator.topN.enabled` switch controls
this path; rank-1 dedup retains its own switch.

The standalone row-fed release measurement on an Apple M1 Max used 1,000,000 rows,
4,096 keys, N=2, projected rank, parallelism 1, two warmups and five alternating trials per
engine. Median elapsed times were **0.378589 s Flink / 0.446913 s native (0.847x)**.
The row source, both row/Arrow transposes and row blackhole sink remain in the measured path;
the benchmark asserts the native first-N node and both transposes. This workload is slower
natively. The feature is retained for coverage and composition inside native islands, where
first-N previously forced a fallback, rather than as a standalone throughput improvement.

Reproduce with `SF_BENCHMARK=true mvn -Pbench -pl :streamfusion-runtime -am test
-Dtest=FirstNBenchmark -Dsurefire.failIfNoSpecifiedTests=false`.

## Gaps

- Hidden-rank retracting OFFSET after an upstream mini-batch aggregate, rank or other operator
  that can change intermediate changelog order. Source changelogs through projections, filters,
  exchanges and batch markers remain native. Preserving the upstream bundle order is the
  remaining composition work in [#102](https://github.com/datafusion-contrib/StreamFusion/issues/102).

- A variable rank range outside the non-null, partition-derived forms above.
  Nullable and independently changing bounds remain in
  [#104](https://github.com/datafusion-contrib/StreamFusion/issues/104).
- A row type the native converter can't carry.
- Time-ordered ranks beyond the existing rank-1 dedup forms and the processing-time first-N
  form above: event-time N > 1, descending processing-time N > 1, updating first-N input,
  first-N with an offset, or a first-N bound beyond the signed 32-bit counter range.

[LIMIT](limit.md) reuses this same operator — a plain row-count limit is Top-N with a constant
rank range starting at 1.

## Window Top-N and window dedup

Window Top-N ranks within a windowing TVF's windows (`PARTITION BY window_start, window_end, key
ORDER BY ...`); window dedup is the same shape specialized to a time-ordered rank-1, keeping the
first or last row per key per window. Both are native for **event-time and proctime** windowing:
the windowing TVF assigns each row to the window(s) covering its rowtime (or, under proctime, the
operator's clock), and the rank operator closes each window on the same chained
processing-time-timer model as the [window aggregate](window-aggregate.md) — the slide must divide
the size. As with the other proctime-driven window operators, this is non-deterministic, so it's
tested for routing/execution but not byte-compared to the host.

For plain `TIMESTAMP` rowtime, window start/end remain wall-clock values regardless of the session
zone. A window dedup keep-last replaces a candidate with an equal rowtime; keep-first and general
Top-N preserve the earlier arrival on a tie. This plan-level tie policy is reapplied after memory
or RocksDB restoration, without changing the retained row or snapshot layout.

The one shape gap is a rank that doesn't start at 1 — i.e. an `OFFSET` on the window rank. Both
shapes also hold the [window-assignment zone gate](window-aggregate.md#matcher-declines): a
`TIMESTAMP_LTZ` time attribute in a session zone with any historical or recurring transition, or a fixed offset not
aligned with the window slide, falls back together with the windowing TVF that feeds them.

Attached start/end columns are local wall-clock values, including inside native expressions.
Window Top-N/dedup preserves these payload columns; it converts the watermark or processing-time
threshold into their fixed-offset domain, firing at the last millisecond of the window. It does
not apply another zone shift on output. Plain TIMESTAMP uses a zero offset. This also supports
input from an aggregate whose boundaries are already rendered locally.

The `-Dstreamfusion.operator.windowRank.enabled` switch covers both shapes; window dedup reuses the
window-rank operator rather than getting its own switch — see [Configuration](../configuration.md).
