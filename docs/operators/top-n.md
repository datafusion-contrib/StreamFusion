# Top-N

FLOAT/DOUBLE sort keys treat positive and negative zero as a tie, so following ORDER BY
columns decide their rank. Only sort-key copies normalize zero; emitted payloads and partition
keys keep the original bits. Snapshot restore normalizes retained sort keys as well, including
older raw snapshots, before ranking new input.

Ordinary Top-N carries MAP and MULTISET payloads recursively, including within ARRAY/ROW or other maps.
They use the existing tagged List-of-entry-struct storage view so NULL keys and values survive
Arrow row encoding and IPC checkpoints. Partition, order and update-fast unique-key fields still
exclude MAP/MULTISET descendants; payload admission does not extend key equality or ordering.
LIMIT/SortLimit, processing-time first-N and ordinary non-time ROW_NUMBER share this carriage.
Updating Top-N/LIMIT inputs with any MAP/MULTISET descendant still fall back: full-row
retraction needs Flink's representation-dependent map equality, not physical List equality.
[Issue #437](https://github.com/datafusion-contrib/StreamFusion/issues/437) tracks that contract,
including equivalent maps with reordered entries. No performance flag bypasses this guard.

**Status:** Native — all three rank strategies, idle-state TTL included; a small set of matcher
gaps below.

Flink lowers a rank filter (`ROW_NUMBER()`/`RANK()`/`DENSE_RANK() OVER (PARTITION BY ... ORDER BY
...)` restricted by `WHERE rn <= n` or `rn BETWEEN offset+1 AND offset+n`) to one of three ranker
implementations, chosen from the input's changelog kind and key structure. StreamFusion runs all
three natively:

- **Append-only ranker** — the input is insert-only; a per-partition sorted list of the current
  top rows. Projected-rank plans also retain Flink's first overflow sort group, even though it
  emits nothing beyond the visible bound. This hidden state can affect later TTL behavior and
  must survive canonical handoff. Mini-batch result diffs include only visible ranks. TTL expires
  per sort-key list — every list write refreshes all of that list's tie rows.
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

Constant AppendFast Top-1 uses Flink's update changelog even when the rank column is hidden:
replacement emits UPDATE_BEFORE when requested, followed by UPDATE_AFTER. Its generic and
specialized implementations follow the same contract in immediate and mini-batch modes.
Projected wider ranks retain the hidden overflow group in both modes, with result diffs limited
to visible ranks. These correctness changes do not change the native checkpoint encoding.

With the RocksDB backend, all three strategies charge their live read-through working sets
rather than accumulating reservations for persisted keys. Active mini-batches retain charges
for their preimages, and the append Top-1 decoded cache remains independently bounded. Bundle
completion returns the released state reservation without discarding durable rank state; the
memory backend continues to charge all resident partitions. See
[Memory management](../memory-management.md#exhaustion-and-spilling) for lifecycle and regression
details. This does not change changelog, TTL or checkpoint encodings.

Unfiltered scalar ROW_NUMBER on Flink 2.2 uses the same retained-row representation through
[a separate OVER matcher](over.md#ordinary-row_number-flink-22). It retains every row and
emits updates by row identity, rather than changing occupants of bounded rank slots. Its narrower
input/TTL gates do not change the filtered Top-N admission described here.

Idle-state TTL is native across all three — see [TTL semantics](index.md#idle-state-ttl) and
[Configuration](../configuration.md) for the flag surface.

Constant AppendFast Top-1 (`offset=0`, `limit=1`, no variable rank bound) uses a
single-winner update loop. It compares the borrowed sort key before obtaining
mutable state, so rejected arrivals neither construct owned payload/sort rows nor
write RocksDB state. Winners replace the retained entry directly. The existing
raw checkpoint codec and key-group assignment are unchanged; memory/RocksDB
recovery does not require a migration. Immediate and logical mini-batch modes
retain their existing emission and TTL rules, including hidden-rank tie writes
in immediate mode. Variable bounds and wider ranks use the general algorithm.
A direct RocksDB Top-1 also retains a FIFO cache of clean decoded winners,
limited to an accounted 64 MiB per operator. State still writes through at each
bundle. Only existing stored keys enter the cache; fresh unique keys bypass it.
Checkpoint/canonical scan clears cached values, and memory reservation denial
clears and disables the cache while execution continues through RocksDB.
`topn.cache.size`, `topn.cache.hitRate` and `topn.cache.bytes` describe resident
keys, hits per distinct state-hydration request and the cache's accounted footprint.
See [Top-1 winner state](../optimizations/top1-winner-state.md) for measured boundaries.

Also native: a projected rank number and both insert-only and retracting changelog input.
`OFFSET` runs natively over insert-only, update-fast and general retracting inputs, with either
a projected or hidden rank. `RANK`/`DENSE_RANK` never reach the matcher at all —
Flink itself rejects them in streaming, so that's parity, not a gap.

An update-fast offset retains the hidden prefix, so unique-key updates can move rows across the
visible boundary. Projected ranks and offsets also preserve the first overflow sort group visited
by Flink's rank iteration. That group can own a unique key whose later version moves into view. Its output uses Flink's positional update
cascades even when rank is not projected. An updated row moving into the hidden prefix retracts
its former visible position before the remaining visible transitions. Checkpoints and canonical
memory/RocksDB transitions retain the prefix, overflow group and original row kinds, then reapply
the selected range on restore. Kind-less legacy native snapshots still read as INSERT rows.

A hidden-rank retracting offset follows the selected Flink backend's value-copy semantics.
With heap state, a cascade mutates shared retained row kinds; later retractions compare those
kinds along with the full payload. RocksDB reads serialized copies instead: only explicitly
written source-sort-group lists retain their mutations. An insertion writes its new row after
emission. A retraction writes its source list before shifting later groups, so those later
emissions do not mutate persisted kinds. Native memory and direct RocksDB preserve these
respective contracts, including within a physical Arrow batch.

Sort-key counts advance even when a payload removal fails. Both stores persist independent
counts, including zero-count keys with retained payloads. The path preserves each per-record
cascade even with mini-batching enabled. Projected-rank and update-fast paths are unchanged.
Sixteen released-SQL cases per supported Flink line cover unique/tied sort keys, both backends,
projected/hidden ranks and two checkpoint restores. The previous native RocksDB path failed
both uninterrupted and recovered hidden-rank controls; the heap/projected controls passed.
[Validation evidence](../benchmarks/results/hidden-offset-backend-copies-2026-10-20.json).

The state encoding is unchanged. Old native RocksDB checkpoints retain their recorded kinds;
future correct copy behavior cannot reconstruct the stock-RocksDB history that the previous
heap-style mutation model did not record. Replaying from an unaffected source/state is needed
when that historical difference matters.

Without OFFSET, each retraction of a selected row emits its removal and any promoted row,
even when an identical duplicate replaces it. Projected ranks retain the corresponding
UPDATE_BEFORE/UPDATE_AFTER pairs at every shifted position. These per-record transitions
apply to constant and partition-derived bounds; mini-batch net diffs may still cancel pairs
whose final materialized value is unchanged.

## Data-dependent bounds

All three value-ordered `ROW_NUMBER` strategies can use a non-null SMALLINT, INT or BIGINT upper bound
that is fixed within each partition. The bound must be a partition key itself, or a pure numeric
expression of the projected partition keys in the preceding Calc. Keys may be direct columns or
computed expressions using arithmetic, MOD, casts and Flink's built-in COALESCE. For example, a
non-null `k` supports both `PARTITION BY k ... WHERE rn <= MOD(k, 3) + 1` and
`PARTITION BY MOD(k, 3) ... WHERE rn <= MOD(k, 3) + 1`. Several computed keys can jointly determine
the bound, and COALESCE can supply a non-null bound for partitions derived from nullable columns.
The rank column can be projected or omitted, and mini-batch materializations remain supported.

The proof recognizes an entire computed key as invariant; it does not infer that the key's inputs
are constant. Several different values of `k` can share `PARTITION BY MOD(k, 3)`, so a bound of
`k` requires first-bound state. Update-fast input additionally requires disabled state TTL. Repeated UDF calls do not establish invariance, even when the function
declares itself deterministic. The whitelist checks the built-in COALESCE identity,
rather than accepting a function by name.

Flink may select its general retracting strategy for a computed partition key over a grouped input,
because its metadata cannot prove the required upsert key through that expression. StreamFusion
preserves Flink's strategy choice; a computed key does not force update-fast execution.

Released Flink stores the first bound per partition and ignores later changes while incrementing
`topn.invalidTopSize`. Proving the bound cannot change lets the native ranker read it from each
Arrow row and reuse its existing rank-buffer state, TTL and memory/RocksDB checkpoint formats.
The proof retains Calc expressions through native substitution and input pruning.

Append-only, general retracting and update-fast input also admit a non-null integral bound that changes
independently of the partition keys. Update-fast input requires disabled state TTL (the default). Native state retains the first bound per partition, ignores later proposals,
and increments `topn.invalidTopSize` for every mismatch, including rows that fail rank admission.
Retractions count mismatches too, and deleting the last row leaves the bound intact. Even a
first retraction with no matching row establishes the bound. The emitted payload keeps each row's
actual proposed bound, allowing later full-row retractions to find the original payload. The bound's
TTL starts when it is created; later reads and row-state writes do not refresh it. After expiry the next proposal
becomes the new bound, while any live ranked rows remain available. A nonpositive bound still
has state even when its row buffer is empty. For append-only input with projected ranks, the buffer
retains the entire first sort-key group extending beyond N, including at N=0, matching Flink's later positional
updates when the bound expires and widens.

This changing-bound path honors the plan's UPDATE_BEFORE setting and emits per-record
transitions even with mini-batching enabled;
naive net diffs would introduce deletions that Flink does not emit when an expired bound shrinks.
Memory checkpoints, RocksDB checkpoints, canonical savepoints and rescaling preserve both the
bound and its independent timestamp. Mismatch counts return through the existing batch JNI call;
there is no JNI call per row. Partition-invariant bounds keep their existing net-diff optimization
and row-only checkpoint layout. Nullable bounds remain on Flink because its primitive row access
does not express ordinary SQL null propagation here. Changing update-fast bounds with positive
TTL remain on Flink: its cached rows can acquire a fresh write timestamp when checkpointed,
independently of the bound's creation clock. The native store currently timestamps rows at ingestion.
A controlled-clock oracle exposed different retained rows after checkpoint and expiry; this
combination is not admitted until those clocks match. The remaining work is tracked in
[#104](https://github.com/datafusion-contrib/StreamFusion/issues/104).

The SQL fallback check uses a one-hour positive TTL so independent host/native-enabled jobs do
not compare rows that expired at different wall-clock times. Expiry and checkpoint clock semantics
are checked by the controlled-clock operator tests. The changing update-fast mini-batch
fallback check also uses a one-hour flush interval and small count-triggered bundles: a one-second
timer can split independent host jobs into different bundles and change the first proposed bound.
The test still requires identical results, no native substitutions, and the expected fallback reason.

With mini-batching, independently changing retracting or update-fast bounds require input whose per-record order
is preserved: changelog sources through projections, filters, exchanges and batch markers qualify.
Upstream aggregates or other stateful operators fall back because their bundle emission order can
change which row establishes the first bound. Without mini-batching, grouped retractions are
supported. Retracting row-state expiry still uses the whole-buffer model described above; the
bound's independent clock does not add per-sort-key TTL to that model.

Initially zero and negative bounds select no materialized rows. For insert-only and update-fast input,
large bounds preserve Flink 2.2.1's variable-range admission rule: after 100 retained rows, a new sort key must strictly improve on the current worst
key, even when the selected bound is greater than 100. This is an admission threshold, not a
100-row output cap; improving arrivals can fill the selected range.
The general retracting strategy retains the full sorted buffer, so a retraction can promote
rows beyond the selected range. A partition-derived bound applies to both per-record changes
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
Changing update-fast bounds are compared against Flink 2.2.1's released operator with TTL disabled,
with and without projected rank and UPDATE_BEFORE. The oracle continues across memory/RocksDB
checkpoints, canonical backend changes and scaling 1→2→1, including bound-only partitions.
SQL checks cover negative, zero, large and changing bounds, NULL payloads and equal-sort ties;
retained metrics require native rows and nonzero mismatch counts.
Partition-invariant update-fast coverage also verifies rescaling, restored TTL timestamps, equal-sort updates at
N=1, and retained overflow ties. A retained-metrics SQL test requires nonempty native Top-N
input and output. Mini-batch aggregate comparisons use an explicit tie-breaker because each
engine may emit a bundle's groups in a different map order; tied arrivals are checked with
ordered per-record changelogs. The changing-bound aggregate fallback check also starts with
enough rows of one key to flush both aggregate phases before any other bound can arrive.
This keeps its first bound and nonempty materialized result independent of processing-time
bundle boundaries, including when both executions use only Flink.
Computed-key tests additionally compare exact ordered changelogs
and mini-batch materializations for arithmetic, integral casts, COALESCE and composite partitions.
Forced SQL failures after a completed checkpoint verify continuation on memory and RocksDB for
append-only input and grouped retractions. Native operator metrics require nonempty Top-N input
and output, with zero bound mismatches for these proven invariant expressions. Changing-bound
SQL tests additionally require the exact mismatch count and the original proposed bounds in
emitted payloads. Controlled-clock tests compare against the released Flink Top-N operator
with and without UPDATE_BEFORE through bound expiry, row refresh, shrinking bounds, empty
buffers, checkpoint restoration and
canonical memory/RocksDB transitions. Rescale tests preserve bound-only keys after deleting
every ranked row as well as keys whose initial bound selected no rows.

The unchanged Flink 2.2.1 streaming `RankITCase`, `DeduplicateITCase`, `LimitITCase` and
`SortLimitITCase` also pass with StreamFusion injected: 131 passed, seven skipped. The matching
batch SQL rank/limit classes add 24 passing cases. The separate upstream rank state-suite run
passes 75 cases with one skip and verifies both native memory and RocksDB initialization. Each
suite run enforces six execution contracts: three native and three expected fallback invocations. These are broader rank regressions; the local
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

The independently changing append-only case uses `MOD(COALESCE(v, 0), 3) + 1` instead, with
one million rows, 4,096 partitions, projected rank and parallelism 1. On the same Apple M1 Max,
release/mimalloc medians after two warmups and five alternating trials were **2.047443 s Flink /
0.681449 s native (3.005x)**, with both transposes and the row blackhole sink. Add
`-Dvariabletopn.changingBound=true` to reproduce it. An invariant-bound control run with the
same build measured **2.012879 s Flink / 0.966614 s native (2.082x)**, retaining the existing
path's approximately 0.966 s native median.

A separate computed-key measurement on the same machine used 8,192 source keys mapped to
4,096 partitions by `MOD(k, 4096)`, with `MOD(MOD(k, 4096), 3) + 1` bounds. One million
append-only rows, projected rank, both transposes and the row blackhole sink remained in the
measured path. Release/mimalloc medians after two warmups and five alternating trials were
**1.985225 s Flink / 0.977894 s native (2.030x)**. Add
`-Dvariabletopn.computedPartition=true` to reproduce this variant. The
[raw trials](../benchmarks/computed-partition-topn-2026-09-18.csv) record the individual times.

The general retracting variant uses the same setup and one million changelog rows, alternating
inserts and deletes in groups of 16,384 rows (four values per key). Release/mimalloc medians
were **0.926187 s Flink / 0.447620 s native (2.069x)** with both transposes included.
Add `-Dvariabletopn.retracting=true` to reproduce it.

With independently changing `MOD(COALESCE(v, 0), 3) + 1` bounds, the same retracting setup
measured **0.857261 s Flink / 0.448346 s native (1.912x)** after two warmups and five alternating
release/mimalloc trials. Emptying each partition between insert cycles exercises retention of its
first bound. Add both `-Dvariabletopn.retracting=true` and `-Dvariabletopn.changingBound=true`
to reproduce this variant. The [raw trials](../benchmarks/retracting-changing-bound-topn-2026-09-18.csv)
record individual times; both transposes and the row blackhole sink remain in the measured path.

The update-fast variant ranks a grouped `COUNT(*)` with 16 row IDs per partition and an ID
sort tie-breaker, using the same one million rows, 4,096 partitions and variable bounds.
Release/mimalloc medians after two warmups and five alternating trials were
**2.598992 s Flink / 0.458960 s native (5.663x)**. This measures the complete GROUP BY → Top-N
pipeline, including both transposes and the row blackhole sink; it does not isolate the ranker
from the aggregate. Add `-Dvariabletopn.updateFast=true` to reproduce it.

The independently changing update-fast variant uses the same grouped workload but computes
`MOD(COALESCE(v, 0), 3) + 1` from the row ID. At 2M input rows, 4,096 partitions and 16 IDs per
partition, Flink took **3.749s** and native **0.779s** (**4.814x**). This 2026-09-19 M1 Max/JDK 17
run used release + mimalloc, parallelism one, disabled TTL, two warmups and five interleaved
measured trials, reporting medians. Both row/Arrow transposes and the grouped aggregate remain in
the measured pipeline. Add `-Dvariabletopn.updateFast=true -Dvariabletopn.changingBound=true
-Dvariabletopn.rows=2000000` to the existing benchmark command. This is one workload, not an
isolated rank-kernel speedup. The [raw trials](../benchmarks/update-fast-changing-bound-topn-2026-09-19.csv)
retain every measured duration; ordered SQL and released-operator tests establish correctness
separately from the benchmark's blackhole sink.

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

- Nullable variable bounds, changing update-fast bounds with positive state TTL, and changing
  bounds after upstream stateful mini-batch operators that can reorder proposals.
  These remain in [#104](https://github.com/datafusion-contrib/StreamFusion/issues/104).
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
the size. Watermarks and bounded-input completion leave unfinished processing-time windows
open; only their processing-time timers fire them. As with the other proctime-driven window
operators, this is non-deterministic, so it's tested for routing/execution but not byte-compared to the host.

For plain `TIMESTAMP` rowtime, window start/end remain wall-clock values regardless of the session
zone. A window dedup keep-last replaces a candidate with an equal rowtime; keep-first and general
Top-N preserve the earlier arrival on a tie. This plan-level tie policy is reapplied after memory
or RocksDB restoration, without changing the retained row or snapshot layout.

Recovery regressions cover string sort keys with a secondary sort, NULL ordering, first/last
ties, exact payloads and rank numbers across raw memory restoration. Persistent recovery also
covers a smaller restored rank limit: the next input trims an oversized retained buffer even
when that input loses. A NaN-tail regression preserves the existing partition-point admission
behavior; it is not a general claim of floating-point ordering parity with Flink.

A proposed admission check before full-row extraction was rejected after its common-case
regressions: unique-key ingestion slowed 3–13%, and retained-row updates slowed 19–24%, despite
repeated-key gains. The production loop is retained. [The complete Criterion comparison](../benchmarks/native-criterion.md#rejected-window-rank-payload-admission-prototype)
includes the unfavorable controls and does not establish a whole-job improvement.

The one shape gap is a rank that does not start at 1, such as an OFFSET on the window rank.
Fixed offsets and regional LTZ calendars use the same assignment as the upstream TVF.

Attached start/end columns are local wall-clock values, including inside native expressions.
Window Top-N/dedup preserves these payload columns; it converts the watermark or processing-time
deadline using each local end and released-Flink regional timer conversion, or the fixed-offset
shift, firing at the last millisecond of the window. It does
not apply another zone shift on output. Plain TIMESTAMP uses a zero offset. This also supports
input from an aggregate whose boundaries are already rendered locally.

Regional RocksDB firing scans keys to collect distinct local window ends before converting their
deadlines. Its subsequent row scan retains only closed groups. Pending rank buffers remain in
RocksDB, and fixed-offset firing retains its ordered-prefix scan.

The `-Dstreamfusion.operator.windowRank.enabled` switch covers both shapes; window dedup reuses the
window-rank operator rather than getting its own switch — see [Configuration](../configuration.md).


## Retained MAP/MULTISET payload qualification

Released SQL comparisons cover eight payload families: string/integer-key maps, multisets,
ARRAY-of-MAP, ROW-containing-MAP, and MAP values containing ARRAY/ROW/MAP. NULL parents, empties,
NULL keys and nullable values pass ordinary Top-N, sorted/arrival LIMIT, hidden OFFSET, first-N
and non-time ROW_NUMBER, plus inner/outer/semi/anti joins. Two checkpoint failures cover memory
and direct RocksDB. Updating joins and ranking cases pin correct fallback, including colliding
map keys reordered on retraction. Three MAP UDF/consumer compositions now run natively: Flink
lifts those callbacks above rank, and their external Map signatures use the default-enabled
[generated callback](calc-filter.md#java-map-scalar-signatures). Flink 1.18 skips eight ordinary non-time
ROW_NUMBER cases because the released host lacks that operator.

The retained-payload revision, before the MAP callback extension, passed 238 checks on Flink 2.2.1 and 230 with eight capability skips
on 1.18.1, including 57 explicit correctness-fallback cases per line. Another 21 existing
stateful map-carrier and nested SQL/JSON controls pass on each line; append-only payload
checks now require native execution while the map-key check retains fallback.

The [benchmark evidence](../benchmarks/results/retained-map-payloads-2026-10-20.json) retains every
trial, independent full output oracle, native profile and allocation probe. The prepared-format
Top-N gate compares production Java rank on BinaryRowData with production Rust rank on Arrow,
including budgeted state and complete map consumption, with five warmups/nine trials. Final
stock/native median ratios range from 1.03x to 6.90x across eight MAP/MULTISET cohorts. Wide
high-churn timings are close and their ranges overlap. This establishes no complete-job or
join-throughput claim. The [batch rejection technique](../optimizations/bounded-rank-payloads.md)
explains the low-churn gain and its measured limits.


Four additional nested-UDF composition controls per release cover a returned ROW containing a
string-to-string MAP, on heap/RocksDB with two restores. Ordinary scalar ordering stays native.
Sorting by a computed collection expression still preserves the existing final-projection/fused-
consumer guard: richer collection representations cannot enter an unverified stateful boundary.
These are callback/schema checks, not additional UDF type admission or throughput evidence.


## First-N canonical handoffs

The shared First-N path also runs on Flink 1.18.1. Focused controls cover real operator snapshots,
native checkpoints and isolated native/stock/native process handoffs with both native stores.
RocksDB exports pin an MVCC read view and convert in Flink's asynchronous writer; memory exports
capture independent native state and convert asynchronously through a temporary stock backend. Concurrent-capture and cancellation controls check reader
ownership independently of the live operator. This qualification does not extend automatically to
all rank algorithms or cross-version upgrades.

The Flink 2.2.1 first-N counter now has automatic stock canonical binding in normal SQL and stock
compiled-plan execution. It preserves `counterState`, the stock INT/TTL serializers, actual grouping
keys, original write ages, stock identities and resolved maximum parallelism. Export reads live
native counters directly; import validates the whole view before inserting state. Other rank
algorithms use separate state contracts.

Qualification passes 60 complete-job cases across both stores/orders and submission forms,
including hidden/projected ranks, keyless and empty state, no-input re-export, native checkpoint
recovery and rescaling at 1→3→2/maxP128. External committed output matches stock and an independent
arrival-count oracle, with custom source/sink state and both transposes retained. Twenty direct
controls additionally cover raw TTL ages/expiry, bounded transfer, malformed input and callback
failure; 32 planner controls verify the actual owner and stock contract. The original released
first-N restore fixture imports two counters and consumes four new native rows.

Ordinary wide decimal/nanosecond root-key layouts, other rank algorithms, backend/version matrices,
full-job TTL expiry and broader connector/failure/delivery cases remain open. Existing native
processing/checkpoint encodings are unchanged. See the exact
[first-N scope](../backends/canonical-state.md#stock-compatible-first-n-counters).
Benchmarking of canonical handoff remains deferred.


The final whole-view import check additionally rejects nonempty native targets, preserving their
existing counters. A late persisted target duplicate first reproduced partial insertion of 1,024
new rows and is now rejected before any are inserted. The earlier 60-job cohort passed before
this stricter initialization check; the final combined 129-check cohort is under qualification.
Shared Flink 1.18 processing/checkpoint controls pass 17 cases without claiming stock canonical
interchange on that line.


A reused first-N result can feed both inputs of a join while retaining one stock counter owner.
Ordinary lowering checks the captured origin, rank settings, key layout and configuration
before consolidating clones, then rebuilds batch sharing. Different first-N limits retain
separate state owners. All 32 cross-engine job scenarios pass across both submission forms/stores/orders, including
no-input re-export, rescaling and different-limit controls; the combined cohort passes 124 checks. Older native graph upgrades and other shared stateful rank
variants remain open in [#569](https://github.com/datafusion-contrib/StreamFusion/issues/569).


First-N configuration equivalence is inspected in the Flink 2.2 lowering layer; the shared
node compares only its rank/key definition and remains compilable on Flink 1.18. The API
placement correction passes 100 current 2.2 controls (including eight ordinary process jobs)
and 31 shared 1.18 processing/checkpoint controls. These cohorts overlap earlier runs and
do not establish Flink 1.18 canonical interchange.


## Value-ordered append canonical state (partial)

Normal Flink 2.2.1 SQL and portable stock compiled plans bind the released append lists and fast
Top-1 winners automatically. Initial admission is AppendFast input, one sort column, INT/BIGINT/
VARCHAR retained fields and start-one constant or variable ranges. Capture the real released
function, partition/sort selectors, key serializer, TTL, stock operator identities and resolved
maximum parallelism. Flink can fold a projected constant Top-1 rank into a downstream expression,
so the actual function determines the retained schema. Stock restore needs only normal Flink,
connector and user dependencies.

The direct adapter preserves `data-state-with-append` sort-key lists or `Top1-Rank-State` winners,
including tie order, hidden overflow rows, stored row kinds and original TTL ages. Variable
`rankEnd` values remain independent, including negative bound-only partitions. Export visits live
memory or raw persisted partitions directly into stock descriptors and serializers, without an
intermediate native snapshot. Raw canonical reads do not expire state during transfer.

Import validates the whole view before inserting native state. Foreign descriptors, mismatched
partition/sort keys, empty lists, invalid non-null payloads, malformed metadata and duplicate
winners/bounds fail without consuming source state. Failed validation leaves an empty target for
retry. Receiver errors retain the original Java exception and release moved Arrow buffers; native
state remains exportable. Closing a failed memory-limited target releases its prior reservation.

Append export stages each retained sort list once in the shared disk spool, carrying its last
original write timestamp into the released TTL serializer. Restore reads raw canonical map/list
values and decodes one row at a time, preserving TTL envelopes and stored row kinds. Duplicate
partition/sort/bound inventories stay on disk. A shared sender bounds transfers by 1,024 rows
and a four-MiB payload estimate, allowing an oversized entry. An encoded KV remains the
working-memory floor; native checkpoint bytes and admission are unchanged.

Stock-bound rank keeps per-record changes under table mini-batching, with pending transposes and
coalescers flushed before the barrier. Append constructors and native restore honor the requested
update-before policy: fast Top-1 emits UPDATE_BEFORE when requested, then UPDATE_AFTER, even with
hidden rank. An upsert stream omits the before-image. First-bound and non-time ROW_NUMBER runtime
modes are installed before restore; otherwise valid bound headers would be rejected during the
base initializer. Unbounded OVER still has its own separate state contract.

Qualification covers 160 distinct complete-job scenarios across ordinary/compiled submission,
both native stores and switch orders. Thirteen continuation variants include hidden/projected
rank, fast Top-1, nullable/NOT NULL/keyless keys, long TTL, mini-batching, descending order and
variable bounds. Further controls cover empty/no-input handoffs, native checkpoint recovery and
1→3→2 rescaling at maxP128. Custom source/sink state and both transposes remain present. Every
stage's committed table state matches an independent oracle; final effects and exact emitted
kind/payload/counts match uninterrupted stock. Metadata/classpath checks retain stock-only restore.
The complete-job matrix uses BIGINT input; the original released append restore fixture additionally
imports three INT/STRING retained rows and consumes two new native rows.

The initial 160-job cohort passed 158 cases and hit two 60-second handoff deadlines while native
RocksDB was still initializing. Both affected ordinary paths passed four unchanged concurrent
reruns, and the corresponding compiled paths passed four concurrent controls. Initial failures
and thread dumps remain retained; their cause is not established. These eight repeated controls
are not additional distinct scenarios. Broader startup/failure stress remains open.

A further 410 distinct direct/planner/processing controls pass across retained cohorts. These
include 96 actual-operator cases with INT/STRING keys, INT/BIGINT/STRING order, non-null order,
distinct ties, reserved field-name collisions and reversed non-leading composite keys. Serialized
future rows and kinds match stock in both directions and under controlled TTL expiry. The harness
advances stock Guava and state-TTL clocks together. Guards reject native snapshot/checkpoint calls
during canonical export. Fifty-two planner cases include variable-bound and rejected-contract
controls. The native suite passes 1,241 tests with two existing ignores; shared Flink 1.18 code
passes 125 processing/checkpoint controls without claiming stock canonical interchange there.

**Pending for the append contract:** OFFSET, multiple sort columns, other retained types,
async/unaligned state, broader shared/upstream-native graphs, full-job TTL expiry and the complete
backend/version/connector/delivery matrix. Existing unbound native paths keep their own recovery
representation; older unbound native checkpoint upgrades are not qualified here. All remain in
[#569](https://github.com/datafusion-contrib/StreamFusion/issues/569). Benchmarking is deferred.

## Updating canonical state (partial)

Normal Flink 2.2.1 SQL and portable stock compiled plans bind released UpdateFast state for
TTL-disabled, start-one constant ranges, one or two sort fields, and INT/BIGINT/VARCHAR retained
fields. Capture the actual function, partition/unique/sort selectors, stock key serializer,
operator identities and resolved maximum parallelism. A fast Top-1 can have its projected constant
rank folded into a downstream expression; use the actual retained function schema and before-image
policy. Stock-bound rank preserves per-record changes under table mini-batching.

When an ordinary updating-rank graph contains native state owners without a qualified canonical
binding, rebuild from its captured stock template. The template accelerates supported roles and
keeps other state owners, including upstream grouped aggregation, on stock Flink. Their state and
identities remain stock-compatible. Wider native aggregate/graph contracts remain separate work;
this route does not qualify a native aggregate's accumulator interchange.

The direct adapter writes `data-state-with-update` MAP: actual unique row key → stock
`Tuple2<RowData, Integer>`. The integer preserves ordering within equal sort values. Fast Top-1
uses `Top1-Rank-State` VALUE. Export visits live memory or raw RocksDB partitions through at most
1,024-row Arrow transfers, without an intermediate native snapshot. Hidden overflow rows and
retained row kinds survive. Restore stages typed unique rows and inner positions in the shared
disk spool, validating duplicate keys and consecutive tie order without whole-job key sets or
per-partition Java sort trees. The shared sender retains owned buffers and a four-MiB payload
estimate as well as the row limit, including the existing nested map/list C Data representation.

Import requires an empty native target and validates all source state before applying any transfer.
Foreign descriptors, key/payload disagreements, duplicate unique keys or tied positions, invalid
non-null fields and nonconsecutive tied positions are rejected with the source retained. Successful
import consumes the projected stock state. A resource-denied target must be closed; the untouched
source can restore into a fresh target. Receiver failures preserve the original Java exception,
release transferred Arrow buffers and leave native state available for another export.

A combined cohort passes 484 direct/planner/processing controls. These include 192 typed cases
with one/two sort fields, nullable and reversed non-leading composite keys, reserved field-name
collisions, fast Top-1, hidden/projected ranks, both stores and both before-image policies. Sixteen
initialization cases exercise both switch orders against uninterrupted stock output. Thirty-two
planner cases capture automatic binding and maximum parallelism; ten further cases keep unqualified
TTL/OFFSET/variable/three-sort/wider-payload rank state on Flink. Error controls cover multi-chunk
ties, all four stored kinds, late corruption/retry, foreign state, nonempty targets, invalid
non-null primitives, receiver failure and memory denial. Two focused regressions restore a native
memory checkpoint into memory/RocksDB, then write canonical state and continue on stock Flink.
Snapshot guards reject an intermediate native snapshot/checkpoint during canonical export.

All 120 complete-process scenarios pass on the corrected native library: both submission forms,
stores and switch orders, hidden/projected ranks, fast Top-1, nullable/non-null/keyless keys,
one/two sort fields, no-input/empty handoffs, native checkpoint recovery and 1→3→2 rescaling at
maxP128. Custom source/sink state and both transposes remain present. Independent stage/final table
checks and exact stock change-kind/payload/counts match, with stock-only restore dependencies.
Rescale controls use one aggregate key per rank partition to avoid nondeterministic cross-channel
aggregate arrival order; continuation cases separately exercise competing rank rows.

The first 120-job cohort passed 116 and exposed four memory-checkpoint failures: raw restore
intentionally installs a timestamp for an enable-TTL migration, but the TTL-disabled canonical
writer forwarded it and rejected its own metadata. The writer now emits no age for that contract,
retaining the existing raw restore/TTL-migration behavior. Focused tests reproduce the old 5,000
versus zero mismatch in both stores, then pass on the fix. The final full 120-job cohort passes;
the original failures remain retained.

The unchanged released updating-rank fixture restores six retained native rows and processes two
new rows, including its original two-field ordering. The eleven selected programs pass in each
engine: nine native cases and two expected normalizer-filter fallbacks. The agent passes 109 audit
controls and excludes validation-only visits from import counts. The native suite passes 1,247
tests with two existing ignores. Shared Flink 1.18 foundation controls pass 125 cases; the added
common-node API also passes its 25 processing controls. Neither establishes 1.18 canonical interchange.

TTL envelopes/cache write ages, independent variable bounds, preserved numeric tie-rank gaps,
OFFSET, additional sort fields, wider native graphs and the broader schema/version/connector/
failure/delivery matrices remain open in [#569](https://github.com/datafusion-contrib/StreamFusion/issues/569).
Stock TTL restoration can retain nonconsecutive inner ranks; this TTL-disabled adapter rejects them
rather than silently renumbering them. Older unbound native checkpoint upgrades remain unqualified.
Benchmarking remains deferred.

## Retracting canonical state (partial)

The direct Flink 2.2.1 adapter preserves three independent structures: `data-state` MAP sort-key
lists of actual rows, `sorted-map` VALUE counts, and optional `rankEnd` VALUE. Presence, list order,
retained row kinds and all three write ages survive. Empty count maps, empty data lists, orphan
rows, count-only/bound-only keys and counters differing from list lengths are preserved. Counts
cannot be reconstructed from rows: unmatched retractions and heap OFFSET row-kind mutations can
leave retained rows whose sort key is absent from the count map, even without TTL.

Export scans live memory or raw RocksDB state, decodes at most one persistent partition at a time,
and sends at most 1,024 Arrow rows per callback. Sort keys have their own typed fields because a
counter may have no corresponding data row. The writer uses released row/list/map serializers and
the actual stock serializable comparator. It writes no intermediate native snapshot. TTL-disabled
export emits zero ages after a native checkpoint's enable-TTL migration, without changing native
clocks. Transient decoded state and Arrow batches reserve the task's memory budget.

Import checks the full descriptor inventory, an empty target, declared non-null fields, partition
and sort selectors, retained values and all metadata before copying state. Source owner keys are
the union of all three descriptors, streamed repeatedly from the shared temporary disk inventory.
The index closes and deletes on failure without adding descriptors to stock source state.
Export stages each retained list once, including present empty headers and independent TTL
stamps. Native counters already arrive in sort order; a read-only disk-backed sorted-map view
validates strict comparator order while the released serializer streams entries once. Heap
component receivers retain ordinary owned collections. Restore validates raw list/counter
snapshot bytes and decodes individual entries, retaining original timestamps, signed counts,
nullable-value rejection and row kinds. Duplicate inventories stay on disk, with bounded row/byte
transfers. The largest encoded KV remains a working-memory floor.
A full validation pass precedes insertion; native frames also
check existing earlier chunks. Successful import consumes the projected source. A denied target
must be closed; source state remains available for a fresh target. Receiver failure preserves the
original Java exception and leaves native state available for another export.

The explicit compatibility state mode mirrors released per-record processing, including heap
aliases versus serialized-backend value copies. Native memory/RocksDB checkpoints preserve its
independent maps and immutable captures freeze mutable row kinds while sharing payload bytes.
Older flat buffers cannot prove their original independent state and are not promoted implicitly.
Normal Flink 2.2.1 SQL and portable stock compiled plans bind the qualified contract automatically.
The planner captures actual stock selectors, serializers, comparator, operator identities, maximum
parallelism and before-image policy. General retracting ranks allow constant ranges including
OFFSET and bounds above `Integer.MAX_VALUE`, or a non-null INT/BIGINT first-observed bound.
Retained fields are INT/BIGINT/VARCHAR, with one or two sort fields, plus the retained
processing-time indicators qualified below. Bound operators use per-record
semantics even under table mini-batching. Graphs with an unqualified upstream state owner retain
its stock layout; failed rank binding rebuilds the captured stock template, including direct
changelog sources without upstream aggregation. Unsupported payloads, three sort fields, nullable
variable bounds and unaligned exchanges retain the stock rank owner.

Tests use 28 immutable traces generated by released stock Flink for OFFSET, matched/unmatched
retractions, count-only keys, independent TTL and changing bounds, hidden/projected rank and both
stores. All 84 direct scenarios pass: stock/native-first handoffs, no-input canonical re-export,
exact complete-state checks and native-checkpoint-to-canonical restoration into either store,
followed by actual stock execution and exact continued changelogs. Sixteen further controls cover
multi-chunk ties/all four kinds, empty descriptors and owner-key union, zero/negative/maximum
counts, late corruption/repair, foreign state, nonempty count-only targets, non-null primitive
validation, memory denial and receiver failure. The counter serializer restores with StreamFusion
classes denied. The complete native suite passes 1,261 tests with two existing ignores.

Another 56 cases exercise actual operator initialization and two successive stock/native switches
in both orders, retaining exact continued changes under OFFSET, TTL and variable bounds. All 484
shared ranking regressions pass on the same library. The disk-inventory cohort additionally
passes all 84 direct state/continuation scenarios, 56 initialization checks and 16 malformed-state
checks on Flink 1.18. This qualifies the exercised component layouts, rather than the wider typed,
complete-job and rescaling matrices below. The native library also checks without RocksDB.

Another 576 typed direct/lifecycle scenarios pass over INT/BIGINT/VARCHAR fields, Unicode, NULLs,
composite/non-leading partition keys, colliding and reserved field names, ascending/descending
sorts and NULL ordering. They cover one/two sort fields, hidden/projected ranks, both before-image
policies, fixed/OFFSET/variable bounds, independent TTL clocks, both stores and successive switches
in both engine orders. Every switch compares the complete stock state; guards reject intermediate
native snapshots. Another 144 planner cases and eight rejected-contract cases pass, alongside 94
append/update planner regressions. Planner checks include ordinary/compiled submission, keyed/global
ranks, mini-batching, generated identities, resolved maximum parallelism and the concrete native
retracting state contract.

RocksDB may reclaim fully expired values during reads or compaction: cross-engine comparisons
exclude expired garbage, while native raw checkpoint round trips still require exact preservation
of all stored rows/counts/kinds/ages/bounds. Heap aliases and serialized-backend copies also retain
different row-kind side effects in released Flink, so complete-job comparisons must use the same
selected backend in each engine.

The unchanged released retracting-rank restore program imports six actual retained rows and
processes two new rows in native Top-N. The twelve selected upstream programs pass per engine,
with ten native paths and two expected normalizer-filter fallbacks. All 118 execution-witness
controls and 49 summary controls pass. The retract witness excludes validation and metadata rows
and requires both actual canonical imports and new input through the opened native family.

The complete-job matrix verifies 168 scenarios across ordinary SQL and portable compiled plans,
both engine orders and selected backends. Fourteen continued-input variants cover projected/hidden
rank, Top-1, keyed/global and non-null keys, two sorts, OFFSET, first-observed bounds, large limits,
TTL configuration, size-three aggregate mini-batches and actual changelog source input. Controls
cover empty and no-input returns, native checkpoint recovery and rescaling 1→3→2 at maximum
parallelism 128. Rescale distributes 64 partition keys with one aggregate key each; competing rows
are exercised separately. Every advancing native leg requires actual Top-N input, both transposes
remain in the plan, custom source offsets and sink committed effects survive, and stock processes
exclude StreamFusion. The mini-batch grouped aggregate uses a native owner bound to the stock
accumulator contract.

The original 168-case qualification combined 160 passing cases and eight targeted reruns after
the reference correction below. A subsequent rerun exposed two mini-batch changelog mismatches
and required the native aggregate bundle-order correction. All eight mini-batch handoffs pass
on the corrected library across both submission forms, engine orders and stores; the 290-case
aggregate state and ordered-output cohort also passes. Earlier failed diagnostics remain archived.
These targeted reruns overlap the 168-case matrix rather than adding new job coverage. The
mini-batch fixture's stock baseline uses the same barrier cuts because barriers legitimately flush
pending aggregate changes. Its latency exceeds the test timeout so only count and barrier drains
contribute to this deterministic comparison. Stock-bound native aggregate bundles drain in the
released BinaryRow HashMap order, including retained table capacity between drains; first-touch
order can otherwise alter the downstream rank changelog even when its final table agrees. TTL job cases configure one-hour retention; controlled expiry and
independent ages are verified in the direct/lifecycle cases. The combined distinct Flink 2.2
operator/planner/job selection has 1,548 passing cases, including the twelve stock-only reference
checks below. Component/smoke/upstream selections overlap and are not added to this count. All 15
operator families and all ten issue acceptance criteria remain partial/open.

The fixed hidden OFFSET job input exposes released Flink row-kind effects on both heap and
RocksDB: its emitted table can retain rows beyond the ideal SQL result, with an extra duplicate
on heap. Twelve stock-only process checks reproduce explicit reference tables after 5, 10 and
20 input rows in both submission forms/stores, with StreamFusion excluded. That fixture requires
those exact released references and exact continued stock changelogs. Other fixtures retain their
independent SQL table oracle. Canonical handoff preserves the saved lists/counts/kinds; it does
not repair released Flink's processing behavior or infer counters from an ideal table.

Wider schemas, three or more sort fields, nullable variable bounds, broader native graphs and the
full version/failure/delivery matrix remain open in
[#569](https://github.com/datafusion-contrib/StreamFusion/issues/569). Benchmarking remains deferred.


## Window rank canonical interchange (partial)

Existing window rank/dedup canonical bindings now retain ROW/ARRAY/MAP payloads in direct native
RocksDB. The shared retained-row codec preserves declared nested fields, NULL/empty containers
and map entry order; sorting still uses the existing qualified sort keys. Canonical savepoints
pin the store and convert bounded rows asynchronously. Scalar checkpoint bytes remain compatible,
and previous snapshot-backed checkpoints migrate one key-group blob at a time, preserving their
processing deadline through aligned or clipped restore. This changes storage for existing
contracts, without qualifying additional sort/key layouts.


Normal SQL and portable stock compiled plans bind the released Flink 2.2.1 event-time
window-rank processor for constant ROW_NUMBER ranges starting at one. TUMBLE, HOP and CUMULATE
retain attached local start/end columns, including SQL offsets. Capture uses the actual stock key
and sort serializers, input schema, processor time zone, original identity and resolved maximum
parallelism. Global owners keep the stock singleton maximum; keyed owners and their columnar
exchange share the captured maximum. Explicit UIDs are optional. Async and unaligned event-time state keep
stock owners. The portable compiled plan remains unchanged.

Canonical export visits live memory or typed RocksDB rows directly into stock `window_rank`
MAP state, with Long window-end namespaces, full sort-key lists, released event timers and the
`watermark` UNION state. No intermediate native snapshot is written. Import validates the complete
row/key/namespace/timer inventory before applying row batches and clears source entries only after
success. Calendar and tie policies are installed before import; canonical and ordinary native
checkpoints retain the input watermark, so immediate returns and older incoming watermarks cannot
reopen closed windows. Export stages each sort-list entry once in the shared disk spool, keyed by
owner, window-end namespace and typed sort key; the released serializer streams each bucket once.
Restore reads raw canonical map/list bytes and decodes one row at a time, with complete serializer,
namespace, sort-key and payload validation before native installation. Timer and observed-window
membership use the disk spool instead of job-sized Java sets, including window deduplication.
A shared sender caps row count and estimates four MiB per transfer, allowing an oversized row.
An encoded stock KV remains the working-memory floor. These mechanisms are shared by both Flink
lines and do not expand planner admission. Native checkpoints retain their existing raw format.

The 425-check rank/dedup cohort includes 252 rank cases, 152 dedup cases and 21 shared bridge/operator
regressions. It covers both native stores, keyed/global state, three shapes, four zones including
DST transitions, rank-number output, NULL scores/keys, arrival-order ties, a 2,500-row sort list,
empty watermark-only snapshots, native-checkpoint continuation and malformed ownership/state.
The separate 384-check planner cohort crosses shapes, zones, keyed/global state, first/last order,
both submission forms and explicit/generated identities. These cohorts do not establish full
connector, type, failure or version qualification.

OFFSET/other rank kinds, broader retained-type/sort parity,
populated-target and resource/callback failure qualification, and the complete connector/delivery/
version matrices remain open in [#569](https://github.com/datafusion-contrib/StreamFusion/issues/569).
Benchmarking remains deferred.


The unchanged released WindowRankRestoreTest and WindowDeduplicateEventTimeRestoreTest suites
pass all fourteen invocations on each engine. Four rank and eight dedup invocations require both
a nonempty successful canonical import and new native input. The two rank-after-aggregate programs
prove native rank processing after restore but have no nonempty rank-import witness; they receive
no rank-state import credit. Their stock upstream aggregate remains outside this native window
aggregate qualification. Released schemas include DECIMAL and VARCHAR fields in addition to the
BIGINT operator fixtures. The agent's 130 canonical witness controls pass, including rejection
of import-only, input-only and unrelated-family activity. Twenty separate planner controls
verify stock fallback for async, unaligned and OFFSET contracts.

A clean Flink 1.18.1 build passes 27 shared processing, bridge and native-checkpoint checks.
Those checks do not qualify stock canonical interchange on 1.18. Native code is unchanged from
the previously qualified library; this milestone does not claim a new native-suite run.


A separate 56-case complete-job matrix passes: 28 cases per family, each with eight continued-input
switches, eight no-input round trips, four ordinary native-checkpoint recoveries and eight rescalings.
Both submission forms, native stores and engine orders are covered; checkpoint cases start native.
The keyed TUMBLE fixtures use BIGINT payloads, general ascending rank without rank-number output
and first-row dedup. They retain both transposes, custom stock source/committing-sink state,
clean stock worker classpaths and metadata, exact externally committed changes and an independent
per-key selection oracle. Rescaling redistributes 64 actual keys through one, three and two native
owners at maximum parallelism 128 while source/sink remain singleton. Positive native work is
required in every native owner. Other shapes, last-row dedup and broader retained schemas have
operator/planner or released-suite evidence, rather than this complete-job recovery/rescale matrix.

The final 2,374-check regression cohort passes all twenty selected classes without failures,
errors or skips. It combines the rank/dedup state and planner controls with existing sort, OVER,
identity, SQL and ordinary rank regressions. These counts overlap earlier cohorts and are not
added cumulatively. Processing-time native windows have no released stock Window TopN/Dedup
counterpart and receive no stock canonical qualification. Benchmarking remains deferred.

Six additional planner controls require actual native processing-time window owners in ordinary
SQL for keyed/global rank and first/last dedup. They also assert that the same stock queries fail
with the released processing-time-window rejection. Event-time canonical routing therefore leaves
the existing native processing-time path intact; unrelated native Calc activity cannot satisfy
these controls.


## Retained processing-time indicators in retracting rank

Released retracting rank can retain a projected PROCTIME indicator as a physical NULL even when
its logical type is NOT NULL. That placeholder is materialized only when a downstream expression
observes processing time. Canonical import accepts this specific placeholder while continuing to
reject NULL in ordinary required payload fields and partition/sort keys. Original stock logical
types, row/key serializers, IDs and state descriptors remain unchanged.

Ordinary Flink 2.2.1 SQL and portable stock compiled plans bind this retracting payload case
automatically. The additional admission is limited to retained processing-time indicators:
ordinary timestamp payloads, indicator partition/sort keys and previously unqualified wider
schemas receive no new native canonical qualification. Existing first-N and other time-ordered
routes keep their own contracts.

Native Calc preserves the same lazy marker: PROCTIME produces a typed physical NULL and
PROCTIME_MATERIALIZE reads the execution clock. Materializing the marker before rank would make
later retractions differ from the saved stock row. This corrects ordinary input processing as
well as canonical handoffs; native checkpoints retain their existing format and export still
visits live state directly.

All 1,032 combined rank/clock checks pass on the corrected component. These include all 576 typed
scenarios, 84 canonical/native-checkpoint traces, operator initialization, exact aggregate/rank
binding sets, and 36 actual SQL clock observations. SQL-derived tests cover all four stock
heap/RocksDB and native memory/RocksDB backend pairs with and without native-checkpoint recovery.
A deliberately corrupted ordinary required payload must fail preflight without changing source
bytes or installing target state; repair/retry succeeds. Immediate and continued state must match
stock serialized state exactly, including the indicator placeholder.

All 64 complete-job cases pass on the same production revision: 28 projected-clock cases and
36 aggregate-plus-rank cases. Both ordinary SQL and portable compiled plans, both native stores
and initial engine orders are covered. They comprise 16 continuation, 16 no-input, eight native
checkpoint, 16 rescale and eight empty-prefix cases. Rescaling redistributes 64 keys through
1→3→2/maxP128, while source and transactional sink tasks stay singleton. The multi-owner jobs
require actual input through both native aggregate and rank. Custom source/coordinator and sink
state, both transposes, generated UIDs, metadata and clean stock workers remain in the path.
Committed deterministic changes and tables match stock plus an independent oracle; every observed
processing-time value must fall within actual execution time. The valid DELETE/UPDATE_BEFORE
inputs and oracle are unchanged from the archived failing regression.

All four unchanged released rank restore programs pass per engine with 199 execution-audit
controls. The retracting program imports six actual saved rows and processes two new native rows.
Another 37 shared Flink 1.18.1 checks pass; they establish common compilation, processing,
checkpoint and clock regressions, not 1.18 canonical interchange. Earlier cohorts overlap this
proof and are not cumulative counts of state schemas. Broader qualification remains in
[#569](https://github.com/datafusion-contrib/StreamFusion/issues/569); benchmarking stays deferred.
