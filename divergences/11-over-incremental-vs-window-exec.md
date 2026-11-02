# Event-time OVER: incremental accumulators, not a DataFusion window exec

**Kind:** algorithmic — internal implementation differs; output is identical.
**Diverges from:** Arroyo (unbounded frame); Flink (bounded ROWS/RANGE frame — recompute
vs retraction, see below).
**Forced by parity:** partially — the *unbounded-preceding* frame makes the
incremental model strictly necessary for bounded memory; matching Flink's
streaming OVER operator is the prime directive either way.

## Their decision
Arroyo's `window_fn.rs` implements SQL `OVER` (window functions) by **buffering
rows per distinct event-time instant** and running a **DataFusion window
`ExecutionPlan`** over them — full delegation: the operator is a harness that
stages rows into the plan via channels and drains its output on the watermark.
This generalizes to any window function DataFusion supports (`ROW_NUMBER`, `LAG`,
`RANK`, …), because the plan does the work.

## What we do instead — which is what Flink itself does
For the unbounded-preceding RANGE and ROWS running aggregates, we
keep, per partition key, only the **incremental accumulators** (the same
DataFusion `Accumulator`s the window aggregates use — `sum`/`min`/`max`/`count`,
plus the int-semantics ones in [01](01-integer-truncating-avg.md)). Each row folds
into its key's accumulators in rowtime order and emits the running value; raw rows
are discarded once folded. This mirrors Flink's own `RowTimeUnboundedOver` operator,
which keeps an accumulator per key, not the rows. RANGE emits after folding the whole timestamp
peer group; ROWS emits after each row, so tied rows have successive running results. The same
accumulator layout and restore path serve both frames; the plan retains the emission mode.
COUNT(*) projects a non-null BIGINT constant into the existing numeric count kernel.

## Why not delegate to a DataFusion window exec here
The frame is **unbounded preceding** — every row's result depends on *all* prior
rows of its key, across every batch and watermark. A DataFusion window
`ExecutionPlan` computes over a materialized input, so delegating would require
**retaining every row ever seen, per key, forever** (or re-reading it from state
each emission). The incremental accumulator keeps a single running state per key
instead — bounded memory, no recomputation, and byte-identical to Flink, which is
built the same way. So here the Arroyo-style delegation is not just unnecessary
but strictly worse, and it would also diverge from Flink's actual operator.

We still use DataFusion's compute: the accumulators *are* DataFusion's. What we do
not adopt is Arroyo's *orchestration* (buffer rows, run a plan), because Flink's
orchestration (incremental per-key state) is both the parity target and the
bounded-memory choice. This is the same reasoning as
[03](03-incremental-window-merge.md), applied to OVER.

## General window functions — also incremental (investigated, not assumed)
`ROW_NUMBER`, `RANK`/`DENSE_RANK`/`FIRST_VALUE`/`LAST_VALUE` are the same `OverAggregate` rel with `UNBOUNDED PRECEDING` frames
(`ROW_NUMBER` uses `ROWS`). An earlier version of this note predicted they would
**delegate to a DataFusion window exec like Arroyo**. Investigating both sides
showed that is wrong here:

- **Arroyo's `window_fn` is not a model for this.** It buckets rows **per distinct
  event-time instant** and runs a DataFusion window exec per instant — so its
  window functions are scoped to one instant, *not* cumulative across the
  partition. Flink's `UNBOUNDED PRECEDING` `ROW_NUMBER` counts all prior rows of
  the partition; Arroyo's per-instant model does not produce that.
- **A DataFusion window exec would need unbounded retention.** With an unbounded
  preceding frame, delegating to a batch window plan means keeping every partition
  row forever (the partition never closes in a stream). Flink keeps a counter, not
  the rows.
- **DataFusion's incremental evaluator can't be checkpointed.** `PartitionEvaluator`
  *can* run incrementally (`ROW_NUMBER` is `n_rows += 1`), but unlike `Accumulator`
  it exposes **no serializable state**, so delegating the compute would break
  exactly-once. To checkpoint we must own the state anyway.

So general window functions take the **same incremental, own-the-state path** as the
running aggregates: a small per-key state (a counter for `ROW_NUMBER`; rank+last-value
for `RANK`/`DENSE_RANK`; the first / current value for `FIRST_/LAST_VALUE`),
serialized for checkpointing, matching Flink's `OverAggregate` exactly. For
`ROW_NUMBER` this is identical to DataFusion's own evaluator (`n+1`), so nothing is
re-derived; for the others the logic is small and Flink-defined.

## Bounded frame — retained rows and incremental arithmetic

A bounded ROWS or RANGE frame retains a per-key sorted buffer. Numeric SUM/SUM0/AVG and COUNT
keep raw incremental arithmetic beside the final retained row, retracting expired values before
adding arrivals. Primitive SUM also retains its independent non-null count; an empty live count
changes the result to NULL without resetting the raw sum. Recomputing the surviving slice loses
FLOAT/DOUBLE rounding and NaN history, just as it loses decimal overflow history. AVG has its
own widened sum and count, independent of SUM's input/result width. Numeric DISTINCT folds
rebuild multiplicity from retained values without reapplying the saved arithmetic.

Numeric MIN/MAX and other selections without retractable views still evaluate the reachable
slice. String reductions retain their existing retractable membership and selection history.
Event-time ROWS uses the slice through the current arrival; RANGE includes all peers up to the current
rowtime. Arithmetic and string views retain the full `n + 1` prior event-time ROWS frame needed for the next
retraction; value-only recomputation can retain `max(n, 1)`. Explicit original-input retention also
keeps the full current frame. RANGE evicts values older than its key's latest rowtime minus offset.
Our orchestration remains the incremental streaming operator rather than Arroyo's per-instant
DataFusion window execution plan, because the state must survive native and stock snapshots.

Event-time ROWS additionally preserves the released stock operator's terminal-timestamp sentinel
behavior. Its minimum-bucket search starts at `Long.MAX_VALUE` and requires a strictly smaller
timestamp, so peers in that bucket cease being retracted once earlier buckets are exhausted.
We retain their original inputs and raw fold rather than applying a logical frame-width truncation.
Native checkpoint/backend transfers keep that full bucket; an older writer cannot recreate peers
it discarded. Completed rows emit in event-timer timestamp order, with stable peer order, instead
of the pending buffer's original arrival order. These are stock runtime semantics rather than
Arroyo/DataFusion's logical ROWS-frame rule. The stock event-time ROWS descriptor/counter/timer
adapter is still pending under [#569](https://github.com/datafusion-contrib/StreamFusion/issues/569).

Processing-time bounded ROWS additionally follows Flink's clock-bucket eviction protocol rather
than truncating an arrival slice. A per-key processed counter and minimum timestamp select the
oldest element of the named bucket. When that bucket empties, the minimum is selected before
appending the current row, with `Long.MAX_VALUE` for an empty map. This preserves Flink's
width-one extra-row behavior and middle-frame eviction when clocks regress. Incremental arithmetic
and string views retract the selected row; other selections evaluate only surviving membership.
Each retained row owns its processing clock independently of the ordering sequence and original
input payload. Native IPC carries an optional clock column; RocksDB uses a sibling frame-clock
table. Separate counter/minimum metadata persists even without TTL and follows frame key groups
through file restore and rescaling. Its owned memory counts toward the state budget. This keeps
Arroyo's streaming/batch boundary while adopting Flink's persistent state machine; Arroyo's
per-instant execution plan cannot express that restore contract. Older native snapshots can use
legacy retained-frame bookkeeping but cannot reconstruct clocks their writers discarded. The
released stock processing-time ROWS contract projects these fields separately from raw arithmetic,
using stock input serializers and a Void processing-time queue. Bounded views and event-time ROWS
remain pending under [#569](https://github.com/datafusion-contrib/StreamFusion/issues/569);
benchmarking is deferred.

The direct processing-time ROWS bridge visits counter/minimum/cleanup metadata separately from
original frames and raw accumulators. The receiver installs complete frame rows, then validates
the corresponding key's metadata before installing a counter. This preserves `Long.MAX_VALUE`
even when the retained frame has other timestamp buckets. It avoids replay and intermediate
native snapshots. The Java adapter reuses the event-time RANGE frame machinery while selecting
processing-time descriptors, counter/minimum checks and timer ownership. Export
rejects legacy frames whose exact scalar bookkeeping was never retained, rather than presenting
inferred frame length/minimum as lossless state.

Bounded event-time RANGE keeps Flink's registered data and cleanup timestamps separately from
the retained frame. Replaced cleanup deadlines leave their older timers armed; those callbacks
advance the per-key late cutoff even without a data row. A sorted, deduplicated timestamp vector
and optional last-fired timestamp preserve this behavior across native checkpoints. Their owned
capacity counts against the state budget, and RocksDB checkpoints persist them under the same
key-group prefix as the frame. This follows Flink's timer protocol rather than Arroyo's
per-instant execution lifetime. Native legacy restore cannot recover timers an older writer
discarded. The stock bounded adapter remains pending in
[#569](https://github.com/datafusion-contrib/StreamFusion/issues/569); benchmarking is deferred.

Direct bounded-frame transfer reads each partition's owned original rows and raw arithmetic
separately. Import installs the saved arithmetic beside the last retained row without folding
those inputs again. A partition batch includes the current native cleanup/RANGE protocol and
timestamp-map reconstruction order; pending input rows remain a separate stream. This is a
temporary Arrow bridge for the eventual stock-schema writer/reader, not a new native checkpoint
format or a claim of completed stock bounded interchange. A partition may be materialized during
conversion; canonical handoffs prioritize correctness and performance validation is deferred.

Flink seeds `lastTriggeringTs = 0` in these operators and drops a row whose rowtime is
`<= 0` as late. We preserve that rule and reject timestamps at or before the key's last firing,
independently of the global watermark. The last retained frame row is also the admission marker,
so the existing frame checkpoint preserves it without a second last-trigger state table. A
zero-width ROWS frame retains one marker that never contributes to the next aggregate. RANGE
keeps Flink's event-time cleanup deadline, including its hysteresis, alongside the frame; firing
that timer clears the frame and admission marker. Unbounded frames use the stricter global
watermark comparison. See the OVER coverage page for restore and expiry behavior.

## Scope

Arithmetic fold columns preserve the exact current sum and independent counts. SUM can replace
a NULL overflowed decimal buffer on later input; SUM0 and AVG retain NULL. The raw scalar remains
independent of DISTINCT membership and of whether the evaluated result is NULL. Native memory
checkpoints read the two earlier fold layouts. Persistent frame rows carry an explicit format
version: expanded arithmetic uses version 2 while readers retain unmarked and version 1 codecs.
Known historical fields keep their values, and absent old fields retain missing markers until the
ordinary native reader seeds them from retained rows. This cannot reconstruct discarded history
and is distinct from direct stock canonical export. Frozen fixtures from an independently built
prior library verify historical layouts without depending on the current writer to recreate them.

Released Flink's RANGE SUM also exposes state-backend iteration order after
decimal overflow. For a 1,000-ms frame with `(time,value)` arrivals
`(100, 10^38-1), (200, -1), (400, 2), (2000, 7)`, the final SUM is 8 on the
heap backend and 7 on RocksDB, while SUM0 and AVG remain NULL. The heap MapState
retracts timestamp groups in HashMap order; RocksDB iterates serialized keys.
The released-operator regression compares both outcomes against the native operator.
The operator retains heap timestamp-map order and distinguishes ordinary checkpoint
from canonical savepoint reconstruction, as described below. Expanded admission,
full SQL and end-to-end performance validation remain required in
[#322](https://github.com/datafusion-contrib/StreamFusion/issues/322).

Running aggregates (`SUM`/`MIN`/`MAX`/`COUNT`/`AVG`; `AVG` via Flink's
`$SUM0`+`COUNT` with the divide on the host), `FIRST_VALUE`/`LAST_VALUE`, and the
window functions `ROW_NUMBER`/`RANK`/`DENSE_RANK`, over the unbounded `RANGE`/`ROWS` frames, the
bounded `ROWS n PRECEDING` frame, or the bounded `RANGE INTERVAL n PRECEDING` frame.
Both event-time and proctime orders are supported; under proctime the operator folds in
arrival order and emits eagerly (no watermark), assigning a monotonic arrival sequence as
the ordering key so the same fold/frames apply — only a proctime bounded-`RANGE` frame
(a wall-clock interval) falls back as non-deterministic. Decimal-valued frame offsets fall back;
this refers to the PRECEDING bound, not decimal aggregate inputs. Ordinary COUNT over decimal,
string and temporal inputs uses Arrow validity and preserves that membership through bounded
frame snapshots and memory/RocksDB transitions. Other runtime non-numeric reductions remain
tracked in [#322](https://github.com/datafusion-contrib/StreamFusion/issues/322).

## Verification
Parity harness: running aggregates and `ROW_NUMBER`, partitioned and not, over
DataStream and Parquet sources, including rowtime ties within the unbounded frame.

### Typed value ownership

Primitive values remain copyable. Decimal values retain their unscaled i128 and declared
precision/scale; timestamps retain milliseconds and fractional nanoseconds. Strings own
immutable UTF-8 text and optional original UTF-16 cache units in a frame sidecar. Running
extrema compare before retaining a replacement, and bounded results share retained payloads.
Memory accounting includes payloads and ownership metadata. This preserves existing numeric
frame layouts rather than widening every primitive value into a heap-allocated scalar.

### RANGE heap membership and restore protocols

The RANGE overflow fixture also checkpoints after timestamp 400 and restores before
the arrival at 2000. On released Flink 1.18.1 and 2.2.1, recovered heap state returns
SUM 7 rather than uninterrupted heap's 8; RocksDB remains 7. Flink's map serializer
reconstructs a `HashMap` using `CollectionUtil.newHashMapWithExpectedSize(size)`.
The released helper uses the default 0.75 load factor and a capacity derived from
the retained count, rather than the old heap table's capacity. Restored iteration
order is therefore part of the contract, alongside live insertion/removal order
and pending timestamp keys. A single timestamp-sort rule cannot preserve all these cases. The operator retains
heap timestamp membership in Java-compatible bucket and tree order, including keys
whose rows have not yet fired. It ranks just the expired groups before removal;
there is no per-result scan of the entire pending map. Retained-row Arrow buffers
continue to own the values. A separate keyed IPC component at the end of the raw
partition carries timestamp membership order, leaving the primitive snapshot layouts
unchanged. The task memory counter includes table, node-arena and free-slot storage.

Canonical heap savepoint restore follows a different released path:
`HeapSavepointRestoreOperation` inserts each user entry into a fresh default `HashMap`.
The operator detects `SavepointKeyedStateHandle` through the restored task state and
rebuilds its native timestamp membership accordingly. The existing OVER restore JNI
signature is unchanged; a separate cold restore hook applies that map rule and
reconciles the memory reservation. A restore-hook failure closes the newly restored
handle. Canonical RocksDB exports retain the host's serialized timestamp order for
the target heap reconstruction.

`OverRangeOrderOracle` produces iteration traces from the running JDK and released
Flink map serializers. Eleven traces cover 1,064 mutation/recovery steps, including
collisions, tree splits/removals, signed hash comparisons, and ordinary/canonical
reconstruction. Both supported released SDK lines produce identical controls on
JDK 17. Actual released OVER operators independently test overflow retractions,
pending timestamp groups, ordinary recovery and all canonical backend pairs.
Full SQL comparisons cover expanded basic-value admission, actual native OVER consumption
and repeated recovery. Production comparisons include both row/Arrow transposes and stock plus
previous-StreamFusion fallback controls; results are recorded on the OVER operator page.

Heap `CopyOnWriteStateMap.get` can copy state while a snapshot is outstanding;
for a map value, `MapSerializer.copy` rebuilds capacity too. Released controls on
both supported lines hold the actual heap snapshot future open or complete it
before further input. Without recovery, the held snapshot yields SUM 7 and the
completed snapshot yields 8 for the same overflow fixture. A second pair records
the final input before capture and triggers the copy on watermark firing instead.

The operator registers small mutable markers in managed heap ValueState per
partition, separately for input membership and string value-map reads. Its namespace is the encoded native partition key and its explicit key
group follows the same BinaryRow hash used for native state redistribution. The
marker serializer's `copy` records the backend's real copy-on-write event; the
native operator observes it before an admitted input or completed group first
touches the partition and rebuilds the affected timestamp/value maps. This uses released
keyed-state APIs and follows snapshot completion through the backend's version
logic rather than approximating it with checkpoint notifications. No aggregate
row or value moves through this marker. Callbacks deduplicate partition keys per
Arrow batch, preserve the backend's key context and use scoped JNI local references.
The native map's actual capacity is re-accounted after the copy.

A cleared frame clears its marker, including after restore. The marker's copy flag
is transient and is never restored as a pending notification. The managed marker
is JVM heap metadata; native table/node/free-slot ownership remains under the task
native off-heap reservation. Release Criterion fixtures exercise the actual heap
backend and production JNI callbacks, including completed and outstanding snapshot futures.

## Cached string representations and bounded views

Released Flink compares two cached Java strings by UTF-16, and otherwise compares their
UTF-8 bytes. Ordinary object copy retains a cache; serialized recovery removes it. Those
comparisons can form a cycle: cached U+1F600 precedes cached U+E000, which precedes binary
U+E100, which precedes U+1F600. Recomputing a fresh bounded-frame minimum would therefore
change a selected value that has not expired. Arroyo/Comet start from Arrow/binary string
values, so this host representation requires a Flink-specific state sidecar.

Metadata-marked Arrow List<UInt16> companions retain original Java units when the boundary
requests cached representations. A Boolean flag cannot preserve two distinct unpaired
surrogates with identical UTF-8 replacement bytes. Reads reconstruct both original bytes
and cache units. Shuffle/checkpoint serialization normalizes caches; RocksDB proctime views
normalize between each output, including multiple rows within one Arrow batch. Released
controls cover cached and binary input, source/computed SQL strings, chained native OVER,
window-to-OVER and both supported Flink lines.

Bounded FIRST/LAST retain selected value/order, value-to-order lists and order-to-value lists
independently of physical frame rows. Raw/persistent checkpoints encode these views as Arrow
IPC in appended binary fields; restore applies serialized key collapse without reconstructing
selection history from the remaining rows. An alias collision can leave a selected value
outside the physical frame. Normal/canonical backend transitions and peer controls preserve
this behavior. Immutable text/cache payloads can be shared with output columns.

Bounded MIN/MAX keep their selected scalar, positive-membership count and counted value map
independently. Retraction rescans only when a winner retires; negative counts balance later
additions without becoming positive membership. Heap scans reproduce JDK bucket and tree-bin
order using the existing timestamp membership implementation. A synthetic Long key stores an
entry identifier in its high word and identifier XOR string hash in its low word, giving the
same raw Java hash. Distinct hashes therefore follow the same signed hash/tree ordering.
Persistent scans use serialized string-key order. Ordinary restore reconstructs capacity from
serialized entry count; canonical heap restore rebuilds a default map in original entry order.
The managed snapshot marker applies actual heap copy-on-write reconstruction to string value
maps as well as timestamp membership, including proctime bounded ROWS. Cleanup drops the marker
and native ownership together. Separating the two markers also preserves snapshot completion
between rowtime input buffering and watermark firing. Original numeric snapshots keep their
existing state layouts.

Large malformed cached-key collisions also expose inherently nondeterministic released behavior.
The retractable FIRST_VALUE helper assigns implicit System.currentTimeMillis order groups:
controlled identical arrivals with tied versus distinct groups select different values after
alias retraction. Identity-ordered full-hash tree entries can serialize different duplicate
counts last, changing restored MIN. Untouched independent stock jobs demonstrate both effects;
deterministic SDK contract tests separately pin the two mechanisms. Native FIRST/LAST use relative
arrival order, and exact-hash ties use stable native identifiers. These cases remain admitted;
there is no unique stock execution output to mirror. Deterministic valid-Unicode tree histories
retain exact parity across uninterrupted execution, normal/canonical restore, and outstanding/
completed heap snapshots. The OVER page links all retained control and performance samples.


## Non-time ROW_NUMBER uses a retained sorted buffer

Released Flink 2.2.1 also plans an ordinary numeric `ORDER BY` as `StreamPhysicalOverAggregate`.
Its exec node chooses `NonTimeRowsUnboundedPrecedingFunction` for ROWS unbounded preceding,
verified against the released planner JAR before broadening StreamFusion admission. A nonmonotonic
arrival changes already-emitted ranks: the inserted row emits +I, then each shifted existing row
emits -U at its previous rank and +U at its new rank. Equal sort keys retain arrival order.
This differs from Top-N's rank-slot replacements and from arrival-order proctime counters.

Arroyo's planner `plan/window_fn.rs` rejects non-windowed input ("Window functions require
already windowed input"); its worker buffers by timestamp and drains a window exec after a
watermark. A non-time Flink rank can never finalize an unbounded partition that way. We instead
reuse StreamFusion's Arrow-row sorted rank state and key-group/checkpoint representation, adding
an explicit row-identity emission mode. No generic OVER time guard is removed. Native operators
still consume and emit Arrow batches, and existing Top-N slot updates remain unchanged.

Admission includes the verified ordinary scalar keys and exact TIMESTAMP/TIMESTAMP_LTZ
precision, one ascending key, and one ROW_NUMBER over ROWS UNBOUNDED PRECEDING
through CURRENT ROW, insert-only input and disabled TTL. NULL placement is explicit; ties remain
stable across physical batches and recovery. Every row remains in state; there is no implicit
Top-N truncation. Accounted off-heap limits fail normally rather than discarding retained ranks.
Updating input, TTL, other order specifications and other non-time analytic functions retain
fallback. Flink 1.18 rejects this query at its released factory boundary; native admission
preserves that failure for all ordinary sort types.

The released heap operator failed an injected restore with a NULL prior accumulator in its
update emission. Native checkpoint validation therefore compares its twice-restored output
with an uninterrupted stock run, independently verifying restored offsets and source cleanup;
it does not claim parity between two successfully restored host/native jobs.

DATE, TIME and BOOLEAN OVER value state stores the selected physical 32-bit value plus
its logical Arrow type, following the existing typed decimal/timestamp selection pattern.
Frame rows and snapshots retain logical DATE/TIME/BOOLEAN scalar fields rather than exposing
an integer carrier. Readers and restored frames fold into the same native state; no new JNI
or checkpoint framing is introduced. Arroyo's watermark-finalized window exec cannot replace
Flink's immediate running and bounded-frame selection, as described above.


STRING and timestamp OVER distinct counts reuse the existing typed membership state rather
than adding a second aggregate engine. Following the borrowed-key membership pattern already
used by GROUP BY, running string counts probe borrowed bytes and own a key on first membership;
timestamps probe their full nanosecond value directly, avoiding a temporary scalar/Arrow array.
String count equality follows Flink binary string bytes, while mixed selections retain their
own UTF-16 representation. Bounded frame storage still retains the existing string cache.

As in Comet's owned native memory accounting, retained hash capacity and owned payload enter
the native budget. Per-row growth is measured on the borrowed aggregate state rather than
through extra key lookups. Restores and persistent probes derive the footprint from decoded
membership; the derived byte counter is not serialized. Native RocksDB probes include text
memberships, closing the numeric-only probe hole exposed by cross-batch running string counts.
These changes keep the existing Arrow boundaries, JNI API and checkpoint wire format.


Bounded decimal DISTINCT SUM/SUM0/AVG use the existing incremental arithmetic slots, with
multiplicity transitions controlling fold/retract. Recomputing each unique frame would violate
Flink's overflow history, so checkpoints retain arithmetic sum/count fields and membership is
rebuilt from already-retained frame rows. Rebuilding membership never replays arithmetic. This
extends the same incremental divergence from Arroyo's watermark-finalized window execution;
there is no second native window engine or new Java/native ownership boundary.

### Complex ordinary ranks keep parent and child NULL ordering separate

Released Flink's generated ARRAY/ROW comparisons always order child NULLs first, independently
of the top-level NULLS clause. Arrow's row codec propagates that clause to children. For
ordinary ARRAY/ROW ROW_NUMBER with NULLS LAST, lower the query to a native projection of
`value IS NULL`, then the existing retained ranker ordered by that flag and the original value
with NULLS FIRST, then a native projection removing the flag. ARRAY/ROW with NULLS FIRST
uses the original single key. This extends the retained-buffer design above; Arroyo's
per-instant window execution cannot provide Flink's cumulative row-identity updates.

Scalar children retain the existing verified order whitelist. Floating and MAP/MULTISET
children stay outside it. The hidden flag is part of the same retained row and checkpoint
schema, so heap and persistent restore use the existing sort/payload codec. Existing bounded
Top-N and previously admitted scalar ranks keep their original sort-key layouts. Restored
nested field names are reconciled with the declared payload schema by the existing recursive
Arrow cast helper; renaming children retains offsets, validity and value buffers. Normal
changelog gathering still owns its output allocations.


### Mutable floating sort groups

Released Flink's generated numeric comparator uses `left > right ? 1 : left < right ? -1 : 0`.
Both signed zeros and every NaN/non-NULL pair compare equal. Its ordinary non-time ROWS
operator linearly scans retained groups and replaces a matched group's key on every insertion.
After NaN arrives, those groups can cease to be numerically sorted. Arroyo/DataFusion's total
window sort and ordinary Arrow-row binary ordering cannot model this contract.

Scalar FLOAT/DOUBLE ROW_NUMBER therefore keeps the existing ranker's rows in retained group
order, uses each group's last row as its current key, and marks the first row with its length.
The existing optional sort-count field encodes these headers as `-(length + 2)`; `-1` stays absent
and ordinary nonnegative OFFSET counts keep their meaning. This introduces no JNI API or new
operator. The raw byte codec preserves the new negative values, and restore normalizes signed
zero keys while preserving tagged group order. Every row's original payload stays independent
of its comparison key; raw NaN bits survive emission and checkpoints. Header counts live in the
existing fixed-size row metadata and participate in the existing managed state serialization.

An initial proxy-key candidate retained the first group representative. Its unit model made
the same mistaken assumption; released SQL rejected eight NaN cases and all eight recovery
profiles. The corrected oracle follows the host's latest-key replacement, including group-order
mutations, and checks complete emitted identities across snapshots. Released raw-bit, memory/
RocksDB recovery, mini-batch and shuffle tests qualify the final implementation. Flink 1.18
receives an explicit compatibility gate: native substitution must not bypass its non-time
OVER factory rejection. Performance evidence and remaining gates live on the OVER page.


## Original bounded inputs for canonical interchange

Flink's bounded OVER `inputState` retains complete input rows, while our ordinary frame buffers
retain aggregate arguments. The latter cannot reconstruct unused original columns. For #569, an
explicit canonical binding therefore retains owned original input rows alongside the existing
frame representation. It leaves the aggregation algorithm intact and does not replace the operator
with Arroyo's window execution plan. Captured ROWS frames retain all `n + 1` current rows, including
COUNT-only frames whose unbound representation can discard one earlier. The sampled processing
clock is distinct from the native arrival-sequence ordering key.

Typed RocksDB keeps the payload in a parallel table at the same key group and frame position.
Its table identifier overlaps the first unbounded DISTINCT table only because bounded and
unbounded shapes never share a store. A persisted marker distinguishes captured frames from
older frames without original rows. The input table follows exact eviction, per-key cleanup and
rescale clipping. Memory and RocksDB native checkpoints preserve these rows; stock canonical
export visits live retained inputs directly and never writes a native snapshot as an intermediate.
The shared Arrow ownership helper follows the existing partial-window buffer pattern and owns
dictionary children/validity as well as the gathered row buffers, so JNI release callbacks do not
survive across calls.

This is state-information preservation, not stock bounded-contract admission. Full stock
accumulator/buffer/timer translation, planner binding and cross-engine qualification remain under
[#569](https://github.com/datafusion-contrib/StreamFusion/issues/569). Performance work is deferred
by the maintainer's functionality-first instruction.


## Bounded RANGE timer callbacks

Arroyo's window-function implementation splits timestamp-sorted batches and schedules timestamp
buckets in a BTreeMap. StreamFusion keeps its existing incremental frame algorithm, and now emits
completed bounded RANGE rows in timestamp order as the released Flink callbacks do. The reasons
for retaining this algorithm remain unchanged: it owns the retractable raw accumulator and full
retained rows needed for direct canonical interchange.

Flink also uses wrapping Java long additions for cleanup deadlines and leaves superseded timers
registered. An overflow can place cleanup before pending data; clearing state does not cancel
future callbacks. The native wrapper splits only those cleanup boundaries that precede later
callbacks or can coincide with data after overflow, clears the key's pending inputs and frame,
and keeps future timers. Ordinary RANGE firings keep the bulk frame path. Orphan callbacks and
the resulting last-trigger-only keys are retained in optional native timer metadata and the
independent direct RANGE protocol. This follows the released Flink operator contract rather than
Arroyo's generic window execution. No intermediate native snapshot is introduced. Performance
measurement and tuning remain deferred for #569.
