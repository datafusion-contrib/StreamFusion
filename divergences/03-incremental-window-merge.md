# Incremental accumulator merge for session (and sliding) windows

**Kind:** algorithmic — internal implementation differs; output is identical.
**Diverges from:** Arroyo.
**Forced by parity:** no — this is a chosen optimization, not a requirement.

## Their decision
Arroyo's session and sliding aggregating windows **store raw input batches** in a
tiered structure (e.g. `batches_by_start_time`) and **re-aggregate** the relevant
rows whenever a window advances or sessions merge. The raw rows are the source of
truth; aggregation is recomputed from them.

## What we do instead — which is what Flink itself does
We keep, per key, only the **incremental accumulators** for each open window, and
when two windows merge we fold one accumulator's partial state into the other via
`merge_batch`. Raw rows are discarded after they are folded in.

This is *not* a freelance optimization: it mirrors Flink's own window operator.
Flink's combiner (`AggCombiner` / `RecordsWindowBuffer`) buffers raw records **in
memory only**, folds them into one accumulator per window, and persists **only the
accumulator** to state — two-phase merges slice accumulators, session merges
window accumulators. So for mergeable aggregates we match the host's strategy
exactly; **only Arroyo differs**, by retaining raw batches and re-aggregating.

## Window distinct state

COUNT(DISTINCT) uses DataFusion's distinct count accumulator within the same window lifetime.
Its mergeable partial is the set of values as an Arrow list, rather than a scalar count. The
local/exchange/global pipeline unions these lists and snapshots the resulting set. This follows
Arroyo's DataFusion partial/final aggregation boundary (`tumbling_aggregating_window.rs` and
the planner's aggregate extension) while retaining our existing per-window accumulator layout.
Flink's local MapView fields are replaced by the list partials, so both native stages share
one explicit intermediate schema. Variable-sized sets currently use the existing snapshot
fallback on RocksDB; they do not enter its fixed-field accumulator row codec.

Filtered COUNT(DISTINCT) in append-only aligned event-time windows reuses that same partial/final
boundary. The existing per-aggregate validity mask rejects values before they enter a set,
without dropping the row that establishes a live window. Each call carries its own list,
even when Flink combines calls sharing an argument into one MapView with per-filter bits
(`AggregateUtil.extractDistinctInformation` and `DistinctAggCodeGen` in released Flink 2.2.1).
Global merging unions selected sets without re-evaluating filters. There is no new native
state or Arrow ownership protocol. Arroyo's tumbling partial/final implementation was
consulted before extending admission; its structure remains unchanged here.

## Fixed-width scalar extrema

DATE, millisecond TIME and BOOLEAN extrema retain one nullable integer payload per accumulator.
Borrowed typed readers fold the selected row positions directly, following the existing timestamp
extrema path. This avoids materializing an Arrow selection array for every group and aggregate.
The one-field nullable Arrow partial remains compatible with DataFusion's previous MIN/MAX state;
readers in both directions use the same value type. Session merges combine the retained values
without constructing one-row arrays.

HOP and CUMULATE can fold these exact, order-independent values once per slice before merging
into the still-open final windows. This extends the existing timestamp/count pane strategy within
our incremental layout. Arroyo's tumbling partial/final and incremental accumulator implementations
were consulted; the choice to retain bounded accumulator state rather than raw batches is unchanged.

## Late local slices

Arroyo's sliding operator bins batches and skips a bin preceding its current watermark bin.
That admission rule is narrower than Flink's overlapping-window contract: a slice can have
fired while a larger window containing it remains open. Flink's local slicing aggregate
accepts the row; its final processor checks the last containing window and updates only
unfired windows. Our local Arrow partials likewise defer late-data admission to the final
merge. The local update path is explicit, so a restored local watermark cannot suppress a
still-useful slice. The global watermark remains checkpointed and prevents reopening a
completed window. Arrow import/release and exception ordering retain the existing bridge
pattern checked against Comet's JNI Arrow import path.

RisingWave's `HopWindowExecutor` is a related reference, but expands each input into window
rows and transforms window-column watermarks; its hash aggregate owns final emission and
state cleanup. It does not supply a matching local-slice implementation to transplant here.
We keep the existing partial-accumulator architecture and Flink's late-window semantics.

## Shared exact sums

Shared COUNT/AVG/SUM reductions keep Arroyo's partial/final boundary and our existing incremental
window layout. Their internal SUM0 uses the same typed running state as ordinary GROUP BY through
a one-field DataFusion accumulator adapter. Flink's SUM0 initializes to zero and latches decimal
overflow, so substituting DataFusion's ordinary nullable SUM would change all-NULL groups and
overflowed partials. Local slices, global merging, signed updates and snapshots retain that buffer
contract; the host planner's outer COUNT correction and exact division remain unchanged.

## Bounded persistent COUNT updates

Ordinary persistent COUNT-only windows retain a bounded decoded accumulator bundle across input
batches. Updates coalesce in memory, while the existing RocksDB accumulator representation remains
the checkpoint source. Watermarks commit only closing groups; checkpoint/canonical boundaries
commit all pending groups. Payload and routing keys share the task off-heap reservation, and a
denied reservation flushes the bundle. Regional shared slices and other aggregates retain their
existing state lifecycle. This extends the host's bounded combining approach instead of retaining
Arroyo-style raw inputs; Comet's pool reservation pattern remains the memory authority.

## Why
Every aggregate we support (`SUM`/`MIN`/`MAX`/`COUNT`, and integer `AVG`) has
associative, commutative, mergeable partial state, so merging accumulators yields
exactly the same answer as re-aggregating the union of rows — but in bounded
memory (state per open window, not per row) and without recomputation on merge.

The result is identical to Flink (and to Arroyo), verified by the parity harness,
including the case where a late, out-of-order element bridges two open sessions.

## Why diverge at all, given the "copy them" default
Because the divergence is only from Arroyo, and we land on the *host's* approach:
matching Flink is the prime directive, and Flink keeps accumulators, not rows.
Arroyo's raw-retain model exists partly to support aggregates whose state is not
cleanly mergeable — a generality we do not need yet.

**Future (non-mergeable aggregates).** When we add aggregates that are not
associative/invertible, the precedent in *both* engines is **not** a "partial
accumulator + all rows" dual store — that redundancy is what they avoid. Instead
the state is chosen per aggregate: mergeable ones keep the small accumulator
(above); non-mergeable ones keep a **value→count multiset** and recompute on
demand (Flink's `MaxWithRetractAccumulator` is a running scalar plus a
`MapView<value, count>`; Arroyo's `IncrementalState::Batch` is a `HashMap<value,
count>`). We would follow that — per-aggregate state selection, value-multiset for
the non-mergeable set — rather than retaining raw rows alongside partials.

## Temporal overlapping slices

Insert-only timestamp extrema combined with ordinary COUNT reduce each input
slice once then merge those scalar partial states directly into open HOP/CUMULATE
windows. This follows Arroyo's sliding partial/final aggregation structure while
retaining StreamFusion's window accumulator state, snapshot codecs and firing
order. Only exact, order-independent extrema/count states take this path; numeric
reductions, retracting inputs and other aggregate combinations keep their existing
fold. Late rows are discarded only when their last assigned window has closed.

Counts use the same non-null Int64 partial state as DataFusion, including logical-null
unions for multi-argument updates. Owned extrema and counts have fixed size; accounting
charges new groups once and releases their recorded footprint when a window closes.
Persistent stores still hydrate and write through each affected window group.


Floating window COUNT DISTINCT retains this same incremental partial/final structure from
Arroyo. DataFusion's generic floating distinct buffer hashes raw payload bytes; Flink's boxed
floating keys canonicalize all NaNs and distinguish signed zero. A compact typed bit-key
accumulator supplies that equality, while reusing DataFusion's declared FLOAT/DOUBLE list
state fields and result type. Existing local/global, session and checkpoint paths consume
those lists unchanged. This changes aggregate semantics, rather than introducing another
window engine. Its owned hash capacity contributes to the existing native reservation,
following Comet's retained-memory accounting pattern.


Complete CUMULATE partials use stride equal to the maximum size. Their end identifies a final
window, rather than a reusable cumulative prefix. Shared-state merging therefore selects
only that end and retires only its own namespace; ordinary smaller-step CUMULATE keeps the
prefix optimization. Late complete partials compare against their own deadline. Cumulative
start rendering and the existing shared store/checkpoint layout remain unchanged, preserving
restore semantics while avoiding a second roll-up of already-final counts.

Updating window extrema reuse the signed value multisets of native GROUP BY. Arroyo's
`arrow/incremental_aggregator.rs` keeps batch state when an accumulator cannot retract;
Flink's cached winner and signed map counters instead require a retractable native multiset.
For local/global windows, Flink's `GlobalAggCombiner` first merges incoming partials into
a fresh accumulator and then merges that accumulator into retained state. We preserve that
boundary in a budgeted native temporary view, drained before watermarks and checkpoints.
Directly merging each partial into retained state can reseek an unmatched negative entry
when a transient winner cancels, producing a different result. Arrow structured partials
and snapshots carry the same cached value, map size and value/count entries on either backend.

Updating SESSION windows preserve the arrival order of namespace merges. Arroyo sorts batch
rows into connected runs before re-aggregation; that remains our append-only path. Signed
accumulators instead follow Flink's merging-window state lifecycle, batching only identical
timestamps and retaining the first existing accumulator before merging the other namespaces.
This avoids moving a bridge ahead of a prior retraction. It also avoids reconstructing retained
state through Arrow partials on every update; the CPU profile and measured one-pass improvement
are recorded with updating window benchmark evidence. Zero input-count namespaces remain
mergeable but do not emit a final result.
