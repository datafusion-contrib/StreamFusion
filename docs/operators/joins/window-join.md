# Window join

**Status:** Native. Both sides carry the same window-attached windowing — `TUMBLE`, `HOP`, or
`CUMULATE` — and match within the shared window rather than an explicit `BETWEEN` bound. Like the
[interval join](interval-join.md), it is not changelog-aware: both inputs must be insert-only, and a
retracting/updating input falls it back per the [insert-only guard](../index.md#global-switches).

## Admission

The same [scalar key whitelist](../window-aggregate.md#scalar-grouping-and-partition-keys)
and type/residual conditions as the [interval join](interval-join.md) — a supported-type
equi-key, null-dropping keys for a non-INNER join, a non-equi residual the native expression engine
can express, and all four join types (INNER/LEFT/RIGHT/FULL) native — plus one condition specific to
this shape: **both sides must carry the same time semantics**, either both event-time windows or
both proctime windows. An event-time/proctime mismatch between the two sides' windowing isn't
native.

User equi-keys may be empty: both window bounds still participate in the join key, so rows
match within the same window and the residual predicate still applies.

An event-time window join closes each window on the watermark, like the windowed aggregate; a
proctime window join closes it on a processing-time timer instead. Watermarks and bounded-input
completion do not fire an unfinished processing-time window.

## Falls back to Flink when

- the join type isn't INNER, LEFT, RIGHT, or FULL;
- the key columns aren't null-dropping for a non-INNER join;
- the equi-key type is outside the supported set;
- the non-equi residual isn't expressible by the native expression engine;
- the two sides' windowing doesn't share time semantics (one event-time, one proctime);
- the two inputs use different assignment calendars, including an LTZ regional calendar on
  one side and plain TIMESTAMP on the other.

Window start/end payload columns remain local wall-clock values. The join compares its watermark
or processing-time clock with each end's converted deadline and fires at the window's final
millisecond. Regional conversion uses released Flink's timer rules; fixed offsets use the
shifted local grid, including offsets not aligned with the slide. Both inputs must use the same time domain. Late rows are rejected using that same
threshold, including after recovery. See the [state upgrade contract](../../backends/canonical-state.md#timestamp-layout-upgrade)
for checkpoints written before the boundary correction.


## Recovery and watermarks

Checkpoints preserve pending rows on both sides and processing-time timer
metadata. Event-time watermarks are replayed after recovery. Rows arriving
before that replay are evaluated against the restarted operator's watermark;
a previously fired window is not automatically classified as late from the
checkpoint alone. Once both input watermarks advance, late rows are rejected
and pending windows fire normally. Released Flink 2.2.1 and 1.18.1 harness
checks cover this sequence for INNER joins on the stock operator and both raw
native and direct RocksDB recovery, including partial firing and no refiring
without new matching rows. This does not establish other join kinds or
multi-source rescaling through this particular regression.

Regional RocksDB firing first collects distinct local window ends and converts their deadlines.
A second scan retains only closed rows for output. Pending payloads remain in RocksDB rather than
being copied into a resident candidate list; fixed-offset firing keeps its existing single scan.

Both stores retain the logical input schema. INSERT-only transport row-kind tags and Java string
cache companions are removed before buffering; retractions and unknown companions fail before
state changes. Older native IPC checkpoints with an INSERT tag are normalized during recovery,
so subsequent input and canonical imports use the same schema. This also prevents transient
metadata columns from entering the persistent row codec or the join output.

## Stock canonical savepoint interoperability

The unmerged [#569 implementation](https://github.com/datafusion-contrib/StreamFusion/issues/569)
has a direct state adapter for released Flink 2.2.1 event-time window joins. A bound operator
exports its live left and right input rows into stock `left-records` and `right-records`
`ListState<RowData>`, using the original join-key serializer, `LongSerializer` window-end
namespaces, and the stock `window-timers` event-time queue. Flink's canonical writer writes that
projection directly; no native checkpoint or IPC snapshot is produced first. Imported rows enter
native pending state without replaying input or firing windows. Memory imports own their Arrow
buffers after Java releases the transfer batch; persistent imports retain individual store records.

Operator harnesses cover both switch directions and both stores, INNER/LEFT/RIGHT/FULL, keyed and
keyless input, NULLs and duplicates, empty saves, partial firing, immediate re-export, and continued
input after an ordinary native checkpoint. Attached TUMBLE, overlapping HOP and CUMULATE namespaces
are checked against released stock output. UTC, a fixed non-hour offset, and the New York spring
DST transition have separate controls. Restored serializer metadata loads with StreamFusion and
planner classes blocked. Malformed keys, namespaces, row flags, deadlines, missing/orphan timers,
processing-time timers, and unowned descriptors fail before importing rows; ownership checks cover
failed imports as well as successful switches.

Canonical plan binding can supply the original stock graph's resolved maximum parallelism to
both native columnar exchanges and the join state owner. This takes precedence over the new
execution environment's setting, preserving the saved key-group layout. Isolated planner-boundary
controls cover a singleton and keyed layouts with maxima 17 and 256 while the environment requests
257. The compiled-plan loader binds these values automatically from the original stock graph.

The stock compiled-plan loader now constructs the runtime state binding automatically. It retains
portable stock JSON and lowers only a private execution graph. The clean workflow passes 50
planner controls and 46 isolated complete-job cases: 36 continued-input switches, four no-input
re-exports, two ordinary native-checkpoint recoveries and four 64-key rescaling cases. These
retain two custom sources, a transactional sink and both transposes; stock workers exclude
StreamFusion. INNER TUMBLE/HOP/CUMULATE cover keyed/keyless paths, while LEFT/RIGHT/FULL use
keyed TUMBLE. Both stores and engine orders are exercised. FULL TUMBLE keys redistribute through
1 → 3 → 2 tasks at maximum parallelism 128; source and sink parallelism stay at one. Committed
sink effects match the stock baseline. Unaligned checkpoints currently retain the stock window
join because canonical exchange reassembly is not yet bound; two negative planner controls
verify this boundary.

Wider rescale/type/version matrices and residual predicates remain
unqualified. The isolated job fixtures use BIGINT keys/payloads; they do not establish every
supported data type. Released Flink 2.2.1 rejects processing-time window joins, so its event-time
contract cannot be bound to a native processing-time operator. Native processing-time execution
and native checkpoint recovery remain available independently.

Benchmarking and tuning of canonical handoff are deferred while functionality is completed.

### Unchanged upstream restore fixtures

The stock compiled-plan integration passes all eight unchanged released Flink 2.2.1
`WindowJoinEventTimeRestoreTest` fixtures on both engines. Six require native window-join work
(INNER/LEFT/RIGHT/FULL TUMBLE, INNER HOP and CUMULATE), with actual imported native rows also
recorded. SEMI and ANTI keep the released stock operator: their contracts require the exact
planner fallback, initialized stock lists/timer service, and successfully processed stock input.
Native Calc work alone cannot satisfy either state route. The sources, frozen plans/savepoints
and expected results come from the released test artifact. These controls qualify those fixtures;
they do not qualify the remaining full #569 matrix. Ordinary SQL binding is described below.
See the [savepoint lane](../../upstream-flink-suite.md#window-join-canonical-restore-qualification).


Qualified compiled plans also work with explicit SQL UID generation disabled. Four native-first
INNER TUMBLE job round trips pass across keyed/keyless variants and both native stores, alongside
explicit-UID controls. State registration uses actual native primary IDs; canonical metadata
continues to retain the original stock graph identities. Wider generated-ID variant/rescale
qualification remains open under
[#569](https://github.com/datafusion-contrib/StreamFusion/issues/569). See
[generated operator IDs](../../backends/canonical-state.md#generated-operator-ids-in-qualified-compiled-plans).


### Ordinary SQL submission

Normal SQL submission now retains the original stock logical identity and binds this join's
stock state contract without compiling or loading a plan. The initial ordinary SQL qualification
and exact key-layout/admission limits are documented in
[ordinary SQL canonical handoffs](../../backends/canonical-state.md#ordinary-sql-canonical-handoffs).
This does not qualify the full ordinary variant/type/rescale/failure/connector matrix; the family
remains partial under [#569](https://github.com/datafusion-contrib/StreamFusion/issues/569).
