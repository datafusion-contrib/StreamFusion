# Window join

**Status:** Native. Both sides carry the same window-attached windowing — `TUMBLE`, `HOP`, or
`CUMULATE` — and match within the shared window rather than an explicit `BETWEEN` bound. Like the
[interval join](interval-join.md), it is not changelog-aware: both inputs must be insert-only, and a
retracting/updating input falls it back per the [insert-only guard](../index.md#global-switches).

## Admission

The same key/type/residual conditions as the [interval join](interval-join.md) — a supported-type
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
