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

An event-time window join closes each window on the watermark, like the windowed aggregate; a
proctime window join closes it on a processing-time timer instead.

## Falls back to Flink when

- the join type isn't INNER, LEFT, RIGHT, or FULL;
- there's no equi key;
- the key columns aren't null-dropping for a non-INNER join;
- the equi-key type is outside the supported set;
- the non-equi residual isn't expressible by the native expression engine;
- the two sides' windowing doesn't share time semantics (one event-time, one proctime).
