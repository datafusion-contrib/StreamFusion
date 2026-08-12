# Regular join

**Status:** Native. An ordinary equi-join over two full changelog inputs — the only one of the five
join shapes that accepts a retracting/updating stream on *both* sides rather than requiring
insert-only input (see the [insert-only guard](../index.md#global-switches)). Each side is held as
keyed state so a later update or delete on either input can retract and re-emit downstream.

## Admission

The native matcher requires:

- an **equi-key** of a supported type on both sides;
- for a non-INNER join, the key columns must be **null-dropping** in the way Flink's own planner
  expects (a non-preserved side's key nulls out correctly on a missed match);
- any residual non-equi predicate must be **expressible by the native expression engine**;
- every input column type must be one the Arrow converter can carry.

[Interval join](interval-join.md), [window join](window-join.md), [temporal table
join](temporal-join.md), and [lookup join](lookup-join.md) all state their admission conditions as a
variant of this same list — this page is the fullest treatment; the others cross-reference it rather
than repeating it.

## Mini-batch coalescing

Under mini-batch execution, a regular INNER/outer join uses one shared count boundary across both
inputs and drains before either input watermark, a checkpoint, or end of input, matching Flink's
two-input bundle contract. Two input shapes are native:

- For two insert-only inputs, the operator retains the physical Arrow batches by reference and
  replays the complete right-side bundle before the left side (left first for RIGHT joins). No row
  can be cancelled in an append-only bundle, so this preserves multiplicity without staging rows in
  an encoded changelog map.
- When planner metadata **proves both join keys contain an input upsert key**, replacement events
  are folded to the first preimage and final postimage per join key before replay.

Flink also reduces a changelog input whose upsert key is not contained in the join key, and cancels
equal opposing records for a changelog input with no unique key. Those two non-unique changelog
bundle shapes remain on StreamFusion's immediate path; they are not silently given the unique-key
contract. SEMI and ANTI joins also remain immediate, as in Flink's regular-join translation.

## Idle-state TTL

`table.exec.state.ttl` runs natively here, per side, with the same semantics as every other
TTL-bearing operator: each stored row carries its last-**write** wall-clock timestamp, expires at
`last_write + ttl` inclusive, and reads as absent (deleted on read) once expired. See [Idle-state
TTL](../index.md#idle-state-ttl) and [Configuration](../../configuration.md) for the flag surface.

## Falls back to Flink when

- the join type isn't one the native operator covers;
- there's no equi key;
- the key columns aren't null-dropping for a non-INNER join;
- the non-equi residual isn't expressible by the native expression engine;
- an input column has a type the Arrow converter can't carry.
