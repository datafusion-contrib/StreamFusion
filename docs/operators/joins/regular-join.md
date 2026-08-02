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

Under mini-batch execution, a regular join coalesces replacement events (multiple updates to the
same output row folded into one) only when Flink's planner metadata **proves both join keys contain
an input upsert key** — i.e. each key value identifies at most one live row per side. Any join that
isn't provably unique on both keys (a one-to-many or many-to-many join) retains the immediate,
per-row changelog path with no coalescing.

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
